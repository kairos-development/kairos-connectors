package bybit

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	connectorpkg "github.com/kairos-development/kairos-contracts/connector"
	"github.com/sirupsen/logrus"
)

const (
	mainnetWSURL = "wss://stream.bybit.com/v5/private"
	testnetWSURL = "wss://stream-testnet.bybit.com/v5/private"
)

// WebSocketConfig contains WebSocket client configuration.
type WebSocketConfig struct {
	APIKey    string
	APISecret string
	Testnet   bool
}

// WebSocketClient manages WebSocket connections to Bybit.
type WebSocketClient struct {
	mu      sync.RWMutex
	writeMu sync.Mutex

	conn      *websocket.Conn
	url       string
	apiKey    string
	apiSecret string
	logger    *logrus.Logger

	connected     bool
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	closeOnce     sync.Once
	pingOnce      sync.Once
	reconnectOnce sync.Once

	reconnectCh           chan struct{}
	reconnectInitialDelay time.Duration
	reconnectMaxDelay     time.Duration
	subscriptions         map[string]struct{}

	messages chan *WebSocketMessage
	events   chan *connectorpkg.StreamEvent
}

// WebSocketMessage represents a parsed WebSocket message.
type WebSocketMessage struct {
	Topic string
	Data  interface{}
}

// NewWebSocketClient creates a new WebSocket client.
func NewWebSocketClient(cfg WebSocketConfig, logger *logrus.Logger) (*WebSocketClient, error) {
	if logger == nil {
		logger = logrus.New()
	}

	url := mainnetWSURL
	if cfg.Testnet {
		url = testnetWSURL
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WebSocketClient{
		url:                   url,
		apiKey:                cfg.APIKey,
		apiSecret:             cfg.APISecret,
		logger:                logger,
		ctx:                   ctx,
		cancel:                cancel,
		reconnectCh:           make(chan struct{}, 1),
		reconnectInitialDelay: time.Second,
		reconnectMaxDelay:     30 * time.Second,
		subscriptions:         make(map[string]struct{}),
		messages:              make(chan *WebSocketMessage, 100),
		events:                make(chan *connectorpkg.StreamEvent, 100),
	}, nil
}

// Connect establishes WebSocket connection and authenticates.
func (ws *WebSocketClient) Connect(ctx context.Context) error {
	if ws.ctx.Err() != nil {
		return context.Canceled
	}

	ws.mu.Lock()
	if ws.connected {
		ws.mu.Unlock()
		return nil
	}
	err := ws.connectLocked(ctx)
	ws.mu.Unlock()

	if err != nil {
		return err
	}

	ws.startBackgroundLoops()
	ws.logger.Info("WebSocket connected and authenticated")
	return nil
}

func (ws *WebSocketClient) connectLocked(ctx context.Context) error {
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, ws.url, nil)
	if err != nil {
		return fmt.Errorf("dial websocket: %w", err)
	}

	ws.conn = conn

	// Authenticate
	if err := ws.authenticate(); err != nil {
		conn.Close()
		ws.conn = nil
		return fmt.Errorf("authenticate: %w", err)
	}

	ws.connected = true

	ws.wg.Add(1)
	go ws.readLoop(conn)

	return nil
}

func (ws *WebSocketClient) startBackgroundLoops() {
	ws.pingOnce.Do(func() {
		ws.wg.Add(1)
		go ws.pingLoop()
	})
	ws.reconnectOnce.Do(func() {
		ws.wg.Add(1)
		go ws.reconnectLoop()
	})
}

// Close closes the WebSocket connection.
func (ws *WebSocketClient) Close() error {
	var err error
	ws.closeOnce.Do(func() {
		ws.mu.Lock()
		ws.connected = false
		ws.cancel()
		conn := ws.conn
		ws.conn = nil
		ws.mu.Unlock()

		if conn != nil {
			err = conn.Close()
		}

		ws.wg.Wait()
		close(ws.messages)
		close(ws.events)

		ws.logger.Info("WebSocket closed")
	})
	if err != nil {
		return fmt.Errorf("close websocket: %w", err)
	}
	return nil
}

func (ws *WebSocketClient) writeJSON(v interface{}) error {
	ws.mu.RLock()
	conn := ws.conn
	connected := ws.connected
	ws.mu.RUnlock()

	if conn == nil || !connected {
		return fmt.Errorf("not connected")
	}

	ws.writeMu.Lock()
	defer ws.writeMu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetWriteDeadline(time.Time{})
	return conn.WriteJSON(v)
}

func (ws *WebSocketClient) writeJSONTo(conn *websocket.Conn, v interface{}) error {
	if conn == nil {
		return fmt.Errorf("not connected")
	}

	ws.writeMu.Lock()
	defer ws.writeMu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetWriteDeadline(time.Time{})
	return conn.WriteJSON(v)
}

func (ws *WebSocketClient) markDisconnected(conn *websocket.Conn, reason string) {
	shouldPublish := false

	ws.mu.Lock()
	if ws.conn == conn {
		ws.connected = false
		ws.conn = nil
		shouldPublish = true
	}
	ws.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}

	if shouldPublish {
		ws.publishStreamEvent(connectorpkg.StreamEventDisconnected, reason)
	}
}

func (ws *WebSocketClient) disconnectCurrent(reason string) {
	shouldPublish := false

	ws.mu.Lock()
	conn := ws.conn
	if ws.connected || ws.conn != nil {
		shouldPublish = true
	}
	ws.connected = false
	ws.conn = nil
	ws.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}

	if shouldPublish {
		ws.publishStreamEvent(connectorpkg.StreamEventDisconnected, reason)
	}
}

func (ws *WebSocketClient) triggerReconnect() {
	select {
	case <-ws.ctx.Done():
		return
	case ws.reconnectCh <- struct{}{}:
	default:
	}
}

func (ws *WebSocketClient) publishStreamEvent(eventType connectorpkg.StreamEventType, reason string) {
	event := &connectorpkg.StreamEvent{
		Type:          eventType,
		Source:        "bybit_private_ws",
		Reason:        reason,
		OccurredAtUTC: time.Now().UTC(),
	}

	select {
	case ws.events <- event:
	case <-ws.ctx.Done():
	default:
		ws.logger.WithFields(logrus.Fields{
			"type":   eventType,
			"reason": reason,
		}).Warn("WebSocket stream event channel full, dropping event")
	}
}

// authenticate sends authentication message to Bybit WebSocket.
func (ws *WebSocketClient) authenticate() error {
	expires := time.Now().UnixMilli() + 10000
	signature := ws.generateSignature(expires)

	authMsg := map[string]interface{}{
		"op": "auth",
		"args": []interface{}{
			ws.apiKey,
			expires,
			signature,
		},
	}

	ws.writeMu.Lock()
	defer ws.writeMu.Unlock()
	return ws.conn.WriteJSON(authMsg)
}

// generateSignature creates HMAC-SHA256 signature for WebSocket authentication.
func (ws *WebSocketClient) generateSignature(expires int64) string {
	message := fmt.Sprintf("GET/realtime%d", expires)
	h := hmac.New(sha256.New, []byte(ws.apiSecret))
	h.Write([]byte(message))
	return hex.EncodeToString(h.Sum(nil))
}

// SubscribeOrders subscribes to order execution updates.
func (ws *WebSocketClient) SubscribeOrders(ctx context.Context) error {
	return ws.subscribe("order")
}

// SubscribePositions subscribes to position updates.
func (ws *WebSocketClient) SubscribePositions(ctx context.Context) error {
	return ws.subscribe("position")
}

// SubscribeBalance subscribes to balance updates.
func (ws *WebSocketClient) SubscribeBalance(ctx context.Context) error {
	return ws.subscribe("wallet")
}

// SubscribeTicker subscribes to market ticker updates.
func (ws *WebSocketClient) SubscribeTicker(ctx context.Context, symbol string) error {
	return ws.subscribe(fmt.Sprintf("tickers.%s", symbol))
}

// subscribe sends a subscription message for a topic.
func (ws *WebSocketClient) subscribe(topic string) error {
	if ws.ctx.Err() != nil {
		return context.Canceled
	}

	ws.mu.Lock()
	ws.subscriptions[topic] = struct{}{}
	conn := ws.conn
	connected := ws.connected
	ws.mu.Unlock()

	if !connected || conn == nil {
		return fmt.Errorf("not connected")
	}

	if err := ws.sendSubscription(conn, topic); err != nil {
		ws.publishStreamEvent(connectorpkg.StreamEventGap, fmt.Sprintf("subscription write failed for %s: %v", topic, err))
		ws.markDisconnected(conn, fmt.Sprintf("subscription write failed for %s", topic))
		ws.triggerReconnect()
		return err
	}

	ws.logger.WithField("topic", topic).Debug("Subscribed to topic")
	return nil
}

func (ws *WebSocketClient) sendSubscription(conn *websocket.Conn, topic string) error {
	subMsg := map[string]interface{}{
		"op":   "subscribe",
		"args": []string{topic},
	}

	if err := ws.writeJSONTo(conn, subMsg); err != nil {
		return fmt.Errorf("send subscription: %w", err)
	}

	return nil
}

// Messages returns the channel for receiving WebSocket messages.
func (ws *WebSocketClient) Messages() <-chan *WebSocketMessage {
	return ws.messages
}

// Events returns the channel for receiving WebSocket lifecycle events.
func (ws *WebSocketClient) Events() <-chan *connectorpkg.StreamEvent {
	return ws.events
}

// readLoop reads messages from WebSocket connection.
func (ws *WebSocketClient) readLoop(conn *websocket.Conn) {
	defer ws.wg.Done()

	for {
		select {
		case <-ws.ctx.Done():
			return
		default:
			if conn == nil {
				return
			}

			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			_, message, err := conn.ReadMessage()
			if err != nil {
				select {
				case <-ws.ctx.Done():
					return
				default:
					ws.publishStreamEvent(connectorpkg.StreamEventGap, fmt.Sprintf("read error: %v", err))
					ws.markDisconnected(conn, "read error")
					ws.logger.WithError(err).Warn("WebSocket read error")
					ws.triggerReconnect()
					return
				}
			}

			ws.handleMessage(message)
		}
	}
}

// handleMessage parses and routes WebSocket messages.
func (ws *WebSocketClient) handleMessage(data []byte) {
	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		ws.logger.WithError(err).Debug("Failed to unmarshal message")
		return
	}

	// Handle pong responses
	if op, ok := msg["op"].(string); ok && op == "pong" {
		return
	}

	// Handle subscription confirmations
	if success, ok := msg["success"].(bool); ok {
		if success {
			ws.logger.Debug("Subscription confirmed")
		} else {
			ws.logger.WithField("msg", msg).Warn("Subscription failed")
		}
		return
	}

	// Extract topic and data
	topic, ok := msg["topic"].(string)
	if !ok {
		return
	}

	dataRaw, ok := msg["data"]
	if !ok {
		return
	}

	wsMsg := &WebSocketMessage{
		Topic: topic,
		Data:  dataRaw,
	}

	select {
	case ws.messages <- wsMsg:
	case <-ws.ctx.Done():
		return
	default:
		ws.logger.Warn("Message channel full, dropping message")
	}
}

// pingLoop sends periodic ping messages to keep connection alive.
func (ws *WebSocketClient) pingLoop() {
	defer ws.wg.Done()

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ws.ctx.Done():
			return
		case <-ticker.C:
			pingMsg := map[string]interface{}{
				"op": "ping",
			}
			if err := ws.writeJSON(pingMsg); err != nil {
				ws.logger.WithError(err).Debug("Failed to send ping")
				ws.publishStreamEvent(connectorpkg.StreamEventGap, fmt.Sprintf("ping failed: %v", err))
				ws.disconnectCurrent("ping failed")
				ws.triggerReconnect()
			}
		}
	}
}

func (ws *WebSocketClient) reconnectLoop() {
	defer ws.wg.Done()

	for {
		select {
		case <-ws.ctx.Done():
			return
		case <-ws.reconnectCh:
			ws.reconnectUntilSuccess()
		}
	}
}

func (ws *WebSocketClient) reconnectUntilSuccess() {
	delay := time.Duration(0)

	for {
		if delay > 0 {
			ws.logger.WithField("delay", delay.String()).Info("Waiting before WebSocket reconnect")
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ws.ctx.Done():
				timer.Stop()
				return
			}
		}

		reconnected, err := ws.reconnectOnceAttempt()
		if err != nil {
			ws.logger.WithError(err).Warn("WebSocket reconnect failed")
			if delay == 0 {
				delay = ws.reconnectInitialDelay
			} else {
				delay *= 2
			}
			if delay > ws.reconnectMaxDelay {
				delay = ws.reconnectMaxDelay
			}
			continue
		}

		if !reconnected {
			return
		}

		ws.logger.Info("WebSocket reconnected successfully")
		ws.publishStreamEvent(connectorpkg.StreamEventReconnected, "websocket reconnect completed")
		return
	}
}

func (ws *WebSocketClient) reconnectOnceAttempt() (bool, error) {
	ctx, cancel := context.WithTimeout(ws.ctx, 30*time.Second)
	defer cancel()

	ws.mu.Lock()
	if ws.connected {
		ws.mu.Unlock()
		return false, nil
	}
	if err := ws.connectLocked(ctx); err != nil {
		ws.mu.Unlock()
		return false, err
	}
	topics := make([]string, 0, len(ws.subscriptions))
	for topic := range ws.subscriptions {
		topics = append(topics, topic)
	}
	conn := ws.conn
	ws.mu.Unlock()

	for _, topic := range topics {
		select {
		case <-ctx.Done():
			ws.markDisconnected(conn, "resubscribe context canceled")
			return false, ctx.Err()
		default:
		}
		if err := ws.sendSubscription(conn, topic); err != nil {
			ws.publishStreamEvent(connectorpkg.StreamEventGap, fmt.Sprintf("resubscribe failed for %s: %v", topic, err))
			ws.markDisconnected(conn, fmt.Sprintf("resubscribe failed for %s", topic))
			return false, err
		}
		ws.logger.WithField("topic", topic).Debug("Resubscribed to topic")
	}

	return true, nil
}
