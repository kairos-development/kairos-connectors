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
	mu sync.RWMutex

	conn      *websocket.Conn
	url       string
	apiKey    string
	apiSecret string
	logger    *logrus.Logger

	connected bool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	messages chan *WebSocketMessage
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
		url:       url,
		apiKey:    cfg.APIKey,
		apiSecret: cfg.APISecret,
		logger:    logger,
		ctx:       ctx,
		cancel:    cancel,
		messages:  make(chan *WebSocketMessage, 100),
	}, nil
}

// Connect establishes WebSocket connection and authenticates.
func (ws *WebSocketClient) Connect(ctx context.Context) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.connected {
		return nil
	}

	// Establish connection
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, ws.url, nil)
	if err != nil {
		return fmt.Errorf("dial websocket: %w", err)
	}

	ws.conn = conn

	// Authenticate
	if err := ws.authenticate(); err != nil {
		conn.Close()
		return fmt.Errorf("authenticate: %w", err)
	}

	ws.connected = true

	// Start message handler
	ws.wg.Add(2)
	go ws.readLoop()
	go ws.pingLoop()

	ws.logger.Info("WebSocket connected and authenticated")

	return nil
}

// Close closes the WebSocket connection.
func (ws *WebSocketClient) Close() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if !ws.connected {
		return nil
	}

	ws.connected = false
	ws.cancel()

	if ws.conn != nil {
		ws.conn.Close()
	}

	ws.wg.Wait()
	close(ws.messages)

	ws.logger.Info("WebSocket closed")

	return nil
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
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	if !ws.connected {
		return fmt.Errorf("not connected")
	}

	subMsg := map[string]interface{}{
		"op":   "subscribe",
		"args": []string{topic},
	}

	if err := ws.conn.WriteJSON(subMsg); err != nil {
		return fmt.Errorf("send subscription: %w", err)
	}

	ws.logger.WithField("topic", topic).Debug("Subscribed to topic")

	return nil
}

// Messages returns the channel for receiving WebSocket messages.
func (ws *WebSocketClient) Messages() <-chan *WebSocketMessage {
	return ws.messages
}

// readLoop reads messages from WebSocket connection.
func (ws *WebSocketClient) readLoop() {
	defer ws.wg.Done()

	for {
		select {
		case <-ws.ctx.Done():
			return
		default:
			ws.mu.RLock()
			conn := ws.conn
			ws.mu.RUnlock()

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
					ws.logger.WithError(err).Warn("WebSocket read error")
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
			ws.mu.RLock()
			if ws.connected && ws.conn != nil {
				pingMsg := map[string]interface{}{
					"op": "ping",
				}
				if err := ws.conn.WriteJSON(pingMsg); err != nil {
					ws.logger.WithError(err).Debug("Failed to send ping")
				}
			}
			ws.mu.RUnlock()
		}
	}
}
