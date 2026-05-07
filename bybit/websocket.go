package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kairos-development/kairos-contracts/connector"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
)

// WSClient manages WebSocket connections to Bybit.
type WSClient struct {
	mu          sync.RWMutex
	conn        *websocket.Conn
	url         string
	apiKey      string
	apiSecret   string
	connected   bool
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	reconnectCh chan struct{}
	client      *Client // Reference to parent client for signature generation
	logger      *logrus.Logger

	// Subscription channels
	orderCh    chan *connector.OrderUpdate
	positionCh chan *connector.PositionUpdate
	balanceCh  chan *connector.BalanceUpdate
	tickerCh   chan *connector.TickerUpdate
}

// NewWSClient creates a new WebSocket client.
func NewWSClient(client *Client, testnet bool, logger *logrus.Logger) *WSClient {
	if logger == nil {
		logger = logrus.New()
	}

	url := "wss://stream.bybit.com/v5/private"
	if testnet {
		url = "wss://stream-testnet.bybit.com/v5/private"
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WSClient{
		url:         url,
		apiKey:      client.apiKey,
		apiSecret:   client.apiSecret,
		client:      client,
		ctx:         ctx,
		cancel:      cancel,
		reconnectCh: make(chan struct{}, 1),
		orderCh:     make(chan *connector.OrderUpdate, 100),
		positionCh:  make(chan *connector.PositionUpdate, 100),
		balanceCh:   make(chan *connector.BalanceUpdate, 100),
		tickerCh:    make(chan *connector.TickerUpdate, 100),
		logger:      logger,
	}
}

// Connect establishes WebSocket connection and authenticates.
func (ws *WSClient) Connect(ctx context.Context) error {
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
	ws.connected = true

	// Authenticate
	if err := ws.authenticate(); err != nil {
		ws.conn.Close()
		ws.connected = false
		return fmt.Errorf("authenticate: %w", err)
	}

	// Start goroutines
	ws.wg.Add(3)
	go ws.handleMessages()
	go ws.pingLoop()
	go ws.reconnectLoop()

	ws.logger.Info("WebSocket connected and authenticated")

	return nil
}

// Disconnect closes the WebSocket connection gracefully.
func (ws *WSClient) Disconnect() error {
	ws.mu.Lock()

	if !ws.connected {
		ws.mu.Unlock()
		return nil
	}

	ws.logger.Info("Disconnecting WebSocket")

	// Mark as disconnected first to prevent new operations
	ws.connected = false

	// Cancel context to stop all goroutines
	ws.cancel()

	// Close WebSocket connection
	if ws.conn != nil {
		ws.conn.Close()
	}

	ws.mu.Unlock()

	// Wait for all goroutines to exit
	ws.wg.Wait()

	// Now safe to close channels
	close(ws.orderCh)
	close(ws.positionCh)
	close(ws.balanceCh)
	close(ws.tickerCh)

	ws.logger.Info("WebSocket disconnected")

	return nil
}

// IsConnected returns true if WebSocket is connected.
func (ws *WSClient) IsConnected() bool {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	return ws.connected
}

// authenticate sends authentication message to Bybit WebSocket.
func (ws *WSClient) authenticate() error {
	expires := time.Now().UnixMilli() + 10000
	signature := ws.generateWSSignature(expires)

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

// generateWSSignature creates signature for WebSocket authentication.
func (ws *WSClient) generateWSSignature(expires int64) string {
	message := fmt.Sprintf("GET/realtime%d", expires)
	return ws.client.generateSignature(expires, message)
}

// Subscribe subscribes to a WebSocket topic.
func (ws *WSClient) Subscribe(topic string) error {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	if !ws.connected {
		return fmt.Errorf("not connected")
	}

	subMsg := map[string]interface{}{
		"op":   "subscribe",
		"args": []string{topic},
	}

	return ws.conn.WriteJSON(subMsg)
}

// handleMessages processes incoming WebSocket messages.
func (ws *WSClient) handleMessages() {
	defer ws.wg.Done()

	for {
		select {
		case <-ws.ctx.Done():
			return
		default:
			// Set read deadline to allow periodic context checks
			ws.mu.RLock()
			conn := ws.conn
			ws.mu.RUnlock()

			if conn == nil {
				return
			}

			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			_, message, err := conn.ReadMessage()
			if err != nil {
				// Check if context was canceled (intentional shutdown)
				select {
				case <-ws.ctx.Done():
					return
				default:
					// Connection error, trigger reconnect
					ws.logger.WithError(err).Warn("WebSocket read error, triggering reconnect")
					ws.mu.Lock()
					ws.connected = false
					ws.mu.Unlock()

					select {
					case ws.reconnectCh <- struct{}{}:
					default:
					}
					return
				}
			}

			// Parse and route message
			ws.routeMessage(message)
		}
	}
}

// routeMessage routes WebSocket messages to appropriate channels.
func (ws *WSClient) routeMessage(message []byte) {
	var msg map[string]interface{}
	if err := json.Unmarshal(message, &msg); err != nil {
		ws.logger.WithError(err).Debug("Failed to unmarshal WebSocket message")
		return
	}

	// Check message type
	topic, ok := msg["topic"].(string)
	if !ok {
		return
	}

	// Route based on topic
	switch {
	case topic == "order":
		ws.parseOrderUpdate(msg)
	case topic == "position":
		ws.parsePositionUpdate(msg)
	case topic == "wallet":
		ws.parseBalanceUpdate(msg)
	case topic == "tickers":
		ws.parseTickerUpdate(msg)
	}
}

// parseOrderUpdate parses order execution update from WebSocket.
func (ws *WSClient) parseOrderUpdate(msg map[string]interface{}) {
	data, ok := msg["data"].([]interface{})
	if !ok || len(data) == 0 {
		return
	}

	for _, item := range data {
		orderData, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		update := &connector.OrderUpdate{
			OrderID:         getStringField(orderData, "orderId"),
			ClientOrderID:   getStringField(orderData, "orderLinkId"),
			ExchangeOrderID: getStringField(orderData, "orderId"),
			Status:          parseOrderStatus(getStringField(orderData, "orderStatus")),
			UpdatedAtUTC:    time.Now().UTC(),
		}

		update.FilledQty, _ = decimal.NewFromString(getStringField(orderData, "cumExecQty"))
		update.RemainingQty, _ = decimal.NewFromString(getStringField(orderData, "leavesQty"))
		update.AvgFillPrice, _ = decimal.NewFromString(getStringField(orderData, "avgPrice"))

		select {
		case ws.orderCh <- update:
		case <-ws.ctx.Done():
			return
		default:
			ws.logger.Warn("Order channel full, dropping update")
		}
	}
}

// parsePositionUpdate parses position update from WebSocket.
func (ws *WSClient) parsePositionUpdate(msg map[string]interface{}) {
	data, ok := msg["data"].([]interface{})
	if !ok || len(data) == 0 {
		return
	}

	for _, item := range data {
		posData, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		update := &connector.PositionUpdate{
			Symbol:       getStringField(posData, "symbol"),
			Side:         parsePositionSide(getStringField(posData, "side")),
			UpdatedAtUTC: time.Now().UTC(),
		}

		update.Quantity, _ = decimal.NewFromString(getStringField(posData, "size"))
		update.EntryPrice, _ = decimal.NewFromString(getStringField(posData, "avgPrice"))
		update.UnrealizedPnL, _ = decimal.NewFromString(getStringField(posData, "unrealisedPnl"))

		select {
		case ws.positionCh <- update:
		case <-ws.ctx.Done():
			return
		default:
			ws.logger.Warn("Position channel full, dropping update")
		}
	}
}

// parseBalanceUpdate parses balance update from WebSocket.
func (ws *WSClient) parseBalanceUpdate(msg map[string]interface{}) {
	data, ok := msg["data"].([]interface{})
	if !ok || len(data) == 0 {
		return
	}

	for _, item := range data {
		balData, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		coins, ok := balData["coin"].([]interface{})
		if !ok {
			continue
		}

		for _, coinItem := range coins {
			coinData, ok := coinItem.(map[string]interface{})
			if !ok {
				continue
			}

			update := &connector.BalanceUpdate{
				Asset:        getStringField(coinData, "coin"),
				UpdatedAtUTC: time.Now().UTC(),
			}

			update.Total, _ = decimal.NewFromString(getStringField(coinData, "walletBalance"))
			update.Available, _ = decimal.NewFromString(getStringField(coinData, "availableToWithdraw"))
			update.Locked, _ = decimal.NewFromString(getStringField(coinData, "locked"))

			select {
			case ws.balanceCh <- update:
			case <-ws.ctx.Done():
				return
			default:
				ws.logger.Warn("Balance channel full, dropping update")
			}
		}
	}
}

// parseTickerUpdate parses ticker update from WebSocket.
func (ws *WSClient) parseTickerUpdate(msg map[string]interface{}) {
	data, ok := msg["data"].(map[string]interface{})
	if !ok {
		return
	}

	update := &connector.TickerUpdate{
		Symbol:       getStringField(data, "symbol"),
		UpdatedAtUTC: time.Now().UTC(),
	}

	update.LastPrice, _ = decimal.NewFromString(getStringField(data, "lastPrice"))
	update.BidPrice, _ = decimal.NewFromString(getStringField(data, "bid1Price"))
	update.AskPrice, _ = decimal.NewFromString(getStringField(data, "ask1Price"))
	update.Volume24h, _ = decimal.NewFromString(getStringField(data, "volume24h"))

	select {
	case ws.tickerCh <- update:
	case <-ws.ctx.Done():
		return
	default:
		ws.logger.Warn("Ticker channel full, dropping update")
	}
}

// Helper functions
func getStringField(data map[string]interface{}, key string) string {
	if val, ok := data[key].(string); ok {
		return val
	}
	return ""
}

func parseOrderStatus(status string) connector.OrderStatus {
	switch status {
	case "New":
		return connector.OrderStatusSubmitted
	case "PartiallyFilled":
		return connector.OrderStatusPartial
	case "Filled":
		return connector.OrderStatusFilled
	case "Cancelled":
		return connector.OrderStatusCanceled
	case "Rejected":
		return connector.OrderStatusRejected
	default:
		return connector.OrderStatusPending
	}
}

func parsePositionSide(side string) connector.PositionSide {
	switch side {
	case "Buy":
		return connector.PositionSideLong
	case "Sell":
		return connector.PositionSideShort
	default:
		return connector.PositionSideFlat
	}
}

// pingLoop sends periodic ping messages to keep connection alive.
func (ws *WSClient) pingLoop() {
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

// reconnectLoop handles automatic reconnection.
func (ws *WSClient) reconnectLoop() {
	defer ws.wg.Done()

	for {
		select {
		case <-ws.ctx.Done():
			return
		case <-ws.reconnectCh:
			// Wait before reconnecting
			ws.logger.Info("Attempting WebSocket reconnection in 5 seconds")

			select {
			case <-time.After(5 * time.Second):
			case <-ws.ctx.Done():
				return
			}

			// Attempt reconnection with timeout
			ctx, cancel := context.WithTimeout(ws.ctx, 30*time.Second)
			if err := ws.Connect(ctx); err != nil {
				ws.logger.WithError(err).Warn("Reconnection failed, will retry")
				// Retry on next trigger
				select {
				case ws.reconnectCh <- struct{}{}:
				default:
				}
			} else {
				ws.logger.Info("WebSocket reconnected successfully")
			}
			cancel()
		}
	}
}

// SubscribeOrders subscribes to order execution updates.
func (c *Client) SubscribeOrders(ctx context.Context) (<-chan *connector.OrderUpdate, error) {
	// WebSocket implementation would be initialized here
	// For now, return a placeholder channel
	return make(<-chan *connector.OrderUpdate), fmt.Errorf("WebSocket not fully implemented")
}

// SubscribePositions subscribes to position updates.
func (c *Client) SubscribePositions(ctx context.Context) (<-chan *connector.PositionUpdate, error) {
	return make(<-chan *connector.PositionUpdate), fmt.Errorf("WebSocket not fully implemented")
}

// SubscribeBalance subscribes to balance updates.
func (c *Client) SubscribeBalance(ctx context.Context) (<-chan *connector.BalanceUpdate, error) {
	return make(<-chan *connector.BalanceUpdate), fmt.Errorf("WebSocket not fully implemented")
}

// SubscribeTicker subscribes to market ticker updates.
func (c *Client) SubscribeTicker(ctx context.Context, symbol string) (<-chan *connector.TickerUpdate, error) {
	return make(<-chan *connector.TickerUpdate), fmt.Errorf("WebSocket not fully implemented")
}
