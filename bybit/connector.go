package bybit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kairos-development/kairos-contracts/connector"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
)

// Connector implements the domain connector interface for Bybit.
type Connector struct {
	mu sync.RWMutex

	client    *Client
	ws        *WebSocketClient
	logger    *logrus.Logger
	connected bool

	// WebSocket channels
	orderUpdates    chan *connector.OrderUpdate
	positionUpdates chan *connector.PositionUpdate
	balanceUpdates  chan *connector.BalanceUpdate
	tickerUpdates   map[string]chan *connector.TickerUpdate

	// Shutdown
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewConnector creates a new Bybit connector.
func NewConnector(cfg Config, logger *logrus.Logger) (*Connector, error) {
	if logger == nil {
		logger = logrus.New()
	}

	client, err := NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Connector{
		client:          client,
		logger:          logger,
		tickerUpdates:   make(map[string]chan *connector.TickerUpdate),
		ctx:             ctx,
		cancel:          cancel,
		orderUpdates:    make(chan *connector.OrderUpdate, 100),
		positionUpdates: make(chan *connector.PositionUpdate, 100),
		balanceUpdates:  make(chan *connector.BalanceUpdate, 100),
	}, nil
}

// Name returns the exchange identifier.
func (c *Connector) Name() string {
	return "bybit"
}

// Connect establishes exchange connections and authenticates.
func (c *Connector) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return nil
	}

	// Validate API key permissions
	perms, err := c.CheckPermissions(ctx)
	if err != nil {
		return fmt.Errorf("check permissions: %w", err)
	}

	if !perms.IsSufficientForTrading() {
		return fmt.Errorf("insufficient permissions: canRead=%v canTrade=%v", perms.CanRead, perms.CanTrade)
	}

	if perms.IsOverPrivileged() {
		c.logger.Warn("API key has dangerous permissions (withdraw/transfer)")
	}

	// Initialize WebSocket client
	wsCfg := WebSocketConfig{
		APIKey:    c.client.apiKey,
		APISecret: c.client.apiSecret,
		Testnet:   c.client.testnet,
	}

	c.ws, err = NewWebSocketClient(wsCfg, c.logger)
	if err != nil {
		return fmt.Errorf("create websocket client: %w", err)
	}

	// Connect WebSocket
	if err := c.ws.Connect(ctx); err != nil {
		return fmt.Errorf("connect websocket: %w", err)
	}

	// Start WebSocket message handler
	c.wg.Add(1)
	go c.handleWebSocketMessages()

	c.connected = true
	c.logger.Info("Bybit connector connected")

	return nil
}

// Disconnect gracefully closes all connections.
func (c *Connector) Disconnect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return nil
	}

	// Cancel context to stop goroutines
	c.cancel()

	// Close WebSocket
	if c.ws != nil {
		if err := c.ws.Close(); err != nil {
			c.logger.WithError(err).Error("Failed to close WebSocket")
		}
	}

	// Wait for goroutines to finish
	c.wg.Wait()

	// Close channels
	close(c.orderUpdates)
	close(c.positionUpdates)
	close(c.balanceUpdates)

	for _, ch := range c.tickerUpdates {
		close(ch)
	}

	c.connected = false
	c.logger.Info("Bybit connector disconnected")

	return nil
}

// IsConnected returns true if the connector is currently connected.
func (c *Connector) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// SubmitOrder sends an order to the exchange.
func (c *Connector) SubmitOrder(ctx context.Context, order *connector.Order) (string, error) {
	if !c.IsConnected() {
		return "", fmt.Errorf("connector not connected")
	}

	exchangeOrderID, err := c.client.SubmitOrder(ctx, order)
	if err != nil {
		return "", fmt.Errorf("submit order: %w", err)
	}

	c.logger.WithFields(logrus.Fields{
		"order_id":          order.ID,
		"exchange_order_id": exchangeOrderID,
		"symbol":            order.Symbol,
		"side":              order.Side,
		"quantity":          order.Quantity.String(),
	}).Info("Order submitted to Bybit")

	return exchangeOrderID, nil
}

// CancelOrder cancels an active order on the exchange.
func (c *Connector) CancelOrder(ctx context.Context, orderID string) error {
	if !c.IsConnected() {
		return fmt.Errorf("connector not connected")
	}

	if err := c.client.CancelOrder(ctx, orderID); err != nil {
		return fmt.Errorf("cancel order: %w", err)
	}

	c.logger.WithField("order_id", orderID).Info("Order canceled on Bybit")

	return nil
}

// GetOpenOrders retrieves all open orders for the account.
func (c *Connector) GetOpenOrders(ctx context.Context) ([]*connector.Order, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("connector not connected")
	}

	orders, err := c.client.GetOpenOrders(ctx)
	if err != nil {
		return nil, fmt.Errorf("get open orders: %w", err)
	}

	return orders, nil
}

// QueryOrder retrieves the current status of an order from the exchange.
func (c *Connector) QueryOrder(ctx context.Context, orderID string) (*connector.Order, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("connector not connected")
	}

	order, err := c.client.QueryOrder(ctx, orderID)
	if err != nil {
		return nil, fmt.Errorf("query order: %w", err)
	}

	return order, nil
}

// GetPosition retrieves the current position for a symbol.
func (c *Connector) GetPosition(ctx context.Context, symbol string) (*connector.Position, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("connector not connected")
	}

	position, err := c.client.GetPosition(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("get position: %w", err)
	}

	return position, nil
}

// GetBalance retrieves the current account balance.
func (c *Connector) GetBalance(ctx context.Context) (*connector.AccountBalance, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("connector not connected")
	}

	balance, err := c.client.GetBalance(ctx)
	if err != nil {
		return nil, fmt.Errorf("get balance: %w", err)
	}

	return balance, nil
}

// GetSymbol retrieves symbol metadata and trading constraints.
func (c *Connector) GetSymbol(ctx context.Context, symbol string) (*connector.Symbol, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("connector not connected")
	}

	symbolInfo, err := c.client.GetSymbol(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("get symbol: %w", err)
	}

	return symbolInfo, nil
}

// RefreshSymbols updates all symbol metadata from the exchange.
func (c *Connector) RefreshSymbols(ctx context.Context) error {
	if !c.IsConnected() {
		return fmt.Errorf("connector not connected")
	}

	if err := c.client.RefreshSymbols(ctx); err != nil {
		return fmt.Errorf("refresh symbols: %w", err)
	}

	c.logger.Info("Symbols refreshed from Bybit")

	return nil
}

// CheckPermissions verifies API key permissions.
func (c *Connector) CheckPermissions(ctx context.Context) (*connector.Permissions, error) {
	perms, err := c.client.CheckPermissions(ctx)
	if err != nil {
		return nil, fmt.Errorf("check permissions: %w", err)
	}

	return perms, nil
}

// SubscribeOrders subscribes to order execution updates via WebSocket.
func (c *Connector) SubscribeOrders(ctx context.Context) (<-chan *connector.OrderUpdate, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("connector not connected")
	}

	if err := c.ws.SubscribeOrders(ctx); err != nil {
		return nil, fmt.Errorf("subscribe orders: %w", err)
	}

	c.logger.Info("Subscribed to order updates")

	return c.orderUpdates, nil
}

// SubscribePositions subscribes to position updates via WebSocket.
func (c *Connector) SubscribePositions(ctx context.Context) (<-chan *connector.PositionUpdate, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("connector not connected")
	}

	if err := c.ws.SubscribePositions(ctx); err != nil {
		return nil, fmt.Errorf("subscribe positions: %w", err)
	}

	c.logger.Info("Subscribed to position updates")

	return c.positionUpdates, nil
}

// SubscribeBalance subscribes to balance updates via WebSocket.
func (c *Connector) SubscribeBalance(ctx context.Context) (<-chan *connector.BalanceUpdate, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("connector not connected")
	}

	if err := c.ws.SubscribeBalance(ctx); err != nil {
		return nil, fmt.Errorf("subscribe balance: %w", err)
	}

	c.logger.Info("Subscribed to balance updates")

	return c.balanceUpdates, nil
}

// SubscribeTicker subscribes to market ticker updates via WebSocket.
func (c *Connector) SubscribeTicker(ctx context.Context, symbol string) (<-chan *connector.TickerUpdate, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("connector not connected")
	}

	c.mu.Lock()
	ch, exists := c.tickerUpdates[symbol]
	if !exists {
		ch = make(chan *connector.TickerUpdate, 100)
		c.tickerUpdates[symbol] = ch
	}
	c.mu.Unlock()

	if err := c.ws.SubscribeTicker(ctx, symbol); err != nil {
		return nil, fmt.Errorf("subscribe ticker: %w", err)
	}

	c.logger.WithField("symbol", symbol).Info("Subscribed to ticker updates")

	return ch, nil
}

// handleWebSocketMessages processes incoming WebSocket messages.
func (c *Connector) handleWebSocketMessages() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return

		case msg := <-c.ws.Messages():
			c.processWebSocketMessage(msg)
		}
	}
}

// processWebSocketMessage routes WebSocket messages to appropriate channels.
func (c *Connector) processWebSocketMessage(msg *WebSocketMessage) {
	switch msg.Topic {
	case "order":
		c.processOrderUpdate(msg)
	case "position":
		c.processPositionUpdate(msg)
	case "wallet":
		c.processBalanceUpdate(msg)
	case "tickers":
		c.processTickerUpdate(msg)
	default:
		c.logger.WithField("topic", msg.Topic).Debug("Unhandled WebSocket message")
	}
}

// processOrderUpdate converts WebSocket order message to domain update.
func (c *Connector) processOrderUpdate(msg *WebSocketMessage) {
	// Parse order data from message
	orderData, ok := msg.Data.(map[string]interface{})
	if !ok {
		c.logger.Error("Invalid order update data")
		return
	}

	update := &connector.OrderUpdate{
		ExchangeOrderID: getString(orderData, "orderId"),
		ClientOrderID:   getString(orderData, "orderLinkId"),
		Status:          unmapOrderStatus(getString(orderData, "orderStatus")),
		UpdatedAtUTC:    time.Now().UTC(),
	}

	update.FilledQty, _ = decimal.NewFromString(getString(orderData, "cumExecQty"))
	update.RemainingQty, _ = decimal.NewFromString(getString(orderData, "leavesQty"))
	update.AvgFillPrice, _ = decimal.NewFromString(getString(orderData, "avgPrice"))

	select {
	case c.orderUpdates <- update:
	case <-c.ctx.Done():
	}
}

// processPositionUpdate converts WebSocket position message to domain update.
func (c *Connector) processPositionUpdate(msg *WebSocketMessage) {
	posData, ok := msg.Data.(map[string]interface{})
	if !ok {
		c.logger.Error("Invalid position update data")
		return
	}

	update := &connector.PositionUpdate{
		Symbol:       getString(posData, "symbol"),
		Side:         unmapPositionSide(getString(posData, "side")),
		UpdatedAtUTC: time.Now().UTC(),
	}

	update.Quantity, _ = decimal.NewFromString(getString(posData, "size"))
	update.EntryPrice, _ = decimal.NewFromString(getString(posData, "avgPrice"))
	update.UnrealizedPnL, _ = decimal.NewFromString(getString(posData, "unrealisedPnl"))

	select {
	case c.positionUpdates <- update:
	case <-c.ctx.Done():
	}
}

// processBalanceUpdate converts WebSocket balance message to domain update.
func (c *Connector) processBalanceUpdate(msg *WebSocketMessage) {
	balData, ok := msg.Data.(map[string]interface{})
	if !ok {
		c.logger.Error("Invalid balance update data")
		return
	}

	update := &connector.BalanceUpdate{
		Asset:        getString(balData, "coin"),
		UpdatedAtUTC: time.Now().UTC(),
	}

	update.Total, _ = decimal.NewFromString(getString(balData, "walletBalance"))
	update.Available, _ = decimal.NewFromString(getString(balData, "availableBalance"))
	update.Locked = update.Total.Sub(update.Available)

	select {
	case c.balanceUpdates <- update:
	case <-c.ctx.Done():
	}
}

// processTickerUpdate converts WebSocket ticker message to domain update.
func (c *Connector) processTickerUpdate(msg *WebSocketMessage) {
	tickerData, ok := msg.Data.(map[string]interface{})
	if !ok {
		c.logger.Error("Invalid ticker update data")
		return
	}

	symbol := getString(tickerData, "symbol")

	update := &connector.TickerUpdate{
		Symbol:       symbol,
		UpdatedAtUTC: time.Now().UTC(),
	}

	update.LastPrice, _ = decimal.NewFromString(getString(tickerData, "lastPrice"))
	update.BidPrice, _ = decimal.NewFromString(getString(tickerData, "bid1Price"))
	update.AskPrice, _ = decimal.NewFromString(getString(tickerData, "ask1Price"))
	update.Volume24h, _ = decimal.NewFromString(getString(tickerData, "volume24h"))

	c.mu.RLock()
	ch, exists := c.tickerUpdates[symbol]
	c.mu.RUnlock()

	if exists {
		select {
		case ch <- update:
		case <-c.ctx.Done():
		}
	}
}

// Helper function to safely extract string from map.
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
