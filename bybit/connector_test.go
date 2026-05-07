package bybit

import (
	"context"
	"testing"
	"time"

	"github.com/kairos-development/kairos-contracts/connector"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
)

func TestNewConnector(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
		Testnet:   true,
	}

	conn, err := NewConnector(cfg, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if conn == nil {
		t.Fatal("expected connector to be created")
	}

	if conn.Name() != "bybit" {
		t.Errorf("expected name 'bybit', got %s", conn.Name())
	}

	if conn.IsConnected() {
		t.Error("expected connector to not be connected initially")
	}
}

func TestConnector_Name(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	if conn.Name() != "bybit" {
		t.Errorf("expected 'bybit', got %s", conn.Name())
	}
}

func TestConnector_IsConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	if conn.IsConnected() {
		t.Error("expected not connected initially")
	}

	// Simulate connection
	conn.mu.Lock()
	conn.connected = true
	conn.mu.Unlock()

	if !conn.IsConnected() {
		t.Error("expected connected after setting flag")
	}
}

func TestConnector_SubmitOrder_NotConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	order := &connector.Order{
		ID:       "order_1",
		Symbol:   "BTCUSDT",
		Side:     connector.OrderSideBuy,
		Type:     connector.OrderTypeLimit,
		Quantity: decimal.NewFromFloat(0.1),
		Price:    decimal.NewFromInt(50000),
	}

	ctx := context.Background()
	_, err := conn.SubmitOrder(ctx, order)

	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

func TestConnector_CancelOrder_NotConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	ctx := context.Background()
	err := conn.CancelOrder(ctx, "order_123")

	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

func TestConnector_GetOpenOrders_NotConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	ctx := context.Background()
	_, err := conn.GetOpenOrders(ctx)

	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

func TestConnector_QueryOrder_NotConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	ctx := context.Background()
	_, err := conn.QueryOrder(ctx, "order_123")

	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

func TestConnector_GetPosition_NotConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	ctx := context.Background()
	_, err := conn.GetPosition(ctx, "BTCUSDT")

	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

func TestConnector_GetBalance_NotConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	ctx := context.Background()
	_, err := conn.GetBalance(ctx)

	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

func TestConnector_GetSymbol_NotConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	ctx := context.Background()
	_, err := conn.GetSymbol(ctx, "BTCUSDT")

	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

func TestConnector_RefreshSymbols_NotConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	ctx := context.Background()
	err := conn.RefreshSymbols(ctx)

	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

func TestConnector_SubscribeOrders_NotConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	ctx := context.Background()
	_, err := conn.SubscribeOrders(ctx)

	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

func TestConnector_SubscribePositions_NotConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	ctx := context.Background()
	_, err := conn.SubscribePositions(ctx)

	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

func TestConnector_SubscribeBalance_NotConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	ctx := context.Background()
	_, err := conn.SubscribeBalance(ctx)

	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

func TestConnector_SubscribeTicker_NotConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	ctx := context.Background()
	_, err := conn.SubscribeTicker(ctx, "BTCUSDT")

	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

func TestConnector_Disconnect_NotConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	ctx := context.Background()
	err := conn.Disconnect(ctx)

	if err != nil {
		t.Fatalf("expected no error when disconnecting while not connected, got %v", err)
	}
}

func TestConnector_ProcessWebSocketMessage_Order(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, logrus.New())

	msg := &WebSocketMessage{
		Topic: "order",
		Data: map[string]interface{}{
			"orderId":     "order_123",
			"orderLinkId": "client_order_123",
			"orderStatus": "Filled",
			"cumExecQty":  "0.1",
			"leavesQty":   "0",
			"avgPrice":    "50000",
		},
	}

	// Process message (should not panic)
	conn.processWebSocketMessage(msg)
}

func TestConnector_ProcessWebSocketMessage_Position(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, logrus.New())

	msg := &WebSocketMessage{
		Topic: "position",
		Data: map[string]interface{}{
			"symbol":        "BTCUSDT",
			"side":          "Buy",
			"size":          "0.1",
			"avgPrice":      "50000",
			"unrealisedPnl": "100",
		},
	}

	conn.processWebSocketMessage(msg)
}

func TestConnector_ProcessWebSocketMessage_Balance(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, logrus.New())

	msg := &WebSocketMessage{
		Topic: "wallet",
		Data: map[string]interface{}{
			"coin":             "USDT",
			"walletBalance":    "10000",
			"availableBalance": "9500",
		},
	}

	conn.processWebSocketMessage(msg)
}

func TestConnector_ProcessWebSocketMessage_Ticker(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, logrus.New())

	msg := &WebSocketMessage{
		Topic: "tickers",
		Data: map[string]interface{}{
			"symbol":    "BTCUSDT",
			"lastPrice": "50000",
			"bid1Price": "49999",
			"ask1Price": "50001",
			"volume24h": "1000",
		},
	}

	conn.processWebSocketMessage(msg)
}

func TestGetString(t *testing.T) {
	data := map[string]interface{}{
		"key1": "value1",
		"key2": 123,
		"key3": nil,
	}

	if getString(data, "key1") != "value1" {
		t.Error("expected 'value1'")
	}

	if getString(data, "key2") != "" {
		t.Error("expected empty string for non-string value")
	}

	if getString(data, "key3") != "" {
		t.Error("expected empty string for nil value")
	}

	if getString(data, "nonexistent") != "" {
		t.Error("expected empty string for nonexistent key")
	}
}

func TestConnector_Channels(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	if conn.orderUpdates == nil {
		t.Error("expected orderUpdates channel to be initialized")
	}

	if conn.positionUpdates == nil {
		t.Error("expected positionUpdates channel to be initialized")
	}

	if conn.balanceUpdates == nil {
		t.Error("expected balanceUpdates channel to be initialized")
	}

	if conn.tickerUpdates == nil {
		t.Error("expected tickerUpdates map to be initialized")
	}
}

func TestConnector_ContextCancellation(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	// Cancel context
	conn.cancel()

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Context should be done
	select {
	case <-conn.ctx.Done():
		// Expected
	default:
		t.Error("expected context to be done after cancel")
	}
}
