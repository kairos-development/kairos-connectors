package bybit

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/kairos-development/kairos-contracts/connector"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
)

func TestNewConnector_PropagatesClientConfigError(t *testing.T) {
	_, err := NewConnector(Config{APISecret: "secret"}, nil)
	if err == nil {
		t.Fatal("expected missing API key error")
	}
}

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

func TestConnector_GetPositions_NotConnected(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, nil)

	ctx := context.Background()
	_, err := conn.GetPositions(ctx)

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

func TestConnector_DisconnectConnectedWithoutWebSocketClosesChannels(t *testing.T) {
	conn := newConnectedConnectorForTest(t, &captureTransport{})

	if err := conn.Disconnect(context.Background()); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	if conn.IsConnected() {
		t.Fatal("expected connector to be disconnected")
	}

	select {
	case _, ok := <-conn.orderUpdates:
		if ok {
			t.Fatal("expected orderUpdates to be closed")
		}
	default:
		t.Fatal("expected orderUpdates close to be observable")
	}

	if err := conn.Disconnect(context.Background()); err != nil {
		t.Fatalf("second disconnect should be idempotent: %v", err)
	}
}

func TestConnector_SubscribeStreamEvents_ReturnsChannelWhenConnected(t *testing.T) {
	conn := newConnectedConnectorForTest(t, &captureTransport{})

	ch, err := conn.SubscribeStreamEvents(context.Background())
	if err != nil {
		t.Fatalf("subscribe stream events: %v", err)
	}
	if ch == nil {
		t.Fatal("expected stream events channel")
	}
}

func TestConnector_ProcessWebSocketEvent_DropsNilAndPublishesEvent(t *testing.T) {
	conn := newConnectorForTest(t)

	conn.processWebSocketEvent(nil)
	conn.processWebSocketEvent(&connector.StreamEvent{Type: connector.StreamEventDisconnected, Reason: "read failed"})

	select {
	case event := <-conn.streamEvents:
		if event.Type != connector.StreamEventDisconnected {
			t.Fatalf("expected disconnected event, got %s", event.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("expected stream event")
	}
}

func TestConnector_RESTDelegationWhenConnected(t *testing.T) {
	t.Run("submit_order", func(t *testing.T) {
		transport := &captureTransport{responseBody: `{"retCode":0,"retMsg":"OK","result":{"orderId":"exchange-1"}}`}
		conn := newConnectedConnectorForTest(t, transport)

		exchangeID, err := conn.SubmitOrder(context.Background(), &connector.Order{
			ID:            "order-1",
			ClientOrderID: "client-1",
			Symbol:        "BTCUSDT",
			Side:          connector.OrderSideBuy,
			Type:          connector.OrderTypeMarket,
			TimeInForce:   connector.TimeInForceGTC,
			Quantity:      decimal.RequireFromString("0.1"),
		})
		if err != nil {
			t.Fatalf("submit order: %v", err)
		}
		if exchangeID != "exchange-1" {
			t.Fatalf("expected exchange ID, got %q", exchangeID)
		}
	})

	t.Run("get_balance", func(t *testing.T) {
		transport := &captureTransport{responseBody: `{"retCode":0,"retMsg":"OK","result":{"list":[]}}`}
		conn := newConnectedConnectorForTest(t, transport)

		balance, err := conn.GetBalance(context.Background())
		if err != nil {
			t.Fatalf("get balance: %v", err)
		}
		if balance == nil {
			t.Fatal("expected balance snapshot")
		}
	})

	t.Run("cancel_order", func(t *testing.T) {
		transport := &captureTransport{responseBody: `{"retCode":0,"retMsg":"OK","result":{}}`}
		conn := newConnectedConnectorForTest(t, transport)

		if err := conn.CancelOrder(context.Background(), "exchange-1"); err != nil {
			t.Fatalf("cancel order: %v", err)
		}
	})

	t.Run("query_order", func(t *testing.T) {
		transport := &captureTransport{responseBody: `{"retCode":0,"retMsg":"OK","result":{"list":[{"orderId":"exchange-1","orderLinkId":"client-1","symbol":"BTCUSDT","side":"Buy","orderType":"Market","price":"","qty":"0.1","cumExecQty":"0","leavesQty":"0.1","avgPrice":"","orderStatus":"New","timeInForce":"GTC"}]}}`}
		conn := newConnectedConnectorForTest(t, transport)

		order, err := conn.QueryOrder(context.Background(), "exchange-1")
		if err != nil {
			t.Fatalf("query order: %v", err)
		}
		if order.ExchangeOrderID != "exchange-1" {
			t.Fatalf("unexpected order: %+v", order)
		}
	})

	t.Run("get_open_orders", func(t *testing.T) {
		transport := &captureTransport{responseBody: `{"retCode":0,"retMsg":"OK","result":{"list":[{"orderId":"exchange-1","orderLinkId":"client-1","symbol":"BTCUSDT","side":"Buy","orderType":"Market","price":"","qty":"0.1","cumExecQty":"0","leavesQty":"0.1","avgPrice":"","orderStatus":"New","timeInForce":"GTC"}]}}`}
		conn := newConnectedConnectorForTest(t, transport)

		orders, err := conn.GetOpenOrders(context.Background())
		if err != nil {
			t.Fatalf("get open orders: %v", err)
		}
		if len(orders) != 1 {
			t.Fatalf("expected one open order, got %d", len(orders))
		}
	})

	t.Run("get_position", func(t *testing.T) {
		transport := &captureTransport{responseBody: `{"retCode":0,"retMsg":"OK","result":{"list":[{"symbol":"BTCUSDT","side":"Buy","size":"0.1","avgPrice":"50000","markPrice":"50100","unrealisedPnl":"10","cumRealisedPnl":"0"}]}}`}
		conn := newConnectedConnectorForTest(t, transport)

		position, err := conn.GetPosition(context.Background(), "BTCUSDT")
		if err != nil {
			t.Fatalf("get position: %v", err)
		}
		if position.Symbol != "BTCUSDT" {
			t.Fatalf("unexpected position: %+v", position)
		}
	})

	t.Run("get_positions", func(t *testing.T) {
		transport := &captureTransport{responseBody: `{"retCode":0,"retMsg":"OK","result":{"list":[{"symbol":"BTCUSDT","side":"Buy","size":"0.1","avgPrice":"50000","markPrice":"50100","unrealisedPnl":"10","cumRealisedPnl":"0"}]}}`}
		conn := newConnectedConnectorForTest(t, transport)

		positions, err := conn.GetPositions(context.Background())
		if err != nil {
			t.Fatalf("get positions: %v", err)
		}
		if len(positions) != 1 {
			t.Fatalf("expected one position, got %d", len(positions))
		}
	})

	t.Run("get_symbol", func(t *testing.T) {
		transport := &captureTransport{responseBody: `{"retCode":0,"retMsg":"OK","result":{"list":[{"symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT","status":"Trading","lotSizeFilter":{"minOrderQty":"0.001","maxOrderQty":"100","qtyStep":"0.001"},"priceFilter":{"minPrice":"1","maxPrice":"1000000","tickSize":"0.1"}}]}}`}
		conn := newConnectedConnectorForTest(t, transport)

		symbol, err := conn.GetSymbol(context.Background(), "BTCUSDT")
		if err != nil {
			t.Fatalf("get symbol: %v", err)
		}
		if symbol.Name != "BTCUSDT" {
			t.Fatalf("unexpected symbol: %+v", symbol)
		}
	})

	t.Run("check_permissions", func(t *testing.T) {
		transport := &captureTransport{responseBody: `{"retCode":0,"retMsg":"OK","result":{"readOnly":0,"permissions":{"ContractTrade":["Order"],"Spot":[],"Wallet":[],"Options":[],"Derivatives":[],"Exchange":[]}}}`}
		conn := newConnectedConnectorForTest(t, transport)

		permissions, err := conn.CheckPermissions(context.Background())
		if err != nil {
			t.Fatalf("check permissions: %v", err)
		}
		if !permissions.CanTrade {
			t.Fatalf("expected trade permission, got %+v", permissions)
		}
	})

	t.Run("refresh_symbols", func(t *testing.T) {
		transport := &captureTransport{responseBody: `{"retCode":0,"retMsg":"OK","result":{}}`}
		conn := newConnectedConnectorForTest(t, transport)

		if err := conn.RefreshSymbols(context.Background()); err != nil {
			t.Fatalf("refresh symbols: %v", err)
		}
	})
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

func TestConnector_ProcessWebSocketMessage_TickerWithSymbolTopic(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, logrus.New())
	ch := make(chan *connector.TickerUpdate, 1)
	conn.tickerUpdates["BTCUSDT"] = ch

	msg := &WebSocketMessage{
		Topic: "tickers.BTCUSDT",
		Data: map[string]interface{}{
			"symbol":    "BTCUSDT",
			"lastPrice": "50000",
			"bid1Price": "49999",
			"ask1Price": "50001",
			"volume24h": "1000",
		},
	}

	conn.processWebSocketMessage(msg)

	select {
	case update := <-ch:
		if update.Symbol != "BTCUSDT" {
			t.Fatalf("expected BTCUSDT update, got %s", update.Symbol)
		}
	case <-time.After(time.Second):
		t.Fatal("expected ticker update")
	}
}

func TestConnector_ProcessWebSocketMessage_PositionArrayPayload(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, logrus.New())

	msg := &WebSocketMessage{
		Topic: "position",
		Data: []interface{}{
			map[string]interface{}{
				"symbol":        "BTCUSDT",
				"side":          "Buy",
				"size":          "0.1",
				"avgPrice":      "50000",
				"unrealisedPnl": "12.5",
			},
		},
	}

	conn.processWebSocketMessage(msg)

	select {
	case update := <-conn.positionUpdates:
		if update.Symbol != "BTCUSDT" {
			t.Fatalf("expected BTCUSDT update, got %s", update.Symbol)
		}
		if !update.Quantity.Equal(decimal.RequireFromString("0.1")) {
			t.Fatalf("expected quantity 0.1, got %s", update.Quantity)
		}
	case <-time.After(time.Second):
		t.Fatal("expected position update")
	}
}

func TestConnector_ProcessWebSocketMessage_DropsMalformedDecimalAndEmitsGap(t *testing.T) {
	cfg := Config{
		APIKey:    "test_key",
		APISecret: "test_secret",
	}

	conn, _ := NewConnector(cfg, logrus.New())
	ch := make(chan *connector.TickerUpdate, 1)
	conn.tickerUpdates["BTCUSDT"] = ch

	msg := &WebSocketMessage{
		Topic: "tickers.BTCUSDT",
		Data: map[string]interface{}{
			"symbol":    "BTCUSDT",
			"lastPrice": "bad-price",
			"bid1Price": "49999",
			"ask1Price": "50001",
			"volume24h": "1000",
		},
	}

	conn.processWebSocketMessage(msg)

	select {
	case <-ch:
		t.Fatal("expected malformed ticker to be dropped")
	default:
	}

	select {
	case event := <-conn.streamEvents:
		if event.Type != connector.StreamEventGap {
			t.Fatalf("expected stream gap, got %s", event.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("expected stream gap event")
	}
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

	if conn.streamEvents == nil {
		t.Error("expected streamEvents channel to be initialized")
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

func newConnectorForTest(t *testing.T) *Connector {
	t.Helper()

	conn, err := NewConnector(Config{APIKey: "test_key", APISecret: "test_secret", Testnet: true}, logrus.New())
	if err != nil {
		t.Fatalf("create connector: %v", err)
	}
	return conn
}

func newConnectedConnectorForTest(t *testing.T, transport http.RoundTripper) *Connector {
	t.Helper()

	conn := newConnectorForTest(t)
	conn.client.baseURL = "https://api.test"
	conn.client.httpClient = &http.Client{Transport: transport}
	conn.connected = true
	return conn
}
