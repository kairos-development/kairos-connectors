package bybit

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/kairos-development/kairos-contracts/connector"
	"github.com/shopspring/decimal"
)

func TestClientSubmitOrder_SendsLimitOrderAndReturnsExchangeID(t *testing.T) {
	transport := &captureTransport{
		responseBody: `{"retCode":0,"retMsg":"OK","result":{"orderId":"exchange-1","orderLinkId":"client-1"}}`,
	}
	client := newTestClient(t, transport)

	exchangeID, err := client.SubmitOrder(context.Background(), &connector.Order{
		ClientOrderID: "client-1",
		Symbol:        "BTCUSDT",
		Side:          connector.OrderSideBuy,
		Type:          connector.OrderTypeLimit,
		TimeInForce:   connector.TimeInForceGTC,
		Quantity:      decimal.RequireFromString("0.1"),
		Price:         decimal.RequireFromString("50000"),
	})
	if err != nil {
		t.Fatalf("submit order: %v", err)
	}
	if exchangeID != "exchange-1" {
		t.Fatalf("expected exchange order ID, got %q", exchangeID)
	}
	if transport.method != http.MethodPost || transport.path != "/v5/order/create" {
		t.Fatalf("unexpected request %s %s", transport.method, transport.path)
	}

	var req OrderRequest
	if err := json.Unmarshal([]byte(transport.body), &req); err != nil {
		t.Fatalf("unmarshal submitted body: %v", err)
	}
	if req.Symbol != "BTCUSDT" || req.Side != "Buy" || req.OrderType != "Limit" || req.Price != "50000" {
		t.Fatalf("unexpected order request: %+v", req)
	}
}

func TestClientSubmitOrder_PropagatesAPIError(t *testing.T) {
	transport := &captureTransport{responseBody: `{"retCode":10001,"retMsg":"invalid order","result":{}}`}
	client := newTestClient(t, transport)

	_, err := client.SubmitOrder(context.Background(), &connector.Order{
		ClientOrderID: "client-1",
		Symbol:        "BTCUSDT",
		Side:          connector.OrderSideSell,
		Type:          connector.OrderTypeMarket,
		TimeInForce:   connector.TimeInForceIOC,
		Quantity:      decimal.RequireFromString("0.1"),
	})
	if err == nil {
		t.Fatal("expected API error")
	}
	if !strings.Contains(err.Error(), "invalid order") {
		t.Fatalf("expected API message in error, got %v", err)
	}
}

func TestClientCancelOrder_SendsCancelRequest(t *testing.T) {
	transport := &captureTransport{responseBody: `{"retCode":0,"retMsg":"OK","result":{}}`}
	client := newTestClient(t, transport)

	if err := client.CancelOrder(context.Background(), "exchange-1"); err != nil {
		t.Fatalf("cancel order: %v", err)
	}
	if transport.method != http.MethodPost || transport.path != "/v5/order/cancel" {
		t.Fatalf("unexpected request %s %s", transport.method, transport.path)
	}
	if !strings.Contains(transport.rawQuery, "orderId=exchange-1") {
		t.Fatalf("expected orderId query, got %q", transport.rawQuery)
	}
}

func TestClientQueryOrder_RejectsMalformedDecimal(t *testing.T) {
	transport := &captureTransport{
		responseBody: `{
			"retCode": 0,
			"retMsg": "OK",
			"result": {
				"list": [{
					"orderId": "exchange-1",
					"orderLinkId": "client-1",
					"symbol": "BTCUSDT",
					"side": "Buy",
					"orderType": "Limit",
					"price": "50000",
					"qty": "bad-qty",
					"cumExecQty": "0",
					"leavesQty": "1",
					"avgPrice": "",
					"orderStatus": "New",
					"timeInForce": "GTC"
				}]
			}
		}`,
	}

	client, err := NewClient(Config{APIKey: "key", APISecret: "secret", Testnet: true})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	client.baseURL = "https://api.test"
	client.httpClient = &http.Client{Transport: transport}

	_, err = client.QueryOrder(context.Background(), "exchange-1")
	if err == nil {
		t.Fatal("expected malformed decimal error")
	}
	if !strings.Contains(err.Error(), "qty") {
		t.Fatalf("expected qty field in error, got %v", err)
	}
}

func TestClientQueryOrder_ReturnsNotFound(t *testing.T) {
	transport := &captureTransport{responseBody: `{"retCode":0,"retMsg":"OK","result":{"list":[]}}`}
	client := newTestClient(t, transport)

	_, err := client.QueryOrder(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected not found error")
	}
	if !strings.Contains(err.Error(), "order not found") {
		t.Fatalf("expected not found error, got %v", err)
	}
}

func TestClientGetOpenOrders_FiltersInactiveOrders(t *testing.T) {
	transport := &captureTransport{
		responseBody: `{
			"retCode":0,
			"retMsg":"OK",
			"result":{"list":[
				{"orderId":"open-1","orderLinkId":"client-1","symbol":"BTCUSDT","side":"Buy","orderType":"Limit","price":"50000","qty":"0.1","cumExecQty":"0","leavesQty":"0.1","avgPrice":"","orderStatus":"New","timeInForce":"GTC"},
				{"orderId":"done-1","orderLinkId":"client-2","symbol":"ETHUSDT","side":"Sell","orderType":"Limit","price":"3000","qty":"1","cumExecQty":"1","leavesQty":"0","avgPrice":"3000","orderStatus":"Filled","timeInForce":"GTC"}
			]}
		}`,
	}
	client := newTestClient(t, transport)

	orders, err := client.GetOpenOrders(context.Background())
	if err != nil {
		t.Fatalf("get open orders: %v", err)
	}
	if len(orders) != 1 || orders[0].ExchangeOrderID != "open-1" {
		t.Fatalf("expected only active open order, got %+v", orders)
	}
}

func TestOrderMappings(t *testing.T) {
	if mapOrderSide(connector.OrderSideBuy) != "Buy" || mapOrderSide(connector.OrderSideSell) != "Sell" {
		t.Fatal("unexpected side mapping")
	}
	if mapOrderType(connector.OrderTypeLimit) != "Limit" || mapOrderType(connector.OrderTypeMarket) != "Market" {
		t.Fatal("unexpected type mapping")
	}
	if mapTimeInForce(connector.TimeInForceIOC) != "IOC" || mapTimeInForce(connector.TimeInForceFOK) != "FOK" {
		t.Fatal("unexpected time-in-force mapping")
	}
	if unmapOrderStatus("PartiallyFilled") != connector.OrderStatusPartial || unmapOrderStatus("Rejected") != connector.OrderStatusRejected {
		t.Fatal("unexpected status mapping")
	}
}

func newTestClient(t *testing.T, transport http.RoundTripper) *Client {
	t.Helper()

	client, err := NewClient(Config{APIKey: "key", APISecret: "secret", Testnet: true})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	client.baseURL = "https://api.test"
	client.httpClient = &http.Client{Transport: transport}
	return client
}
