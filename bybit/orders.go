package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/kairos-development/kairos-contracts/connector"
	"github.com/shopspring/decimal"
)

// OrderRequest represents an order placement request.
type OrderRequest struct {
	Category    string `json:"category"`
	Symbol      string `json:"symbol"`
	Side        string `json:"side"`
	OrderType   string `json:"orderType"`
	Qty         string `json:"qty"`
	Price       string `json:"price,omitempty"`
	TimeInForce string `json:"timeInForce"`
	OrderLinkId string `json:"orderLinkId"`
}

// OrderResponse represents an order placement response.
type OrderResponse struct {
	OrderID     string `json:"orderId"`
	OrderLinkID string `json:"orderLinkId"`
}

// SubmitOrder sends an order to the exchange.
func (c *Client) SubmitOrder(ctx context.Context, order *connector.Order) (string, error) {
	// Map domain order to Bybit request
	req := OrderRequest{
		Category:    "linear", // USDT perpetual
		Symbol:      order.Symbol,
		Side:        mapOrderSide(order.Side),
		OrderType:   mapOrderType(order.Type),
		Qty:         order.Quantity.String(),
		TimeInForce: mapTimeInForce(order.TimeInForce),
		OrderLinkId: order.ClientOrderID,
	}

	if order.Type == connector.OrderTypeLimit {
		req.Price = order.Price.String()
	}

	// Marshal request
	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	// Send request
	resp, err := c.doRequest(ctx, http.MethodPost, "/order/create", url.Values{}, body)
	if err != nil {
		return "", fmt.Errorf("submit order: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	var apiResp APIResponse
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return "", fmt.Errorf("unmarshal response: %w", err)
	}

	if apiResp.RetCode != 0 {
		return "", fmt.Errorf("API error: %s (code: %d)", apiResp.RetMsg, apiResp.RetCode)
	}

	var orderResp OrderResponse
	if err := json.Unmarshal(apiResp.Result, &orderResp); err != nil {
		return "", fmt.Errorf("unmarshal order response: %w", err)
	}

	return orderResp.OrderID, nil
}

// CancelOrder cancels an active order on the exchange.
func (c *Client) CancelOrder(ctx context.Context, orderID string) error {
	params := url.Values{}
	params.Set("category", "linear")
	params.Set("orderId", orderID)

	resp, err := c.doRequest(ctx, http.MethodPost, "/order/cancel", params, nil)
	if err != nil {
		return fmt.Errorf("cancel order: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	var apiResp APIResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}

	if apiResp.RetCode != 0 {
		return fmt.Errorf("API error: %s (code: %d)", apiResp.RetMsg, apiResp.RetCode)
	}

	return nil
}

// QueryOrderResponse represents an order query response.
type QueryOrderResponse struct {
	OrderID     string `json:"orderId"`
	OrderLinkID string `json:"orderLinkId"`
	Symbol      string `json:"symbol"`
	Side        string `json:"side"`
	OrderType   string `json:"orderType"`
	Price       string `json:"price"`
	Qty         string `json:"qty"`
	CumExecQty  string `json:"cumExecQty"`
	LeavesQty   string `json:"leavesQty"`
	AvgPrice    string `json:"avgPrice"`
	OrderStatus string `json:"orderStatus"`
	TimeInForce string `json:"timeInForce"`
	CreatedTime string `json:"createdTime"`
	UpdatedTime string `json:"updatedTime"`
}

// QueryOrder retrieves the current status of an order from the exchange.
func (c *Client) QueryOrder(ctx context.Context, orderID string) (*connector.Order, error) {
	params := url.Values{}
	params.Set("category", "linear")
	params.Set("orderId", orderID)

	resp, err := c.doRequest(ctx, http.MethodGet, "/order/realtime", params, nil)
	if err != nil {
		return nil, fmt.Errorf("query order: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var apiResp APIResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	if apiResp.RetCode != 0 {
		return nil, fmt.Errorf("API error: %s (code: %d)", apiResp.RetMsg, apiResp.RetCode)
	}

	var result struct {
		List []QueryOrderResponse `json:"list"`
	}
	if err := json.Unmarshal(apiResp.Result, &result); err != nil {
		return nil, fmt.Errorf("unmarshal order response: %w", err)
	}

	if len(result.List) == 0 {
		return nil, fmt.Errorf("order not found")
	}

	orderResp := result.List[0]

	// Map to domain order
	order := &connector.Order{
		ID:              orderID,
		ClientOrderID:   orderResp.OrderLinkID,
		ExchangeOrderID: orderResp.OrderID,
		Symbol:          orderResp.Symbol,
		Side:            unmapOrderSide(orderResp.Side),
		Type:            unmapOrderType(orderResp.OrderType),
		Status:          unmapOrderStatus(orderResp.OrderStatus),
		TimeInForce:     unmapTimeInForce(orderResp.TimeInForce),
		UpdatedAtUTC:    time.Now().UTC(),
	}

	order.Quantity, _ = decimal.NewFromString(orderResp.Qty)
	order.Price, _ = decimal.NewFromString(orderResp.Price)
	order.FilledQty, _ = decimal.NewFromString(orderResp.CumExecQty)
	order.RemainingQty, _ = decimal.NewFromString(orderResp.LeavesQty)
	order.AvgFillPrice, _ = decimal.NewFromString(orderResp.AvgPrice)

	return order, nil
}

// Mapping functions
func mapOrderSide(side connector.OrderSide) string {
	if side == connector.OrderSideBuy {
		return "Buy"
	}
	return "Sell"
}

func unmapOrderSide(side string) connector.OrderSide {
	if side == "Buy" {
		return connector.OrderSideBuy
	}
	return connector.OrderSideSell
}

func mapOrderType(orderType connector.OrderType) string {
	if orderType == connector.OrderTypeLimit {
		return "Limit"
	}
	return "Market"
}

func unmapOrderType(orderType string) connector.OrderType {
	if orderType == "Limit" {
		return connector.OrderTypeLimit
	}
	return connector.OrderTypeMarket
}

func mapTimeInForce(tif connector.TimeInForce) string {
	switch tif {
	case connector.TimeInForceGTC:
		return "GTC"
	case connector.TimeInForceIOC:
		return "IOC"
	case connector.TimeInForceFOK:
		return "FOK"
	default:
		return "GTC"
	}
}

func unmapTimeInForce(tif string) connector.TimeInForce {
	switch tif {
	case "GTC":
		return connector.TimeInForceGTC
	case "IOC":
		return connector.TimeInForceIOC
	case "FOK":
		return connector.TimeInForceFOK
	default:
		return connector.TimeInForceGTC
	}
}
