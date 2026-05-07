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

// GetOpenOrders retrieves all open orders for the account.
func (c *Client) GetOpenOrders(ctx context.Context) ([]*connector.Order, error) {
	params := url.Values{}
	params.Set("category", "linear")
	params.Set("openOnly", "0") // Get all active orders

	resp, err := c.doRequest(ctx, http.MethodGet, "/order/realtime", params, nil)
	if err != nil {
		return nil, fmt.Errorf("get open orders: %w", err)
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
		return nil, fmt.Errorf("unmarshal orders response: %w", err)
	}

	orders := make([]*connector.Order, 0, len(result.List))

	for _, orderResp := range result.List {
		// Only include active orders
		status := unmapOrderStatus(orderResp.OrderStatus)
		if status != connector.OrderStatusSubmitted && status != connector.OrderStatusPartial {
			continue
		}

		order := &connector.Order{
			ExchangeOrderID: orderResp.OrderID,
			ClientOrderID:   orderResp.OrderLinkID,
			Symbol:          orderResp.Symbol,
			Side:            unmapOrderSide(orderResp.Side),
			Type:            unmapOrderType(orderResp.OrderType),
			Status:          status,
			TimeInForce:     unmapTimeInForce(orderResp.TimeInForce),
			UpdatedAtUTC:    time.Now().UTC(),
		}

		order.Quantity, _ = decimal.NewFromString(orderResp.Qty)
		order.Price, _ = decimal.NewFromString(orderResp.Price)
		order.FilledQty, _ = decimal.NewFromString(orderResp.CumExecQty)
		order.RemainingQty, _ = decimal.NewFromString(orderResp.LeavesQty)
		order.AvgFillPrice, _ = decimal.NewFromString(orderResp.AvgPrice)

		orders = append(orders, order)
	}

	return orders, nil
}
