package bybit

import (
	"context"
	"fmt"

	"github.com/kairos-development/kairos-contracts/connector"
)

// SubscribeOrders is unsupported on the low-level REST client.
// Use Connector.SubscribeOrders, which owns the WebSocket runtime.
func (c *Client) SubscribeOrders(ctx context.Context) (<-chan *connector.OrderUpdate, error) {
	return nil, fmt.Errorf("websocket subscriptions are available through Connector")
}

// SubscribePositions is unsupported on the low-level REST client.
// Use Connector.SubscribePositions, which owns the WebSocket runtime.
func (c *Client) SubscribePositions(ctx context.Context) (<-chan *connector.PositionUpdate, error) {
	return nil, fmt.Errorf("websocket subscriptions are available through Connector")
}

// SubscribeBalance is unsupported on the low-level REST client.
// Use Connector.SubscribeBalance, which owns the WebSocket runtime.
func (c *Client) SubscribeBalance(ctx context.Context) (<-chan *connector.BalanceUpdate, error) {
	return nil, fmt.Errorf("websocket subscriptions are available through Connector")
}

// SubscribeTicker is unsupported on the low-level REST client.
// Use Connector.SubscribeTicker, which owns the WebSocket runtime.
func (c *Client) SubscribeTicker(ctx context.Context, symbol string) (<-chan *connector.TickerUpdate, error) {
	return nil, fmt.Errorf("websocket subscriptions are available through Connector")
}
