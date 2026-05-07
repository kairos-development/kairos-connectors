package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/kairos-development/kairos-contracts/connector"
	"github.com/shopspring/decimal"
)

// GetPositions retrieves all positions for the account.
func (c *Client) GetPositions(ctx context.Context) ([]*connector.Position, error) {
	params := url.Values{}
	params.Set("category", "linear")

	resp, err := c.doRequest(ctx, http.MethodGet, "/position/list", params, nil)
	if err != nil {
		return nil, fmt.Errorf("get positions: %w", err)
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
		List []PositionResponse `json:"list"`
	}
	if err := json.Unmarshal(apiResp.Result, &result); err != nil {
		return nil, fmt.Errorf("unmarshal positions response: %w", err)
	}

	positions := make([]*connector.Position, 0, len(result.List))

	for _, posResp := range result.List {
		qty, _ := decimal.NewFromString(posResp.Size)

		// Skip flat positions
		if qty.IsZero() {
			continue
		}

		position := &connector.Position{
			Symbol: posResp.Symbol,
			Side:   unmapPositionSide(posResp.Side),
		}

		position.Quantity = qty
		position.EntryPrice, _ = decimal.NewFromString(posResp.EntryPrice)
		position.CurrentPrice, _ = decimal.NewFromString(posResp.MarkPrice)
		position.UnrealizedPnL, _ = decimal.NewFromString(posResp.UnrealisedPnl)
		position.RealizedPnL, _ = decimal.NewFromString(posResp.CumRealisedPnl)

		positions = append(positions, position)
	}

	return positions, nil
}
