package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/kairos-development/kairos-contracts/connector"
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
		qty, err := parseRequiredDecimal("size", posResp.Size)
		if err != nil {
			return nil, err
		}

		// Skip flat positions
		if qty.IsZero() {
			continue
		}

		position := &connector.Position{
			Symbol: posResp.Symbol,
			Side:   unmapPositionSide(posResp.Side),
		}

		position.Quantity = qty
		if position.EntryPrice, err = parseOptionalDecimal("avgPrice", posResp.EntryPrice); err != nil {
			return nil, err
		}
		if position.CurrentPrice, err = parseOptionalDecimal("markPrice", posResp.MarkPrice); err != nil {
			return nil, err
		}
		if position.UnrealizedPnL, err = parseOptionalDecimal("unrealisedPnl", posResp.UnrealisedPnl); err != nil {
			return nil, err
		}
		if position.RealizedPnL, err = parseOptionalDecimal("cumRealisedPnl", posResp.CumRealisedPnl); err != nil {
			return nil, err
		}

		positions = append(positions, position)
	}

	return positions, nil
}
