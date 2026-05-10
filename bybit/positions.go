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

// PositionResponse represents a position query response.
type PositionResponse struct {
	Symbol         string `json:"symbol"`
	Side           string `json:"side"`
	Size           string `json:"size"`
	PositionValue  string `json:"positionValue"`
	EntryPrice     string `json:"avgPrice"`
	MarkPrice      string `json:"markPrice"`
	UnrealisedPnl  string `json:"unrealisedPnl"`
	CumRealisedPnl string `json:"cumRealisedPnl"`
	CreatedTime    string `json:"createdTime"`
	UpdatedTime    string `json:"updatedTime"`
}

// GetPosition retrieves the current position for a symbol.
func (c *Client) GetPosition(ctx context.Context, symbol string) (*connector.Position, error) {
	params := url.Values{}
	params.Set("category", "linear")
	params.Set("symbol", symbol)

	resp, err := c.doRequest(ctx, http.MethodGet, "/position/list", params, nil)
	if err != nil {
		return nil, fmt.Errorf("get position: %w", err)
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
		return nil, fmt.Errorf("unmarshal position response: %w", err)
	}

	if len(result.List) == 0 {
		// No position found, return flat position
		return &connector.Position{
			Symbol:   symbol,
			Side:     connector.PositionSideFlat,
			Quantity: decimal.Zero,
		}, nil
	}

	posResp := result.List[0]

	// Map to domain position
	position := &connector.Position{
		Symbol: posResp.Symbol,
		Side:   unmapPositionSide(posResp.Side),
	}

	if position.Quantity, err = parseRequiredDecimal("size", posResp.Size); err != nil {
		return nil, err
	}
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

	return position, nil
}
