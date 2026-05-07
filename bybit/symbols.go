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

// InstrumentResponse represents a symbol/instrument response.
type InstrumentResponse struct {
	Symbol        string `json:"symbol"`
	BaseCoin      string `json:"baseCoin"`
	QuoteCoin     string `json:"quoteCoin"`
	Status        string `json:"status"`
	LotSizeFilter struct {
		MinOrderQty string `json:"minOrderQty"`
		MaxOrderQty string `json:"maxOrderQty"`
		QtyStep     string `json:"qtyStep"`
	} `json:"lotSizeFilter"`
	PriceFilter struct {
		MinPrice string `json:"minPrice"`
		MaxPrice string `json:"maxPrice"`
		TickSize string `json:"tickSize"`
	} `json:"priceFilter"`
}

// GetSymbol retrieves symbol metadata and trading constraints.
func (c *Client) GetSymbol(ctx context.Context, symbol string) (*connector.Symbol, error) {
	params := url.Values{}
	params.Set("category", "linear")
	params.Set("symbol", symbol)

	resp, err := c.doRequest(ctx, http.MethodGet, "/market/instruments-info", params, nil)
	if err != nil {
		return nil, fmt.Errorf("get symbol: %w", err)
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
		List []InstrumentResponse `json:"list"`
	}
	if err := json.Unmarshal(apiResp.Result, &result); err != nil {
		return nil, fmt.Errorf("unmarshal symbol response: %w", err)
	}

	if len(result.List) == 0 {
		return nil, fmt.Errorf("symbol not found: %s", symbol)
	}

	instResp := result.List[0]

	// Map to domain symbol
	sym := &connector.Symbol{
		Name:          instResp.Symbol,
		BaseCurrency:  instResp.BaseCoin,
		QuoteCurrency: instResp.QuoteCoin,
		Status:        unmapSymbolStatus(instResp.Status),
		UpdatedAtUTC:  time.Now().UTC(),
	}

	sym.MinOrderQty, _ = decimal.NewFromString(instResp.LotSizeFilter.MinOrderQty)
	sym.MaxOrderQty, _ = decimal.NewFromString(instResp.LotSizeFilter.MaxOrderQty)
	sym.StepSize, _ = decimal.NewFromString(instResp.LotSizeFilter.QtyStep)

	sym.MinPrice, _ = decimal.NewFromString(instResp.PriceFilter.MinPrice)
	sym.MaxPrice, _ = decimal.NewFromString(instResp.PriceFilter.MaxPrice)
	sym.TickSize, _ = decimal.NewFromString(instResp.PriceFilter.TickSize)

	// Default values for fees and min notional
	sym.MakerFee = decimal.NewFromFloat(0.0001) // 0.01%
	sym.TakerFee = decimal.NewFromFloat(0.0006) // 0.06%
	sym.MinNotional = decimal.NewFromInt(10)    // $10 minimum

	return sym, nil
}

// RefreshSymbols updates all symbol metadata from the exchange.
func (c *Client) RefreshSymbols(ctx context.Context) error {
	params := url.Values{}
	params.Set("category", "linear")

	resp, err := c.doRequest(ctx, http.MethodGet, "/market/instruments-info", params, nil)
	if err != nil {
		return fmt.Errorf("refresh symbols: %w", err)
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

	// Successfully refreshed symbols
	// In a full implementation, this would update a local cache
	return nil
}

func unmapSymbolStatus(status string) connector.SymbolStatus {
	switch status {
	case "Trading":
		return connector.SymbolStatusTrading
	case "Closed":
		return connector.SymbolStatusSuspended
	case "Delivering":
		return connector.SymbolStatusMaintenance
	default:
		return connector.SymbolStatusSuspended
	}
}
