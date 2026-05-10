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
)

// BalanceResponse represents a balance query response.
type BalanceResponse struct {
	AccountType string `json:"accountType"`
	Coin        []struct {
		Coin             string `json:"coin"`
		WalletBalance    string `json:"walletBalance"`
		AvailableBalance string `json:"availableToWithdraw"`
		Locked           string `json:"locked"`
	} `json:"coin"`
}

// GetBalance retrieves the current account balance.
func (c *Client) GetBalance(ctx context.Context) (*connector.AccountBalance, error) {
	params := url.Values{}
	params.Set("accountType", "UNIFIED")

	resp, err := c.doRequest(ctx, http.MethodGet, "/account/wallet-balance", params, nil)
	if err != nil {
		return nil, fmt.Errorf("get balance: %w", err)
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
		List []BalanceResponse `json:"list"`
	}
	if err := json.Unmarshal(apiResp.Result, &result); err != nil {
		return nil, fmt.Errorf("unmarshal balance response: %w", err)
	}

	if len(result.List) == 0 {
		return &connector.AccountBalance{
			Balances:     []connector.Balance{},
			UpdatedAtUTC: time.Now().UTC(),
		}, nil
	}

	balResp := result.List[0]
	balances := make([]connector.Balance, 0, len(balResp.Coin))

	for _, coin := range balResp.Coin {
		total, err := parseRequiredDecimal("walletBalance", coin.WalletBalance)
		if err != nil {
			return nil, err
		}
		available, err := parseOptionalDecimal("availableToWithdraw", coin.AvailableBalance)
		if err != nil {
			return nil, err
		}
		locked, err := parseOptionalDecimal("locked", coin.Locked)
		if err != nil {
			return nil, err
		}

		// Skip zero balances
		if total.IsZero() {
			continue
		}

		balances = append(balances, connector.Balance{
			Asset:        coin.Coin,
			Total:        total,
			Available:    available,
			Locked:       locked,
			UpdatedAtUTC: time.Now().UTC(),
		})
	}

	return &connector.AccountBalance{
		Balances:     balances,
		UpdatedAtUTC: time.Now().UTC(),
	}, nil
}
