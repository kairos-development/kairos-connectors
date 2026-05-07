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

// APIResponse represents a standard Bybit API response.
type APIResponse struct {
	RetCode int             `json:"retCode"`
	RetMsg  string          `json:"retMsg"`
	Result  json.RawMessage `json:"result"`
	Time    int64           `json:"time"`
}

// PermissionsResponse represents the API key permissions response.
type PermissionsResponse struct {
	ID          string `json:"id"`
	Note        string `json:"note"`
	APIKey      string `json:"apiKey"`
	ReadOnly    int    `json:"readOnly"`
	Secret      string `json:"secret"`
	Permissions struct {
		ContractTrade []string `json:"ContractTrade"`
		Spot          []string `json:"Spot"`
		Wallet        []string `json:"Wallet"`
		Options       []string `json:"Options"`
		Derivatives   []string `json:"Derivatives"`
		Exchange      []string `json:"Exchange"`
	} `json:"permissions"`
	IPs           []string `json:"ips"`
	Type          int      `json:"type"`
	DeadlineAt    int64    `json:"deadlineDay"`
	ExpiredAt     int64    `json:"expiredAt"`
	CreatedAt     int64    `json:"createdAt"`
	Unified       int      `json:"unified"`
	UTA           int      `json:"uta"`
	UserID        int      `json:"userID"`
	InviterID     int      `json:"inviterID"`
	VipLevel      string   `json:"vipLevel"`
	MktMakerLevel string   `json:"mktMakerLevel"`
	AffiliateID   int      `json:"affiliateID"`
}

// CheckPermissions verifies API key permissions.
func (c *Client) CheckPermissions(ctx context.Context) (*connector.Permissions, error) {
	params := url.Values{}

	resp, err := c.doRequest(ctx, http.MethodGet, "/user/query-api", params, nil)
	if err != nil {
		return nil, fmt.Errorf("request permissions: %w", err)
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

	var permResp PermissionsResponse
	if err := json.Unmarshal(apiResp.Result, &permResp); err != nil {
		return nil, fmt.Errorf("unmarshal permissions: %w", err)
	}

	// Parse permissions
	perms := &connector.Permissions{
		CanRead:     permResp.ReadOnly == 0, // ReadOnly=0 means full access
		CanTrade:    false,
		HasWithdraw: false,
		HasTransfer: false,
	}

	// Check for trade permissions
	if len(permResp.Permissions.ContractTrade) > 0 ||
		len(permResp.Permissions.Spot) > 0 ||
		len(permResp.Permissions.Derivatives) > 0 {
		perms.CanTrade = true
	}

	// Check for dangerous permissions
	for _, perm := range permResp.Permissions.Wallet {
		if perm == "AccountTransfer" {
			perms.HasTransfer = true
		}
		if perm == "Withdraw" {
			perms.HasWithdraw = true
		}
	}

	return perms, nil
}
