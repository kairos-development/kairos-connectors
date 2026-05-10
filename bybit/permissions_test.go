package bybit

import (
	"context"
	"net/http"
	"testing"
)

func TestClientCheckPermissions_DetectsTradeAndDangerousScopes(t *testing.T) {
	transport := &captureTransport{
		responseBody: `{
			"retCode": 0,
			"retMsg": "OK",
			"result": {
				"readOnly": 0,
				"permissions": {
					"ContractTrade": ["Order"],
					"Spot": [],
					"Wallet": ["AccountTransfer", "Withdraw"],
					"Options": [],
					"Derivatives": [],
					"Exchange": []
				}
			}
		}`,
	}

	client, err := NewClient(Config{APIKey: "key", APISecret: "secret", Testnet: true})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	client.baseURL = "https://api.test"
	client.httpClient = &http.Client{Transport: transport}

	permissions, err := client.CheckPermissions(context.Background())
	if err != nil {
		t.Fatalf("check permissions: %v", err)
	}

	if !permissions.CanRead || !permissions.CanTrade {
		t.Fatalf("expected read and trade permissions, got %+v", permissions)
	}
	if !permissions.HasTransfer || !permissions.HasWithdraw {
		t.Fatalf("expected dangerous wallet permissions to be detected, got %+v", permissions)
	}
}

func TestClientCheckPermissions_PropagatesAPIError(t *testing.T) {
	transport := &captureTransport{
		responseBody: `{"retCode": 10003, "retMsg": "invalid api key", "result": {}}`,
	}

	client, err := NewClient(Config{APIKey: "key", APISecret: "secret", Testnet: true})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	client.baseURL = "https://api.test"
	client.httpClient = &http.Client{Transport: transport}

	if _, err := client.CheckPermissions(context.Background()); err == nil {
		t.Fatal("expected API error")
	}
}
