package bybit

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestClientGetBalance_RejectsMalformedDecimal(t *testing.T) {
	transport := &captureTransport{
		responseBody: `{
			"retCode": 0,
			"retMsg": "OK",
			"result": {
				"list": [{
					"accountType": "UNIFIED",
					"coin": [{
						"coin": "USDT",
						"walletBalance": "bad-balance",
						"availableToWithdraw": "1",
						"locked": "0"
					}]
				}]
			}
		}`,
	}

	client, err := NewClient(Config{APIKey: "key", APISecret: "secret", Testnet: true})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	client.baseURL = "https://api.test"
	client.httpClient = &http.Client{Transport: transport}

	_, err = client.GetBalance(context.Background())
	if err == nil {
		t.Fatal("expected malformed decimal error")
	}
	if !strings.Contains(err.Error(), "walletBalance") {
		t.Fatalf("expected walletBalance field in error, got %v", err)
	}
}

func TestClientGetBalance_SkipsZeroBalances(t *testing.T) {
	transport := &captureTransport{
		responseBody: `{
			"retCode": 0,
			"retMsg": "OK",
			"result": {
				"list": [{
					"accountType": "UNIFIED",
					"coin": [
						{"coin": "USDT", "walletBalance": "0", "availableToWithdraw": "0", "locked": "0"},
						{"coin": "BTC", "walletBalance": "0.5", "availableToWithdraw": "0.4", "locked": "0.1"}
					]
				}]
			}
		}`,
	}

	client, err := NewClient(Config{APIKey: "key", APISecret: "secret", Testnet: true})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	client.baseURL = "https://api.test"
	client.httpClient = &http.Client{Transport: transport}

	balance, err := client.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("get balance: %v", err)
	}
	if len(balance.Balances) != 1 || balance.Balances[0].Asset != "BTC" {
		t.Fatalf("expected only non-zero BTC balance, got %+v", balance.Balances)
	}
}
