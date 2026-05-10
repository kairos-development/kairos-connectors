package bybit

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestClientGetPosition_ReturnsFlatWhenMissing(t *testing.T) {
	transport := &captureTransport{
		responseBody: `{"retCode":0,"retMsg":"OK","result":{"list":[]}}`,
	}

	client, err := NewClient(Config{APIKey: "key", APISecret: "secret", Testnet: true})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	client.baseURL = "https://api.test"
	client.httpClient = &http.Client{Transport: transport}

	position, err := client.GetPosition(context.Background(), "BTCUSDT")
	if err != nil {
		t.Fatalf("get position: %v", err)
	}
	if position.Symbol != "BTCUSDT" || !position.Quantity.IsZero() {
		t.Fatalf("expected flat BTCUSDT position, got %+v", position)
	}
}

func TestClientGetPositions_RejectsMalformedDecimal(t *testing.T) {
	transport := &captureTransport{
		responseBody: `{
			"retCode": 0,
			"retMsg": "OK",
			"result": {
				"list": [{
					"symbol": "BTCUSDT",
					"side": "Buy",
					"size": "1",
					"avgPrice": "bad-entry",
					"markPrice": "50100",
					"unrealisedPnl": "1",
					"cumRealisedPnl": "0"
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

	_, err = client.GetPositions(context.Background())
	if err == nil {
		t.Fatal("expected malformed decimal error")
	}
	if !strings.Contains(err.Error(), "avgPrice") {
		t.Fatalf("expected avgPrice field in error, got %v", err)
	}
}
