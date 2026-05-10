package bybit

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/kairos-development/kairos-contracts/connector"
	"github.com/shopspring/decimal"
)

func TestClientGetSymbol_MapsMetadataWithExactDefaultFees(t *testing.T) {
	transport := &captureTransport{
		responseBody: `{
			"retCode": 0,
			"retMsg": "OK",
			"result": {
				"list": [{
					"symbol": "BTCUSDT",
					"baseCoin": "BTC",
					"quoteCoin": "USDT",
					"status": "Trading",
					"lotSizeFilter": {"minOrderQty": "0.001", "maxOrderQty": "100", "qtyStep": "0.001"},
					"priceFilter": {"minPrice": "1", "maxPrice": "1000000", "tickSize": "0.1"}
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

	symbol, err := client.GetSymbol(context.Background(), "BTCUSDT")
	if err != nil {
		t.Fatalf("get symbol: %v", err)
	}

	if symbol.Status != connector.SymbolStatusTrading {
		t.Fatalf("expected trading status, got %s", symbol.Status)
	}
	if !symbol.MakerFee.Equal(decimal.RequireFromString("0.0001")) {
		t.Fatalf("expected exact maker fee, got %s", symbol.MakerFee)
	}
	if !symbol.TakerFee.Equal(decimal.RequireFromString("0.0006")) {
		t.Fatalf("expected exact taker fee, got %s", symbol.TakerFee)
	}
}

func TestClientGetSymbol_RejectsMalformedDecimal(t *testing.T) {
	transport := &captureTransport{
		responseBody: `{
			"retCode": 0,
			"retMsg": "OK",
			"result": {
				"list": [{
					"symbol": "BTCUSDT",
					"baseCoin": "BTC",
					"quoteCoin": "USDT",
					"status": "Trading",
					"lotSizeFilter": {"minOrderQty": "bad-min", "maxOrderQty": "100", "qtyStep": "0.001"},
					"priceFilter": {"minPrice": "1", "maxPrice": "1000000", "tickSize": "0.1"}
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

	_, err = client.GetSymbol(context.Background(), "BTCUSDT")
	if err == nil {
		t.Fatal("expected malformed decimal error")
	}
	if !strings.Contains(err.Error(), "minOrderQty") {
		t.Fatalf("expected minOrderQty field in error, got %v", err)
	}
}

func TestClientRefreshSymbols_HandlesSuccessAndAPIError(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		transport := &captureTransport{responseBody: `{"retCode":0,"retMsg":"OK","result":{}}`}
		client := newTestClient(t, transport)

		if err := client.RefreshSymbols(context.Background()); err != nil {
			t.Fatalf("refresh symbols: %v", err)
		}
		if transport.path != "/v5/market/instruments-info" {
			t.Fatalf("unexpected path %q", transport.path)
		}
	})

	t.Run("api_error", func(t *testing.T) {
		transport := &captureTransport{responseBody: `{"retCode":110001,"retMsg":"symbol unavailable","result":{}}`}
		client := newTestClient(t, transport)

		err := client.RefreshSymbols(context.Background())
		if err == nil {
			t.Fatal("expected API error")
		}
		if !strings.Contains(err.Error(), "symbol unavailable") {
			t.Fatalf("expected API message in error, got %v", err)
		}
	})
}

func TestUnmapSymbolStatus(t *testing.T) {
	if unmapSymbolStatus("Closed") != connector.SymbolStatusSuspended {
		t.Fatal("expected closed status to map to suspended")
	}
	if unmapSymbolStatus("Delivering") != connector.SymbolStatusMaintenance {
		t.Fatal("expected delivering status to map to maintenance")
	}
	if unmapSymbolStatus("Unknown") != connector.SymbolStatusSuspended {
		t.Fatal("expected unknown status to fail safe as suspended")
	}
}
