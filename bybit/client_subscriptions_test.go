package bybit

import (
	"context"
	"strings"
	"testing"
)

func TestClientSubscriptionMethodsReturnExplicitConnectorOnlyError(t *testing.T) {
	client, err := NewClient(Config{APIKey: "key", APISecret: "secret", Testnet: true})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	checks := []struct {
		name string
		fn   func() error
	}{
		{name: "orders", fn: func() error { _, err := client.SubscribeOrders(context.Background()); return err }},
		{name: "positions", fn: func() error { _, err := client.SubscribePositions(context.Background()); return err }},
		{name: "balance", fn: func() error { _, err := client.SubscribeBalance(context.Background()); return err }},
		{name: "ticker", fn: func() error { _, err := client.SubscribeTicker(context.Background(), "BTCUSDT"); return err }},
	}

	for _, tt := range checks {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if err == nil {
				t.Fatal("expected explicit unsupported error")
			}
			if !strings.Contains(err.Error(), "Connector") {
				t.Fatalf("expected Connector guidance in error, got %v", err)
			}
		})
	}
}
