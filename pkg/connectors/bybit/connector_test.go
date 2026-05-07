package bybit

import (
	"context"
	"net"
	"testing"
	"time"
)

type stubResolver struct {
	calls int
}

func (r *stubResolver) LookupIPAddr(_ context.Context, _ string) ([]net.IPAddr, error) {
	r.calls++
	return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
}

func TestValidatePermissions(t *testing.T) {
	if _, err := ValidatePermissions(Permissions{CanTrade: false}); err == nil {
		t.Fatal("expected missing trade permission error")
	}
	result, err := ValidatePermissions(Permissions{CanTrade: true, CanWithdraw: true})
	if err == nil || !result.RequiresAck {
		t.Fatal("expected over-privileged permission error")
	}
}

func TestDNSCacheResolve(t *testing.T) {
	resolver := &stubResolver{}
	cache := NewDNSCache(resolver, time.Minute)
	if _, err := cache.Resolve(context.Background(), "api.bybit.com"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := cache.Resolve(context.Background(), "api.bybit.com"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolver.calls != 1 {
		t.Fatalf("expected one resolver call, got %d", resolver.calls)
	}
}
