package bybit

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRateLimiter_DefaultsNonPositiveRateAndReportsTokens(t *testing.T) {
	rl := NewRateLimiter(0)
	defer rl.Stop()

	if got := rl.Tokens(); got != 1 {
		t.Fatalf("expected one default token, got %d", got)
	}
	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := rl.Tokens(); got != 0 {
		t.Fatalf("expected token to be consumed, got %d", got)
	}
}

func TestRateLimiter_WaitReturnsWhenStopped(t *testing.T) {
	rl := NewRateLimiter(1)
	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("consume initial token: %v", err)
	}

	rl.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := rl.Wait(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled after stop, got %v", err)
	}
}

func TestRateLimiter_WaitRespectsContextCancellation(t *testing.T) {
	rl := NewRateLimiter(1)
	defer rl.Stop()

	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("consume initial token: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := rl.Wait(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}
