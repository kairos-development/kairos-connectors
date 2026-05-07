package bybit

import (
	"context"
	"sync"
	"time"
)

// RateLimiter implements token bucket rate limiting for API requests.
type RateLimiter struct {
	mu           sync.Mutex
	tokens       int
	maxTokens    int
	refillRate   int // tokens per second
	lastRefill   time.Time
	refillTicker *time.Ticker
	stopChan     chan struct{}
	stopOnce     sync.Once
}

// NewRateLimiter creates a new rate limiter with the specified rate.
func NewRateLimiter(requestsPerSecond int) *RateLimiter {
	rl := &RateLimiter{
		tokens:     requestsPerSecond,
		maxTokens:  requestsPerSecond,
		refillRate: requestsPerSecond,
		lastRefill: time.Now(),
		stopChan:   make(chan struct{}),
	}

	// Start refill goroutine
	rl.refillTicker = time.NewTicker(time.Second)
	go rl.refillLoop()

	return rl
}

// Wait blocks until a token is available or context is canceled.
func (rl *RateLimiter) Wait(ctx context.Context) error {
	for {
		rl.mu.Lock()
		if rl.tokens > 0 {
			rl.tokens--
			rl.mu.Unlock()
			return nil
		}
		rl.mu.Unlock()

		// Wait for refill or context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Retry
		}
	}
}

// refillLoop periodically refills tokens.
func (rl *RateLimiter) refillLoop() {
	for {
		select {
		case <-rl.refillTicker.C:
			rl.mu.Lock()
			rl.tokens = rl.maxTokens
			rl.lastRefill = time.Now()
			rl.mu.Unlock()
		case <-rl.stopChan:
			return
		}
	}
}

// Stop stops the rate limiter.
func (rl *RateLimiter) Stop() {
	rl.stopOnce.Do(func() {
		close(rl.stopChan)
	})
	rl.refillTicker.Stop()
}

// Tokens returns the current number of available tokens.
func (rl *RateLimiter) Tokens() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.tokens
}
