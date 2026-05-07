package bybit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const (
	maxDNSCacheTTL = time.Minute
)

var (
	// ErrPermissionsTradeRequired reports a key missing trade permission.
	ErrPermissionsTradeRequired = errors.New("bybit key is missing trade permission")
	// ErrPermissionsWithdrawForbidden reports a key with prohibited withdrawal or transfer permission.
	ErrPermissionsWithdrawForbidden = errors.New("bybit key grants withdraw or transfer permission")
)

// ProxyConfig configures outbound proxy routing.
type ProxyConfig struct {
	SOCKS5 string `yaml:"socks5" json:"socks5"`
	HTTP   string `yaml:"http" json:"http"`
}

// URL returns the highest-priority configured proxy URL.
func (c ProxyConfig) URL() (*url.URL, error) {
	if c.SOCKS5 != "" {
		return url.Parse(c.SOCKS5)
	}
	if c.HTTP != "" {
		return url.Parse(c.HTTP)
	}
	return nil, nil
}

// Permissions models the effective Bybit API key permission set.
type Permissions struct {
	CanTrade    bool
	CanWithdraw bool
	CanTransfer bool
}

// ValidationResult describes the outcome of API key validation.
type ValidationResult struct {
	Critical    bool
	RequiresAck bool
	Message     string
}

// ValidatePermissions validates the exchange permission envelope.
func ValidatePermissions(p Permissions) (ValidationResult, error) {
	if !p.CanTrade {
		return ValidationResult{Critical: true, Message: ErrPermissionsTradeRequired.Error()}, ErrPermissionsTradeRequired
	}
	if p.CanWithdraw || p.CanTransfer {
		return ValidationResult{Critical: true, RequiresAck: true, Message: ErrPermissionsWithdrawForbidden.Error()}, ErrPermissionsWithdrawForbidden
	}
	return ValidationResult{Message: "permissions valid"}, nil
}

// Resolver resolves hostnames for the connector.
type Resolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

type cacheEntry struct {
	ips       []net.IPAddr
	expiresAt time.Time
}

// DNSCache is a TTL-bounded connector DNS cache.
type DNSCache struct {
	resolver Resolver
	ttl      time.Duration
	mu       sync.Mutex
	entries  map[string]cacheEntry
}

// NewDNSCache constructs a bounded connector DNS cache.
func NewDNSCache(resolver Resolver, ttl time.Duration) *DNSCache {
	if ttl <= 0 || ttl > maxDNSCacheTTL {
		ttl = maxDNSCacheTTL
	}
	return &DNSCache{resolver: resolver, ttl: ttl, entries: make(map[string]cacheEntry)}
}

// Resolve resolves and caches a hostname.
func (c *DNSCache) Resolve(ctx context.Context, host string) ([]net.IPAddr, error) {
	c.mu.Lock()
	entry, ok := c.entries[host]
	if ok && time.Now().Before(entry.expiresAt) {
		ips := append([]net.IPAddr(nil), entry.ips...)
		c.mu.Unlock()
		return ips, nil
	}
	c.mu.Unlock()

	ips, err := c.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.entries[host] = cacheEntry{ips: append([]net.IPAddr(nil), ips...), expiresAt: time.Now().Add(c.ttl)}
	c.mu.Unlock()
	return append([]net.IPAddr(nil), ips...), nil
}

// Client is the baseline Bybit connector scaffold.
type Client struct {
	HTTPClient *http.Client
	Proxy      ProxyConfig
	Resolver   *DNSCache
}

// NewClient constructs a baseline HTTP client configured for proxy routing.
func NewClient(proxy ProxyConfig, resolver Resolver) (*Client, error) {
	proxyURL, err := proxy.URL()
	if err != nil {
		return nil, fmt.Errorf("parse proxy url: %w", err)
	}
	transport := &http.Transport{}
	if proxyURL != nil {
		transport.Proxy = http.ProxyURL(proxyURL)
	}
	return &Client{
		HTTPClient: &http.Client{Transport: transport, Timeout: 15 * time.Second},
		Proxy:      proxy,
		Resolver:   NewDNSCache(resolver, maxDNSCacheTTL),
	}, nil
}
