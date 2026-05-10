package bybit

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	// Bybit API endpoints
	mainnetBaseURL = "https://api.bybit.com"
	testnetBaseURL = "https://api-testnet.bybit.com"

	// API version
	apiVersion = "/v5"

	// Rate limits (requests per second)
	defaultRateLimit = 10
)

// Client implements the connector.Connector interface for Bybit.
type Client struct {
	apiKey      string
	apiSecret   string
	baseURL     string
	httpClient  *http.Client
	rateLimiter *RateLimiter
	testnet     bool
}

// Config contains Bybit connector configuration.
type Config struct {
	APIKey    string
	APISecret string
	Testnet   bool
	ProxyURL  string
	Timeout   time.Duration
}

// NewClient creates a new Bybit connector client.
func NewClient(cfg Config) (*Client, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("API key is required")
	}
	if cfg.APISecret == "" {
		return nil, fmt.Errorf("API secret is required")
	}

	baseURL := mainnetBaseURL
	if cfg.Testnet {
		baseURL = testnetBaseURL
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	// Create HTTP client
	transport := &http.Transport{}

	// Configure proxy if provided
	if cfg.ProxyURL != "" {
		proxyURL, err := url.Parse(cfg.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy URL: %w", err)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	}

	httpClient := &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}

	return &Client{
		apiKey:      cfg.APIKey,
		apiSecret:   cfg.APISecret,
		baseURL:     baseURL,
		httpClient:  httpClient,
		rateLimiter: NewRateLimiter(defaultRateLimit),
		testnet:     cfg.Testnet,
	}, nil
}

// Name returns the exchange identifier.
func (c *Client) Name() string {
	return "bybit"
}

// Connect establishes connection and validates credentials.
func (c *Client) Connect(ctx context.Context) error {
	// Check permissions
	perms, err := c.CheckPermissions(ctx)
	if err != nil {
		return fmt.Errorf("check permissions: %w", err)
	}

	if !perms.IsSufficientForTrading() {
		return fmt.Errorf("API key lacks required permissions (read: %v, trade: %v)",
			perms.CanRead, perms.CanTrade)
	}

	return nil
}

// Disconnect closes all connections.
func (c *Client) Disconnect(ctx context.Context) error {
	// Stop rate limiter goroutine
	if c.rateLimiter != nil {
		c.rateLimiter.Stop()
	}

	// Close HTTP client connections
	c.httpClient.CloseIdleConnections()
	return nil
}

// IsConnected returns true if the connector is connected.
func (c *Client) IsConnected() bool {
	// For REST-only client, always return true if initialized
	return c.httpClient != nil
}

// generateSignature creates HMAC-SHA256 signature for Bybit API.
func (c *Client) generateSignature(timestamp int64, params string) string {
	message := fmt.Sprintf("%d%s%s", timestamp, c.apiKey, params)
	h := hmac.New(sha256.New, []byte(c.apiSecret))
	h.Write([]byte(message))
	return hex.EncodeToString(h.Sum(nil))
}

// buildAuthHeaders creates authenticated request headers.
func (c *Client) buildAuthHeaders(params string) map[string]string {
	timestamp := time.Now().UnixMilli()
	signature := c.generateSignature(timestamp, params)

	return map[string]string{
		"X-BAPI-API-KEY":     c.apiKey,
		"X-BAPI-SIGN":        signature,
		"X-BAPI-TIMESTAMP":   strconv.FormatInt(timestamp, 10),
		"X-BAPI-SIGN-TYPE":   "2",
		"X-BAPI-RECV-WINDOW": "5000",
		"Content-Type":       "application/json",
	}
}

// doRequest executes an HTTP request with rate limiting.
func (c *Client) doRequest(ctx context.Context, method, path string, params url.Values, body []byte) (*http.Response, error) {
	// Wait for rate limiter
	if err := c.rateLimiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter: %w", err)
	}

	// Build URL
	reqURL := c.baseURL + apiVersion + path
	if len(params) > 0 {
		reqURL += "?" + params.Encode()
	}

	var bodyReader io.Reader
	payload := ""
	if len(body) > 0 {
		payload = string(body)
		bodyReader = bytes.NewReader(body)
	} else if len(params) > 0 {
		payload = params.Encode()
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, method, reqURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Add authentication headers
	headers := c.buildAuthHeaders(payload)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}

	return resp, nil
}
