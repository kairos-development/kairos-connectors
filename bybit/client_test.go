package bybit

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

func TestNewClient_ValidatesConfig(t *testing.T) {
	if _, err := NewClient(Config{}); err == nil {
		t.Fatal("expected missing API key error")
	}

	if _, err := NewClient(Config{APIKey: "key"}); err == nil {
		t.Fatal("expected missing API secret error")
	}

	_, err := NewClient(Config{APIKey: "key", APISecret: "secret", ProxyURL: "://bad"})
	if err == nil {
		t.Fatal("expected invalid proxy error")
	}
}

func TestClientIdentityAndDisconnect(t *testing.T) {
	client, err := NewClient(Config{APIKey: "key", APISecret: "secret", Testnet: true})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	if client.Name() != "bybit" {
		t.Fatalf("expected bybit name, got %q", client.Name())
	}
	if !client.IsConnected() {
		t.Fatal("expected initialized REST client to be connected")
	}
	if err := client.Disconnect(context.Background()); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
}

func TestClientDoRequest_SendsAndSignsPostBody(t *testing.T) {
	const body = `{"symbol":"BTCUSDT","qty":"1"}`

	transport := &captureTransport{}

	client, err := NewClient(Config{APIKey: "key", APISecret: "secret", Testnet: true})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	client.baseURL = "https://api.test"
	client.httpClient = &http.Client{Transport: transport}

	resp, err := client.doRequest(context.Background(), http.MethodPost, "/order/create", nil, []byte(body))
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if transport.body != body {
		t.Fatalf("expected body %q, got %q", body, transport.body)
	}

	expectedSignature := client.generateSignature(parseTimestamp(t, transport.timestamp), body)
	if transport.signature != expectedSignature {
		t.Fatalf("expected signature %q, got %q", expectedSignature, transport.signature)
	}
}

func TestClientDoRequest_SignsQueryWhenBodyIsEmpty(t *testing.T) {
	params := url.Values{}
	params.Set("category", "linear")
	params.Set("orderId", "order-1")

	transport := &captureTransport{}

	client, err := NewClient(Config{APIKey: "key", APISecret: "secret", Testnet: true})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	client.baseURL = "https://api.test"
	client.httpClient = &http.Client{Transport: transport}

	resp, err := client.doRequest(context.Background(), http.MethodGet, "/order/realtime", params, nil)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if transport.rawQuery != params.Encode() {
		t.Fatalf("expected query %q, got %q", params.Encode(), transport.rawQuery)
	}

	expectedSignature := client.generateSignature(parseTimestamp(t, transport.timestamp), params.Encode())
	if transport.signature != expectedSignature {
		t.Fatalf("expected signature %q, got %q", expectedSignature, transport.signature)
	}
}

func TestClientDoRequest_PropagatesTransportError(t *testing.T) {
	client, err := NewClient(Config{APIKey: "key", APISecret: "secret", Testnet: true})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	client.baseURL = "https://api.test"
	client.httpClient = &http.Client{Transport: errorTransport{err: errors.New("network down")}}

	_, err = client.doRequest(context.Background(), http.MethodGet, "/order/realtime", nil, nil)
	if err == nil {
		t.Fatal("expected transport error")
	}
	if !strings.Contains(err.Error(), "network down") {
		t.Fatalf("expected wrapped transport error, got %v", err)
	}
}

type captureTransport struct {
	body         string
	method       string
	path         string
	signature    string
	timestamp    string
	rawQuery     string
	responseBody string
}

func (t *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var rawBody []byte
	if req.Body != nil {
		defer req.Body.Close()

		var err error
		rawBody, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
	}

	t.body = string(rawBody)
	t.method = req.Method
	t.path = req.URL.Path
	t.signature = req.Header.Get("X-BAPI-SIGN")
	t.timestamp = req.Header.Get("X-BAPI-TIMESTAMP")
	t.rawQuery = req.URL.RawQuery

	responseBody := t.responseBody
	if responseBody == "" {
		responseBody = `{"retCode":0,"retMsg":"OK","result":{}}`
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewBufferString(responseBody)),
		Request:    req,
	}, nil
}

type errorTransport struct {
	err error
}

func (t errorTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, t.err
}

func parseTimestamp(t *testing.T, raw string) int64 {
	t.Helper()

	ts, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse timestamp %q: %v", raw, err)
	}
	return ts
}
