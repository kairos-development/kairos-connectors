package bybit

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kairos-development/kairos-contracts/connector"
	"github.com/sirupsen/logrus"
)

func TestWebSocketClient_ReconnectsAndResubscribes(t *testing.T) {
	var connections atomic.Int32
	subscriptions := make(chan string, 4)
	upgrader := websocket.Upgrader{}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("local TCP listener unavailable in this test environment: %v", err)
	}

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer conn.Close()

		id := connections.Add(1)
		if err := readWSOp(conn, "auth"); err != nil {
			t.Errorf("read auth: %v", err)
			return
		}

		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}

			var msg map[string]interface{}
			if err := json.Unmarshal(raw, &msg); err != nil {
				t.Errorf("unmarshal message: %v", err)
				return
			}

			if msg["op"] == "ping" {
				_ = conn.WriteJSON(map[string]interface{}{"op": "pong"})
				continue
			}

			if msg["op"] != "subscribe" {
				continue
			}

			args, ok := msg["args"].([]interface{})
			if !ok || len(args) != 1 {
				t.Errorf("unexpected subscribe args: %#v", msg["args"])
				return
			}
			topic, ok := args[0].(string)
			if !ok {
				t.Errorf("unexpected subscribe topic type: %#v", args[0])
				return
			}
			subscriptions <- topic

			if id == 1 {
				return
			}
		}
	}))
	server.Listener = listener
	server.Start()
	defer server.Close()

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	ws, err := NewWebSocketClient(WebSocketConfig{
		APIKey:    "test-key",
		APISecret: "test-secret",
		Testnet:   true,
	}, logger)
	if err != nil {
		t.Fatalf("create websocket client: %v", err)
	}
	ws.url = "ws" + strings.TrimPrefix(server.URL, "http")
	ws.reconnectInitialDelay = 10 * time.Millisecond
	ws.reconnectMaxDelay = 20 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := ws.Connect(ctx); err != nil {
		t.Fatalf("connect websocket: %v", err)
	}
	defer ws.Close()

	if err := ws.SubscribeOrders(ctx); err != nil {
		t.Fatalf("subscribe orders: %v", err)
	}

	assertSubscription(t, subscriptions, "order")
	assertSubscription(t, subscriptions, "order")
	assertStreamEvent(t, ws.Events(), connector.StreamEventGap)
	assertStreamEvent(t, ws.Events(), connector.StreamEventReconnected)
	if got := connections.Load(); got < 2 {
		t.Fatalf("expected at least 2 websocket connections, got %d", got)
	}
}

func TestNewWebSocketClient_DefaultsAndCloseIdempotent(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	ws, err := NewWebSocketClient(WebSocketConfig{APIKey: "key", APISecret: "secret", Testnet: true}, logger)
	if err != nil {
		t.Fatalf("create websocket client: %v", err)
	}
	if !strings.Contains(ws.url, "stream-testnet.bybit.com") {
		t.Fatalf("expected testnet URL, got %q", ws.url)
	}
	if ws.generateSignature(12345) == "" {
		t.Fatal("expected non-empty signature")
	}

	if err := ws.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := ws.Close(); err != nil {
		t.Fatalf("second close should be idempotent: %v", err)
	}
}

func TestWebSocketClient_SubscribeRequiresConnection(t *testing.T) {
	ws, err := NewWebSocketClient(WebSocketConfig{APIKey: "key", APISecret: "secret", Testnet: true}, logrus.New())
	if err != nil {
		t.Fatalf("create websocket client: %v", err)
	}
	defer ws.Close()

	if err := ws.SubscribeOrders(context.Background()); err == nil {
		t.Fatal("expected not connected error")
	}
	if _, ok := ws.subscriptions["order"]; !ok {
		t.Fatal("expected failed subscription to be remembered for reconnect")
	}
	if err := ws.SubscribePositions(context.Background()); err == nil {
		t.Fatal("expected positions subscription to require connection")
	}
	if err := ws.SubscribeBalance(context.Background()); err == nil {
		t.Fatal("expected balance subscription to require connection")
	}
	if err := ws.SubscribeTicker(context.Background(), "BTCUSDT"); err == nil {
		t.Fatal("expected ticker subscription to require connection")
	}
}

func TestWebSocketClient_WriteAndSendSubscriptionRequireConnection(t *testing.T) {
	ws, err := NewWebSocketClient(WebSocketConfig{APIKey: "key", APISecret: "secret", Testnet: true}, logrus.New())
	if err != nil {
		t.Fatalf("create websocket client: %v", err)
	}
	defer ws.Close()

	if err := ws.writeJSON(map[string]interface{}{"op": "ping"}); err == nil {
		t.Fatal("expected writeJSON to require connection")
	}
	if err := ws.sendSubscription(nil, "order"); err == nil {
		t.Fatal("expected sendSubscription to require connection")
	}
}

func TestWebSocketClient_StreamEventHelpers(t *testing.T) {
	ws, err := NewWebSocketClient(WebSocketConfig{APIKey: "key", APISecret: "secret", Testnet: true}, logrus.New())
	if err != nil {
		t.Fatalf("create websocket client: %v", err)
	}
	defer ws.Close()

	ws.publishStreamEvent(connector.StreamEventGap, "manual test")
	assertStreamEvent(t, ws.Events(), connector.StreamEventGap)

	ws.triggerReconnect()
	select {
	case <-ws.reconnectCh:
	case <-time.After(time.Second):
		t.Fatal("expected reconnect trigger")
	}

	ws.disconnectCurrent("no active connection")
	assertNoStreamEvent(t, ws.Events())
}

func TestWebSocketClient_ReconnectAttemptNoopsWhenAlreadyConnected(t *testing.T) {
	ws, err := NewWebSocketClient(WebSocketConfig{APIKey: "key", APISecret: "secret", Testnet: true}, logrus.New())
	if err != nil {
		t.Fatalf("create websocket client: %v", err)
	}
	defer ws.Close()

	ws.mu.Lock()
	ws.connected = true
	ws.mu.Unlock()

	reconnected, err := ws.reconnectOnceAttempt()
	if err != nil {
		t.Fatalf("reconnect attempt: %v", err)
	}
	if reconnected {
		t.Fatal("expected no reconnect when already connected")
	}
}

func TestWebSocketClient_HandleMessageRoutesTopicData(t *testing.T) {
	ws, err := NewWebSocketClient(WebSocketConfig{APIKey: "key", APISecret: "secret", Testnet: true}, logrus.New())
	if err != nil {
		t.Fatalf("create websocket client: %v", err)
	}
	defer ws.Close()

	ws.handleMessage([]byte(`{"op":"pong"}`))
	assertNoWSMessage(t, ws.Messages())

	ws.handleMessage([]byte(`{"success":true}`))
	assertNoWSMessage(t, ws.Messages())

	ws.handleMessage([]byte(`{"topic":"order","data":{"orderId":"order-1"}}`))
	select {
	case msg := <-ws.Messages():
		if msg.Topic != "order" {
			t.Fatalf("expected order topic, got %q", msg.Topic)
		}
	case <-time.After(time.Second):
		t.Fatal("expected websocket message")
	}
}

func TestWebSocketClient_ConnectReadsTopicMessage(t *testing.T) {
	upgrader := websocket.Upgrader{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("local TCP listener unavailable in this test environment: %v", err)
	}

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer conn.Close()

		if err := readWSOp(conn, "auth"); err != nil {
			t.Errorf("read auth: %v", err)
			return
		}
		if err := conn.WriteJSON(map[string]interface{}{
			"topic": "order",
			"data":  map[string]interface{}{"orderId": "order-1"},
		}); err != nil {
			t.Errorf("write topic message: %v", err)
			return
		}
		<-r.Context().Done()
	}))
	server.Listener = listener
	server.Start()
	defer server.Close()

	logger := logrus.New()
	logger.SetOutput(io.Discard)
	ws, err := NewWebSocketClient(WebSocketConfig{APIKey: "key", APISecret: "secret", Testnet: true}, logger)
	if err != nil {
		t.Fatalf("create websocket client: %v", err)
	}
	ws.url = "ws" + strings.TrimPrefix(server.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := ws.Connect(ctx); err != nil {
		t.Fatalf("connect websocket: %v", err)
	}
	defer ws.Close()

	select {
	case msg := <-ws.Messages():
		if msg.Topic != "order" {
			t.Fatalf("expected order topic, got %q", msg.Topic)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected websocket topic message")
	}
}

func readWSOp(conn *websocket.Conn, want string) error {
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		var msg map[string]interface{}
		if err := json.Unmarshal(raw, &msg); err != nil {
			return err
		}
		if msg["op"] == want {
			return nil
		}
	}
}

func assertStreamEvent(t *testing.T, ch <-chan *connector.StreamEvent, want connector.StreamEventType) {
	t.Helper()

	timeout := time.After(2 * time.Second)
	for {
		select {
		case event := <-ch:
			if event == nil {
				continue
			}
			if event.Type == want {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for stream event %q", want)
		}
	}
}

func assertSubscription(t *testing.T, ch <-chan string, want string) {
	t.Helper()

	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("expected subscription %q, got %q", want, got)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for subscription %q", want)
	}
}

func assertNoWSMessage(t *testing.T, ch <-chan *WebSocketMessage) {
	t.Helper()

	select {
	case msg := <-ch:
		t.Fatalf("expected no websocket message, got %+v", msg)
	default:
	}
}

func assertNoStreamEvent(t *testing.T, ch <-chan *connector.StreamEvent) {
	t.Helper()

	select {
	case event := <-ch:
		t.Fatalf("expected no stream event, got %+v", event)
	default:
	}
}
