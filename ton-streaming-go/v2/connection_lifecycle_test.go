package v2

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wsclient "github.com/fasthttp/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

func TestRateLimitKeySurvivesRequestReuse(t *testing.T) {
	app := fiber.New()
	request := &fasthttp.RequestCtx{}
	request.Request.Header.Set("X-Limiting-Key", "first-client")
	ctx := app.AcquireCtx(request)
	key, _ := ParseRateLimitHeaders(ctx.GetReqHeaders())
	app.ReleaseCtx(ctx)

	request.Request.Reset()
	request.Request.Header.Set("X-Limiting-Key", "other-client")
	if key != "first-client" {
		t.Fatalf("saved limiting key changed after request reuse: %q", key)
	}
}

func TestDisconnectReleasesQuotaBeforeCleanupAndIsIdempotent(t *testing.T) {
	manager := NewClientManager()
	limits := RateLimitConfig{MaxParallelConnections: 1}
	client := &Client{ID: "old", LimitingKey: "key", Connected: true, done: make(chan struct{})}
	if err := manager.rateLimiter.RegisterConnection(client.LimitingKey, client.ID, limits); err != nil {
		t.Fatal(err)
	}
	var transportCloses atomic.Int32
	client.closeTransport = func(_ error) error { transportCloses.Add(1); return nil }

	// No manager loop runs: queued subscription cleanup cannot release quota.
	var calls sync.WaitGroup
	for i := 0; i < 16; i++ {
		calls.Add(1)
		go func() {
			defer calls.Done()
			disconnectClient(manager, client, nil)
		}()
	}
	calls.Wait()
	if err := manager.rateLimiter.RegisterConnection("key", "replacement", limits); err != nil {
		t.Fatalf("quota still occupied before queued cleanup: %v", err)
	}
	if got := transportCloses.Load(); got != 1 {
		t.Fatalf("transport closed %d times, want 1", got)
	}
	if got := len(manager.unregister); got != 1 {
		t.Fatalf("queued %d cleanups, want 1", got)
	}
	select {
	case <-client.done:
	default:
		t.Fatal("client workers were not notified of disconnect")
	}

	// A late cleanup must not affect the replacement connection.
	disconnectClient(manager, client, nil)
	if err := manager.rateLimiter.RegisterConnection("key", "extra", limits); err == nil {
		t.Fatal("late cleanup released the replacement's quota")
	}
}

func TestSSEBodyCloseReleasesQuotaAfterRequestReuse(t *testing.T) {
	manager := NewClientManager()
	limits := RateLimitConfig{MaxParallelConnections: 2}
	if err := manager.rateLimiter.RegisterConnection("first-client", "existing", limits); err != nil {
		t.Fatal(err)
	}
	app := fiber.New()
	request := &fasthttp.RequestCtx{}
	request.Request.Header.SetMethod("POST")
	request.Request.Header.SetContentType("application/json")
	request.Request.Header.Set("X-Limiting-Key", "first-client")
	request.Request.Header.Set("X-Max-Parallel-Connections", "2")
	request.Request.Header.Set("X-Max-Subscribed-Addr", "1")
	request.Request.SetBodyString(`{"types":["trace"],"trace_external_hash_norms":["` + strings.Repeat("0", 64) + `"]}`)
	ctx := app.AcquireCtx(request)
	err := SSEHandler(manager)(ctx)
	app.ReleaseCtx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if request.Response.StatusCode() != 200 {
		t.Fatalf("subscription failed: %s", request.Response.Body())
	}
	defer request.Response.CloseBodyStream()
	stream := request.Response.BodyStream().(io.ReadCloser)
	reader := bufio.NewReader(stream)
	for _, want := range []string{"event: connected\n", "data: {\"status\":\"subscribed\"}\n", "\n"} {
		line, err := reader.ReadString('\n')
		if err != nil || line != want {
			t.Fatalf("connected frame: got %q, error %v; want %q", line, err, want)
		}
	}

	request.Request.Reset()
	request.Request.Header.Set("X-Limiting-Key", "other-client")
	if err := request.Response.CloseBodyStream(); err != nil {
		t.Fatal(err)
	}
	if err := manager.rateLimiter.RegisterConnection("first-client", "replacement", limits); err != nil {
		t.Fatalf("body Close did not immediately release the SSE slot: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if got := len(manager.unregister); got != 1 {
		t.Fatalf("body close queued %d cleanups, want 1", got)
	}
}

func TestSSEBodyCloseStopsWriter(t *testing.T) {
	for _, blockedWrite := range []bool{false, true} {
		name := "idle"
		if blockedWrite {
			name = "blocked_write"
		}
		t.Run(name, func(t *testing.T) {
			manager := NewClientManager()
			client := &Client{Connected: true, done: make(chan struct{})}
			started, stopped := make(chan struct{}), make(chan struct{})
			stream := newSSEStream(manager, client, func(w *bufio.Writer) {
				defer close(stopped)
				close(started)
				if !blockedWrite {
					<-client.done
					return
				}
				payload := make([]byte, 8192)
				for {
					if _, err := w.Write(payload); err != nil {
						return
					}
				}
			})
			<-started
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("SSE writer did not stop after closing its response body")
			}
		})
	}
}

func TestWSCloseReplyReleasesQuotaBeforeManagerCleanup(t *testing.T) {
	manager := NewClientManager()
	go manager.Run()
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Get("/ws", websocket.New(WebSocketHandler(manager)))
	listener := fasthttputil.NewInmemoryListener()
	serverDone := make(chan error, 1)
	go func() { serverDone <- app.Listener(listener) }()
	defer func() {
		_ = listener.Close()
		_ = app.ShutdownWithTimeout(time.Second)
		<-serverDone
	}()
	dialer := wsclient.Dialer{
		NetDial:          func(_, _ string) (net.Conn, error) { return listener.Dial() },
		HandshakeTimeout: time.Second,
	}
	dial := func() *wsclient.Conn {
		t.Helper()
		conn, _, err := dialer.Dial("ws://localhost/ws", map[string][]string{
			"X-Limiting-Key":             {"ws-client"},
			"X-Max-Parallel-Connections": {"1"},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		// Over-limit connections can close before the client sends its ping.
		_ = conn.WriteMessage(wsclient.TextMessage, []byte(`{"operation":"ping"}`))
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		var response StatusResponse
		if err := json.Unmarshal(data, &response); err != nil || response.Status != "pong" {
			t.Fatalf("WS admission failed: %s (%v)", data, err)
		}
		return conn
	}
	first := dial()
	defer first.Close()
	deadline := time.Now().Add(time.Second)
	for {
		manager.mu.RLock()
		registered := len(manager.clients) == 1
		manager.mu.RUnlock()
		if registered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("initial WS was not registered")
		}
		time.Sleep(time.Millisecond)
	}

	// Hold subscription cleanup until after the peer has reconnected.
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if err := first.WriteControl(wsclient.CloseMessage, wsclient.FormatCloseMessage(1000, ""), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := first.ReadMessage(); !wsclient.IsCloseError(err, 1000) {
		t.Fatalf("expected normal Close reply, got %v", err)
	}
	replacement := dial()
	defer replacement.Close()
}

func TestSlowConsumerDisconnectDoesNotBlockOtherClients(t *testing.T) {
	manager := NewClientManager()
	go manager.Run()
	limits := RateLimitConfig{MaxParallelConnections: 1}
	slow := &Client{ID: "slow", LimitingKey: "slow-key", Connected: true, done: make(chan struct{}), Subscription: Subscription{SubscribedAddresses: AddressSet{replayAddress(1): {}}, EventTypes: makeEventSet([]EventType{EventTransactions})}}
	if err := manager.rateLimiter.RegisterConnection(slow.LimitingKey, slow.ID, limits); err != nil {
		t.Fatal(err)
	}
	writing := make(chan struct{})
	var once sync.Once
	slow.SendEvent = func(clientMessage) error { once.Do(func() { close(writing) }); <-slow.done; return context.Canceled }
	closing := make(chan error, 1)
	releaseClose := make(chan struct{})
	slow.closeTransport = func(reason error) error { closing <- reason; <-releaseClose; return nil }
	received := make(chan []byte, 1)
	fast := &Client{ID: "fast", Connected: true, done: make(chan struct{}), Subscription: Subscription{SubscribedAddresses: AddressSet{replayAddress(1): {}}, EventTypes: makeEventSet([]EventType{EventTransactions})}, SendEvent: func(msg clientMessage) error { received <- msg.data; return nil }}
	t.Cleanup(func() {
		close(releaseClose)
		disconnectClient(manager, slow, nil)
		disconnectClient(manager, fast, nil)
	})
	for _, client := range []*Client{slow, fast} {
		ready := make(chan struct{})
		manager.register <- clientRegistration{client: client, ready: ready}
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatal("client registration stalled")
		}
	}
	manager.sendNotification(replayTransaction(1), clientSet{slow.ID: {}})
	select {
	case <-writing:
	case <-time.After(time.Second):
		t.Fatal("slow sender did not start")
	}
	for i := 0; i < 65; i++ {
		manager.sendNotification(replayTransaction(uint64(i+2)), clientSet{slow.ID: {}})
	}
	select {
	case reason := <-closing:
		if !errors.Is(reason, errSlowConsumer) {
			t.Fatalf("wrong close reason: %v", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("slow client was not disconnected")
	}
	// The transport close is still blocked, but quota and the broadcast loop
	// must already be available to everyone else.
	if err := manager.rateLimiter.RegisterConnection("slow-key", "replacement", limits); err != nil {
		t.Fatalf("quota still occupied: %v", err)
	}
	manager.sendNotification(replayTransaction(100), clientSet{fast.ID: {}})
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("one slow close blocked delivery to another client")
	}
}

func TestWebSocketSlowConsumerCloseFrame(t *testing.T) {
	manager := NewClientManager()
	go manager.Run()
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Get("/ws", websocket.New(WebSocketHandler(manager)))
	listener := fasthttputil.NewInmemoryListener()
	serverDone := make(chan error, 1)
	go func() { serverDone <- app.Listener(listener) }()
	defer func() { _ = listener.Close(); _ = app.ShutdownWithTimeout(time.Second); <-serverDone }()
	dialer := wsclient.Dialer{NetDial: func(_, _ string) (net.Conn, error) { return listener.Dial() }, HandshakeTimeout: time.Second}
	conn, _, err := dialer.Dial("ws://localhost/ws", map[string][]string{"X-Limiting-Key": {"ws-slow"}, "X-Max-Parallel-Connections": {"1"}, "X-Max-Subscribed-Addr": {"1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteJSON(map[string]any{"operation": "subscribe", "types": []string{"transactions"}, "addresses": []string{string(replayAddress(1))}, "min_finality": "pending"}); err != nil {
		t.Fatal(err)
	}
	_, ack, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var status StatusResponse
	if err := json.Unmarshal(ack, &status); err != nil || status.Status != "subscribed" {
		t.Fatalf("subscribe failed: %s", ack)
	}
	manager.mu.RLock()
	var client *Client
	for _, candidate := range manager.clients {
		client = candidate
	}
	manager.mu.RUnlock()
	if client == nil {
		t.Fatal("WebSocket client was not registered")
	}
	// Stop application writes while leaving the WebSocket control-frame path
	// available. Closing must not acquire this mutex or use the full send queue.
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	for i := 0; i < 67; i++ {
		manager.sendNotification(replayTransaction(uint64(i+1)), clientSet{client.ID: {}})
	}
	_, _, err = conn.ReadMessage()
	var closeError *wsclient.CloseError
	if !errors.As(err, &closeError) || closeError.Code != wsclient.CloseTryAgainLater || closeError.Text != "slow consumer" {
		t.Fatalf("expected 1013 slow consumer, got %v", err)
	}
	if err := manager.rateLimiter.RegisterConnection("ws-slow", "replacement", RateLimitConfig{MaxParallelConnections: 1}); err != nil {
		t.Fatalf("quota was not released before the Close frame: %v", err)
	}
}
