package v2

import (
	"bufio"
	"encoding/json"
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
	client.closeTransport = func() error { transportCloses.Add(1); return nil }

	// No manager loop runs: queued subscription cleanup cannot release quota.
	var calls sync.WaitGroup
	for i := 0; i < 16; i++ {
		calls.Add(1)
		go func() {
			defer calls.Done()
			disconnectClient(manager, client)
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
	disconnectClient(manager, client)
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
