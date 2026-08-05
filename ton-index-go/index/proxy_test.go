package index

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

func TestV2HTTPClientHasBoundedConnectionPool(t *testing.T) {
	client := newV2HTTPClient()
	if client.MaxConnsPerHost != v2MaxConnections {
		t.Fatalf("connection limit = %d, want %d", client.MaxConnsPerHost, v2MaxConnections)
	}
	if client.MaxConnWaitTimeout != v2ConnectionWaitLimit {
		t.Fatalf("connection wait limit = %s, want %s", client.MaxConnWaitTimeout, v2ConnectionWaitLimit)
	}
}

func TestV2HTTPClientReusesConnection(t *testing.T) {
	var openedConnections atomic.Int32
	listener := fasthttputil.NewInmemoryListener()
	server := &fasthttp.Server{Handler: func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("ok")
	}}
	serverStopped := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(serverStopped)
	}()

	client := newV2HTTPClient()
	client.Dial = func(string) (net.Conn, error) {
		openedConnections.Add(1)
		return listener.Dial()
	}
	defer func() {
		client.CloseIdleConnections()
		_ = listener.Close()
		<-serverStopped
	}()

	for i := 0; i < 2; i++ {
		body, err := executeV2Request(client, fasthttp.MethodGet, "http://api-v2.test/", nil, time.Second)
		if err != nil {
			t.Fatalf("request %d failed: %v", i+1, err)
		}
		if string(body) != "ok" {
			t.Fatalf("request %d returned %q, want %q", i+1, body, "ok")
		}
	}

	if got := openedConnections.Load(); got != 1 {
		t.Fatalf("opened %d connections for two sequential requests, want 1", got)
	}
}
