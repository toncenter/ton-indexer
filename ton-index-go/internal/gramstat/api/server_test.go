package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

type readerFunc func(context.Context, string, url.Values) (json.RawMessage, error)

func (f readerFunc) Read(ctx context.Context, name string, q url.Values) (json.RawMessage, error) {
	return f(ctx, name, q)
}
func logger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestHTTPBoundaryAndExactValues(t *testing.T) {
	var calls atomic.Int32
	s := New(readerFunc(func(_ context.Context, name string, p url.Values) (json.RawMessage, error) {
		calls.Add(1)
		if name == "chain" {
			return json.RawMessage(`[{"day":"2026-10-22","supply_nanoton":"5258373238061259412","fees_nanoton":null}]`), nil
		}
		return json.RawMessage(`[]`), nil
	}), logger())
	for _, target := range []string{
		"/api/v1/dashboard", "/api/v1/dashboard?from=2026-10-22&to=2026-10-22",
		"/api/v1/dashboard?from=2026-02-30&to=2026-03-02", "/api/v1/dashboard?from=2020-01-01&to=2026-10-22",
		"/api/v1/dashboard?from=2026-10-21&to=2026-10-28&sql=DROP", "/api/v1/coverage?readonly=0",
		"/api/v1/locations?round=1&round=2", "/api/v1/locations?round=1%3BDROP", "/api/v1/locations?round=4294967296",
		"/api/v1/dashboard?from=2026-10-21;to=2026-10-28",
	} {
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest("GET", target, nil))
		if w.Code != 400 {
			t.Fatalf("%s: %d", target, w.Code)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid inputs reached source")
	}
	for method, code := range map[string]int{"POST": 405, "DELETE": 405, "OPTIONS": 405} {
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest(method, "/api/v1/coverage", nil))
		if w.Code != code {
			t.Fatal(w.Code)
		}
	}
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/dashboard?from=2026-10-21&to=2026-10-28", nil))
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"5258373238061259412"`) || !strings.Contains(w.Body.String(), `"fees_nanoton":null`) {
			t.Fatalf("%d %s", w.Code, w.Body.String())
		}
	}
	if calls.Load() != 4 {
		t.Fatal("dashboard cache did not reuse complete response")
	}
}

func TestSingleflightAndSafeFailure(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	s := New(readerFunc(func(ctx context.Context, name string, p url.Values) (json.RawMessage, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return json.RawMessage(`[]`), nil
	}), logger())
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			s.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/coverage", nil))
			if w.Code != 200 {
				t.Errorf("status %d", w.Code)
			}
		}()
	}
	<-entered
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("loads=%d", calls.Load())
	}
	s = New(readerFunc(func(context.Context, string, url.Values) (json.RawMessage, error) {
		return nil, errors.New("internal-host secret SQL")
	}), logger())
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/coverage", nil))
	if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), "secret") || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Body.String())
	}
}

func TestStrictReadTransport(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("readonly") != "1" || q.Has("async_insert") || q.Get("max_execution_time") != "10" {
			t.Errorf("unexpected settings: %v", q)
		}
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer upstream.Close()
	ch, err := gramstat.NewClickHouse(upstream.URL, "reader", "password")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (ClickHouseReader{CH: ch}).Read(context.Background(), "coverage", nil); err != nil {
		t.Fatal(err)
	}
}
