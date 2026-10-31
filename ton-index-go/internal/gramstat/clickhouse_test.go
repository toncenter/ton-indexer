package gramstat

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClickHouseExactInsertAndLateError(t *testing.T) {
	failed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != "reader_writer" || p != "test-password" || strings.Contains(r.URL.String(), "test-password") {
			t.Error("credentials must be sent only in Authorization")
		}
		if r.URL.Query().Get("wait_end_of_query") != "1" || r.URL.Query().Get("async_insert") != "0" {
			t.Error("write must wait for server completion")
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"value":9007199254740993`) {
			t.Errorf("integer precision lost: %s", body)
		}
		if !strings.Contains(string(body), `"amount_nanoton":"100000000000000000001"`) {
			t.Errorf("monetary precision lost: %s", body)
		}
		if failed {
			// ClickHouse can report a query exception in an HTTP 200 body.
			_, _ = io.WriteString(w, "Code: 241. DB::Exception: memory limit exceeded")
		}
	}))
	defer server.Close()
	ch, _ := NewClickHouse(server.URL, "reader_writer", "test-password")
	amount := "100000000000000000001"
	rows := []DailyMetric{{Metric: "test", Value: 9007199254740993, AmountNanoton: &amount}}
	if err := ch.WriteDaily(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	failed = true
	if err := ch.WriteDaily(context.Background(), rows); err == nil {
		t.Fatal("accepted failed insert")
	}
}

func TestClickHouseRejectsCredentialURLs(t *testing.T) {
	if _, err := NewClickHouse("http://user:secret@localhost:8123", "", ""); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("credential-bearing URLs must be rejected without disclosure")
	}
}
