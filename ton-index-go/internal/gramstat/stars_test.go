package gramstat

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestStarsClickHouseSnapshots(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local not installed")
	}
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	run := func(query, input string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, "local", "--path", dir, "--max_threads", "2", "--multiquery", "--query", query)
		cmd.Stdin = strings.NewReader(input)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("ClickHouse: %v\n%s", err, out)
		}
		return string(out)
	}
	run(StarsSchema+StarsSchema, "")
	// Exercise the real writer's JSON, including quoted UInt64 and Decimal values.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("async_insert") != "0" || r.URL.Query().Get("wait_end_of_query") != "1" {
			t.Error("write must wait for completion")
		}
		data, _ := io.ReadAll(r.Body)
		run(r.URL.Query().Get("query"), string(data))
	}))
	defer server.Close()
	ch, _ := NewClickHouse(server.URL, "reader_writer", "test-password")
	qty := uint64(9007199254740993)
	total := "27021597764222979"
	row := DailyStars{Day: "2026-10-30", Purchases: 4, UnknownPurchases: 1, KnownStarsTotal: total,
		CalculatedAt: "2026-10-31 12:00:00.000000", DataThrough: "2026-10-31 00:00:00", SourceMCSeqno: 123,
		Buckets: []StarsBucket{{PaymentAsset: "TON", Product: "standard", StarsPerPurchase: &qty, QuantityStatus: "known", Purchases: 3, StarsTotal: &total},
			{PaymentAsset: "USDT", Product: "prepaid", QuantityStatus: "zero_quantity", Purchases: 1}}}
	if err := ch.WriteStars(ctx, []DailyStars{row}); err != nil {
		t.Fatal(err)
	}
	if got := run("SELECT sum(purchases),sum(stars_total),countIf(stars_per_purchase IS NULL) FROM gramstat.telegram_stars_buckets", ""); got != "4\t27021597764222979\t1\n" {
		t.Fatalf("bucket precision/nulls: %s", got)
	}
	if got := run("SELECT purchases,known_stars_total,isNull(stars_total) FROM gramstat.telegram_stars_daily FINAL", ""); got != "4\t27021597764222979\t1\n" {
		t.Fatalf("daily total: %s", got)
	}
	// A corrected distribution drops an entire currency/size; no stale bucket survives.
	row.Buckets = row.Buckets[:1]
	row.Purchases = 3
	row.UnknownPurchases = 0
	row.StarsTotal = &total
	row.CalculatedAt = "2026-10-31 12:00:01.000000"
	if err := ch.WriteStars(ctx, []DailyStars{row}); err != nil {
		t.Fatal(err)
	}
	if got := run("SELECT count(),sum(purchases) FROM gramstat.telegram_stars_buckets", ""); got != "1\t3\n" {
		t.Fatalf("stale removed bucket: %s", got)
	}
	// The >2^53 quantity survives the view's numeric JSON representation.
	text := run("SELECT payment_asset,product,stars_per_purchase,quantity_status,purchases,stars_total FROM gramstat.telegram_stars_buckets FORMAT JSONEachRow", "")
	var wire struct {
		StarsPerPurchase *uint64 `json:"stars_per_purchase"`
	}
	if err := json.Unmarshal([]byte(text), &wire); err != nil || wire.StarsPerPurchase == nil || *wire.StarsPerPurchase != qty {
		t.Fatalf("quantity precision lost: %s %v", text, err)
	}
	zero := "0"
	row.Buckets = []StarsBucket{}
	row.Purchases = 0
	row.KnownStarsTotal = zero
	row.StarsTotal = &zero
	row.CalculatedAt = "2026-10-31 12:00:02.000000"
	if err := ch.WriteStars(ctx, []DailyStars{row}); err != nil {
		t.Fatal(err)
	}
	if got := run("SELECT count() FROM gramstat.telegram_stars_buckets", ""); got != "0\n" {
		t.Fatalf("zero snapshot did not clear distribution: %s", got)
	}
}
