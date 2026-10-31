package actions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

func TestStarsExactTotalsAndUnknowns(t *testing.T) {
	day := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	qty := uint64(9007199254740993)
	b := gramstat.StarsBucket{PaymentAsset: "TON", Product: "standard", StarsPerPurchase: &qty, QuantityStatus: "known", Purchases: 3}
	s := Snapshot{Days: []DayCounts{{Day: day, StarsPurchases: 4, StarsBuckets: []gramstat.StarsBucket{b,
		{PaymentAsset: "USDT", Product: "prepaid", QuantityStatus: "missing_or_unrecognized_quantity", Purchases: 1}}}},
		Through: day.Add(36 * time.Hour), CalculatedAt: day.Add(40 * time.Hour), SourceMC: 123}
	rows, err := starsRows(day, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].KnownStarsTotal != "27021597764222979" || rows[0].StarsTotal != nil || rows[0].UnknownPurchases != 1 || rows[0].Buckets[1].StarsTotal != nil {
		t.Fatalf("unknown/precision lost: %+v", rows)
	}
	if rows[1].Purchases != 0 || rows[1].KnownStarsTotal != "0" || rows[1].StarsTotal == nil || *rows[1].StarsTotal != "0" || rows[1].Buckets == nil || rows[1].IsPartial != 1 || rows[1].DataThrough != "2026-10-31 12:00:00" {
		t.Fatalf("empty/partial snapshot: %+v", rows[1])
	}
	for _, test := range []struct {
		name    string
		count   uint64
		buckets []gramstat.StarsBucket
	}{
		{"missing", 4, []gramstat.StarsBucket{b}},
		{"duplicate", 6, []gramstat.StarsBucket{b, b}},
		{"zero count", 0, []gramstat.StarsBucket{{PaymentAsset: "TON", Product: "standard", QuantityStatus: "zero_quantity"}}},
		{"unknown with size", 3, []gramstat.StarsBucket{{PaymentAsset: "TON", Product: "standard", QuantityStatus: "zero_quantity", StarsPerPurchase: &qty, Purchases: 3}}},
		{"known without size", 3, []gramstat.StarsBucket{{PaymentAsset: "TON", Product: "standard", QuantityStatus: "known", Purchases: 3}}},
		{"unsupported asset", 3, []gramstat.StarsBucket{{PaymentAsset: "FAKE", Product: "standard", QuantityStatus: "known", StarsPerPurchase: &qty, Purchases: 3}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			s.Days[0].StarsPurchases = test.count
			s.Days[0].StarsBuckets = test.buckets
			if _, err := starsRows(day, s); err == nil {
				t.Fatal("invalid distribution accepted")
			}
		})
	}
	qty = math.MaxUint64
	b.Purchases = math.MaxUint64
	s.Days[0].StarsPurchases = math.MaxUint64
	s.Days[0].StarsBuckets = []gramstat.StarsBucket{b}
	if _, err := starsRows(day, s); err == nil {
		t.Fatal("Decimal(38,0) overflow accepted")
	}
}

type starsSource struct{ snapshot Snapshot }

func (s starsSource) Collect(context.Context, time.Time, time.Time) (Snapshot, error) {
	return s.snapshot, nil
}

type starsSink struct {
	calls                []string
	failStars, failDaily bool
}

func (s *starsSink) StoredDays(context.Context, []string, time.Time) (map[string][]time.Time, error) {
	return nil, nil
}
func (s *starsSink) WriteStars(context.Context, []gramstat.DailyStars) error {
	s.calls = append(s.calls, "stars")
	if s.failStars {
		return errors.New("insert failed")
	}
	return nil
}
func (s *starsSink) WriteDaily(context.Context, []gramstat.DailyMetric) error {
	s.calls = append(s.calls, "daily")
	if s.failDaily {
		return errors.New("insert failed")
	}
	return nil
}

func TestStarsCompletionMarkerAfterDistribution(t *testing.T) {
	day := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	sink := &starsSink{failStars: true}
	h := Handler{Source: starsSource{Snapshot{Through: day.Add(24 * time.Hour), CalculatedAt: day.Add(25 * time.Hour), SourceMC: 123}}, Sink: sink, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	req := gramstat.Request{From: day, To: day.Add(24 * time.Hour), AsOf: day.Add(48 * time.Hour), LookbackDays: 3}
	if err := h.Run(context.Background(), req); err == nil || !reflect.DeepEqual(sink.calls, []string{"stars"}) {
		t.Fatalf("marker written after failed distribution: %v %v", err, sink.calls)
	}
	sink.calls = nil
	sink.failStars = false
	sink.failDaily = true
	if err := h.Run(context.Background(), req); err == nil || !reflect.DeepEqual(sink.calls, []string{"stars", "daily"}) {
		t.Fatalf("marker failure hidden: %v %v", err, sink.calls)
	}
	sink.calls = nil
	sink.failDaily = false
	if err := h.Run(context.Background(), req); err != nil || !reflect.DeepEqual(sink.calls, []string{"stars", "daily"}) {
		t.Fatalf("retry did not republish snapshot before marker: %v %v", err, sink.calls)
	}
}
