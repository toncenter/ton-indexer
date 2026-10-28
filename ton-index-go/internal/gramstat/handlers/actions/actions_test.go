package actions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

func TestDailyRowsZeroAndSourceLag(t *testing.T) {
	from := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	s := Snapshot{
		Days: []DayCounts{{Day: from, JettonSenders: 2, JettonReceivers: 3, JettonTransfers: 5,
			NFTSenders: 4, NFTReceivers: 6, NFTTransfers: 7}},
		Through: from.Add(36 * time.Hour), CalculatedAt: from.Add(40 * time.Hour), SourceMC: 101,
	}
	rows, err := dailyRows(from, from.Add(72*time.Hour), s)
	if err != nil || len(rows) != 12 {
		t.Fatalf("only covered days should be emitted: %v, %v", rows, err)
	}
	want := map[string]uint64{"jetton_senders": 2, "jetton_receivers": 3, "jetton_transfers": 5,
		"nft_senders": 4, "nft_receivers": 6, "nft_transfers": 7}
	for _, row := range rows {
		value, exists := want[row.Metric]
		if !exists {
			t.Fatalf("unexpected metric: %s", row.Metric)
		}
		if row.Day == "2026-10-21" {
			if row.Value != value || row.IsPartial != 0 {
				t.Fatalf("wrong complete day: %+v", row)
			}
		} else if row.Day != "2026-10-22" || row.Value != 0 || row.IsPartial != 1 || row.DataThrough != "2026-10-22 12:00:00" {
			t.Fatalf("wrong empty/partial day: %+v", row)
		}
	}
	s.Days[0].JettonReceivers = 6
	if _, err := dailyRows(from, from.Add(72*time.Hour), s); err == nil {
		t.Fatal("more distinct receivers than transfers accepted")
	}
	s.Days[0].JettonReceivers = 3
	s.Through = from
	if _, err := dailyRows(from, from.Add(72*time.Hour), s); err == nil {
		t.Fatal("unavailable interval must not become zero")
	}
}

type failedSource struct{}

func (failedSource) Collect(context.Context, time.Time, time.Time) (Snapshot, error) {
	return Snapshot{}, errors.New("database unavailable")
}

type testSink struct{ writes int }

func (*testSink) StoredDays(context.Context, []string, time.Time) (map[string][]time.Time, error) {
	return nil, nil
}
func (s *testSink) WriteDaily(context.Context, []gramstat.DailyMetric) error { s.writes++; return nil }

func TestSourceFailureDoesNotPublish(t *testing.T) {
	sink := &testSink{}
	h := Handler{Source: failedSource{}, Sink: sink, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	err := h.Run(context.Background(), gramstat.Request{AsOf: time.Now(), LookbackDays: 3})
	if err == nil || sink.writes != 0 {
		t.Fatalf("failed calculation published: %v, %d", err, sink.writes)
	}
}
