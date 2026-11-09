package actions

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

func TestMonthlyRowsCalendarAndPartialCoverage(t *testing.T) {
	from := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	through := time.Date(2024, 3, 10, 12, 0, 0, 0, time.UTC)
	s := Snapshot{Days: []DayCounts{{Day: from, JettonSenders: 7, SwapUsers: 3}}, Through: through, CalculatedAt: through, SourceMC: 100}
	rows, err := monthlyRows(from, through, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 10 || rows[0].Metric != "monthly_jetton_senders" || rows[0].Value != 7 || rows[0].IsPartial != 0 || rows[0].DataThrough != "2024-03-01 00:00:00" || rows[5].Value != 0 || rows[5].IsPartial != 1 {
		t.Fatal(rows)
	}
	s.Through = from
	if _, err := monthlyRows(from, through, s); err == nil {
		t.Fatal("missing source became zero")
	}
}

type monthlyWindowSource struct{ from, to time.Time }

func (s *monthlyWindowSource) CollectMonthly(_ context.Context, from, to time.Time) (Snapshot, error) {
	s.from, s.to = from, to
	return Snapshot{Through: to, CalculatedAt: to, SourceMC: 100}, nil
}

func TestMonthlyRefreshCapsCurrentMonthAtAsOf(t *testing.T) {
	source, sink := &monthlyWindowSource{}, &testSink{}
	h := MonthlyHandler{Source: source, Sink: sink, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	now := time.Date(2026, 11, 9, 13, 50, 0, 0, time.UTC)
	req := gramstat.Request{AsOf: now, LookbackDays: 3}
	if err := h.Run(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if !source.from.Equal(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)) || !source.to.Equal(now) || sink.writes != 1 {
		t.Fatal(source, sink)
	}
	req.From = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	req.To = time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	if err := h.Run(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	req.To = time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	if err := h.Run(context.Background(), req); err == nil {
		t.Fatal("truncated past month accepted")
	}
}
