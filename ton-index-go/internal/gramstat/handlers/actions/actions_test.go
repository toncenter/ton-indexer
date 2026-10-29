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
	if err != nil || len(rows) != 28 {
		t.Fatalf("only covered days should be emitted: %v, %v", rows, err)
	}
	want := map[string]uint64{"jetton_senders": 2, "jetton_receivers": 3, "jetton_transfers": 5,
		"nft_senders": 4, "nft_receivers": 6, "nft_transfers": 7,
		"swaps_total": 0, "swaps_users": 0, "ton_dns_mints": 0, "liquid_staking_deposits": 0,
		"liquid_staking_deposits_tonstakers": 0, "liquid_staking_deposits_stakee": 0,
		"liquid_staking_deposits_kton": 0, "liquid_staking_deposits_sgram": 0}
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

func TestBreakdownsAndExactStakingAmounts(t *testing.T) {
	ptr := func(s string) *string { return &s }
	day := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	d := DayCounts{Day: day, Swaps: 3, SwapUsers: 1,
		SwapsByDEX:            map[string]uint64{"stonfi_v2": 2, "unknown": 1},
		LiquidStakingDeposits: 2, LiquidStakingAmount: ptr("100000000000000000004.00"),
		LiquidStakingByPool: map[string]PoolDeposits{
			"tonstakers": {Count: 1, AmountNanoton: ptr("100000000000000000001")},
			"stakee":     {Count: 1, AmountNanoton: ptr("3")},
		},
	}
	snapshot := Snapshot{Days: []DayCounts{d}, Through: day.Add(24 * time.Hour), CalculatedAt: day.Add(25 * time.Hour), SourceMC: 100}
	rows, err := dailyRows(day, snapshot.Through, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		switch row.Metric {
		case "liquid_staking_deposits":
			if row.Value != 2 || row.AmountNanoton == nil || *row.AmountNanoton != "100000000000000000004" {
				t.Fatalf("precision lost: %+v", row)
			}
		case "liquid_staking_deposits_kton":
			if row.Value != 0 || row.AmountNanoton == nil || *row.AmountNanoton != "0" {
				t.Fatalf("missing zero pool: %+v", row)
			}
		case "swaps_total":
			if row.Value != 3 || row.Breakdown["stonfi_v2"] != 2 || row.Breakdown["unknown"] != 1 {
				t.Fatalf("wrong breakdown: %+v", row)
			}
		}
	}
	// Unknown amount in one nonempty pool makes only its and the total sum NULL.
	snapshot.Days[0].LiquidStakingAmount = nil
	snapshot.Days[0].LiquidStakingByPool["tonstakers"] = PoolDeposits{Count: 1}
	rows, err = dailyRows(day, snapshot.Through, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Metric == "liquid_staking_deposits" && (row.Value != 2 || row.AmountNanoton != nil) {
			t.Fatalf("unknown became zero: %+v", row)
		}
		if row.Metric == "liquid_staking_deposits_stakee" && (row.AmountNanoton == nil || *row.AmountNanoton != "3") {
			t.Fatalf("known pool lost: %+v", row)
		}
	}
	snapshot.Days[0].LiquidStakingAmount = ptr("3")
	if _, err := dailyRows(day, snapshot.Through, snapshot); err == nil {
		t.Fatal("partial sum accepted as full total")
	}
	for _, invalid := range []string{"-1", "0.5", "100000000000000000000000000000000000000"} {
		if _, err := canonicalAmount(&invalid, 1); err == nil {
			t.Fatalf("invalid amount accepted: %s", invalid)
		}
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
