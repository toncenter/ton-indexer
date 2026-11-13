package validators

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

func aprFixture() (StakeRound, Coverage, Coverage) {
	current := Coverage{Since: 100, Until: 65636, Through: 65636, Hash: strings.Repeat("a", 64), Digest: "roster", Version: Version}
	next := Coverage{Since: 65636, Until: 131172, Through: 131172, Hash: strings.Repeat("b", 64), Digest: "next", Version: Version}
	r := StakeRound{Since: current.Since, Until: current.Until, Selected: stakePtr("1000000000000000000"),
		Reward: stakePtr("1000000000000000"), Remaining: stakePtr("999000000000000000"),
		UnfreezeTime: stakePtr(uint32(98404)), ConfigHash: current.Hash, RosterDigest: current.Digest, Status: "complete"}
	return r, current, next
}

func TestAPRTwoRoundsAndExactAmounts(t *testing.T) {
	r, current, next := aprFixture()
	now := time.Unix(200000, 0)
	prepareAPR(&r, current, next, now)
	// 0.1% per two-round cycle, NOT the 48.1201171875% one-round rate.
	if r.APR == nil || *r.APR != "24.060058593750" || *r.APRCycleSeconds != 131072 {
		t.Fatalf("wrong two-round annualization: %+v", r)
	}
	// Different historical round lengths must be added, not assumed/doubled.
	next.Until += 32768
	next.Through = next.Until
	prepareAPR(&r, current, next, now)
	if r.APR == nil || *r.APR != "19.248046875000" || *r.APRCycleSeconds != 163840 {
		t.Fatalf("ignored next interval: %+v", r)
	}
	// Identical ratios at 38-digit precision; no float conversion or fines in numerator.
	r.Selected, r.Reward = stakePtr("99999999999999999999999999999999999000"), stakePtr("99999999999999999999999999999999999")
	prepareAPR(&r, current, next, now)
	if r.APR == nil || *r.APR != "19.248046875000" {
		t.Fatal("lost exact nanoGRAM ratio")
	}
	r.Reward = stakePtr("0")
	prepareAPR(&r, current, next, now)
	if r.APR == nil || *r.APR != "0.000000000000" {
		t.Fatal("known zero reward lost")
	}
}

func TestAPRMissingInputsNeverBecomeZero(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*StakeRound, *Coverage, *Coverage)
	}{
		{"no selected", func(r *StakeRound, _, _ *Coverage) { r.Selected = nil }},
		{"zero selected", func(r *StakeRound, _, _ *Coverage) { r.Selected = stakePtr("0") }},
		{"no reward", func(r *StakeRound, _, _ *Coverage) { r.Reward = nil }},
		{"negative reward", func(r *StakeRound, _, _ *Coverage) { r.Reward = stakePtr("-1") }},
		{"no release", func(r *StakeRound, _, _ *Coverage) { r.UnfreezeTime = nil }},
		{"invalid source", func(r *StakeRound, _, _ *Coverage) { r.Status = "invalid_source" }},
		{"current incomplete", func(_ *StakeRound, c, _ *Coverage) { c.Through-- }},
		{"current hash changed", func(_ *StakeRound, c, _ *Coverage) { c.Hash = "changed" }},
		{"empty interval", func(_ *StakeRound, c, _ *Coverage) { c.Until = c.Since }},
		{"missing next", func(_ *StakeRound, _, n *Coverage) { *n = Coverage{} }},
		{"gap", func(_ *StakeRound, _, n *Coverage) { n.Since++ }},
		{"overlap", func(_ *StakeRound, _, n *Coverage) { n.Since-- }},
		{"unverified next", func(_ *StakeRound, _, n *Coverage) { n.Hash = "" }},
		{"next incomplete", func(_ *StakeRound, _, n *Coverage) { n.Through-- }},
		{"next in future", func(_ *StakeRound, _, n *Coverage) { n.Until = 300000; n.Through = n.Until }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, current, next := aprFixture()
			prepareAPR(&r, current, next, time.Unix(200000, 0))
			test.change(&r, &current, &next)
			prepareAPR(&r, current, next, time.Unix(200000, 0))
			if r.APR != nil || r.APRStatus == "complete" {
				t.Fatalf("stale or fabricated APR: %+v", r)
			}
		})
	}
}

func TestAPRHourlyRepairsNextRoundAndOldReward(t *testing.T) {
	r, current, next := aprFixture()
	f := &stakeFixture{
		inputs:   []StakeInput{{Since: current.Since, Until: current.Until, ElectionID: stakePtr(uint32(90)), Submitted: r.Selected, Selected: r.Selected, ElectionLinks: 1}},
		coverage: map[uint32]Coverage{current.Since: current}, hashes: map[uint32]string{},
		results: map[uint32]StakeResult{90: {ElectionID: 90, Remaining: *r.Remaining, Reward: r.Reward, VSetHash: current.Hash, MC: 50, Time: *r.UnfreezeTime, TxHash: "release"}},
	}
	h := StakeHandler{Source: f, Sink: f, ResultsDB: "raw", Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	req := gramstat.Request{AsOf: time.Unix(200000, 0), LookbackDays: 3}
	run := func() {
		t.Helper()
		if err := h.Run(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	run()
	if len(f.written) != 1 || f.written[0].APR != nil {
		t.Fatal("missing successor fabricated")
	}
	// Successor is outside the requested stake inputs but present in verified history.
	f.coverage[next.Since] = next
	run()
	if len(f.written) != 1 || f.written[0].APR == nil || *f.written[0].APR != "24.060058593750" {
		t.Fatal("late successor lost")
	}
	run()
	if len(f.written) != 0 {
		t.Fatal("unchanged APR rewritten")
	}
	result := f.results[90]
	result.Reward = stakePtr("0")
	f.results[90] = result
	run()
	if len(f.written) != 1 || f.written[0].APR == nil || *f.written[0].APR != "0.000000000000" {
		t.Fatal("corrected reward lost")
	}
	delete(f.coverage, next.Since)
	run()
	if len(f.written) != 1 || f.written[0].APR != nil {
		t.Fatal("removed successor left stale APR")
	}
}
