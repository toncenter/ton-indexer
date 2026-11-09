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

func stakePtr[T any](v T) *T { return &v }

func TestStakeExactPrincipalAndProvenance(t *testing.T) {
	input := StakeInput{Since: 100, Until: 200, ElectionID: stakePtr(uint32(90)), Submitted: stakePtr("900719925474099312345"), Selected: stakePtr("800719925474099312345"), ElectionLinks: 1}
	proof := Coverage{Hash: strings.Repeat("a", 64)}
	result := StakeResult{ElectionID: 90, Remaining: "800719925474099312344", VSetHash: strings.Repeat("A", 64), MC: 50, Time: 250, TxHash: "release"}
	now := time.Unix(300, 0)
	row, err := prepareStake(input, proof, &result, "raw", now)
	if err != nil || row.Status != "complete" || *row.Remaining != "800719925474099312344" {
		t.Fatalf("precision/provenance lost: %+v %v", row, err)
	}
	later, err := prepareStake(input, proof, &result, "raw", now.Add(time.Hour))
	if err != nil || later.SnapshotHash != row.SnapshotHash {
		t.Fatal("refresh clock changes content hash")
	}
	for _, test := range []struct {
		name   string
		change func(*StakeResult)
	}{
		{"wrong set", func(r *StakeResult) { r.VSetHash = strings.Repeat("b", 64) }},
		{"wrong election", func(r *StakeResult) { r.ElectionID++ }},
		{"greater than selected", func(r *StakeResult) { r.Remaining = "800719925474099312346" }},
		{"fractional", func(r *StakeResult) { r.Remaining = "1.5" }},
		{"future release", func(r *StakeResult) { r.Time = 301 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := result
			test.change(&r)
			row, err := prepareStake(input, proof, &r, "raw", now)
			if err == nil || row.Remaining != nil || row.Selected == nil || row.Status != "invalid_source" {
				t.Fatalf("bad result accepted: %+v %v", row, err)
			}
		})
	}
	row, err = prepareStake(input, proof, nil, "raw", now)
	if err != nil || row.Remaining != nil || row.Status != "missing_result" {
		t.Fatal("missing is not zero")
	}
	result.Remaining = "0"
	row, err = prepareStake(input, proof, &result, "raw", now)
	if err != nil || row.Remaining == nil || *row.Remaining != "0" {
		t.Fatal("known zero lost")
	}
	input.ElectionLinks = 2
	row, err = prepareStake(input, proof, &result, "raw", now)
	if err == nil || row.Submitted != nil || row.Selected != nil || row.Remaining != nil {
		t.Fatal("ambiguous cycle was published")
	}
}

type stakeFixture struct {
	inputs  []StakeInput
	results map[uint32]StakeResult
	hashes  map[uint32]string
	written []StakeRound
	from    time.Time
}

func (s *stakeFixture) CollectStake(_ context.Context, from, _ time.Time) ([]StakeInput, error) {
	s.from = from
	return s.inputs, nil
}
func (s *stakeFixture) Coverage(context.Context, bool) (map[uint32]Coverage, error) {
	return map[uint32]Coverage{100: {Since: 100, Until: 200, Hash: strings.Repeat("a", 64), Version: Version}}, nil
}
func (s *stakeFixture) StakeResults(context.Context, string, time.Time) (map[uint32]StakeResult, error) {
	return s.results, nil
}
func (s *stakeFixture) StakeHashes(context.Context) (map[uint32]string, error) { return s.hashes, nil }
func (s *stakeFixture) WriteStake(_ context.Context, rows []StakeRound) error {
	s.written = rows
	for _, r := range rows {
		s.hashes[r.Since] = r.SnapshotHash
	}
	return nil
}

func TestStakeHourlyRepairsOldResultsWithoutDuplicateWrites(t *testing.T) {
	f := &stakeFixture{inputs: []StakeInput{{Since: 100, Until: 200, ElectionID: stakePtr(uint32(90)), Submitted: stakePtr("1000"), Selected: stakePtr("900"), ElectionLinks: 1}}, results: map[uint32]StakeResult{}, hashes: map[uint32]string{}}
	h := StakeHandler{Source: f, Sink: f, ResultsDB: "raw", Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	req := gramstat.Request{AsOf: time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC), LookbackDays: 3}
	run := func() {
		t.Helper()
		if err := h.Run(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	run()
	if f.from.Unix() != 0 || len(f.written) != 1 || f.written[0].Remaining != nil {
		t.Fatal("old rounds were not reconciled")
	}
	run()
	if len(f.written) != 0 {
		t.Fatal("unchanged data rewritten")
	}
	f.results[90] = StakeResult{ElectionID: 90, Remaining: "890", VSetHash: strings.Repeat("a", 64), MC: 50, Time: 250, TxHash: "release"}
	run()
	if len(f.written) != 1 || *f.written[0].Remaining != "890" {
		t.Fatal("late result outside lookback lost")
	}
	delete(f.results, 90)
	run()
	if len(f.written) != 1 || f.written[0].Remaining != nil {
		t.Fatal("removed result left stale value")
	}
}
