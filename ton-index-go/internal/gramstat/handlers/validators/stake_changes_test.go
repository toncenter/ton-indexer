package validators

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

func TestStakeChangeExactClassification(t *testing.T) {
	i := func(v int64) *big.Int { return big.NewInt(v) }
	// 1,000 GRAM expected reward; inclusive 2% tolerance dominates 5 GRAM.
	for _, tc := range []struct {
		delta int64
		want  int
	}{
		{0, 3}, {-1, 4}, {980_000_000_000, 0}, {1_020_000_000_000, 0},
		{979_999_999_999, 1}, {1_020_000_000_001, 2}, {500_000_000_000, 1},
	} {
		if got := classifyStakeChange(i(tc.delta), i(100_000_000_000_000), i(10_000_000_000_000), i(1_000_000_000_000_000)); got != tc.want {
			t.Fatalf("%d: %d != %d", tc.delta, got, tc.want)
		}
	}
	// Exact nanoGRAM boundary above float's integer precision, with absolute tolerance.
	principal, _ := new(big.Int).SetString("900719925474099312345", 10)
	for _, tc := range []struct {
		delta int64
		want  int
	}{{15_000_000_000, 0}, {15_000_000_001, 2}, {4_999_999_999, 1}} {
		if got := classifyStakeChange(i(tc.delta), principal, i(10_000_000_000), principal); got != tc.want {
			t.Fatal(tc, got)
		}
	}
}

func changesFixture() ([]StakeInput, map[uint32]Coverage, map[uint32]StakeRound, map[uint32]StakeMembers) {
	inputs := []StakeInput{{Since: 100, Until: 200}, {Since: 200, Until: 300}, {Since: 300, Until: 400}}
	proofs := map[uint32]Coverage{}
	for _, in := range inputs {
		proofs[in.Since] = Coverage{Since: in.Since, Until: in.Until, Through: in.Until, Hash: strings.Repeat("a", 64), Digest: fmt.Sprint(in.Since), Version: Version}
	}
	members := map[uint32]StakeMembers{}
	for _, since := range []uint32{100, 300} {
		m := StakeMembers{Digest: fmt.Sprint(since), Positions: map[string]StakePosition{}}
		for j := 0; j < 6; j++ {
			holderID := j
			amount := int64(100000)
			if since == 300 {
				amount += []int64{1000, 500, 1500, 0, -100, 0}[j]
				if j == 5 {
					holderID = 6
				}
			}
			m.Positions[fmt.Sprintf("%064x", int(since)+j)] = StakePosition{Holder: fmt.Sprintf("-1:%064x", holderID), Stake: big.NewInt(amount * 1_000_000_000)}
		}
		members[since] = m
	}
	stakes := map[uint32]StakeRound{
		100: {Since: 100, Until: 200, Selected: stakePtr("600000000000000"), Remaining: stakePtr("600000000000000"), Reward: stakePtr("6000000000000"), UnfreezeTime: stakePtr(uint32(250)), UnfreezeMC: stakePtr(uint32(30)), Status: "complete"},
		300: {Since: 300, Until: 400, Selected: stakePtr("602900000000000"), Status: "missing_result"},
	}
	return inputs, proofs, stakes, members
}

func TestStakeChangesTwoChainsKeyRotationCoverageAndPenalties(t *testing.T) {
	inputs, proofs, stakes, members := changesFixture()
	build := func(minima map[uint32]map[string]*big.Int) StakeChangeRound {
		r := StakeChangeRound{Since: 300, Until: 400}
		prepareStakeChanges(&r, inputs[0], inputs[1], inputs[2], proofs, stakes, members, minima, time.Unix(350, 0))
		return r
	}
	r := build(nil)
	if r.Status != "complete" || *r.Matched != 5 || *r.New != 1 || *r.Exited != 1 || *r.PreviousHolders != 6 || *r.CurrentHolders != 6 {
		t.Fatalf("coverage/rotation: %+v", r)
	}
	for _, v := range []*uint32{r.NearReward, r.BelowReward, r.AboveReward, r.Unchanged, r.Decreased} {
		if v == nil || *v != 1 {
			t.Fatal("five exclusive groups", r)
		}
	}
	// Multiple keys under one holder count once.
	m := members[100]
	key := fmt.Sprintf("%064x", 100)
	pos := m.Positions[key]
	pos.Stake = new(big.Int).Div(pos.Stake, big.NewInt(2))
	m.Positions[key] = pos
	m.Positions["extra"] = pos
	members[100] = m
	if r := build(nil); r.Status != "complete" || *r.Matched != 5 {
		t.Fatal("holder split counted twice")
	}
	// A gap in the middle must not silently connect the two chains.
	inputs[1].Until--
	if r := build(nil); r.Status != "unverified_intervals" || r.Matched != nil {
		t.Fatal("gap crossed")
	}
	inputs[1].Until++
	// Fine the first holder completely; allocation must use remaining capital.
	old := stakes[100]
	old.Remaining = stakePtr("500000000000000")
	old.Reward = stakePtr("5000000000000")
	stakes[100] = old
	if r := build(nil); r.Status != "unknown_penalty_allocation" || r.NearReward != nil {
		t.Fatal("unknown fine allocated")
	}
	mins := map[uint32]map[string]*big.Int{100: {}}
	for k, p := range members[100].Positions {
		mins[100][k] = p.Stake
		if p.Holder == fmt.Sprintf("-1:%064x", 0) {
			mins[100][k] = big.NewInt(0)
		}
	}
	r = build(mins)
	if r.Status != "complete" || *r.NearReward != 0 || *r.AboveReward != 2 {
		t.Fatalf("fine not applied to reward share: %+v", r)
	}
	mins[100][key] = big.NewInt(1)
	if r := build(mins); r.Status != "unknown_penalty_allocation" {
		t.Fatal("unreconciled final principal accepted")
	}
	delete(members[300].Positions, fmt.Sprintf("%064x", 300))
	if r := build(nil); r.Status != "incomplete_current_holders" {
		t.Fatal("partial holders shown as complete")
	}
}

type changesStore struct {
	inputs  []StakeInput
	proofs  map[uint32]Coverage
	members map[uint32]StakeMembers
	results map[uint32]StakeResult
	hashes  map[uint32]string
	written []StakeChangeRound
}

func (f *changesStore) CollectStake(context.Context, time.Time, time.Time) ([]StakeInput, error) {
	return f.inputs, nil
}
func (f *changesStore) CollectStakeMembers(context.Context, time.Time) (map[uint32]StakeMembers, error) {
	return f.members, nil
}
func (f *changesStore) Coverage(context.Context, bool) (map[uint32]Coverage, error) {
	return f.proofs, nil
}
func (f *changesStore) StakeResults(context.Context, string, time.Time) (map[uint32]StakeResult, error) {
	return f.results, nil
}
func (f *changesStore) StakeMemberMinima(context.Context, string, []StakeRound) (map[uint32]map[string]*big.Int, error) {
	return nil, nil
}
func (f *changesStore) StakeChangeHashes(context.Context) (map[uint32]string, error) {
	return f.hashes, nil
}
func (f *changesStore) WriteStakeChanges(_ context.Context, rows []StakeChangeRound) error {
	f.written = rows
	for _, r := range rows {
		f.hashes[r.Since] = r.SnapshotHash
	}
	return nil
}

func TestStakeChangesHourlyReconcilesCorrectionsAndClearsMissing(t *testing.T) {
	in, p, stakes, m := changesFixture()
	for i := range in {
		in[i].ElectionID = stakePtr(in[i].Since)
		in[i].ElectionLinks = 1
		in[i].Selected = stakes[in[i].Since].Selected
		in[i].Submitted = in[i].Selected
	}
	f := &changesStore{inputs: in, proofs: p, members: m, hashes: map[uint32]string{}, results: map[uint32]StakeResult{100: {ElectionID: 100, Remaining: *stakes[100].Remaining, Reward: stakes[100].Reward, VSetHash: p[100].Hash, MC: 30, Time: 250, TxHash: "tx"}}}
	h := StakeChangesHandler{Source: f, Sink: f, ResultsDB: "raw", Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	req := gramstat.Request{AsOf: time.Unix(500, 0), LookbackDays: 3}
	run := func() {
		t.Helper()
		if err := h.Run(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	run()
	if len(f.written) != 3 || f.written[2].Status != "complete" {
		t.Fatal("missing initial history", f.written)
	}
	run()
	if len(f.written) != 0 {
		t.Fatal("unchanged history rewritten")
	}
	delete(f.results, 100)
	run()
	if len(f.written) != 1 || f.written[0].NearReward != nil || f.written[0].Status != "missing_stake_or_reward" {
		t.Fatal("stale classification survived", f.written)
	}
}
