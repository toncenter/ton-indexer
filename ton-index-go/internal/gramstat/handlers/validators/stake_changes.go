package validators

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

const StakeChangesVersion = "stake-changes-v1"

//go:embed stake_changes_schema.sql
var StakeChangesSchema string

type StakePosition struct {
	Holder string
	Stake  *big.Int
}

type StakeMembers struct {
	Digest    string
	Positions map[string]StakePosition // normalized public key; holder keys survive rotation
}

type StakeChangeRound struct {
	Since             uint32  `json:"utime_since"`
	Until             uint32  `json:"utime_until"`
	ConfigHash        string  `json:"config34_hash"`
	Digest            string  `json:"roster_digest"`
	PreviousSince     uint32  `json:"previous_utime_since"`
	MiddleSince       uint32  `json:"middle_utime_since"`
	PreviousHash      string  `json:"previous_config34_hash"`
	PreviousDigest    string  `json:"previous_roster_digest"`
	MiddleHash        string  `json:"middle_config34_hash"`
	MiddleDigest      string  `json:"middle_roster_digest"`
	CurrentSelected   *string `json:"current_selected_nanogram"`
	PreviousSelected  *string `json:"previous_selected_nanogram"`
	PreviousRemaining *string `json:"previous_remaining_nanogram"`
	PreviousReward    *string `json:"previous_reward_nanogram"`
	ResultMC          *uint32 `json:"result_mc"`
	PreviousHolders   *uint32 `json:"previous_holders"`
	CurrentHolders    *uint32 `json:"current_holders"`
	Matched           *uint32 `json:"matched"`
	New               *uint32 `json:"new_holders"`
	Exited            *uint32 `json:"exited_holders"`
	NearReward        *uint32 `json:"near_reward"`
	BelowReward       *uint32 `json:"below_reward"`
	AboveReward       *uint32 `json:"above_reward"`
	Unchanged         *uint32 `json:"unchanged"`
	Decreased         *uint32 `json:"decreased"`
	Status            string  `json:"status"`
	SourceDigest      string  `json:"source_digest"`
	SnapshotHash      string  `json:"snapshot_hash"`
	Version           string  `json:"calc_version"`
	CalculatedAt      string  `json:"calculated_at"`
}

type StakeChangesSource interface {
	StakeSource
	CollectStakeMembers(context.Context, time.Time) (map[uint32]StakeMembers, error)
}

type StakeChangesSink interface {
	Coverage(context.Context, bool) (map[uint32]Coverage, error)
	StakeResults(context.Context, string, time.Time) (map[uint32]StakeResult, error)
	StakeMemberMinima(context.Context, string, []StakeRound) (map[uint32]map[string]*big.Int, error)
	StakeChangeHashes(context.Context) (map[uint32]string, error)
	WriteStakeChanges(context.Context, []StakeChangeRound) error
}

type StakeChangesHandler struct {
	Source    StakeChangesSource
	Sink      StakeChangesSink
	ResultsDB string
	Log       *slog.Logger
}

func (*StakeChangesHandler) Name() string { return "validator_stake_changes" }

func (h *StakeChangesHandler) Run(ctx context.Context, req gramstat.Request) error {
	from, to, err := req.DailyWindow()
	if err != nil {
		return err
	}
	if req.From.IsZero() {
		from = time.Unix(0, 0).UTC()
	}
	if req.AsOf.Before(to) {
		to = req.AsOf
	}
	// Read the whole small cycle history, including predecessors outside an
	// explicit window. Never derive n-2 by subtracting a fixed number of seconds.
	inputs, err := h.Source.CollectStake(ctx, time.Unix(0, 0).UTC(), to)
	if err != nil {
		return err
	}
	proofs, err := h.Sink.Coverage(ctx, false)
	if err != nil {
		return err
	}
	results, err := h.Sink.StakeResults(ctx, h.ResultsDB, req.AsOf)
	if err != nil {
		return err
	}
	started := time.Now()
	members, err := h.Source.CollectStakeMembers(ctx, to)
	if err != nil {
		return err
	}
	h.Log.Info("stake change holders read", "rounds", len(members), "elapsed", time.Since(started))
	stored, err := h.Sink.StakeChangeHashes(ctx)
	if err != nil {
		return err
	}
	sort.Slice(inputs, func(i, j int) bool { return inputs[i].Since < inputs[j].Since })
	stakes := make(map[uint32]StakeRound)
	var fined []StakeRound
	for _, in := range inputs {
		p := proofs[in.Since]
		if p.Version != Version || p.Until != in.Until || p.Hash == "" {
			continue
		}
		var result *StakeResult
		if in.ElectionID != nil {
			if r, ok := results[*in.ElectionID]; ok {
				result = &r
			}
		}
		r, _ := prepareStake(in, p, result, h.ResultsDB, req.AsOf)
		stakes[in.Since] = r
		if r.Remaining != nil && r.Selected != nil && *r.Remaining != *r.Selected {
			fined = append(fined, r)
		}
	}
	minima, err := h.Sink.StakeMemberMinima(ctx, h.ResultsDB, fined)
	if err != nil {
		return err
	}
	var changed []StakeChangeRound
	statuses := map[string]int{}
	for i, in := range inputs {
		if int64(in.Until) <= from.Unix() {
			continue
		}
		p := proofs[in.Since]
		r := StakeChangeRound{Since: in.Since, Until: in.Until, ConfigHash: p.Hash, Digest: p.Digest, Version: StakeChangesVersion, Status: "missing_predecessors"}
		if i >= 2 {
			old, mid := inputs[i-2], inputs[i-1]
			prepareStakeChanges(&r, old, mid, in, proofs, stakes, members, minima, req.AsOf)
		}
		data, _ := json.Marshal(r)
		hash := sha256.Sum256(data)
		r.SnapshotHash = hex.EncodeToString(hash[:])
		r.CalculatedAt = timestamp(req.AsOf)
		statuses[r.Status]++
		if stored[r.Since] != r.SnapshotHash {
			changed = append(changed, r)
		}
	}
	if err = h.Sink.WriteStakeChanges(ctx, changed); err != nil {
		return err
	}
	h.Log.Info("validator stake changes published", "changed", len(changed), "statuses", statuses)
	return nil
}

func prepareStakeChanges(r *StakeChangeRound, old, mid, current StakeInput, proofs map[uint32]Coverage, stakes map[uint32]StakeRound, members map[uint32]StakeMembers, minima map[uint32]map[string]*big.Int, now time.Time) {
	r.PreviousSince, r.MiddleSince = old.Since, mid.Since
	p, m, c := proofs[old.Since], proofs[mid.Since], proofs[current.Since]
	r.PreviousHash, r.PreviousDigest, r.MiddleHash, r.MiddleDigest = p.Hash, p.Digest, m.Hash, m.Digest
	r.Status = "unverified_intervals"
	for _, in := range []StakeInput{old, mid, current} {
		proof := proofs[in.Since]
		if proof.Version != Version || proof.Since != in.Since || proof.Until != in.Until || proof.Hash == "" || proof.Digest == "" || in.Since >= in.Until {
			return
		}
	}
	if old.Until != mid.Since || mid.Until != current.Since || p.Through < p.Until || m.Through < m.Until || int64(c.Since) > now.Unix() {
		return
	}
	before, after := stakes[old.Since], stakes[current.Since]
	r.CurrentSelected, r.PreviousSelected, r.PreviousRemaining, r.PreviousReward, r.ResultMC = after.Selected, before.Selected, before.Remaining, before.Reward, before.UnfreezeMC
	r.Status = "missing_stake_or_reward"
	if before.Status == "invalid_source" || after.Status == "invalid_source" || before.Selected == nil || after.Selected == nil || before.Remaining == nil || before.Reward == nil || before.UnfreezeTime == nil {
		return
	}
	if *before.UnfreezeTime > current.Since {
		r.Status = "late_release"
		return
	}
	previous, err := holderStakes(members[old.Since], p.Digest, *before.Selected)
	if err != nil {
		r.Status = "incomplete_previous_holders"
		return
	}
	next, err := holderStakes(members[current.Since], c.Digest, *after.Selected)
	if err != nil {
		r.Status = "incomplete_current_holders"
		return
	}
	remaining := previous
	if *before.Remaining != *before.Selected {
		final := StakeMembers{Digest: p.Digest, Positions: map[string]StakePosition{}}
		for key, pos := range members[old.Since].Positions {
			amount := minima[old.Since][key]
			if amount == nil || amount.Sign() < 0 || amount.Cmp(pos.Stake) > 0 {
				r.Status = "unknown_penalty_allocation"
				return
			}
			final.Positions[key] = StakePosition{Holder: pos.Holder, Stake: amount}
		}
		remaining, err = holderStakes(final, p.Digest, *before.Remaining)
		if err != nil {
			r.Status = "unknown_penalty_allocation"
			return
		}
	}
	total, _ := stakeAmount(before.Remaining)
	reward, _ := stakeAmount(before.Reward)
	if total == nil || total.Sign() == 0 || reward == nil {
		r.Status = "missing_stake_or_reward"
		return
	}
	// Include source amounts/mapping even if corrections keep category counts unchanged.
	data, _ := json.Marshal([]any{previous, next, remaining, before.UnfreezeHash})
	hash := sha256.Sum256(data)
	r.SourceDigest = hex.EncodeToString(hash[:])
	nOld, nNew := uint32(len(previous)), uint32(len(next))
	var matched, added, exited uint32
	var counts [5]uint32
	for holder, capital := range previous {
		newCapital, ok := next[holder]
		if !ok {
			exited++
			continue
		}
		matched++
		delta := new(big.Int).Sub(newCapital, capital)
		counts[classifyStakeChange(delta, remaining[holder], reward, total)]++
	}
	for holder := range next {
		if _, ok := previous[holder]; !ok {
			added++
		}
	}
	r.PreviousHolders, r.CurrentHolders, r.Matched, r.New, r.Exited = &nOld, &nNew, &matched, &added, &exited
	if matched == 0 {
		r.Status = "no_matched_holders"
		return
	}
	r.NearReward, r.BelowReward, r.AboveReward, r.Unchanged, r.Decreased = &counts[0], &counts[1], &counts[2], &counts[3], &counts[4]
	r.Status = "complete"
}

func holderStakes(m StakeMembers, digest, expected string) (map[string]*big.Int, error) {
	if m.Digest != digest || digest == "" || len(m.Positions) == 0 {
		return nil, fmt.Errorf("unverified members")
	}
	out := map[string]*big.Int{}
	sum := new(big.Int)
	for _, pos := range m.Positions {
		parts := strings.Split(pos.Holder, ":")
		if len(parts) != 2 || parts[0] != "-1" || pos.Stake == nil || pos.Stake.Sign() < 0 {
			return nil, fmt.Errorf("missing position")
		}
		hash, err := normalizeHash(parts[1])
		if err != nil {
			return nil, err
		}
		holder := "-1:" + hash
		if out[holder] == nil {
			out[holder] = new(big.Int)
		}
		out[holder].Add(out[holder], pos.Stake)
		sum.Add(sum, pos.Stake)
	}
	want, err := stakeAmount(&expected)
	if err != nil || sum.Cmp(want) != 0 {
		return nil, fmt.Errorf("incomplete principal")
	}
	return out, nil
}

// Categories: near reward, positive below reward, above reward, unchanged, down.
// Compare exact rational reward shares: |delta - principal*reward/total| <=
// max(2% of that reward, 5 GRAM). Zero/negative changes take precedence.
func classifyStakeChange(delta, principal, reward, total *big.Int) int {
	if delta.Sign() == 0 {
		return 3
	}
	if delta.Sign() < 0 {
		return 4
	}
	expected := new(big.Int).Mul(principal, reward)
	actual := new(big.Int).Mul(delta, total)
	difference := new(big.Int).Abs(new(big.Int).Sub(actual, expected))
	if new(big.Int).Mul(difference, big.NewInt(50)).Cmp(expected) <= 0 || difference.Cmp(new(big.Int).Mul(big.NewInt(5_000_000_000), total)) <= 0 {
		return 0
	}
	if actual.Cmp(expected) < 0 {
		return 1
	}
	return 2
}
