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

const LockedVersion = "stake-locked-v1"

//go:embed locked_schema.sql
var LockedSchema string

// These are contract families, not a whitelist of official deployments. In
// particular, whales_proxy includes conventional pools as well as liquid pools.
func poolFamily(contractType string) string {
	switch contractType {
	case "liquid_staking_controller":
		return "Tonstakers-like"
	case "hipo_controller":
		return "Hipo-like"
	case "whales_proxy":
		return "Whales-like"
	case "bemo_nominator":
		return "Bemo-like"
	}
	return ""
}

type LockedMember struct {
	Since        uint32  `json:"utime_since"`
	Index        uint16  `json:"idx"`
	Pubkey       string  `json:"pubkey"`
	ADNL         string  `json:"adnl_addr"`
	Holder       string  `json:"holder"`
	Stake        *string `json:"stake_nanogram"`
	ContractType *string `json:"contract_type"`
	TypeStatus   string  `json:"type_status"`
	Family       string  `json:"family"`
	Protocol     string  `json:"protocol"`
	Evidence     string  `json:"affiliation_evidence"`
	ISP          *string `json:"isp"`
	ISPStatus    string  `json:"isp_status"`
	ObservedAt   *uint32 `json:"observed_at"`
	RecordID     *string `json:"record_id"`
	Group        string  `json:"group"`
}

type LockedRound struct {
	Since            uint32            `json:"utime_since"`
	Until            uint32            `json:"utime_until"`
	PreviousSince    uint32            `json:"previous_utime_since"`
	ConfigHash       string            `json:"config34_hash"`
	Digest           string            `json:"roster_digest"`
	PreviousHash     string            `json:"previous_config34_hash"`
	PreviousDigest   string            `json:"previous_roster_digest"`
	CurrentSelected  *string           `json:"current_selected_nanogram"`
	PreviousSelected *string           `json:"previous_selected_nanogram"`
	Total            *string           `json:"total_nanogram"`
	Telegram         *string           `json:"telegram_nanogram"`
	Pools            *string           `json:"pools_nanogram"`
	Others           *string           `json:"others_nanogram"`
	Unattributed     *string           `json:"unattributed_nanogram"`
	Breakdown        map[string]string `json:"pool_breakdown"`
	Positions        uint32            `json:"positions"`
	Typed            uint32            `json:"typed_positions"`
	ISPObserved      uint32            `json:"isp_observed_positions"`
	Official         uint32            `json:"affiliated_pool_positions"`
	Overlaps         uint32            `json:"overlap_positions"`
	Through          uint32            `json:"data_through"`
	PreviousThrough  uint32            `json:"previous_data_through"`
	Partial          uint8             `json:"is_partial"`
	Status           string            `json:"status"`
	Members          []LockedMember    `json:"members"`
	SnapshotHash     string            `json:"snapshot_hash"`
	Version          string            `json:"calc_version"`
	CalculatedAt     string            `json:"calculated_at"`
}

type LockedSource interface {
	StakeSource
	CollectStakeMembersRange(context.Context, time.Time, time.Time) (map[uint32]StakeMembers, error)
}

type AttributionSource interface {
	CollectAttribution(context.Context, []Round, map[uint32]GeoRound) (Attribution, error)
	CollectTelegram(context.Context, time.Time) (map[string]TelegramEvidence, error)
}

type LockedSink interface {
	Coverage(context.Context, bool) (map[uint32]Coverage, error)
	Rounds(context.Context, []uint32) ([]Round, error)
	GeoRounds(context.Context, []uint32) (map[uint32]GeoRound, error)
	LockedCoverage(context.Context) (map[uint32]LockedRound, error)
	WriteLocked(context.Context, []LockedRound) error
}

type LockedHandler struct {
	Source      LockedSource
	Attribution AttributionSource
	Resolver    *AffiliationResolver
	Sink        LockedSink
	Log         *slog.Logger
}

func (*LockedHandler) Name() string { return "validator_stake_locked" }

func (h *LockedHandler) Run(ctx context.Context, req gramstat.Request) error {
	from, to, err := req.DailyWindow()
	if err != nil {
		return err
	}
	if req.AsOf.Before(to) {
		to = req.AsOf
	}
	inputs, err := h.Source.CollectStake(ctx, time.Unix(0, 0).UTC(), to)
	if err != nil {
		return err
	}
	proofs, err := h.Sink.Coverage(ctx, false)
	if err != nil {
		return err
	}
	if len(proofs) == 0 {
		return fmt.Errorf("no verified rounds; run validators first")
	}
	stored, err := h.Sink.LockedCoverage(ctx)
	if err != nil {
		return err
	}
	geoProofs, err := h.Sink.Coverage(ctx, true)
	if err != nil {
		return err
	}
	sort.Slice(inputs, func(i, j int) bool { return inputs[i].Since < inputs[j].Since })
	var targets []int
	for i, in := range inputs {
		if int64(in.Since) >= to.Unix() {
			continue
		}
		old, exists := stored[in.Since]
		p := proofs[in.Since]
		if !req.From.IsZero() {
			if int64(in.Until) <= from.Unix() {
				continue
			}
		} else if exists && i > 0 && int64(in.Until) <= from.Unix() {
			prev := inputs[i-1]
			// Refresh corrected verified/geo snapshots, including closed history.
			if old.Version == LockedVersion && old.ConfigHash == p.Hash && old.Digest == p.Digest &&
				old.PreviousSince == prev.Since && old.PreviousHash == proofs[prev.Since].Hash && old.PreviousDigest == proofs[prev.Since].Digest &&
				sameAmount(old.CurrentSelected, in.Selected) && sameAmount(old.PreviousSelected, prev.Selected) &&
				old.Through == geoProofs[in.Since].Through && old.PreviousThrough == geoProofs[prev.Since].Through {
				continue
			}
		}
		targets = append(targets, i)
	}
	if len(targets) == 0 {
		return nil
	}
	telegram, err := h.Attribution.CollectTelegram(ctx, to)
	if err != nil {
		return err
	}
	for start := 0; start < len(targets); start += 16 {
		batch := targets[start:min(start+16, len(targets))]
		idsMap := map[uint32]bool{}
		for _, i := range batch {
			idsMap[inputs[i].Since] = true
			if i > 0 {
				idsMap[inputs[i-1].Since] = true
			}
		}
		var ids []uint32
		for id := range idsMap {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		rounds, err := h.Sink.Rounds(ctx, ids)
		if err != nil {
			return err
		}
		geos, err := h.Sink.GeoRounds(ctx, ids)
		if err != nil {
			return err
		}
		members, err := h.Source.CollectStakeMembersRange(ctx, time.Unix(int64(ids[0]), 0), time.Unix(int64(ids[len(ids)-1])+1, 0))
		if err != nil {
			return err
		}
		attrs, err := h.Attribution.CollectAttribution(ctx, rounds, geos)
		if err != nil {
			return err
		}
		attrs.Telegram = telegram
		if h.Resolver != nil {
			attrs.Affiliations, err = h.Resolver.Resolve(ctx, rounds, members, attrs)
			if err != nil {
				return err
			}
		}
		byRound := map[uint32]Round{}
		for _, r := range rounds {
			byRound[r.Since] = r
		}
		var ready []LockedRound
		statuses := map[string]int{}
		for _, i := range batch {
			in := inputs[i]
			var prev StakeInput
			if i > 0 {
				prev = inputs[i-1]
			}
			r := prepareLocked(in, prev, byRound, members, geos, attrs, to)
			sealLocked(&r, req.AsOf)
			statuses[r.Status]++
			if stored[in.Since].SnapshotHash != r.SnapshotHash {
				ready = append(ready, r)
			}
		}
		if err = h.Sink.WriteLocked(ctx, ready); err != nil {
			return err
		}
		h.Log.Info("two-round stake published", "changed", len(ready), "statuses", statuses)
	}
	return nil
}

func prepareLocked(current, previous StakeInput, rounds map[uint32]Round, members map[uint32]StakeMembers, geos map[uint32]GeoRound, attrs Attribution, asof time.Time) LockedRound {
	c, p := rounds[current.Since], rounds[previous.Since]
	r := LockedRound{Since: current.Since, Until: current.Until, PreviousSince: previous.Since,
		ConfigHash: c.ConfigHash, Digest: c.RosterDigest, PreviousHash: p.ConfigHash, PreviousDigest: p.RosterDigest,
		Version: LockedVersion, Status: "missing_adjacent_round", Breakdown: map[string]string{}, Members: []LockedMember{}}
	if previous.Until != current.Since || previous.Since == 0 {
		return r
	}
	for _, in := range []StakeInput{previous, current} {
		round := rounds[in.Since]
		digest := round.RosterDigest
		if round.Version != Version || round.ConfigHash == "" || digest == "" || round.Since != in.Since || round.Until != in.Until || round.validate() != nil || round.RosterDigest != digest || round.Through <= round.Since || int64(round.Since) >= asof.Unix() {
			r.Status = "unverified_round"
			return r
		}
		if in.Since == previous.Since && round.Through < round.Until {
			r.Status = "incomplete_previous_round"
			return r
		}
		if in.Selected == nil || in.ElectionID == nil || in.ElectionLinks != 1 {
			r.Status = "missing_selected_stake"
			return r
		}
		if _, err := holderStakes(members[in.Since], digest, *in.Selected); err != nil {
			r.Status = "incomplete_principal"
			return r
		}
	}
	r.CurrentSelected, r.PreviousSelected = current.Selected, previous.Selected
	a, _ := stakeAmount(current.Selected)
	b, _ := stakeAmount(previous.Selected)
	total := new(big.Int).Add(a, b)
	r.Total = amountString(total)
	telegram, pools, unknown := new(big.Int), new(big.Int), new(big.Int)
	families := map[string]*big.Int{}
	missingRoundAttribution := false
	for _, round := range []Round{p, c} {
		typedBefore, ispBefore := r.Typed, r.ISPObserved
		geo := geos[round.Since]
		geoValid := geo.Version == Version && geo.ConfigHash == round.ConfigHash && geo.RosterDigest == round.RosterDigest && geo.Until == round.Until && geo.Through > round.Since && geo.Through <= round.Through && int64(geo.Through) <= asof.Unix()
		observations := map[string]GeoMember{}
		if geoValid {
			if round.Since == current.Since {
				r.Through = geo.Through
			} else {
				r.PreviousThrough = geo.Through
			}
			for _, g := range geo.Members {
				observations[g.Pubkey] = g
			}
		}
		for _, m := range round.Members {
			pos := members[round.Since].Positions[m.Pubkey]
			x := LockedMember{Since: round.Since, Index: m.Index, Pubkey: m.Pubkey, ADNL: m.ADNL, Holder: strings.ToUpper(pos.Holder), Stake: amountString(pos.Stake), Group: "others", TypeStatus: "missing_type", ISPStatus: "missing_observation"}
			if typ, ok := attrs.Types[[2]uint32{round.Since, uint32(m.Index)}]; ok {
				x.TypeStatus = "conflicting_type"
				if !typ.Conflict && typ.Until == round.Until && typ.Pubkey == m.Pubkey && typ.ADNL == m.ADNL && typ.Holder == x.Holder && typ.Weight == m.Weight {
					x.ContractType = typ.ContractType
					if typ.ContractType != nil && *typ.ContractType != "" {
						x.TypeStatus = "observed"
						r.Typed++
						x.Family = poolFamily(*typ.ContractType)
					}
				}
			}
			g, ok := observations[m.Pubkey]
			if ok && g.ADNL == m.ADNL && g.OrderingConflicts == 0 && g.ObservedAt != nil && g.RecordID != nil && *g.ObservedAt >= round.Since && *g.ObservedAt < geo.Through {
				x.ObservedAt, x.RecordID = g.ObservedAt, g.RecordID
				if isp, ok := attrs.ISPs[*g.RecordID]; ok && !isp.Conflict && isp.ADNL == m.ADNL && isp.Time == *g.ObservedAt {
					x.ISP = isp.ISP
					if isp.ISP != nil && *isp.ISP != "" {
						x.ISPStatus = "observed"
						r.ISPObserved++
					}
				}
			}
			// First observation is never applied before its timestamp. A conflicting
			// ISP observed before this cutoff disables historical carry-forward.
			cutoff := uint32(min(int64(round.Through), asof.Unix()))
			if evidence, ok := attrs.Telegram[m.ADNL]; ok && evidence.First < cutoff && (evidence.FirstOther == nil || *evidence.FirstOther >= cutoff) && x.ISPStatus != "observed" {
				label := "Telegram Messenger"
				x.ISP = &label
				x.ISPStatus = "historical_telegram"
				x.ObservedAt, x.RecordID = &evidence.First, &evidence.RecordID
				r.ISPObserved++
			}
			isTG := x.ISP != nil && *x.ISP == "Telegram Messenger"
			if x.Family != "" {
				x.Protocol, x.Evidence = affiliationFor(round.Since, x.Holder, *x.ContractType, attrs.Affiliations)
				if x.Protocol != "" {
					r.Official++
				}
			}
			if isTG && x.Family != "" {
				x.Group = "conflict"
				r.Overlaps++
			} else if isTG {
				x.Group = "telegram"
				telegram.Add(telegram, pos.Stake)
			} else if x.Family != "" {
				x.Group = "pools"
				pools.Add(pools, pos.Stake)
				label := x.Family
				if x.Protocol != "" {
					label = x.Protocol
				}
				if families[label] == nil {
					families[label] = new(big.Int)
				}
				families[label].Add(families[label], pos.Stake)
			} else if x.TypeStatus != "observed" || x.ISPStatus != "observed" || (x.ContractType != nil && (*x.ContractType == "other" || *x.ContractType == "unknown")) {
				unknown.Add(unknown, pos.Stake)
			}
			r.Members = append(r.Members, x)
		}
		missingRoundAttribution = missingRoundAttribution || r.Typed == typedBefore || r.ISPObserved == ispBefore
	}
	r.Positions = uint32(len(r.Members))
	r.Unattributed = amountString(unknown)
	if c.Through < c.Until {
		r.Partial = 1
	}
	if r.Overlaps > 0 {
		r.Status = "overlapping_attribution"
		return r
	}
	if missingRoundAttribution {
		r.Status = "missing_attribution"
		return r
	}
	r.Telegram, r.Pools = amountString(telegram), amountString(pools)
	r.Others = amountString(new(big.Int).Sub(new(big.Int).Sub(total, telegram), pools))
	for label, v := range families {
		r.Breakdown[label] = v.String()
	}
	r.Status = "complete"
	if r.Typed < r.Positions || r.ISPObserved < r.Positions {
		r.Status = "partial_attribution"
	}
	return r
}

func amountString(n *big.Int) *string { s := n.String(); return &s }

func sameAmount(a, b *string) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }

func sealLocked(r *LockedRound, now time.Time) {
	r.SnapshotHash, r.CalculatedAt = "", ""
	data, _ := json.Marshal(r)
	hash := sha256.Sum256(data)
	r.SnapshotHash = hex.EncodeToString(hash[:])
	r.CalculatedAt = timestamp(now)
}
