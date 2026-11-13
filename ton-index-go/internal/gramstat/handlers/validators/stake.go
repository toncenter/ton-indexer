package validators

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

const StakeVersion = "stake-v3"

//go:embed stake_schema.sql
var StakeSchema string

type StakeRound struct {
	Since           uint32  `json:"utime_since"`
	Until           uint32  `json:"utime_until"`
	ElectionID      *uint32 `json:"election_id"`
	Submitted       *string `json:"submitted_nanogram"`
	Selected        *string `json:"selected_nanogram"`
	Remaining       *string `json:"remaining_nanogram"`
	Reward          *string `json:"reward_nanogram"`
	APR             *string `json:"apr_percent"`
	APRCycleSeconds *uint32 `json:"apr_cycle_seconds"`
	APRNextUntil    *uint32 `json:"apr_next_utime_until"`
	APRNextHash     *string `json:"apr_next_config34_hash"`
	APRNextDigest   *string `json:"apr_next_roster_digest"`
	APRStatus       string  `json:"apr_status"`
	ElectionMC      *uint32 `json:"election_source_mc_seqno"`
	CycleMC         uint32  `json:"cycle_source_mc_seqno"`
	UnfreezeMC      *uint32 `json:"unfreeze_mc_seqno"`
	UnfreezeTime    *uint32 `json:"unfreeze_utime"`
	UnfreezeHash    *string `json:"unfreeze_tx_hash"`
	ResultHash      *string `json:"result_vset_hash"`
	ConfigHash      string  `json:"config34_hash"`
	RosterDigest    string  `json:"roster_digest"`
	ResultsSource   string  `json:"results_source"`
	Status          string  `json:"status"`
	SnapshotHash    string  `json:"snapshot_hash"`
	Version         string  `json:"calc_version"`
	CalculatedAt    string  `json:"calculated_at"`
}

type StakeResult struct {
	ElectionID uint32  `json:"election_id"`
	Remaining  string  `json:"remaining"`
	Reward     *string `json:"reward"`
	VSetHash   string  `json:"vset_hash"`
	MC         uint32  `json:"mc"`
	Time       uint32  `json:"time"`
	TxHash     string  `json:"tx_hash"`
}

type StakeSource interface {
	CollectStake(context.Context, time.Time, time.Time) ([]StakeInput, error)
}
type StakeSink interface {
	Coverage(context.Context, bool) (map[uint32]Coverage, error)
	StakeResults(context.Context, string, time.Time) (map[uint32]StakeResult, error)
	StakeHashes(context.Context) (map[uint32]string, error)
	WriteStake(context.Context, []StakeRound) error
}
type StakeHandler struct {
	Source    StakeSource
	Sink      StakeSink
	ResultsDB string
	Log       *slog.Logger
}

func (*StakeHandler) Name() string { return "validator_stake" }

func (h *StakeHandler) Run(ctx context.Context, req gramstat.Request) error {
	from, to, err := req.DailyWindow()
	if err != nil {
		return err
	}
	// The source is only a few thousand rows. Reconcile all established history
	// hourly so late results and corrected old amounts are picked up automatically.
	if req.From.IsZero() {
		from = time.Unix(0, 0).UTC()
	}
	if req.AsOf.Before(to) {
		to = req.AsOf
	}
	if !from.Before(to) {
		return fmt.Errorf("validator stake interval is not in the past")
	}
	verified, err := h.Sink.Coverage(ctx, false)
	if err != nil {
		return err
	}
	if len(verified) == 0 {
		return fmt.Errorf("no verified rounds; run validators first")
	}
	inputs, err := h.Source.CollectStake(ctx, from, to)
	if err != nil {
		return err
	}
	results, err := h.Sink.StakeResults(ctx, h.ResultsDB, req.AsOf)
	if err != nil {
		return err
	}
	stored, err := h.Sink.StakeHashes(ctx)
	if err != nil {
		return err
	}
	var ready []StakeRound
	var failures []error
	var submitted, selected, remaining, apr, unverified int
	for _, in := range inputs {
		proof, ok := verified[in.Since]
		if !ok || proof.Version != Version || proof.Until != in.Until || proof.Hash == "" {
			unverified++
			continue
		}
		var result *StakeResult
		if in.ElectionID != nil {
			if r, ok := results[*in.ElectionID]; ok {
				result = &r
			}
		}
		row, err := prepareStake(in, proof, result, h.ResultsDB, req.AsOf)
		if err != nil {
			failures = append(failures, fmt.Errorf("round %d: %w", in.Since, err))
		}
		if row.Submitted != nil {
			submitted++
		}
		if row.Selected != nil {
			selected++
		}
		if row.Remaining != nil {
			remaining++
		}
		prepareAPR(&row, proof, verified[row.Until], req.AsOf)
		sealStake(&row, req.AsOf)
		if row.APR != nil {
			apr++
		}
		if stored[row.Since] != row.SnapshotHash {
			ready = append(ready, row)
		}
	}
	if err = h.Sink.WriteStake(ctx, ready); err != nil {
		return err
	}
	h.Log.Info("validator stake published", "changed", len(ready), "submitted", submitted, "selected", selected, "remaining", remaining, "apr", apr, "unverified", unverified, "invalid", len(failures))
	return errors.Join(failures...)
}

func stakeAmount(s *string) (*big.Int, error) {
	if s == nil {
		return nil, nil
	}
	if len(*s) == 0 || len(*s) > 38 {
		return nil, fmt.Errorf("invalid nanoGRAM amount")
	}
	for _, c := range *s {
		if c < '0' || c > '9' {
			return nil, fmt.Errorf("invalid nanoGRAM amount")
		}
	}
	n, ok := new(big.Int).SetString(*s, 10)
	if !ok {
		return nil, fmt.Errorf("invalid nanoGRAM amount")
	}
	return n, nil
}

func prepareStake(in StakeInput, proof Coverage, result *StakeResult, db string, now time.Time) (StakeRound, error) {
	r := StakeRound{Since: in.Since, Until: in.Until, ElectionID: in.ElectionID, Submitted: in.Submitted, Selected: in.Selected,
		ElectionMC: in.ElectionMC, CycleMC: in.CycleMC, ConfigHash: proof.Hash, RosterDigest: proof.Digest,
		ResultsSource: db + ".validator_round_results", Version: StakeVersion}
	var problem error
	if in.ElectionID == nil {
		// Without the explicit election link none of the three stages can be joined.
		r.Submitted, r.Selected = nil, nil
		r.Status = "missing_election_link"
	} else if in.ElectionLinks != 1 {
		r.Submitted, r.Selected = nil, nil
		problem = fmt.Errorf("ambiguous election link")
	} else {
		submitted, e1 := stakeAmount(r.Submitted)
		selected, e2 := stakeAmount(r.Selected)
		if e1 != nil || e2 != nil || (submitted != nil && selected != nil && selected.Cmp(submitted) > 0) {
			r.Submitted, r.Selected = nil, nil
			problem = fmt.Errorf("invalid submitted/selected principal")
		} else if result != nil {
			remaining, e := stakeAmount(&result.Remaining)
			_, rewardErr := stakeAmount(result.Reward)
			hash, he := normalizeHash(result.VSetHash)
			if e != nil || rewardErr != nil || he != nil || !strings.EqualFold(hash, proof.Hash) || result.ElectionID != *in.ElectionID ||
				result.MC == 0 || result.Time < in.Until || int64(result.Time) > now.Unix() || result.TxHash == "" ||
				(selected != nil && remaining.Cmp(selected) > 0) || (submitted != nil && remaining.Cmp(submitted) > 0) {
				problem = fmt.Errorf("invalid final principal or result provenance")
			} else {
				r.Remaining = &result.Remaining
				r.Reward = result.Reward
				r.UnfreezeMC, r.UnfreezeTime, r.UnfreezeHash, r.ResultHash = &result.MC, &result.Time, &result.TxHash, &hash
			}
		}
		if r.Remaining == nil {
			r.Status = "missing_result"
		} else if r.Submitted == nil || r.Selected == nil {
			r.Status = "partial"
		} else {
			r.Status = "complete"
		}
	}
	if problem != nil {
		r.Status = "invalid_source"
	}
	sealStake(&r, now)
	return r, problem
}

func sealStake(r *StakeRound, now time.Time) {
	// Hash the payload, not the refresh clock or the previous hash.
	r.SnapshotHash, r.CalculatedAt = "", ""
	data, _ := json.Marshal(r)
	hash := sha256.Sum256(data)
	r.SnapshotHash = hex.EncodeToString(hash[:])
	r.CalculatedAt = timestamp(now)
}

var stakeDatabase = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (s Store) StakeResults(ctx context.Context, db string, asOf time.Time) (map[uint32]StakeResult, error) {
	if !stakeDatabase.MatchString(db) {
		return nil, fmt.Errorf("GRAMSTAT_STAKE_RESULTS_DB must be a database identifier")
	}
	data, err := s.CH.Query(ctx, `SELECT election_id,toString(total_stake) AS remaining,toString(bonuses) AS reward,vset_hash,
        unfreeze_mc_seqno AS mc,toUInt32(unfreeze_utime) AS time,unfreeze_tx_hash AS tx_hash
        FROM `+db+`.validator_round_results FINAL
        WHERE unfreeze_utime<=toDateTime({asof:UInt32})
		ORDER BY election_id FORMAT JSONEachRow`, url.Values{"param_asof": {fmt.Sprint(asOf.Unix())}, "readonly": {"1"}, "max_threads": {"2"}, "max_execution_time": {"60"}})
	if err != nil {
		return nil, err
	}
	rows, err := decodeRows[StakeResult](data)
	if err != nil {
		return nil, err
	}
	result := make(map[uint32]StakeResult, len(rows))
	for _, r := range rows {
		if _, exists := result[r.ElectionID]; exists {
			return nil, fmt.Errorf("duplicate final election result")
		}
		result[r.ElectionID] = r
	}
	return result, nil
}

func (s Store) StakeHashes(ctx context.Context) (map[uint32]string, error) {
	data, err := s.CH.Query(ctx, "SELECT utime_since,snapshot_hash FROM gramstat.validator_stake_rounds FINAL FORMAT JSONEachRow", nil)
	if err != nil {
		return nil, err
	}
	rows, err := decodeRows[StakeRound](data)
	result := make(map[uint32]string, len(rows))
	for _, r := range rows {
		result[r.Since] = r.SnapshotHash
	}
	return result, err
}

func (s Store) WriteStake(ctx context.Context, rows []StakeRound) error {
	if len(rows) == 0 {
		return nil
	}
	data, err := encodeRows(rows)
	if err != nil {
		return err
	}
	return s.CH.InsertJSON(ctx, "gramstat.validator_stake_rounds", data)
}
