package validators

import (
	"context"
	"fmt"
	"math/big"
	"net/url"
)

// Fines only reduce frozen principal. Per-member minima observed before release
// are usable ONLY if their complete sum equals the independently recorded final
// principal. Otherwise prepareStakeChanges leaves a gap, rather than distributing
// a round-level fine uniformly or treating missing member stakes as zero.
func (s Store) StakeMemberMinima(ctx context.Context, db string, rounds []StakeRound) (map[uint32]map[string]*big.Int, error) {
	if !stakeDatabase.MatchString(db) {
		return nil, fmt.Errorf("invalid results database")
	}
	out := map[uint32]map[string]*big.Int{}
	for start := 0; start < len(rounds); start += 64 {
		type bound struct {
			Since uint32 `json:"since"`
			MC    uint32 `json:"mc"`
		}
		var bounds []bound
		for _, r := range rounds[start:min(start+64, len(rounds))] {
			if r.UnfreezeMC != nil {
				bounds = append(bounds, bound{r.Since, *r.UnfreezeMC})
			}
		}
		data, err := encodeRows(bounds)
		if err != nil {
			return nil, err
		}
		query := `SELECT m.utime_since AS since,upper(m.validator_pubkey) AS pubkey,toString(min(m.stake)) AS remaining
 FROM ` + db + `.validator_cycle_members AS m FINAL INNER JOIN bounds b ON m.utime_since=b.since
 WHERE m.utime_since IN (SELECT since FROM bounds) AND m.source_mc_seqno<=b.mc
 GROUP BY m.utime_since,pubkey ORDER BY m.utime_since,pubkey FORMAT JSONEachRow`
		data, err = s.CH.QueryExternal(ctx, query, url.Values{"readonly": {"1"}, "max_threads": {"2"}, "max_execution_time": {"90"}}, "bounds", "since UInt32, mc UInt32", data)
		if err != nil {
			return nil, err
		}
		type row struct {
			Since     uint32  `json:"since"`
			Pubkey    string  `json:"pubkey"`
			Remaining *string `json:"remaining"`
		}
		rows, err := decodeRows[row](data)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			n, err := stakeAmount(r.Remaining)
			if err != nil {
				return nil, err
			}
			if out[r.Since] == nil {
				out[r.Since] = map[string]*big.Int{}
			}
			out[r.Since][r.Pubkey] = n
		}
	}
	return out, nil
}

func (s Store) StakeChangeHashes(ctx context.Context) (map[uint32]string, error) {
	data, err := s.CH.Query(ctx, "SELECT utime_since,snapshot_hash FROM gramstat.validator_stake_changes FINAL FORMAT JSONEachRow", nil)
	if err != nil {
		return nil, err
	}
	rows, err := decodeRows[StakeChangeRound](data)
	out := map[uint32]string{}
	for _, r := range rows {
		out[r.Since] = r.SnapshotHash
	}
	return out, err
}

func (s Store) WriteStakeChanges(ctx context.Context, rows []StakeChangeRound) error {
	if len(rows) == 0 {
		return nil
	}
	data, err := encodeRows(rows)
	if err != nil {
		return err
	}
	return s.CH.InsertJSON(ctx, "gramstat.validator_stake_changes", data)
}
