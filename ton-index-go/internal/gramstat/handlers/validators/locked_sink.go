package validators

import (
	"context"
	"net/url"
	"strconv"
	"strings"
)

func (s Store) GeoRounds(ctx context.Context, ids []uint32) (map[uint32]GeoRound, error) {
	out := map[uint32]GeoRound{}
	if len(ids) == 0 {
		return out, nil
	}
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatUint(uint64(id), 10)
	}
	data, err := s.CH.Query(ctx, "SELECT * FROM gramstat.validator_geo_rounds FINAL WHERE utime_since IN ("+strings.Join(parts, ",")+") FORMAT JSONEachRow", url.Values{"output_format_json_named_tuples_as_objects": {"1"}, "output_format_json_quote_64bit_integers": {"0"}})
	if err != nil {
		return out, err
	}
	rows, err := decodeRows[GeoRound](data)
	for _, r := range rows {
		out[r.Since] = r
	}
	return out, err
}

func (s Store) LockedCoverage(ctx context.Context) (map[uint32]LockedRound, error) {
	data, err := s.CH.Query(ctx, `SELECT utime_since,utime_until,previous_utime_since,config34_hash,roster_digest,
 previous_config34_hash,previous_roster_digest,toString(current_selected_nanogram) AS current_selected_nanogram,
 toString(previous_selected_nanogram) AS previous_selected_nanogram,data_through,previous_data_through,
 status,snapshot_hash,calc_version FROM gramstat.validator_stake_locked_rounds FINAL FORMAT JSONEachRow`, nil)
	if err != nil {
		return nil, err
	}
	rows, err := decodeRows[LockedRound](data)
	out := map[uint32]LockedRound{}
	for _, r := range rows {
		out[r.Since] = r
	}
	return out, err
}

func (s Store) WriteLocked(ctx context.Context, rows []LockedRound) error {
	if len(rows) == 0 {
		return nil
	}
	data, err := encodeRows(rows)
	if err != nil {
		return err
	}
	return s.CH.InsertJSON(ctx, "gramstat.validator_stake_locked_rounds", data)
}
