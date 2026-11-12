package wallets

import (
	"context"
	_ "embed"
	"fmt"
	"net/url"
	"time"
)

//go:embed live_counts.sql
var liveCountsSQL string

// Current activity has its own commit marker. It cannot certify a closed day,
// feed complete-month unions, or replace cumulative activation publications.
func (h *Handler) live(ctx context.Context, params url.Values, s source) error {
	from := time.Unix(int64(s.Before), 0).UTC().Truncate(24 * time.Hour)
	if s.Verified != 1 || from.Unix() < int64(s.From) || from.Unix() == int64(s.Before) {
		return nil
	}
	p := url.Values{}
	for k, v := range params {
		p[k] = append([]string(nil), v...)
	}
	p.Set("param_day", from.Format(time.DateOnly))
	set(p, "from", uint64(from.Unix()))
	set(p, "to", uint64(s.Before))
	var ready struct {
		N uint32 `json:"n"`
	}
	if err := h.query(ctx, `SELECT toUInt32(count()) AS n FROM {sink:Identifier}.wallet_live_days
 WHERE source_id={source:String} AND registry={registry:String} AND day=toDate({day:String})
 AND through_mc={end:UInt32} AND complete_before={to:UInt32} FORMAT JSONEachRow`, p, &ready); err != nil {
		return err
	}
	if ready.N > 0 {
		return nil
	}
	var bounds struct {
		Lo string `json:"lo"`
		Hi string `json:"hi"`
	}
	if err := h.query(ctx, `SELECT toString(min(start_lt)) AS lo,toString(max(end_lt)) AS hi
 FROM {raw:Identifier}.blocks WHERE gen_utime >= {from:UInt32} AND gen_utime < {to:UInt32}
 AND mc_block_seqno BETWEEN {first:UInt32} AND {end:UInt32} FORMAT JSONEachRow`, p, &bounds); err != nil {
		return err
	}
	id, err := newID()
	if err != nil {
		return err
	}
	p.Set("param_run", id)
	p.Set("param_lo", bounds.Lo)
	p.Set("param_hi", bounds.Hi)
	p.Set("param_proof", s.Proof)
	if err = h.query(ctx, liveCountsSQL, p, nil); err != nil {
		return fmt.Errorf("current wallet activity: %w", err)
	}
	if err = h.query(ctx, `INSERT INTO {sink:Identifier}.wallet_live_days
 SELECT {source:String},{registry:String},toDate({day:String}),{run:UUID},
 {end:UInt32},{to:UInt32},{proof:String},now64(6)`, p, nil); err != nil {
		return err
	}
	if h.Log != nil {
		h.Log.Info("current wallet activity published", "day", from.Format(time.DateOnly), "complete_before", s.Before, "through_mc", s.End)
	}
	return nil
}
