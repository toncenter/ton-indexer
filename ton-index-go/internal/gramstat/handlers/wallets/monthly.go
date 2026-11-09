package wallets

import (
	"context"
	_ "embed"
	"fmt"
	"net/url"
	"time"
)

//go:embed monthly.sql
var monthlySQL string

func (h *Handler) monthly(ctx context.Context, p url.Values, from, to time.Time) error {
	// Exact aggregate states can be large even for a single storage granule.
	// Keep monthly preparation single-threaded and separately bounded; these
	// settings never affect daily preparation or scalar API reads.
	monthlyParams := make(url.Values, len(p)+3)
	for k, v := range p {
		monthlyParams[k] = append([]string(nil), v...)
	}
	p = monthlyParams
	p.Set("max_threads", "1")
	p.Set("max_block_size", "1")
	p.Set("max_memory_usage", "8589934592")
	p.Set("param_monthly_from", from.Format(time.DateOnly))
	p.Set("param_monthly_to", to.Format(time.DateOnly))
	var missing struct {
		Data []struct {
			Day string `json:"date"`
		} `json:"data"`
	}
	// Window endpoints are eligible only when every one of the 30 days is ready.
	if err := h.query(ctx, `SELECT toString(day) AS date FROM (
 SELECT day,count() OVER (ORDER BY toUInt32(day) RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) AS days
 FROM (SELECT DISTINCT day FROM {sink:Identifier}.wallet_days
 WHERE source_id={source:String} AND registry={registry:String} AND history_verified=1
 AND day >= toDate({monthly_from:String})-29 AND day < toDate({monthly_to:String})))
 WHERE days=30 AND day>=toDate({monthly_from:String}) AND day NOT IN (
 SELECT day FROM {sink:Identifier}.wallet_monthly WHERE source_id={source:String} AND registry={registry:String})
 ORDER BY day FORMAT JSON`, p, &missing); err != nil {
		return err
	}
	for _, row := range missing.Data {
		p.Set("param_monthly_day", row.Day)
		if err := h.query(ctx, monthlySQL, p, nil); err != nil {
			return fmt.Errorf("monthly wallet day %s: %w", row.Day, err)
		}
		if h.Log != nil {
			h.Log.Info("monthly wallet day prepared", "day", row.Day)
		}
	}
	return nil
}
