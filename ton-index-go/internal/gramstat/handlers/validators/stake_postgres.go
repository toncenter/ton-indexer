package validators

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type StakeInput struct {
	Since, Until        uint32
	ElectionID          *uint32
	Submitted, Selected *string
	ElectionMC          *uint32
	CycleMC             uint32
	ElectionLinks       int
}

// CollectStake uses the existing sticky election link and maximum selected
// principal maintained by the indexer. It never sums config observations.
func (p Postgres) CollectStake(ctx context.Context, from, to time.Time) ([]StakeInput, error) {
	if p.DSN == "" {
		return nil, fmt.Errorf("GRAMSTAT_PG_DSN is required for validator_stake")
	}
	cfg, err := pgx.ParseConfig(p.DSN)
	if err != nil {
		return nil, fmt.Errorf("invalid GRAMSTAT_PG_DSN")
	}
	cfg.ConnectTimeout = 10 * time.Second
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	cfg.RuntimeParams["application_name"] = "gramstat-validator-stake"
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.RuntimeParams["statement_timeout"] = "300000"
	cfg.RuntimeParams["lock_timeout"] = "5000"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(ctx, `WITH cycles AS (
        SELECT *, count(*) OVER (PARTITION BY election_id) AS election_links
        FROM public.validator_cycles
    )
    SELECT c.utime_since,c.utime_until,c.election_id,
        CASE WHEN e.finished AND NOT e.failed THEN e.total_stake::text END,
        c.total_stake::text,e.source_mc_seqno,c.source_mc_seqno,c.election_links
    FROM cycles c LEFT JOIN public.validator_elections e USING(election_id)
    WHERE c.utime_until>$1 AND c.utime_since<$2 ORDER BY c.utime_since`, from.Unix(), to.Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []StakeInput
	for rows.Next() {
		var r StakeInput
		if err = rows.Scan(&r.Since, &r.Until, &r.ElectionID, &r.Submitted, &r.Selected, &r.ElectionMC, &r.CycleMC, &r.ElectionLinks); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
