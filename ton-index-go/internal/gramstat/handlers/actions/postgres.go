package actions

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// One index-bounded scan supplies all three metrics. Do not exclude child
// actions: transfers inside swaps and other composite actions also participate.
const dailySQL = `SELECT
    (to_timestamp(start_utime) AT TIME ZONE 'UTC')::date AS day,
    count(DISTINCT source) FILTER (WHERE type = 'jetton_transfer') AS jetton_senders,
    count(*) FILTER (WHERE type = 'jetton_transfer') AS jetton_transfers,
    count(*) FILTER (WHERE type = 'nft_transfer') AS nft_transfers
FROM public.actions
WHERE trace_mc_seqno_end >= $1 AND trace_mc_seqno_end <= $2
  AND start_utime >= $3 AND start_utime < $4
  AND success = true AND type IN ('jetton_transfer', 'nft_transfer')
GROUP BY 1 ORDER BY 1`

type Postgres struct{ DSN string }

func (p Postgres) Collect(ctx context.Context, from, to time.Time) (snapshot Snapshot, err error) {
	if p.DSN == "" {
		return snapshot, fmt.Errorf("GRAMSTAT_PG_DSN is required for actions")
	}
	cfg, err := pgxpool.ParseConfig(p.DSN)
	if err != nil {
		return snapshot, fmt.Errorf("invalid GRAMSTAT_PG_DSN")
	}
	cfg.MaxConns = 1
	cfg.ConnConfig.ConnectTimeout = 10 * time.Second
	// Re-plan with the actual MC range; a cached generic plan can choose a scan
	// of all history after several executions despite the selective MC index.
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	cfg.ConnConfig.RuntimeParams["application_name"] = "gramstat-aggregate"
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "300000"
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "5000"
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return snapshot, err
	}
	defer pool.Close()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return snapshot, err
	}
	defer tx.Rollback(context.Background())
	var lower, upper int64
	var sourceTime int64
	if err = tx.QueryRow(ctx, `SELECT seqno FROM public.blocks
WHERE workchain = -1 AND gen_utime < $1
ORDER BY gen_utime DESC, seqno DESC LIMIT 1`, from.Unix()).Scan(&lower); err != nil {
		return snapshot, fmt.Errorf("MC before range: %w", err)
	}
	if err = tx.QueryRow(ctx, `SELECT trace_mc_seqno_end FROM public.actions
ORDER BY trace_mc_seqno_end DESC LIMIT 1`).Scan(&upper); err != nil {
		return snapshot, fmt.Errorf("latest action MC: %w", err)
	}
	if lower < 0 || upper <= 0 || upper > int64(^uint32(0)) || lower > upper {
		return snapshot, fmt.Errorf("source has no valid MC range for this interval")
	}
	if err = tx.QueryRow(ctx, `SELECT gen_utime FROM public.blocks
WHERE workchain = -1 AND seqno = $1 LIMIT 1`, upper).Scan(&sourceTime); err != nil {
		return snapshot, fmt.Errorf("source MC time: %w", err)
	}
	snapshot.Through = to
	if head := time.Unix(sourceTime, 0).UTC(); head.Before(to) {
		snapshot.Through = head
	}
	if !snapshot.Through.After(from) {
		return snapshot, fmt.Errorf("source is behind the requested interval")
	}
	snapshot.SourceMC = uint32(upper)
	if err = tx.QueryRow(ctx, `SELECT transaction_timestamp()`).Scan(&snapshot.CalculatedAt); err != nil {
		return snapshot, err
	}
	rows, err := tx.Query(ctx, dailySQL, lower, upper, from.Unix(), snapshot.Through.Unix())
	if err != nil {
		return snapshot, err
	}
	defer rows.Close()
	for rows.Next() {
		var d DayCounts
		var senders, transfers, nfts int64
		if err = rows.Scan(&d.Day, &senders, &transfers, &nfts); err != nil {
			return snapshot, err
		}
		if senders < 0 || transfers < 0 || nfts < 0 {
			return snapshot, fmt.Errorf("negative count")
		}
		d.JettonSenders, d.JettonTransfers, d.NFTTransfers = uint64(senders), uint64(transfers), uint64(nfts)
		snapshot.Days = append(snapshot.Days, d)
	}
	if err = rows.Err(); err != nil {
		return snapshot, err
	}
	return snapshot, tx.Commit(ctx)
}
