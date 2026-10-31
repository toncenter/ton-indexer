package actions

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// One index-bounded scan supplies all metrics and breakdowns. GROUPING SETS
// computes daily distinct users across all DEXes, not a sum of per-DEX users.
// Child actions are included, just as in the original transfer metrics.
//
//go:embed daily.sql
var dailySQL string

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
	cfg.ConnConfig.RuntimeParams["jit"] = "off"
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
		var senders, receivers, transfers, nftSenders, nftReceivers, nfts int64
		var swaps, users, deposits, dnsMints int64
		var dexJSON, poolsJSON, starsJSON []byte
		var starsPurchases int64
		if err = rows.Scan(&d.Day, &senders, &receivers, &transfers, &nftSenders, &nftReceivers, &nfts,
			&swaps, &users, &dexJSON, &deposits, &d.LiquidStakingAmount, &poolsJSON, &dnsMints, &starsPurchases, &starsJSON); err != nil {
			return snapshot, err
		}
		if senders < 0 || receivers < 0 || transfers < 0 || nftSenders < 0 || nftReceivers < 0 || nfts < 0 ||
			swaps < 0 || users < 0 || deposits < 0 || dnsMints < 0 || starsPurchases < 0 {
			return snapshot, fmt.Errorf("negative count")
		}
		d.JettonSenders, d.JettonReceivers, d.JettonTransfers = uint64(senders), uint64(receivers), uint64(transfers)
		d.NFTSenders, d.NFTReceivers, d.NFTTransfers = uint64(nftSenders), uint64(nftReceivers), uint64(nfts)
		d.Swaps, d.SwapUsers, d.LiquidStakingDeposits, d.TonDNSMints = uint64(swaps), uint64(users), uint64(deposits), uint64(dnsMints)
		d.StarsPurchases = uint64(starsPurchases)
		if err = json.Unmarshal(dexJSON, &d.SwapsByDEX); err != nil {
			return snapshot, fmt.Errorf("decode DEX breakdown: %w", err)
		}
		if err = json.Unmarshal(poolsJSON, &d.LiquidStakingByPool); err != nil {
			return snapshot, fmt.Errorf("decode liquid staking breakdown: %w", err)
		}
		if err = json.Unmarshal(starsJSON, &d.StarsBuckets); err != nil {
			return snapshot, fmt.Errorf("decode Stars distribution: %w", err)
		}
		snapshot.Days = append(snapshot.Days, d)
	}
	if err = rows.Err(); err != nil {
		return snapshot, err
	}
	return snapshot, tx.Commit(ctx)
}
