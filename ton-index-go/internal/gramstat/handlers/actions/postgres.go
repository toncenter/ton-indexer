package actions

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// One index-bounded scan supplies all metrics and breakdowns. Only daily totals
// compute distinct users; the small breakdowns use separate additive aggregates.
// Child actions are included, just as in the original transfer metrics.
//
//go:embed daily.sql
var dailySQL string

//go:embed stars_source.sql
var starsSourceSQL string

// The targeted historical rebuild scans only Fragment's account index and
// looks up actions by their complete primary key. Matching both LT fields also
// prevents stale account-index entries from duplicating corrected actions.
// Do not cap trace_end_utime at the requested end: traces may finish much later.
func starsOnlySQL() string {
	_, tail, ok := strings.Cut(dailySQL, "parsed AS MATERIALIZED (")
	if !ok {
		panic("missing shared Stars parser")
	}
	tail, _, ok = strings.Cut(tail, "\nSELECT d.day,")
	if !ok {
		panic("missing daily projection")
	}
	return strings.TrimSpace(starsSourceSQL) + ", parsed AS MATERIALIZED (" + tail + `
SELECT day, 0, 0, 0, 0, 0, 0, 0, 0, '{}'::jsonb, 0, '0'::text,
       '{}'::jsonb, 0, purchases, buckets, '{}'::jsonb FROM star_days ORDER BY day`
}

type Postgres struct {
	DSN              string
	StatementTimeout time.Duration
	WorkMemMB        int
	ParallelWorkers  int
	IncludeMonthly   bool
	StarsOnly        bool
	TotalOnly        bool
}

func (p Postgres) Collect(ctx context.Context, from, to time.Time) (snapshot Snapshot, err error) {
	if !from.Equal(from.UTC().Truncate(24 * time.Hour)) {
		return snapshot, fmt.Errorf("daily actions require a UTC day boundary")
	}
	query := dailySQL
	if p.TotalOnly {
		if p.IncludeMonthly || p.StarsOnly {
			return snapshot, fmt.Errorf("total-only collection cannot include other metrics")
		}
		query = totalOnlySQL
	}
	if p.StarsOnly {
		if p.IncludeMonthly {
			return snapshot, fmt.Errorf("Stars-only collection cannot include monthly activity")
		}
		query = starsOnlySQL()
	}
	if p.IncludeMonthly {
		genesis := time.Date(2019, 11, 15, 0, 0, 0, 0, time.UTC)
		if !from.Equal(monthStart(from)) && !from.Equal(genesis) {
			return snapshot, fmt.Errorf("combined monthly backfill requires a month boundary or genesis")
		}
		query = combinedSQL()
	}
	return p.collect(ctx, from, to, query, p.IncludeMonthly)
}

func (p Postgres) CollectMonthly(ctx context.Context, from, to time.Time) (Snapshot, error) {
	return p.collect(ctx, from, to, monthlySQL, false)
}

// Reuse the materialized source for full calendar-month uniques, avoiding a
// second scan of the raw table. An incomplete historical trailing month is
// excluded; the independent monthly refresh supplies recent months.
func combinedSQL() string {
	query := strings.TrimSuffix(strings.TrimSpace(dailySQL), "ORDER BY d.day")
	query = strings.Replace(query, "SELECT d.day,", "SELECT 'daily' AS granularity, d.day,", 1)
	return query + `
UNION ALL
SELECT 'monthly', date_trunc('month',day)::date,
 count(DISTINCT source) FILTER (WHERE type='jetton_transfer'),
 count(DISTINCT destination) FILTER (WHERE type='jetton_transfer'), 0,
 count(DISTINCT source) FILTER (WHERE type='nft_transfer'),
 count(DISTINCT destination) FILTER (WHERE type='nft_transfer'), 0, 0,
 count(DISTINCT source) FILTER (WHERE type='jetton_swap'),
 '{}'::jsonb, 0, '0'::text, '{}'::jsonb, 0, 0, '[]'::jsonb, '{}'::jsonb
FROM src WHERE day < date_trunc('month',to_timestamp($4) AT TIME ZONE 'UTC')::date
AND type IN ('jetton_transfer','nft_transfer','jetton_swap')
GROUP BY 2 ORDER BY 1,2`
}

func (p Postgres) collect(ctx context.Context, from, to time.Time, query string, combined bool) (snapshot Snapshot, err error) {
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
	statementTimeout := p.StatementTimeout
	if statementTimeout == 0 {
		statementTimeout = 5 * time.Minute
	}
	if statementTimeout < time.Millisecond {
		return snapshot, fmt.Errorf("actions statement timeout must be positive")
	}
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(statementTimeout.Milliseconds(), 10)
	workMem := p.WorkMemMB
	if workMem == 0 {
		workMem = 64
	}
	if workMem < 1 || workMem > 1024 || p.ParallelWorkers < 0 || p.ParallelWorkers > 4 {
		return snapshot, fmt.Errorf("actions work memory must be 1..1024 MiB and parallel workers 0..4")
	}
	cfg.ConnConfig.RuntimeParams["max_parallel_workers_per_gather"] = strconv.Itoa(p.ParallelWorkers)
	cfg.ConnConfig.RuntimeParams["work_mem"] = strconv.Itoa(workMem) + "MB"
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "5000"
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	cfg.ConnConfig.RuntimeParams["jit"] = "off"
	if p.TotalOnly {
		// Correlated MC/time predicates severely underestimate recent rows. This
		// count-only query can hash into a small day/type set without a giant sort.
		cfg.ConnConfig.RuntimeParams["enable_sort"] = "off"
	}
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
		if !errors.Is(err, pgx.ErrNoRows) {
			return snapshot, fmt.Errorf("MC before range: %w", err)
		}
		// The first chain day has no preceding MC. Only accept the verified
		// genesis anchor, not a truncated database or dates before genesis.
		var firstMC, firstTime int64
		if err = tx.QueryRow(ctx, `SELECT seqno,gen_utime FROM public.blocks
WHERE workchain=-1 ORDER BY seqno ASC LIMIT 1`).Scan(&firstMC, &firstTime); err != nil {
			return snapshot, fmt.Errorf("first MC: %w", err)
		}
		if firstMC != 1 || !time.Unix(firstTime, 0).UTC().Truncate(24*time.Hour).Equal(from) {
			return snapshot, fmt.Errorf("source has no verified opening MC for requested day")
		}
		lower = 0
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
	snapshot.MonthlyIncluded = combined
	if err = tx.QueryRow(ctx, `SELECT transaction_timestamp()`).Scan(&snapshot.CalculatedAt); err != nil {
		return snapshot, err
	}
	args := []any{lower, upper, from.Unix(), snapshot.Through.Unix()}
	if strings.Contains(query, "$5") {
		args = append(args, TotalTypes)
	}
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return snapshot, err
	}
	defer rows.Close()
	for rows.Next() {
		var d DayCounts
		var senders, receivers, transfers, nftSenders, nftReceivers, nfts int64
		var swaps, users, deposits, dnsMints int64
		var dexJSON, poolsJSON, starsJSON, typesJSON []byte
		var starsPurchases int64
		granularity := "daily"
		dest := []any{&d.Day, &senders, &receivers, &transfers, &nftSenders, &nftReceivers, &nfts,
			&swaps, &users, &dexJSON, &deposits, &d.LiquidStakingAmount, &poolsJSON, &dnsMints, &starsPurchases, &starsJSON, &typesJSON}
		if combined {
			dest = append([]any{&granularity}, dest...)
		}
		if err = rows.Scan(dest...); err != nil {
			return snapshot, err
		}
		if senders < 0 || receivers < 0 || transfers < 0 || nftSenders < 0 || nftReceivers < 0 || nfts < 0 ||
			swaps < 0 || users < 0 || deposits < 0 || dnsMints < 0 || starsPurchases < 0 {
			return snapshot, fmt.Errorf("negative count")
		}
		d.JettonSenders, d.JettonReceivers, d.JettonTransfers = uint64(senders), uint64(receivers), uint64(transfers)
		d.NFTSenders, d.NFTReceivers, d.NFTTransfers = uint64(nftSenders), uint64(nftReceivers), uint64(nfts)
		d.Swaps, d.SwapUsers, d.LiquidStakingDeposits, d.TonDNSMints = uint64(swaps), uint64(users), uint64(deposits), uint64(dnsMints)
		if err = json.Unmarshal(dexJSON, &d.SwapsByDEX); err != nil {
			return snapshot, fmt.Errorf("decode DEX breakdown: %w", err)
		}
		if err = json.Unmarshal(typesJSON, &d.TotalByType); err != nil {
			return snapshot, fmt.Errorf("decode action type breakdown: %w", err)
		}
		if err = json.Unmarshal(poolsJSON, &d.LiquidStakingByPool); err != nil {
			return snapshot, fmt.Errorf("decode liquid staking breakdown: %w", err)
		}
		var payments []starsPaymentGroup
		if err = json.Unmarshal(starsJSON, &payments); err != nil {
			return snapshot, fmt.Errorf("decode Stars distribution: %w", err)
		}
		if err = filterStarsPayments(&d, uint64(starsPurchases), payments); err != nil {
			return snapshot, fmt.Errorf("filter Stars payments: %w", err)
		}
		switch granularity {
		case "daily":
			snapshot.Days = append(snapshot.Days, d)
		case "monthly":
			snapshot.Months = append(snapshot.Months, d)
		default:
			return snapshot, fmt.Errorf("unexpected activity granularity")
		}
	}
	if err = rows.Err(); err != nil {
		return snapshot, err
	}
	return snapshot, tx.Commit(ctx)
}
