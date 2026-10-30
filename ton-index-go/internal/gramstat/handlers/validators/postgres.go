package validators

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

type Postgres struct{ DSN string }

// Collect discovers expected rounds even when the destination has an interior
// gap. Old complete rounds need only their small metadata, not another roster read.
func (p Postgres) Collect(ctx context.Context, from, to, refresh time.Time, stored map[uint32]Coverage) ([]Round, error) {
	if p.DSN == "" {
		return nil, fmt.Errorf("GRAMSTAT_PG_DSN is required for validators")
	}
	cfg, err := pgx.ParseConfig(p.DSN)
	if err != nil {
		return nil, fmt.Errorf("invalid GRAMSTAT_PG_DSN")
	}
	cfg.ConnectTimeout = 10 * time.Second
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	cfg.RuntimeParams["application_name"] = "gramstat-validators"
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.RuntimeParams["statement_timeout"] = "300000"
	cfg.RuntimeParams["lock_timeout"] = "5000"
	cfg.RuntimeParams["timezone"] = "UTC"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	var calculated time.Time
	var head int64
	if err = tx.QueryRow(ctx, `SELECT transaction_timestamp(),gen_utime FROM public.blocks WHERE workchain=-1 ORDER BY gen_utime DESC,seqno DESC LIMIT 1`).Scan(&calculated, &head); err != nil {
		return nil, err
	}
	through := min(to.Unix(), head)
	if through <= from.Unix() {
		return nil, fmt.Errorf("validator source is behind requested interval")
	}
	rows, err := tx.Query(ctx, `SELECT utime_since,utime_until,total,main,total_weight::text,source_mc_seqno
FROM public.validator_cycles WHERE utime_until>$1 AND utime_since<$2 ORDER BY utime_since`, from.Unix(), through)
	if err != nil {
		return nil, err
	}
	var rounds []Round
	for rows.Next() {
		var r Round
		var weight string
		if err = rows.Scan(&r.Since, &r.Until, &r.Total, &r.Main, &weight, &r.SourceMC); err != nil {
			rows.Close()
			return nil, err
		}
		if r.TotalWeight, err = strconv.ParseUint(weight, 10, 64); err != nil {
			rows.Close()
			return nil, fmt.Errorf("invalid total weight: %w", err)
		}
		old, ok := stored[r.Since]
		if ok && old.Version == Version && old.Through >= r.Until && old.Until == r.Until && int64(r.Until) <= refresh.Unix() {
			continue
		}
		r.CalculatedAt = timestamp(calculated)
		r.Through = uint32(min(through, int64(r.Until)))
		r.Version = Version
		if r.Through < r.Until {
			r.Partial = 1
		}
		rounds = append(rounds, r)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	ids := make([]int64, len(rounds))
	positions := make(map[uint32]int, len(rounds))
	for i, r := range rounds {
		ids[i] = int64(r.Since)
		positions[r.Since] = i
	}
	if len(ids) > 0 {
		rows, err = tx.Query(ctx, `SELECT utime_since,validator_index,validator_pubkey,adnl_addr,weight::text
FROM public.validator_cycle_members WHERE utime_since=ANY($1::integer[]) ORDER BY utime_since,validator_index`, ids)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var since uint32
			var m Member
			var weight string
			if err = rows.Scan(&since, &m.Index, &m.Pubkey, &m.ADNL, &weight); err != nil {
				rows.Close()
				return nil, err
			}
			if m.Weight, err = strconv.ParseUint(weight, 10, 64); err != nil {
				rows.Close()
				return nil, err
			}
			i := positions[since]
			rounds[i].Members = append(rounds[i].Members, m)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return nil, err
		}
	}
	for i := range rounds {
		r := &rounds[i]
		// This is an activation candidate, not proof by timestamp. Config #34 is
		// separately decoded and compared before any round is published.
		var key bool
		var previous int64
		err = tx.QueryRow(ctx, `SELECT b.seqno,b.gen_utime,b.root_hash::text,b.file_hash::text,b.key_block,p.gen_utime
FROM (SELECT seqno,gen_utime,root_hash,file_hash,key_block FROM public.blocks
      WHERE workchain=-1 AND gen_utime >= $1 AND gen_utime < $1+300
      ORDER BY gen_utime,seqno LIMIT 1) b
JOIN public.blocks p ON p.workchain=-1 AND p.seqno=b.seqno-1 LIMIT 1`, int64(r.Since)).Scan(&r.AnchorMC, &r.AnchorTime, &r.AnchorRoot, &r.AnchorFile, &key, &previous)
		if err == pgx.ErrNoRows {
			r.AnchorMC = 0
			continue
		}
		if err != nil {
			return nil, err
		}
		if !key || previous >= int64(r.Since) || r.AnchorTime >= r.Until {
			r.AnchorMC = 0
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return rounds, nil
}
