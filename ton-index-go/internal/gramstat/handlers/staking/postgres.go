package staking

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type Postgres struct{ DSN string }

func readObservation(row pgx.Row) (*Observation, error) {
	var o Observation
	var payload []byte
	if err := row.Scan(&o.MC, &o.Time, &payload); err == pgx.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(payload, &o.Snapshot); err != nil {
		return nil, err
	}
	return &o, nil
}

func (p Postgres) Collect(ctx context.Context, from, to time.Time) ([]Point, error) {
	if p.DSN == "" {
		return nil, fmt.Errorf("GRAMSTAT_PG_DSN is required for staking")
	}
	cfg, err := pgx.ParseConfig(p.DSN)
	if err != nil {
		return nil, fmt.Errorf("invalid GRAMSTAT_PG_DSN")
	}
	cfg.ConnectTimeout = 10 * time.Second
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.RuntimeParams["statement_timeout"] = "300000"
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
	var head int64
	if err = tx.QueryRow(ctx, `SELECT gen_utime FROM public.blocks WHERE workchain=-1 ORDER BY seqno DESC LIMIT 1`).Scan(&head); err != nil {
		return nil, err
	}
	cache := map[string]Origin{}
	var points []Point
	for day := from; day.Before(to); day = day.AddDate(0, 0, 1) {
		end := min(day.AddDate(0, 0, 1).Unix(), to.Unix(), head+1)
		if end <= day.Unix() {
			break
		}
		point := Point{Day: day.Format(time.DateOnly), Origins: map[string]Origin{}}
		if end < day.AddDate(0, 0, 1).Unix() {
			point.Partial = 1
		}
		err = tx.QueryRow(ctx, `SELECT seqno,gen_utime FROM public.blocks WHERE workchain=-1 AND gen_utime<$1 ORDER BY gen_utime DESC,seqno DESC LIMIT 1`, end).Scan(&point.AnchorMC, &point.AnchorTime)
		if err != nil {
			return nil, err
		}
		point.State, err = readObservation(tx.QueryRow(ctx, `SELECT mc_seqno,gen_utime,payload FROM public.elector_stake_states WHERE mc_seqno<=$1 ORDER BY mc_seqno DESC LIMIT 1`, point.AnchorMC))
		if err != nil {
			return nil, err
		}
		if point.State != nil && point.State.Complete {
			for _, e := range point.State.Elections {
				key := electionKey(e)
				o, ok := cache[key]
				if !ok {
					first, err := readObservation(tx.QueryRow(ctx, `SELECT s.mc_seqno,s.gen_utime,s.payload
FROM public.elector_frozen_elections e JOIN public.elector_stake_states s USING(mc_seqno)
WHERE e.election_id=$1 AND upper(e.vset_hash)=upper($2) ORDER BY e.mc_seqno LIMIT 1`, e.ID, e.Hash))
					if err != nil {
						return nil, err
					}
					if first != nil {
						o.First = *first
						o.Previous, err = readObservation(tx.QueryRow(ctx, `SELECT mc_seqno,gen_utime,payload FROM public.elector_stake_states WHERE mc_seqno<$1 ORDER BY mc_seqno DESC LIMIT 1`, first.MC))
						if err != nil {
							return nil, err
						}
					}
					cache[key] = o
				}
				point.Origins[key] = o
			}
			addresses := []string{}
			for _, e := range point.State.Elections {
				for _, position := range e.Positions {
					addresses = append(addresses, position.Holder)
				}
			}
			rows, err := tx.Query(ctx, `SELECT DISTINCT ON (account) payload FROM public.stake_holder_states
WHERE account=ANY($1::tonaddr[]) AND mc_seqno<=$2 ORDER BY account,mc_seqno DESC`, addresses, point.AnchorMC)
			if err != nil {
				return nil, err
			}
			point.State.Holders = nil
			for rows.Next() {
				var raw []byte
				var h Holder
				if err = rows.Scan(&raw); err != nil {
					rows.Close()
					return nil, err
				}
				if err = json.Unmarshal(raw, &h); err != nil {
					rows.Close()
					return nil, err
				}
				point.State.Holders = append(point.State.Holders, h)
			}
			rows.Close()
			if err = rows.Err(); err != nil {
				return nil, err
			}
		}
		points = append(points, point)
	}
	return points, tx.Commit(ctx)
}
