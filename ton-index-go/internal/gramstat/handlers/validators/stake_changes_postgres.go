package validators

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

func (p Postgres) CollectStakeMembers(ctx context.Context, to time.Time) (map[uint32]StakeMembers, error) {
	if p.DSN == "" {
		return nil, fmt.Errorf("GRAMSTAT_PG_DSN is required")
	}
	cfg, err := pgx.ParseConfig(p.DSN)
	if err != nil {
		return nil, fmt.Errorf("invalid GRAMSTAT_PG_DSN")
	}
	cfg.ConnectTimeout = 10 * time.Second
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	cfg.RuntimeParams["application_name"] = "gramstat-stake-changes"
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.RuntimeParams["statement_timeout"] = "300000"
	cfg.RuntimeParams["lock_timeout"] = "5000"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	ids, err := conn.Query(ctx, "SELECT utime_since FROM public.validator_cycles WHERE utime_since<$1 ORDER BY utime_since", to.Unix())
	if err != nil {
		return nil, err
	}
	var starts []int64
	for ids.Next() {
		var since int64
		if err = ids.Scan(&since); err != nil {
			ids.Close()
			return nil, err
		}
		starts = append(starts, since)
	}
	err = ids.Err()
	ids.Close()
	if err != nil {
		return nil, err
	}
	out := map[uint32]StakeMembers{}
	for start := 0; start < len(starts); start += 64 {
		end := min(start+64, len(starts))
		until := to.Unix()
		if end < len(starts) {
			until = starts[end]
		}
		batch, err := collectStakeMemberBatch(ctx, conn, starts[start], until)
		if err != nil {
			return nil, err
		}
		for since, m := range batch {
			out[since] = m
		}
	}
	return out, nil
}

func collectStakeMemberBatch(ctx context.Context, conn *pgx.Conn, from, to int64) (map[uint32]StakeMembers, error) {
	rows, err := conn.Query(ctx, `SELECT c.utime_since,c.utime_until,c.total,c.main,c.total_weight::text,
 m.validator_index,m.validator_pubkey,m.adnl_addr,m.weight::text,m.stake::text,p.stake_holder_address::text
 FROM public.validator_cycles c JOIN public.validator_cycle_members m USING(utime_since)
 LEFT JOIN LATERAL (SELECT stake_holder_address FROM public.validator_election_participants p
  WHERE p.election_id=c.election_id AND p.validator_pubkey=m.validator_pubkey OFFSET 0) p ON true
 WHERE c.utime_since>=$1 AND c.utime_since<$2 ORDER BY c.utime_since,m.validator_index`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uint32]StakeMembers{}
	var round Round
	var positions map[string]StakePosition
	flush := func() {
		if len(round.Members) == 0 {
			return
		}
		if err := round.validate(); err == nil {
			out[round.Since] = StakeMembers{Digest: round.RosterDigest, Positions: positions}
		}
	}
	for rows.Next() {
		var r Round
		var m Member
		var weight, totalWeight string
		var amount, holder *string
		if err := rows.Scan(&r.Since, &r.Until, &r.Total, &r.Main, &totalWeight, &m.Index, &m.Pubkey, &m.ADNL, &weight, &amount, &holder); err != nil {
			return nil, err
		}
		if r.TotalWeight, err = strconv.ParseUint(totalWeight, 10, 64); err != nil {
			return nil, err
		}
		if m.Weight, err = strconv.ParseUint(weight, 10, 64); err != nil {
			return nil, err
		}
		if round.Since != r.Since {
			flush()
			round = r
			positions = map[string]StakePosition{}
		}
		key, err := normalizeHash(m.Pubkey)
		if err != nil {
			return nil, err
		}
		stake, err := stakeAmount(amount)
		if err != nil {
			return nil, err
		}
		pos := StakePosition{Stake: stake}
		if holder != nil {
			pos.Holder = *holder
		}
		positions[key] = pos
		round.Members = append(round.Members, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	flush()
	return out, nil
}
