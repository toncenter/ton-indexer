package dexvolume

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/handlers/wallets"
)

// The classification partition key is the MC in which the whole trace
// closed. Cutting on it keeps all sibling actions together across midnight.
type Day struct {
	Date      time.Time
	MC        uint32
	Routes    []Route
	Ambiguous uint64
	Complex   int
}

const swapsSQL = `WITH s AS (
 SELECT trace_id,start_lt,end_utime,success,jetton_swap_data j,tx_hashes,
 count(*) OVER (PARTITION BY trace_id) AS n
 FROM public.actions
 WHERE trace_mc_seqno_end > $1 AND trace_mc_seqno_end <= $2
 AND type='jetton_swap'::varchar
 AND NOT ('jetton_swap'::text=ANY(coalesce(ancestor_type::text[],'{}'::text[])))
) SELECT trace_id::text,start_lt,end_utime,success,coalesce((j).dex,''),
 coalesce(((j).dex_incoming_transfer).asset::text,''),coalesce(((j).dex_incoming_transfer).amount::text,''),coalesce(((j).dex_incoming_transfer).source::text,''),
 coalesce(((j).dex_outgoing_transfer).asset::text,''),coalesce(((j).dex_outgoing_transfer).amount::text,''),coalesce(((j).dex_outgoing_transfer).destination::text,''),
 CASE WHEN n BETWEEN 2 AND 64 THEN tx_hashes::text[] END,coalesce(((j).dex_outgoing_transfer).destination_jetton_wallet::text,''),
 CASE WHEN success AND cardinality((j).peer_swaps) BETWEEN 2 AND 64 THEN to_jsonb((j).peer_swaps)::text END,
 CASE WHEN n BETWEEN 2 AND 64 THEN coalesce(((j).dex_incoming_transfer).destination_jetton_wallet::text,'') ELSE '' END
 FROM s`

func ReadDay(ctx context.Context, conn *pgx.Conn, date time.Time, audit func(string, []Swap, []Route, int)) (Day, error) {
	out := Day{Date: date}
	if date != date.UTC().Truncate(24*time.Hour) {
		return out, fmt.Errorf("expected UTC midnight")
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	bounds := make([]int32, 5)
	for i := range bounds {
		err = tx.QueryRow(ctx, `SELECT seqno FROM public.blocks WHERE workchain=-1 AND gen_utime<$1 ORDER BY gen_utime DESC,seqno DESC LIMIT 1`, date.Add(time.Duration(i)*6*time.Hour).Unix()).Scan(&bounds[i])
		if err != nil {
			return out, err
		}
	}
	var through int64
	err = tx.QueryRow(ctx, `SELECT gen_utime FROM public.blocks WHERE workchain=-1 AND seqno=(SELECT trace_mc_seqno_end FROM public.actions ORDER BY trace_mc_seqno_end DESC LIMIT 1)`).Scan(&through)
	if err != nil {
		return out, err
	}
	if through < date.Add(24*time.Hour).Unix() || bounds[4] <= bounds[0] {
		return out, fmt.Errorf("actions have not covered this UTC day")
	}
	out.MC = uint32(bounds[4])
	for i := 0; i < 4; i++ {
		batch := map[string][]Swap{}
		rows, err := tx.Query(ctx, swapsSQL, bounds[i], bounds[i+1])
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var id string
			var a Swap
			var hops []byte
			if err = rows.Scan(&id, &a.StartLT, &a.End, &a.OK, &a.Dex, &a.In.Asset, &a.In.Amount, &a.In.Holder, &a.Out.Asset, &a.Out.Amount, &a.Out.Holder, &a.Hashes, &a.OutWallet, &hops, &a.InDexWallet); err != nil {
				rows.Close()
				return out, err
			}
			a.In.Asset = asset(a.In.Asset)
			a.Out.Asset = asset(a.Out.Asset)
			a.Via, err = peerFlow(hops, a.In, a.Out)
			if err != nil {
				rows.Close()
				return out, err
			}
			batch[id] = append(batch[id], a)
		}
		if rows.Err() != nil {
			return out, rows.Err()
		}
		var complex []string
		for id, ss := range batch {
			ok := false
			for _, s := range ss {
				ok = ok || s.OK
			}
			if !ok {
				continue
			}
			if len(ss) == 1 {
				out.Routes = append(out.Routes, single(ss[0]))
				continue
			}
			if len(ss) > 64 {
				out.Ambiguous++
				continue
			}
			complex = append(complex, id)
		}
		sort.Strings(complex)
		out.Complex += len(complex)
		for len(complex) > 0 {
			n := min(64, len(complex))
			ids := complex[:n]
			complex = complex[n:]
			graphs, err := readGraphs(ctx, tx, ids, batch, bounds[i], bounds[i+1])
			if err != nil {
				return out, err
			}
			for _, id := range ids {
				g := graphs[id]
				if g == nil {
					out.Ambiguous++
					continue
				}
				rs, missing := Resolve(batch[id], *g)
				if audit != nil {
					audit(id, batch[id], rs, missing)
				}
				out.Routes = append(out.Routes, rs...)
				out.Ambiguous += uint64(missing)
			}
		}
	}
	return out, tx.Commit(ctx)
}

func readGraphs(ctx context.Context, tx pgx.Tx, ids []string, swaps map[string][]Swap, lo, hi int32) (map[string]*Graph, error) {
	// Avoid reading pathological or unfinished traces. Leave their volume unknown.
	trees := map[string]*Graph{}
	rows, err := tx.Query(ctx, `SELECT trace_id::text,mc_seqno_start FROM public.traces
 WHERE trace_id=ANY(($1::text[])::tonhash[]) AND mc_seqno_end>$2 AND mc_seqno_end<=$3
 AND state='complete' AND nodes_ BETWEEN 1 AND 2048`, ids, lo, hi)
	if err != nil {
		return nil, err
	}
	var valid []string
	minMC := hi
	for rows.Next() {
		var id string
		var start int32
		if err = rows.Scan(&id, &start); err != nil {
			rows.Close()
			return nil, err
		}
		minMC = min(minMC, start)
		valid = append(valid, id)
		trees[id] = &Graph{Root: id, Parent: map[string]string{}, Receiver: map[string]string{}, Start: map[int64][]string{}, RootFunds: map[string]string{}, PtonIn: map[string]bool{}}
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	if len(valid) == 0 {
		return trees, nil
	}
	rows, err = tx.Query(ctx, `SELECT trace_id::text,tx_hash::text,msg_hash::text,direction::text,coalesce(created_lt,tx_lt),coalesce(destination::text,''),
 (source IS NULL AND direction='in' AND tx_hash=trace_id),
 CASE WHEN direction='out' AND tx_hash=trace_id THEN coalesce(value::text,'') ELSE '' END,
 coalesce(opcode=32736093 AND direction='in',false)
 FROM public.messages WHERE trace_id=ANY(($1::text[])::tonhash[]) AND mc_seqno BETWEEN $2 AND $3`, valid, minMC, hi)
	if err != nil {
		return nil, err
	}
	type key struct{ id, h string }
	in, out := map[key]string{}, map[key]string{}
	rootFunds := map[key]string{}
	for rows.Next() {
		var id, txh, h, dir, dest string
		var lt int64
		var external, pton bool
		var attached string
		if err = rows.Scan(&id, &txh, &h, &dir, &lt, &dest, &external, &attached, &pton); err != nil {
			rows.Close()
			return nil, err
		}
		g := trees[id]
		if g == nil {
			continue
		}
		k := key{id, h}
		switch dir {
		case "in":
			if in[k] != "" && in[k] != txh {
				delete(trees, id)
				continue
			}
			in[k] = txh
			g.Receiver[txh] = dest
			if pton {
				g.PtonIn[txh] = true
			}
			g.Start[lt] = append(g.Start[lt], txh)
			if txh == id {
				g.Owner = dest
				g.External = external
			}
		case "out":
			if out[k] != "" && out[k] != txh {
				delete(trees, id)
				continue
			}
			out[k] = txh
			if attached != "" {
				rootFunds[k] = attached
			}
		}
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	for k, child := range in {
		if parent := out[k]; parent != "" && trees[k.id] != nil {
			trees[k.id].Parent[child] = parent
			if funds, ok := rootFunds[k]; ok {
				trees[k.id].RootFunds[child] = funds
			}
		}
	}
	// Separate root-level messages can be independent wallet commands.
	// A router distributing a common balance is not evidence of user intents.
	var candidates []string
	for id, g := range trees {
		branches := map[string]bool{}
		for _, s := range swaps[id] {
			if p := g.path(s); len(p) >= 2 {
				branches[p[1]] = true
			}
		}
		if len(branches) > 1 {
			candidates = append(candidates, id)
		}
	}
	if len(candidates) > 0 {
		rows, err = tx.Query(ctx, `SELECT t.trace_id::text,s.code_hash::text FROM public.transactions t
		JOIN public.account_states s ON s.hash=t.account_state_hash_before WHERE t.trace_id=ANY(($1::text[])::tonhash[])
		AND t.hash=t.trace_id AND t.mc_block_seqno BETWEEN $2 AND $3 AND s.code_hash IS NOT NULL`, candidates, minMC, hi)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, code string
			if err = rows.Scan(&id, &code); err != nil {
				rows.Close()
				return nil, err
			}
			trees[id].Wallet = wallets.KnownCode(code)
		}
		if rows.Err() != nil {
			return nil, rows.Err()
		}
	}
	return trees, nil
}
