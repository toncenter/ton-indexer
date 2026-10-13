package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

const scanQuery = `select m.mc_seqno, t.hash, t.lt, coalesce(m.created_lt, m.tx_lt), m.msg_hash, m.source, m.destination,
       coalesce(t.aborted, true), coalesce(t.compute_success, false), coalesce(m.bounced, false), m.body_hash
from messages m
join transactions t on t.hash = m.tx_hash and t.lt = m.tx_lt and t.mc_block_seqno = m.mc_seqno
where m.opcode = $1 and (coalesce(m.created_lt, m.tx_lt), m.msg_hash) > ($2, $3) and m.direction = 'in'
order by coalesce(m.created_lt, m.tx_lt), m.msg_hash
limit $4`

const (
	pageSize  = 2000
	pagePause = 100 * time.Millisecond
)

type scanRow struct {
	McSeqno     int32
	TxHash      string
	Lt          int64
	KeyLt       int64
	MsgHash     string
	Source      *string
	Destination *string
	Aborted     bool
	Success     bool
	Bounced     bool
	BodyHash    *string
}

type candidate struct {
	Lt       int64
	McSeqno  int32
	TxHash   string
	Op       int32
	BodyHash string
	Source   string
}

func scanCandidates(ctx context.Context, conn *pgx.Conn, items map[string][]candidate) error {
	candidates := 0
	for _, op := range []int32{opChangeDNSRecord, opEditContent} {
		log.Printf("scanning opcode %x", op)
		// tonhash rejects an empty hash; lt -1 includes all nonnegative logical times.
		lt, hash := int64(-1), base64.StdEncoding.EncodeToString(make([]byte, 32))
		for page := 1; ; page++ {
			rows, err := conn.Query(ctx, scanQuery, op, lt, hash, pageSize)
			if err != nil {
				return fmt.Errorf("opcode %x after lt %d: %w", op, lt, err)
			}
			batch, err := pgx.CollectRows(rows, pgx.RowToStructByPos[scanRow])
			if err != nil {
				return err
			}
			for _, r := range batch {
				if r.Aborted || !r.Success || r.Bounced || r.Source == nil || r.Destination == nil {
					continue
				}
				if _, ok := items[*r.Destination]; !ok {
					continue
				}
				c := candidate{Lt: r.Lt, McSeqno: r.McSeqno, TxHash: r.TxHash, Op: op, Source: *r.Source}
				if r.BodyHash != nil {
					c.BodyHash = *r.BodyHash
				}
				items[*r.Destination] = append(items[*r.Destination], c)
				candidates++
			}
			if len(batch) > 0 {
				last := batch[len(batch)-1]
				lt, hash = last.KeyLt, last.MsgHash
			}
			if page%50 == 0 || len(batch) < pageSize {
				log.Printf("opcode %x: page %d, lt %d, candidates %d", op, page, lt, candidates)
			}
			if len(batch) < pageSize {
				break
			}
			time.Sleep(pagePause)
		}
	}
	return nil
}

func resolve(ctx context.Context, kv *sourceKV, items map[string][]candidate) ([]writerPayload, error) {
	for item, writes := range items {
		if len(writes) == 0 {
			delete(items, item)
			continue
		}
		sort.SliceStable(writes, func(i, j int) bool { return writes[i].Lt > writes[j].Lt })
	}
	var payloads []writerPayload
	var missing []string
	for round := 1; len(items) > 0; round++ {
		var hashes []string
		for _, writes := range items {
			if c := writes[0]; c.Op == opChangeDNSRecord && c.BodyHash != "" {
				hashes = append(hashes, c.BodyHash)
			}
		}
		if len(hashes) >= 1000 || round%50 == 0 {
			log.Printf("resolve round %d: %d items left, %d bodies to read, %d writers found", round, len(items), len(hashes), len(payloads))
		}
		bodies, err := kv.bodies(ctx, hashes)
		if err != nil {
			return nil, fmt.Errorf("mget message_contents: %w", err)
		}
		for item, writes := range items {
			c := writes[0]
			items[item] = writes[1:]
			if len(writes) == 1 {
				delete(items, item)
			}
			op := "edit_content"
			if c.Op == opChangeDNSRecord {
				op = "change_dns_record"
				body, ok := bodies[c.BodyHash]
				if !ok {
					missing = append(missing, item)
					delete(items, item)
					continue
				}
				var mc struct {
					Body string `json:"body"`
				}
				if err := json.Unmarshal([]byte(body), &mc); err != nil {
					return nil, fmt.Errorf("decode message_contents %s: %w", c.BodyHash, err)
				}
				if wallet, err := changesWalletKey(mc.Body); err != nil || !wallet {
					continue
				}
			}
			// pgton text is already canonical; retain the old exclusion of non-standard sources.
			wc, _, _ := strings.Cut(c.Source, ":")
			if _, err := strconv.ParseInt(wc, 10, 32); err != nil {
				continue
			}
			payloads = append(payloads, writerPayload{SchemaVersion: 1, SourceMcSeqno: c.McSeqno,
				NftItemAddress: item, Writer: c.Source, WriteLt: strconv.FormatInt(c.Lt, 10), WriteTxHash: c.TxHash, WriteOp: op})
			delete(items, item)
		}
	}
	sort.Slice(payloads, func(i, j int) bool { return payloads[i].NftItemAddress < payloads[j].NftItemAddress })
	sort.Strings(missing)
	log.Printf("missing body %d: %v", len(missing), missing)
	return payloads, nil
}

func writeAll(ctx context.Context, dst redis.UniversalClient, out string, payloads []writerPayload) error {
	if out != "" {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		for _, w := range payloads {
			if err := enc.Encode(w); err != nil {
				return err
			}
		}
		if err := os.WriteFile(out, buf.Bytes(), 0o644); err != nil {
			return err
		}
	}
	applied := 0
	if dst != nil {
		if err := setCurrentScript.Load(ctx, dst).Err(); err != nil {
			return err
		}
		for start := 0; start < len(payloads); start += kvBatch {
			pipe := dst.Pipeline()
			var cmds []*redis.Cmd
			for _, w := range payloads[start:min(start+kvBatch, len(payloads))] {
				data, err := json.Marshal(w)
				if err != nil {
					return err
				}
				key := rowKey("dns_wallet_writers", w.NftItemAddress)
				cmds = append(cmds, setCurrentScript.EvalSha(ctx, pipe, []string{key, key + ":payload"}, w.SourceMcSeqno, string(data)))
			}
			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}
			for _, cmd := range cmds {
				if n, err := cmd.Int(); err != nil {
					return err
				} else if n == 1 {
					applied++
				}
			}
			if done := min(start+kvBatch, len(payloads)); done%10000 == 0 {
				log.Printf("written %d of %d, applied %d", done, len(payloads), applied)
			}
			time.Sleep(kvPause)
		}
	}
	log.Printf("writers %d, applied %d, target written %v", len(payloads), applied, dst != nil)
	return nil
}

func run(ctx context.Context, kv *sourceKV, dst redis.UniversalClient, dsn string, out string) error {
	items, err := kv.loadItems(ctx)
	if err != nil {
		return fmt.Errorf("load DNS items: %w", err)
	}
	log.Printf("DNS items %d", len(items))
	conn, err := openPG(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	if err := scanCandidates(ctx, conn, items); err != nil {
		return err
	}
	payloads, err := resolve(ctx, kv, items)
	if err != nil {
		return err
	}
	return writeAll(ctx, dst, out, payloads)
}
