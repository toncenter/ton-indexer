package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	keyPrefix         = "ton-index:v1"
	opChangeDNSRecord = 0x4eb1f0f9
	opEditContent     = 0x1a0b9d51
	kvBatch           = 100
	kvPause           = 200 * time.Millisecond
)

var walletKey = sha256.Sum256([]byte("wallet"))

var dnsRoots = []string{
	"0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF",
	"0:80D78A35F955A14B679FAA887FF4CD5BFC0F43B4A4EEA2A7E6927F3701B273C2",
}

type sourceKV struct {
	c redis.UniversalClient
}

func (k *sourceKV) loadItems(ctx context.Context) (map[string][]candidate, error) {
	items := map[string][]candidate{}
	for _, root := range dnsRoots {
		key := keyPrefix + ":idx:nft_items:collection:" + root + ":by_index"
		for start := int64(0); ; start += 1000 {
			time.Sleep(kvPause)
			members, err := k.c.ZRange(ctx, key, start, start+999).Result()
			if err != nil {
				return nil, err
			}
			for _, member := range members {
				_, item, ok := strings.Cut(member, ":")
				if !ok || item == "" {
					return nil, fmt.Errorf("invalid member in %s: %q", key, member)
				}
				items[item] = nil
			}
			if len(members) < 1000 {
				break
			}
		}
	}
	return items, nil
}

func (k *sourceKV) bodies(ctx context.Context, hashes []string) (map[string]string, error) {
	bodies := map[string]string{}
	for start := 0; start < len(hashes); start += kvBatch {
		batch := hashes[start:min(start+kvBatch, len(hashes))]
		keys := make([]string, len(batch))
		for i, hash := range batch {
			keys[i] = rowKey("message_contents", hash) + ":payload"
		}
		time.Sleep(kvPause)
		values, err := k.c.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, err
		}
		for i, value := range values {
			if s, ok := value.(string); ok {
				bodies[batch[i]] = s
			}
		}
	}
	return bodies, nil
}

// Copied from kvrocks_set_current_script in KvrocksState.cpp.
var setCurrentScript = redis.NewScript(`
local current = redis.call('HGET', KEYS[1], 'source_mc_seqno')
if (not current) or tonumber(current) <= tonumber(ARGV[1]) then
  redis.call('HSET', KEYS[1], 'source_mc_seqno', ARGV[1])
  redis.call('SET', KEYS[2], ARGV[2])
  return 1
end
return 0
`)

func rowKey(table, id string) string {
	return keyPrefix + ":" + table + ":" + id
}

type writerPayload struct {
	SchemaVersion  int    `json:"schema_version"`
	SourceMcSeqno  int32  `json:"source_mc_seqno"`
	NftItemAddress string `json:"nft_item_address"`
	Writer         string `json:"writer"`
	WriteLt        string `json:"write_lt"`
	WriteTxHash    string `json:"write_tx_hash"`
	WriteOp        string `json:"write_op"`
}

func openPG(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse -pg: %w", err)
	}
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.RuntimeParams["statement_timeout"] = "30s"
	cfg.RuntimeParams["jit"] = "off"
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	return pgx.ConnectConfig(ctx, cfg)
}

func changesWalletKey(bodyB64 string) (bool, error) {
	boc, err := base64.StdEncoding.DecodeString(bodyB64)
	if err != nil {
		return false, err
	}
	c, err := cell.FromBOC(boc)
	if err != nil {
		return false, err
	}
	s := c.BeginParse()
	op, err := s.LoadUInt(32)
	if err != nil || op != opChangeDNSRecord {
		return false, err
	}
	if _, err := s.LoadUInt(64); err != nil {
		return false, err
	}
	key, err := s.LoadSlice(256)
	if err != nil {
		return false, err
	}
	return bytes.Equal(key, walletKey[:]), nil
}

func main() {
	var pgDsn, kvrocksAddr, kvrocksPassword string
	flag.StringVar(&pgDsn, "pg", "postgresql://localhost:5432", "PostgreSQL connection string of the archive database")
	flag.StringVar(&kvrocksAddr, "kvrocks", "", "Kvrocks address (the master)")
	flag.StringVar(&kvrocksPassword, "kvrocks-password", "", "Kvrocks password")
	dryRun := flag.Bool("dry-run", false, "do not write to Kvrocks")
	out := flag.String("out", "", "writer payloads as JSON lines, sorted by item")
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "Usage: dns-wallet-writers [flags]\n\nBackfill DNS wallet writers from the archive database into Kvrocks.")
		flag.PrintDefaults()
	}
	flag.Parse()
	if kvrocksAddr == "" {
		log.Fatal("-kvrocks is required")
	}

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: kvrocksAddr, Password: kvrocksPassword, ReadTimeout: 30 * time.Second})
	defer client.Close()
	var dst redis.UniversalClient
	if !*dryRun {
		dst = client
	}
	if err := run(ctx, &sourceKV{c: client}, dst, pgDsn, *out); err != nil {
		log.Fatal(err)
	}
}
