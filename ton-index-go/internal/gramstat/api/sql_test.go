package api

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/handlers/validators"
)

func TestQueriesWithRealClickHouse(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local unavailable")
	}
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	run := func(sql string) []byte {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, "local", "--path", dir, "--max_threads", "2", "--multiquery", "--param_from", "2026-10-21", "--param_to", "2026-10-28", "--param_round", "1792540800", "--param_raw", "raw", "--param_metrics", "metrics", "--param_mc_from", "1", "--param_mc_to", "3", "--param_source", "fixture", "--join_use_nulls", "1", "--query", sql)
		b, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v\n%s", err, b)
		}
		return b
	}
	run(gramstat.DailySchema + gramstat.StarsSchema + gramstat.ChainSchema + validators.Schema + `
CREATE DATABASE raw; CREATE DATABASE metrics;
CREATE TABLE raw.blocks(workchain Int32, seqno UInt32, gen_utime UInt32, transaction_count Int32,
 transaction_fees Nullable(Decimal(38,0)), import_fees Nullable(Decimal(38,0)), created Nullable(Decimal(38,0)), burned Nullable(Decimal(38,0)))
ENGINE=ReplacingMergeTree ORDER BY(seqno,workchain);
CREATE TABLE metrics.total_supply(mc_seqno UInt32, gen_utime DateTime('UTC'), total Decimal(38,0)) ENGINE=ReplacingMergeTree ORDER BY mc_seqno;
INSERT INTO raw.blocks VALUES(-1,1,1792627200,3,100,50,25,0),(-1,1,1792627200,3,100,50,25,0),(0,1,1792627200,4,NULL,50,25,0),(-1,2,1792540800,1,100,0,0,0);
INSERT INTO metrics.total_supply VALUES(1,1792627200,5258373238061259412);
INSERT INTO gramstat.validator_rounds (utime_since,utime_until,total,roster_digest,config34_hash,calculated_at) VALUES(1792540800,1792606336,380,'current','hash','2026-10-22 00:00:00');
INSERT INTO gramstat.validator_geo_rounds(utime_since,utime_until,total,located,countries,roster_digest,config34_hash,calculated_at) VALUES(1792540800,1792606336,379,378,22,'old','hash','2026-10-22 00:00:00');
`)
	prep, err := os.ReadFile(filepath.Join("..", "..", "..", "cmd", "gramstat-api", "deploy", "prepare-chain.sql"))
	if err != nil {
		t.Fatal(err)
	}
	run(string(prep))
	for name, query := range queries {
		b := run(query + " FORMAT JSON")
		var result struct {
			Data []map[string]any `json:"data"`
		}
		if err = json.Unmarshal(b, &result); err != nil {
			t.Fatalf("%s: %v %s", name, err, b)
		}
		if name == "chain" {
			if len(result.Data) != 2 {
				t.Fatal(string(b))
			}
			r := result.Data[1]
			if r["supply_nanoton"] != "5258373238061259412" || r["fees_nanoton"] != nil || r["transactions"] != "7" || r["is_partial"] != float64(0) {
				t.Fatal(string(b))
			}
		}
		if name == "rounds" && (len(result.Data) != 1 || result.Data[0]["countries"] != nil || result.Data[0]["located"] != nil) {
			t.Fatal("stale geo joined: " + string(b))
		}
		if name == "locations" && len(result.Data) != 0 {
			t.Fatal("stale map published")
		}
	}
	// API projections must not leak source hostnames, raw IPs or internal record IDs.
	if strings.Contains(queries["locations"], "record_id") {
		t.Fatal("internal evidence leaked")
	}
}
