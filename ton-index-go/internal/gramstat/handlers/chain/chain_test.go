package chain

import (
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

func TestFullHistorySupplySQL(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local unavailable")
	}
	for _, tc := range []struct {
		name, change string
		reject       bool
		supply       *string
	}{
		{name: "dedup_and_burn_boundary"},
		{name: "null_final_balance", change: "ALTER TABLE raw.blocks UPDATE global_balance=NULL WHERE workchain=-1 AND seqno=6 SETTINGS mutations_sync=1", supply: ptr("")},
		{name: "unknown_burn", change: "ALTER TABLE raw.blocks UPDATE burned=NULL WHERE workchain=-1 AND seqno=5 SETTINGS mutations_sync=1", supply: ptr("")},
		{name: "missing_mc", change: "ALTER TABLE raw.blocks DELETE WHERE workchain=-1 AND seqno=5 SETTINGS mutations_sync=1", reject: true},
		{name: "missing_delta", change: "ALTER TABLE metrics.account_deltas DELETE WHERE mc_seqno=5 SETTINGS mutations_sync=1", reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			dir := t.TempDir()
			run := func(sql string) []byte {
				t.Helper()
				b, err := exec.CommandContext(ctx, binary, "local", "--path", dir, "--max_threads", "2", "--multiquery",
					"--param_raw", "raw", "--param_metrics", "metrics", "--param_end", "8", "--query", sql).CombinedOutput()
				if err != nil {
					t.Fatalf("%v\n%s", err, b)
				}
				return b
			}
			run(`CREATE DATABASE raw; CREATE DATABASE metrics;
CREATE TABLE raw.blocks(workchain Int32,seqno Int32,mc_block_seqno UInt32,gen_utime UInt32,
transaction_count UInt32,transaction_fees Nullable(Decimal(38,0)),import_fees Nullable(Decimal(38,0)),
created Nullable(Decimal(38,0)),burned Nullable(Decimal(38,0)),global_balance Nullable(Decimal(38,0)))
ENGINE=ReplacingMergeTree ORDER BY(workchain,seqno);
CREATE TABLE metrics.account_deltas(mc_seqno UInt32,gen_utime DateTime('UTC')) ENGINE=ReplacingMergeTree ORDER BY mc_seqno;
INSERT INTO raw.blocks VALUES
(-1,3,3,1573862400,1,10,1,100,0,NULL),
(-1,4,4,1573862410,2,20,2,200,0,5000000000000000001),
(-1,4,4,1573862410,2,20,2,200,0,5000000000000000001),
(-1,5,5,1573948800,3,30,3,300,2,5000000000000000101),
(-1,6,6,1573948810,4,40,4,400,3,5000000000000000201),
(0,1,6,1573948810,5,NULL,5,500,99,NULL),
(-1,7,7,1574035200,6,60,6,600,4,5000000000000000301),
(-1,8,8,1574121600,7,70,7,700,5,5000000000000000401);
INSERT INTO metrics.account_deltas SELECT toUInt32(seqno),toDateTime(gen_utime,'UTC') FROM raw.blocks FINAL WHERE workchain=-1;`)
			if tc.change != "" {
				run(tc.change)
			}
			// Exercise the real branch predicates with a compact synthetic mainnet.
			query := strings.ReplaceAll(strings.ReplaceAll(dailySQL, "103190382", "7"), "30410593", "5")
			data := run(query)
			body, count, err := publishRows(data, 8, gramstat.Request{})
			if tc.reject {
				if err == nil {
					t.Fatal("accepted incomplete prefix")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if count != 4 {
				t.Fatalf("%d rows", count)
			}
			var rows []map[string]any
			for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
				var r map[string]any
				if err := json.Unmarshal([]byte(line), &r); err != nil {
					t.Fatal(err)
				}
				rows = append(rows, r)
			}
			if rows[0]["supply_nanoton"] != "5000000000000000001" || rows[0]["transactions"] != "3" {
				t.Fatal(rows[0])
			}
			if tc.supply != nil {
				if rows[1]["supply_nanoton"] != nil {
					t.Fatal(rows[1])
				}
			} else if rows[1]["supply_nanoton"] != "5000000000000000196" {
				t.Fatal(rows[1])
			}
			if rows[1]["fees_nanoton"] != nil || rows[1]["transactions"] != "12" {
				t.Fatal(rows[1])
			}
			if rows[2]["supply_nanoton"] != "5000000000000000301" {
				t.Fatal("v17 double burning", rows[2])
			}
			from, _ := time.Parse(time.DateOnly, "2019-11-17")
			filtered, n, err := publishRows(data, 8, gramstat.Request{From: from, To: from.AddDate(0, 0, 1)})
			if err != nil || n != 1 || !strings.Contains(string(filtered), `"day":"2019-11-17"`) {
				t.Fatalf("filter: %s %v", filtered, err)
			}
			if tc.supply == nil && !strings.Contains(string(filtered), "5000000000000000196") {
				t.Fatal("viewport reset burn prefix")
			}
		})
	}
}

func ptr(s string) *string { return &s }
