package gramstat

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestPrepareChainDaily(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local unavailable")
	}
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	run := func(sql string) []byte {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, "local", "--path", dir, "--max_threads", "2", "--multiquery",
			"--param_from", "2026-10-21", "--param_to", "2026-10-28", "--param_raw", "raw", "--param_metrics", "metrics",
			"--param_mc_from", "1", "--param_mc_to", "3", "--param_source", "fixture", "--query", sql)
		b, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v\n%s", err, b)
		}
		return b
	}
	run("CREATE DATABASE gramstat;" + ChainSchema + `
CREATE DATABASE raw; CREATE DATABASE metrics;
CREATE TABLE raw.blocks(workchain Int32, seqno UInt32, gen_utime UInt32, transaction_count Int32,
 transaction_fees Nullable(Decimal(38,0)), import_fees Nullable(Decimal(38,0)), created Nullable(Decimal(38,0)), burned Nullable(Decimal(38,0)))
ENGINE=ReplacingMergeTree ORDER BY(seqno,workchain);
CREATE TABLE metrics.total_supply(mc_seqno UInt32, gen_utime DateTime('UTC'), total Decimal(38,0)) ENGINE=ReplacingMergeTree ORDER BY mc_seqno;
INSERT INTO raw.blocks VALUES(-1,1,1792627200,3,100,50,25,0),(-1,1,1792627200,3,100,50,25,0),(0,1,1792627200,4,NULL,50,25,0),(-1,2,1792540800,1,100,0,0,0);
INSERT INTO metrics.total_supply VALUES(1,1792627200,5258373238061259412);
`)
	prep, err := os.ReadFile(filepath.Join("..", "..", "cmd", "gramstat-aggregate", "deploy", "prepare-chain.sql"))
	if err != nil {
		t.Fatal(err)
	}
	run(string(prep))
	run(string(prep))
	b := run(`SELECT day,toString(transactions) AS transactions,toString(supply_nanoton) AS supply,
toString(fees_nanoton) AS fees,is_partial FROM gramstat.chain_daily FINAL ORDER BY day FORMAT JSON`)
	var result struct {
		Data []struct {
			Day, Transactions string
			Supply, Fees      *string
			IsPartial         uint8 `json:"is_partial"`
		} `json:"data"`
	}
	if err = json.Unmarshal(b, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 2 {
		t.Fatal(string(b))
	}
	a, r := result.Data[0], result.Data[1]
	if a.IsPartial != 1 || a.Supply != nil || r.Supply == nil || *r.Supply != "5258373238061259412" || r.Fees != nil || r.Transactions != "7" || r.IsPartial != 0 {
		t.Fatal(string(b))
	}
}
