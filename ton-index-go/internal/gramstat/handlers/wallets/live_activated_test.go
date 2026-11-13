package wallets

import (
	"context"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestActivationStreamCommitRetryMCTiesAndTransactionDay(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s := &localStore{binary: binary, path: t.TempDir()}
	run := func(q string, p url.Values) string {
		t.Helper()
		b, e := s.Query(ctx, q, p)
		if e != nil {
			t.Fatal(e)
		}
		return string(b)
	}
	fixture := `CREATE DATABASE gramstat; CREATE DATABASE raw; CREATE DATABASE metrics;` + Schema + `
 CREATE TABLE raw.blocks(workchain Int32,shard Int64,seqno UInt32,mc_block_seqno UInt32,gen_utime UInt32,start_lt UInt64,end_lt UInt64) ENGINE=ReplacingMergeTree ORDER BY (workchain,shard,seqno);
 CREATE TABLE metrics.deployments(account String,lt UInt64,mc_seqno UInt32,gen_utime DateTime('UTC'),tx_hash String,init_code_hash Nullable(String)) ENGINE=MergeTree ORDER BY (mc_seqno,account);
 INSERT INTO raw.blocks VALUES(-1,-9223372036854775808,10,10,1794441599,100,200),(-1,-9223372036854775808,11,11,1794441660,200,300),(-1,-9223372036854775808,12,12,1794441660,300,400),
 (0,-9223372036854775808,1,11,1794441599,200,300);
 INSERT INTO metrics.deployments VALUES('old',1,9,1794441500,'old','HASH'),('old',320,12,1794441660,'redeploy','HASH'),('late',250,11,1794441599,'late','HASH'),('new',350,12,1794441660,'new','HASH'),('new',350,12,1794441660,'new','HASH'),('excluded',360,12,1794441660,'excluded','other');
 INSERT INTO gramstat.wallet_days VALUES('fixture','REGISTRY','2026-11-11','00000000-0000-0000-0000-000000000001',11,1794441600,1,'proof',1,9007199254740992,10,10,now64(6));
 INSERT INTO gramstat.wallet_activations_mc VALUES('fixture','REGISTRY','2026-11-11','00000000-0000-0000-0000-000000000001',10,1794441599,0,9007199254740993);`
	run(strings.ReplaceAll(strings.ReplaceAll(fixture, "HASH", codes[0]), "REGISTRY", Registry), nil)
	p := url.Values{"param_sink": {"gramstat"}, "param_source": {"fixture"}, "param_registry": {Registry}, "param_metrics": {"metrics"}, "param_raw": {"raw"}, "param_first": {"9"}, "param_end": {"12"}, "param_previous": {"0"}, "param_hashes": {registryParameter()}}
	run(firstSQL, p)
	h := Handler{CH: s}
	source := source{First: 9, End: 12, From: 1794355200, Before: 1794441900, Verified: 1}
	s.fail = "INSERT INTO {sink:Identifier}.wallet_activation_stream_checkpoints"
	if err = h.liveActivated(ctx, p, source); err == nil {
		t.Fatal("failed checkpoint publication accepted")
	}
	if got := run("SELECT count() FROM gramstat.wallet_activation_stream_state", nil); got != "0\n" {
		t.Fatal("uncommitted prefix visible", got)
	}
	s.fail = ""
	for i := 0; i < 2; i++ {
		if err = h.liveActivated(ctx, p, source); err != nil {
			t.Fatal(err)
		}
	}
	if got := run("SELECT base_mc,through_mc,activated,activations FROM gramstat.wallet_activation_stream_state", nil); got != "10\t12\t9007199254740995\t1\n" {
		t.Fatal("precision, redeploy or transaction-day error", got)
	}
	if got := run("SELECT mc_seqno,any(activated),count() FROM gramstat.wallet_activation_stream GROUP BY mc_seqno ORDER BY mc_seqno", nil); got != "11\t9007199254740994\t2\n12\t9007199254740995\t2\n" {
		t.Fatal("retry/ties changed count", got)
	}
	source.End = 11
	if err = h.liveActivated(ctx, p, source); err == nil {
		t.Fatal("regressing source accepted")
	}
}
