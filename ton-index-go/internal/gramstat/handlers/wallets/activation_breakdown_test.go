package wallets

import (
	"context"
	"fmt"
	"net/url"
	"os/exec"
	"reflect"
	"testing"
	"time"
)

func TestActivationVersionMigrationAndFirstCode(t *testing.T) {
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
	run(`CREATE DATABASE gramstat; CREATE DATABASE metrics;`+Schema+`
 CREATE TABLE metrics.deployments(account String,lt UInt64,mc_seqno UInt32,gen_utime DateTime('UTC'),tx_hash String,init_code_hash Nullable(String))
 ENGINE=MergeTree ORDER BY (account,lt,tx_hash);`+fmt.Sprintf(`
 INSERT INTO metrics.deployments VALUES
 ('old',1,1,1794441599,'old','%s'),
 ('old',2,2,1794441601,'redeploy','%s'),
 ('tg',3,2,1794441601,'tg','%s'),('tg',3,2,1794441601,'tg','%s'),
 ('v3',1,1,1794441599,'not-wallet','excluded'),('v3',4,2,1794441602,'v3','%s'),
 ('beta1',5,3,1794441603,'beta1','%s'),('beta2',6,4,1794441604,'beta2','%s'),
 ('late',7,5,1794527999,'late-shard','%s'),
 ('future',8,6,1794528001,'future','%s');
 INSERT INTO gramstat.wallet_days VALUES
 ('s','%s','2026-11-12','00000000-0000-0000-0000-000000000001',5,1794528000,1,'proof',5,1,2,4,now64(6),{}),
 ('s','%s','2026-11-14','00000000-0000-0000-0000-000000000002',6,1794700800,1,'proof',0,7,6,6,now64(6),{});`,
		codes[6], codes[18], codes[18], codes[18], codes[6], codes[9], codes[10], codes[18], codes[18], Registry, Registry), nil)
	p := url.Values{"param_sink": {"gramstat"}, "param_metrics": {"metrics"}, "param_source": {"s"}, "param_registry": {Registry},
		"param_first": {"1"}, "param_previous": {"0"}, "param_end": {"6"}, "param_hashes": {registryParameter()}}
	run(firstSQL, p)
	h := Handler{CH: s}
	start := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	set(p, "day_from", uint64(start.Unix()))
	set(p, "before", uint64(start.AddDate(0, 0, 3).Unix()))
	live, err := h.activationVersions(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	history, err := h.readActivationVersions(ctx, p, activationHistorySQL)
	if err != nil || !reflect.DeepEqual(live, history) {
		t.Fatal("ordered history and lifetime-event join disagree", live, history, err)
	}
	for i := 0; i < 2; i++ {
		if err := h.backfillActivationVersions(ctx, p, start, start.AddDate(0, 0, 3)); err != nil {
			t.Fatal(err)
		}
	}
	if got := run(`SELECT day,count(),uniqExact(run_id),argMax(activation_versions['wallet_tg'],calculated_at),
 argMax(activation_versions['wallet_v3r2'],calculated_at),argMax(activation_versions['wallet_v5_beta'],calculated_at),
 length(argMax(activation_versions,calculated_at)) FROM gramstat.wallet_days GROUP BY day ORDER BY day`, nil); got != "2026-11-12\t2\t1\t2\t1\t2\t18\n2026-11-14\t2\t1\t0\t0\t0\t18\n" {
		t.Fatal(got)
	}
	// A total mismatch must never produce a completed map.
	run(fmt.Sprintf(`INSERT INTO gramstat.wallet_days VALUES
 ('s','%s','2026-11-13','00000000-0000-0000-0000-000000000003',6,1794614400,1,'proof',9,6,6,6,now64(6),{})`, Registry), nil)
	if err := h.backfillActivationVersions(ctx, p, start, start.AddDate(0, 0, 3)); err == nil {
		t.Fatal("published mismatched total")
	}
	if got := run(`SELECT count(),max(length(activation_versions)) FROM gramstat.wallet_days WHERE day='2026-11-13'`, nil); got != "1\t0\n" {
		t.Fatal(got)
	}
	run(fmt.Sprintf(`INSERT INTO metrics.deployments VALUES ('v3',4,2,1794441602,'v3','%s')`, codes[18]), nil)
	if _, err := h.readActivationVersions(ctx, p, activationHistorySQL); err == nil {
		t.Fatal("conflicting codes for the same first event accepted")
	}
}
