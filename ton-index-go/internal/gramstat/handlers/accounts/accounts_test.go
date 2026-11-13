package accounts

import (
	"encoding/json"
	"os/exec"
	"testing"
)

func TestAccountsBaselineAndDeltaPrefix(t *testing.T) {
	binary, e := exec.LookPath("clickhouse")
	if e != nil {
		t.Skip("clickhouse-local unavailable")
	}
	for _, tc := range []struct {
		name, change string
		fail         bool
	}{
		{name: "transitions_and_dedup"},
		{name: "gap", change: "ALTER TABLE metrics.account_deltas DELETE WHERE mc_seqno=4 SETTINGS mutations_sync=1;", fail: true},
		{name: "negative_count", change: "ALTER TABLE metrics.account_deltas UPDATE delta_active=-20 WHERE mc_seqno=6 SETTINGS mutations_sync=1;", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			run := func(q string) ([]byte, error) {
				return exec.Command(binary, "local", "--path", dir, "--max_threads", "2", "--multiquery", "--param_metrics", "metrics", "--param_baseline_mc", "2", "--param_baseline_uninit", "0", "--param_baseline_active", "7", "--param_baseline_frozen", "0", "--param_baseline_id", "verified", "--param_end", "6", "--query", q).CombinedOutput()
			}
			b, e := run("CREATE DATABASE gramstat;CREATE DATABASE metrics;" + Schema + `
CREATE TABLE metrics.account_deltas(mc_seqno UInt32,gen_utime DateTime('UTC'),delta_uninit Int64,delta_active Int64,delta_frozen Int64) ENGINE=ReplacingMergeTree ORDER BY(gen_utime,mc_seqno);
INSERT INTO metrics.account_deltas VALUES
(3,1573862400,1,0,0),(3,1573862400,1,0,0),
(4,1573862400,-1,1,0),(5,1573866000,0,-1,1),(6,1573866010,0,0,-1);` + tc.change)
			if e != nil {
				t.Fatal(e, string(b))
			}
			b, e = run("INSERT INTO gramstat.accounts_hourly " + hourlySQL)
			if tc.fail {
				if e == nil {
					t.Fatal("accepted invalid prefix")
				}
				return
			}
			if e != nil {
				t.Fatal(e, string(b))
			}
			b, e = run("SELECT hour,toString(opening_active) AS opening,toString(uninit) AS uninit,toString(active) AS active,toString(frozen) AS frozen FROM gramstat.accounts_hourly FINAL ORDER BY hour FORMAT JSON")
			if e != nil {
				t.Fatal(e, string(b))
			}
			var r struct {
				Data []struct{ Opening, Uninit, Active, Frozen string }
			}
			if e = json.Unmarshal(b, &r); e != nil {
				t.Fatal(e, string(b))
			}
			if len(r.Data) != 2 || r.Data[0].Opening != "7" || r.Data[0].Active != "8" || r.Data[0].Uninit != "0" || r.Data[1].Opening != "8" || r.Data[1].Active != "7" || r.Data[1].Frozen != "0" {
				t.Fatal(string(b))
			}
		})
	}
}
