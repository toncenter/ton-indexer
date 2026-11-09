package wallets

import (
	"os/exec"
	"testing"
)

func TestMonthlyExactWindow(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local unavailable")
	}
	dir := t.TempDir()
	run := func(sql string) string {
		t.Helper()
		out, err := exec.Command(binary, "local", "--path", dir, "--max_threads", "2", "--multiquery", "--param_sink", "gramstat", "--param_source", "s", "--param_registry", "r", "--param_monthly_day", "2025-01-31", "--query", sql).CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		return string(out)
	}
	run("CREATE DATABASE gramstat;" + Schema + `
 INSERT INTO gramstat.wallet_days SELECT 's','r',toDate('2025-01-01')+toUInt32(number),toUUID('00000000-0000-0000-0000-000000000001'),1,1,1,'proof',0,0,0,0,now64(6) FROM numbers(31);
 INSERT INTO gramstat.wallet_activity SELECT 's','r',toDate('2025-01-01')+toUInt32(number),toUUID('00000000-0000-0000-0000-000000000001'),86400,0,uniqExactState(account)
 FROM (SELECT number,arrayJoin(['shared',toString(number)]) account FROM numbers(31)) GROUP BY number;
 -- A large unpublished state must not contribute.
 INSERT INTO gramstat.wallet_activity SELECT 's','r',toDate('2025-01-31'),toUUID('00000000-0000-0000-0000-000000000002'),86400,0,uniqExactState(toString(number)) FROM numbers(1000);
 `)
	run(monthlySQL)
	if got := run("SELECT active,length(runs) FROM gramstat.wallet_monthly"); got != "31\t30\n" {
		t.Fatal(got)
	}
	run("TRUNCATE TABLE gramstat.wallet_monthly; ALTER TABLE gramstat.wallet_days DELETE WHERE day='2025-01-15' SETTINGS mutations_sync=1;")
	run(monthlySQL)
	if got := run("SELECT count() FROM gramstat.wallet_monthly"); got != "0\n" {
		t.Fatal("incomplete window published", got)
	}
	// All 30 certified empty days are a real zero.
	run("INSERT INTO gramstat.wallet_days SELECT 's','r',toDate('2025-01-15'),toUUID('00000000-0000-0000-0000-000000000001'),1,1,1,'proof',0,0,0,0,now64(6); TRUNCATE TABLE gramstat.wallet_activity;")
	run(monthlySQL)
	if got := run("SELECT active FROM gramstat.wallet_monthly"); got != "0\n" {
		t.Fatal(got)
	}
}
