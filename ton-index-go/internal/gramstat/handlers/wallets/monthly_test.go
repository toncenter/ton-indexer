package wallets

import (
	"fmt"
	"os/exec"
	"testing"
)

func TestMonthlyExactWindow(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local unavailable")
	}
	dir := t.TempDir()
	month := "2025-01-31"
	run := func(sql string) string {
		t.Helper()
		out, err := exec.Command(binary, "local", "--path", dir, "--max_threads", "2", "--multiquery", "--param_sink", "gramstat", "--param_source", "s", "--param_registry", "r", "--param_monthly_day", month, "--query", sql).CombinedOutput()
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
	if got := run("SELECT active,length(runs) FROM gramstat.wallet_months"); got != "32\t31\n" {
		t.Fatal(got)
	}
	run("TRUNCATE TABLE gramstat.wallet_months; ALTER TABLE gramstat.wallet_days DELETE WHERE day='2025-01-15' SETTINGS mutations_sync=1;")
	run(monthlySQL)
	if got := run("SELECT count() FROM gramstat.wallet_months"); got != "0\n" {
		t.Fatal("incomplete window published", got)
	}
	// A complete certified empty month is a real zero.
	run("INSERT INTO gramstat.wallet_days SELECT 's','r',toDate('2025-01-15'),toUUID('00000000-0000-0000-0000-000000000001'),1,1,1,'proof',0,0,0,0,now64(6); TRUNCATE TABLE gramstat.wallet_activity;")
	run(monthlySQL)
	if got := run("SELECT active FROM gramstat.wallet_months"); got != "0\n" {
		t.Fatal(got)
	}
	for _, tc := range []struct {
		month string
		days  int
	}{{"2024-02-01", 29}, {"2025-02-01", 28}, {"2025-04-01", 30}} {
		month = tc.month
		run("TRUNCATE TABLE gramstat.wallet_days; TRUNCATE TABLE gramstat.wallet_months;" + fmt.Sprintf("INSERT INTO gramstat.wallet_days SELECT 's','r',toDate('%s')+toUInt32(number),toUUID('00000000-0000-0000-0000-000000000001'),1,1,1,'proof',0,0,0,0,now64(6) FROM numbers(%d);", month, tc.days-1))
		run(monthlySQL)
		if got := run("SELECT count() FROM gramstat.wallet_months"); got != "0\n" {
			t.Fatal("partial month published", month, got)
		}
		run(fmt.Sprintf("INSERT INTO gramstat.wallet_days SELECT 's','r',toDate('%s')+%d,toUUID('00000000-0000-0000-0000-000000000001'),1,1,1,'proof',0,0,0,0,now64(6)", month, tc.days-1))
		run(monthlySQL)
		if got := run("SELECT active,length(runs) FROM gramstat.wallet_months"); got != fmt.Sprintf("0\t%d\n", tc.days) {
			t.Fatal(month, got)
		}
	}
}
