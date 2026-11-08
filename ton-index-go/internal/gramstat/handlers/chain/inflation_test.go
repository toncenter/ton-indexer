package chain

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
)

func TestInflation24HourWindows(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local unavailable")
	}
	for _, tc := range []struct {
		name, change string
		unknown      bool
		expected     string
	}{
		{name: "issuance_jump", expected: "9.125"},
		{name: "negative", change: "ALTER TABLE raw.blocks UPDATE burned=200 WHERE workchain=-1 SETTINGS mutations_sync=1", expected: "-166.075"},
		{name: "unknown_burning", change: "ALTER TABLE raw.blocks UPDATE burned=NULL WHERE seqno=12 SETTINGS mutations_sync=1", unknown: true},
		{name: "unknown_supply", change: "ALTER TABLE gramstat.supply_hourly UPDATE supply_nanoton=NULL WHERE last_mc=27 SETTINGS mutations_sync=1", unknown: true},
		{name: "missing_hour", change: "ALTER TABLE gramstat.supply_hourly DELETE WHERE last_mc=12 SETTINGS mutations_sync=1", unknown: true},
		{name: "missing_flows", change: "ALTER TABLE raw.blocks DELETE WHERE seqno=12 SETTINGS mutations_sync=1", unknown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			run := func(sql string) []byte {
				t.Helper()
				b, e := exec.Command(binary, "local", "--path", dir, "--max_threads", "2", "--multiquery", "--param_raw", "raw", "--param_end", "28", "--query", sql).CombinedOutput()
				if e != nil {
					t.Fatalf("%v\n%s", e, b)
				}
				return b
			}
			run("CREATE DATABASE gramstat; CREATE DATABASE raw;" + Schema + `
CREATE TABLE raw.blocks(workchain Int32,seqno UInt32,mc_block_seqno UInt32,gen_utime UInt32,created Nullable(Decimal(38,0)),burned Nullable(Decimal(38,0))) ENGINE=ReplacingMergeTree ORDER BY(workchain,seqno);
INSERT INTO raw.blocks SELECT -1,number+3,number+3,1573862400+number*3600,if(number=24,20,10),0 FROM numbers(26);
INSERT INTO raw.blocks SELECT * FROM raw.blocks WHERE seqno=27;
INSERT INTO gramstat.supply_hourly SELECT toDateTime(1573862400+number*3600,'UTC'),number+3,number+3,1573862400+number*3600,1573862400+number*3600+3599,0,0,1000000,1,number=25,28,now64(6) FROM numbers(26);`)
			if tc.change != "" {
				run(tc.change)
			}
			run("INSERT INTO gramstat.inflation_hourly " + inflationSQL)
			b := run("SELECT toString(annualized_percent) AS rate,toString(minted_24h_nanoton) AS minted,toString(burned_24h_nanoton) AS burned,point_at-window_from AS seconds FROM gramstat.inflation_hourly FINAL ORDER BY point_at FORMAT JSON")
			var rows struct {
				Data []struct {
					Rate, Minted, Burned *string
					Seconds              int
				}
			}
			if e := json.Unmarshal(b, &rows); e != nil {
				t.Fatal(e, string(b))
			}
			expectedRows := 25
			if tc.name == "missing_hour" {
				expectedRows = 24
			}
			if len(rows.Data) != expectedRows {
				t.Fatal(string(b))
			}
			for _, r := range rows.Data[:len(rows.Data)-1] {
				if r.Rate != nil {
					t.Fatal("incomplete window published", string(b))
				}
			}
			last := rows.Data[len(rows.Data)-1]
			if last.Seconds != 86400 {
				t.Fatal(last)
			}
			if tc.unknown {
				if last.Rate != nil {
					t.Fatal(string(b))
				}
			} else if last.Rate == nil || *last.Rate != tc.expected {
				t.Fatal(string(b))
			}
			if !tc.unknown && (last.Minted == nil || !strings.HasPrefix(*last.Minted, "250")) {
				t.Fatal(last)
			}
		})
	}
}
