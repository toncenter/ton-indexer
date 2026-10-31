package gramstat

import (
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestClickHouseSchemaRefreshAndReplacing(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local is not installed")
	}
	query, params := storedDaysQuery([]string{"foo", "bar"}, time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC))
	// Start with an existing seven-column deployment, then apply the production
	// create/upgrade statements twice. Old rows must survive with NULL/empty defaults.
	sql := `CREATE DATABASE gramstat;
CREATE TABLE gramstat.daily_metrics (
 metric LowCardinality(String), day Date, value UInt64,
 calculated_at DateTime64(6,'UTC'), data_through DateTime('UTC'),
 source_mc_seqno UInt32, is_partial UInt8
) ENGINE=ReplacingMergeTree(calculated_at) PARTITION BY toYYYYMM(day) ORDER BY (metric,day);
INSERT INTO gramstat.daily_metrics VALUES
('foo','2026-10-21',99,'2026-10-23 01:00:00.000000','2026-10-22 00:00:00',100,0),
('foo','2026-10-21',0,'2026-10-23 01:00:00.000001','2026-10-22 00:00:00',101,0),
('foo','2026-10-22',7,'2026-10-23 01:00:00.000000','2026-10-23 00:00:00',100,0),
('foo','2026-10-22',8,'2026-10-23 01:00:00.000001','2026-10-22 12:00:00',101,1),
('bar','2026-10-21',9007199254740993,'2026-10-23 01:00:00','2026-10-22 00:00:00',101,0);
` + DailySchema + DailyMigration + DailyMigration + query + `;
SELECT metric, value FROM gramstat.daily_metrics FINAL
WHERE day='2026-10-21' ORDER BY metric FORMAT TabSeparated;
SELECT isNull(amount_nanoton), empty(breakdown)
FROM gramstat.daily_metrics FINAL WHERE metric='bar';
`
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	run := func(query, input string) string {
		t.Helper()
		args := []string{"local", "--path", dir, "--max_threads", "2", "--multiquery", "--query", query}
		for k, values := range params {
			args = append(args, "--"+k, values[0])
		}
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Stdin = strings.NewReader(input)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("production DDL/JSON/coverage SQL: %v\n%s", err, out)
		}
		return string(out)
	}
	text := run(sql, "")
	if strings.Count(text, `"day":"2026-10-21"`) != 2 || strings.Contains(text, `"day":"2026-10-22"`) || !strings.Contains(text, "bar\t9007199254740993\nfoo\t0\n") {
		t.Fatalf("incorrect coverage, replacement or integer precision:\n%s", text)
	}
	if !strings.HasSuffix(text, "1\t1\n") {
		t.Fatalf("migration defaults failed:\n%s", text)
	}
	amount := "100000000000000000001"
	row := DailyMetric{Metric: "staking", Day: "2026-10-21", Value: 1,
		CalculatedAt: "2026-10-23 01:00:00", DataThrough: "2026-10-22 00:00:00", SourceMCSeqno: 101,
		AmountNanoton: &amount, Breakdown: map[string]uint64{"old_dex": 1}}
	data, _ := json.Marshal(row)
	run("INSERT INTO gramstat.daily_metrics FORMAT JSONEachRow", string(data))
	if got := run("SELECT toString(amount_nanoton) FROM gramstat.daily_metrics FINAL WHERE metric='staking'", ""); got != amount+"\n" {
		t.Fatalf("monetary precision lost: %s", got)
	}
	row.CalculatedAt = "2026-10-23 01:00:01"
	row.AmountNanoton = nil
	row.Breakdown = map[string]uint64{"new_dex": 1}
	data, _ = json.Marshal(row)
	run("INSERT INTO gramstat.daily_metrics FORMAT JSONEachRow", string(data))
	if got := run("SELECT isNull(amount_nanoton), mapContains(breakdown,'old_dex'), breakdown['new_dex'] FROM gramstat.daily_metrics FINAL WHERE metric='staking'", ""); got != "1\t0\t1\n" {
		t.Fatalf("replacement of unknown/map failed: %s", got)
	}
}
