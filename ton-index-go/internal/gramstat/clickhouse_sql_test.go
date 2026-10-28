package gramstat

import (
	"context"
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
	sql := DailySchema + `
INSERT INTO gramstat.daily_metrics VALUES
('foo','2026-10-21',99,'2026-10-23 01:00:00.000000','2026-10-22 00:00:00',100,0),
('foo','2026-10-21',0,'2026-10-23 01:00:00.000001','2026-10-22 00:00:00',101,0),
('foo','2026-10-22',7,'2026-10-23 01:00:00.000000','2026-10-23 00:00:00',100,0),
('foo','2026-10-22',8,'2026-10-23 01:00:00.000001','2026-10-22 12:00:00',101,1),
('bar','2026-10-21',9007199254740993,'2026-10-23 01:00:00','2026-10-22 00:00:00',101,0);
` + query + `;
SELECT metric, value FROM gramstat.daily_metrics FINAL
WHERE day='2026-10-21' ORDER BY metric FORMAT TabSeparated;
`
	args := []string{"local", "--path", t.TempDir(), "--max_threads", "2", "--multiquery", "--query", sql}
	for k, values := range params {
		args = append(args, "--"+k, values[0])
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("production DDL/coverage SQL: %v\n%s", err, out)
	}
	text := string(out)
	if strings.Count(text, `"day":"2026-10-21"`) != 2 || strings.Contains(text, `"day":"2026-10-22"`) || !strings.Contains(text, "bar\t9007199254740993\nfoo\t0\n") {
		t.Fatalf("incorrect coverage, replacement or integer precision:\n%s", out)
	}
}
