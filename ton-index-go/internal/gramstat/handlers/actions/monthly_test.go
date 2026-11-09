package actions

import (
	"testing"
	"time"
)

func TestMonthlyRowsCalendarAndPartialCoverage(t *testing.T) {
	from := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	through := time.Date(2024, 3, 10, 12, 0, 0, 0, time.UTC)
	s := Snapshot{Days: []DayCounts{{Day: from, JettonSenders: 7, SwapUsers: 3}}, Through: through, CalculatedAt: through, SourceMC: 100}
	rows, err := monthlyRows(from, through, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 10 || rows[0].Metric != "monthly_jetton_senders" || rows[0].Value != 7 || rows[0].IsPartial != 0 || rows[0].DataThrough != "2024-03-01 00:00:00" || rows[5].Value != 0 || rows[5].IsPartial != 1 {
		t.Fatal(rows)
	}
	s.Through = from
	if _, err := monthlyRows(from, through, s); err == nil {
		t.Fatal("missing source became zero")
	}
}
