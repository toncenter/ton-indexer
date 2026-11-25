package gramstat

import (
	"fmt"
	"time"
)

func Day(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func (r Request) DailyWindow() (from, to time.Time, err error) {
	if r.AsOf.IsZero() || r.LookbackDays < 1 {
		return from, to, fmt.Errorf("observation time and positive lookback are required")
	}
	to = r.AsOf.UTC().Truncate(time.Second)
	from = Day(to).AddDate(0, 0, -r.LookbackDays)
	if r.From.IsZero() != r.To.IsZero() {
		return from, to, fmt.Errorf("--from and --to must be supplied together")
	}
	if !r.From.IsZero() {
		from = r.From.UTC()
		if !from.Equal(Day(from)) || !r.To.Equal(Day(r.To)) || r.To.After(Day(to).AddDate(0, 0, 1)) {
			return from, to, fmt.Errorf("bounds must be UTC dates, with --to no later than tomorrow")
		}
		if r.To.Before(to) {
			to = r.To.UTC()
		}
	}
	if !from.Before(to) {
		return from, to, fmt.Errorf("empty or reversed date range")
	}
	return from, to, nil
}

// CatchUpStart repairs missing closed days within the already established history
// as well as a missed tail. A newly added metric starts at the same history floor.
// An empty destination starts with the normal lookback, never an implicit full replay.
func CatchUpStart(from, today time.Time, metrics []string, stored map[string][]time.Time) time.Time {
	floor := from
	for _, name := range metrics {
		for _, day := range stored[name] {
			if day.Before(floor) {
				floor = day
			}
		}
	}
	for _, name := range metrics {
		present := make(map[string]bool, len(stored[name]))
		for _, day := range stored[name] {
			present[day.Format(time.DateOnly)] = true
		}
		for day := floor; day.Before(today); day = day.AddDate(0, 0, 1) {
			if !present[day.Format(time.DateOnly)] {
				if day.Before(from) {
					from = day
				}
				break
			}
		}
	}
	return from
}
