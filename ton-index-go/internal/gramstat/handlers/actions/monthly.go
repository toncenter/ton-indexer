package actions

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

// Only unique-address metrics require a separate monthly source calculation.
// Additive counts and amounts are combined from the prepared daily observations.
const monthlySQL = `SELECT date_trunc('month',to_timestamp(start_utime) AT TIME ZONE 'UTC')::date,
 count(DISTINCT source) FILTER (WHERE type='jetton_transfer'),
 count(DISTINCT destination) FILTER (WHERE type='jetton_transfer'), 0,
 count(DISTINCT source) FILTER (WHERE type='nft_transfer'),
 count(DISTINCT destination) FILTER (WHERE type='nft_transfer'), 0, 0,
 count(DISTINCT source) FILTER (WHERE type='jetton_swap'),
 '{}'::jsonb, 0, '0'::text, '{}'::jsonb, 0, 0, '[]'::jsonb, '{}'::jsonb
 FROM public.actions
 WHERE trace_mc_seqno_end >= $1 AND trace_mc_seqno_end <= $2
 AND start_utime >= $3 AND start_utime < $4 AND success=true
 AND type IN ('jetton_transfer','nft_transfer','jetton_swap')
 GROUP BY 1 ORDER BY 1`

type MonthlySource interface {
	CollectMonthly(context.Context, time.Time, time.Time) (Snapshot, error)
}

type MonthlyHandler struct {
	Source MonthlySource
	Sink   interface {
		WriteDaily(context.Context, []gramstat.DailyMetric) error
	}
	Log *slog.Logger
}

func (*MonthlyHandler) Name() string { return "actions_monthly" }

func monthStart(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func (h *MonthlyHandler) Run(ctx context.Context, req gramstat.Request) error {
	from, to, err := req.DailyWindow()
	if err != nil {
		return err
	}
	if req.From.IsZero() {
		from = monthStart(req.AsOf).AddDate(0, -1, 0)
	}
	genesis := time.Date(2019, 11, 15, 0, 0, 0, 0, time.UTC)
	if (!from.Equal(monthStart(from)) && !from.Equal(genesis)) || (!req.To.IsZero() && !req.To.Equal(monthStart(req.To)) && !req.To.Equal(gramstat.Day(req.AsOf).AddDate(0, 0, 1))) {
		return fmt.Errorf("monthly actions require complete calendar boundaries, genesis, or tomorrow UTC")
	}
	h.Log.Info("monthly actions collecting", "from", from, "to", to)
	snapshot, err := h.Source.CollectMonthly(ctx, from, to)
	if err != nil {
		return err
	}
	rows, err := monthlyRows(from, to, snapshot)
	if err != nil {
		return err
	}
	if err = h.Sink.WriteDaily(ctx, rows); err != nil {
		return err
	}
	h.Log.Info("monthly actions published", "rows", len(rows), "from", from, "data_through", snapshot.Through)
	return nil
}

func monthlyRows(from, to time.Time, s Snapshot) ([]gramstat.DailyMetric, error) {
	if !s.Through.After(from) || s.Through.After(to) || s.CalculatedAt.IsZero() || s.SourceMC == 0 {
		return nil, fmt.Errorf("invalid monthly snapshot")
	}
	byMonth := map[time.Time]DayCounts{}
	for _, d := range s.Days {
		if !d.Day.Equal(monthStart(d.Day)) || d.Day.Before(monthStart(from)) || !d.Day.Before(s.Through) {
			return nil, fmt.Errorf("invalid monthly observation")
		}
		if _, ok := byMonth[d.Day]; ok {
			return nil, fmt.Errorf("duplicate month")
		}
		byMonth[d.Day] = d
	}
	var rows []gramstat.DailyMetric
	for month := monthStart(from); month.Before(s.Through); month = month.AddDate(0, 1, 0) {
		d := byMonth[month]
		end := month.AddDate(0, 1, 0)
		partial := uint8(0)
		if end.After(s.Through) {
			end = s.Through
			partial = 1
		}
		if from.After(month) {
			partial = 1
		}
		names := []string{"jetton_senders", "jetton_receivers", "nft_senders", "nft_receivers", "swaps_users"}
		values := []uint64{d.JettonSenders, d.JettonReceivers, d.NFTSenders, d.NFTReceivers, d.SwapUsers}
		for i, name := range names {
			rows = append(rows, gramstat.DailyMetric{Metric: "monthly_" + name, Day: month.Format(time.DateOnly), Value: values[i], CalculatedAt: s.CalculatedAt.UTC().Format("2006-01-02 15:04:05.000000"), DataThrough: end.UTC().Format("2006-01-02 15:04:05"), SourceMCSeqno: s.SourceMC, IsPartial: partial})
		}
	}
	return rows, nil
}
