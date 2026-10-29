// Package actions materializes daily activity from the PostgreSQL classifier.
package actions

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

var Metrics = []string{
	"jetton_senders", "jetton_receivers", "jetton_transfers",
	"nft_senders", "nft_receivers", "nft_transfers",
	"swaps_total", "swaps_users", "liquid_staking_deposits", "ton_dns_mints",
	"liquid_staking_deposits_tonstakers", "liquid_staking_deposits_stakee",
	"liquid_staking_deposits_kton", "liquid_staking_deposits_sgram",
}

var liquidPools = []string{"tonstakers", "stakee", "kton", "sgram"}

type PoolDeposits struct {
	Count         uint64  `json:"count"`
	AmountNanoton *string `json:"amount_nanoton"`
}

type DayCounts struct {
	Day                                                  time.Time
	JettonSenders, JettonReceivers, JettonTransfers      uint64
	NFTSenders, NFTReceivers, NFTTransfers               uint64
	Swaps, SwapUsers, LiquidStakingDeposits, TonDNSMints uint64
	LiquidStakingAmount                                  *string
	SwapsByDEX                                           map[string]uint64
	LiquidStakingByPool                                  map[string]PoolDeposits
}

type Snapshot struct {
	Days         []DayCounts
	Through      time.Time
	CalculatedAt time.Time
	SourceMC     uint32
}

type Source interface {
	Collect(context.Context, time.Time, time.Time) (Snapshot, error)
}

type Sink interface {
	StoredDays(context.Context, []string, time.Time) (map[string][]time.Time, error)
	WriteDaily(context.Context, []gramstat.DailyMetric) error
}

type Handler struct {
	Source Source
	Sink   Sink
	Log    *slog.Logger
}

func (*Handler) Name() string { return "actions" }

func (h *Handler) Run(ctx context.Context, req gramstat.Request) error {
	from, to, err := req.DailyWindow()
	if err != nil {
		return err
	}
	if req.From.IsZero() {
		stored, err := h.Sink.StoredDays(ctx, Metrics, gramstat.Day(req.AsOf))
		if err != nil {
			return fmt.Errorf("read refresh coverage: %w", err)
		}
		from = gramstat.CatchUpStart(from, gramstat.Day(req.AsOf), Metrics, stored)
	}
	for start := from; start.Before(to); {
		end := start.AddDate(0, 0, 7)
		if end.After(to) {
			end = to
		}
		started := time.Now()
		snapshot, err := h.Source.Collect(ctx, start, end)
		if err != nil {
			return fmt.Errorf("collect %s..%s: %w", start.Format(time.RFC3339), end.Format(time.RFC3339), err)
		}
		rows, err := dailyRows(start, end, snapshot)
		if err != nil {
			return err
		}
		// Publish only after the entire chunk has been read and checked. Retries
		// replace absolute values, including a corrected decrease or an actual zero.
		if err := h.Sink.WriteDaily(ctx, rows); err != nil {
			return fmt.Errorf("publish daily metrics: %w", err)
		}
		h.Log.Info("actions published", "from", start.Format(time.DateOnly), "to", end.Format(time.RFC3339),
			"rows", len(rows), "source_mc", snapshot.SourceMC, "data_through", snapshot.Through, "elapsed", time.Since(started))
		if snapshot.Through.Before(end) {
			h.Log.Warn("source has not reached requested end; later days left absent", "requested_end", end, "data_through", snapshot.Through)
			break
		}
		start = end
	}
	return nil
}

func dailyRows(from, to time.Time, snapshot Snapshot) ([]gramstat.DailyMetric, error) {
	if !snapshot.Through.After(from) || snapshot.Through.After(to) || snapshot.CalculatedAt.IsZero() || snapshot.SourceMC == 0 {
		return nil, fmt.Errorf("invalid or unavailable source snapshot")
	}
	byDay := make(map[string]DayCounts, len(snapshot.Days))
	for _, d := range snapshot.Days {
		key := d.Day.Format(time.DateOnly)
		if _, exists := byDay[key]; exists || d.Day.Before(from) || !d.Day.Before(snapshot.Through) || !d.Day.Equal(gramstat.Day(d.Day)) ||
			d.JettonSenders > d.JettonTransfers || d.JettonReceivers > d.JettonTransfers ||
			d.NFTSenders > d.NFTTransfers || d.NFTReceivers > d.NFTTransfers {
			return nil, fmt.Errorf("invalid daily counts for %s", key)
		}
		if d.SwapUsers > d.Swaps {
			return nil, fmt.Errorf("more swap users than swaps for %s", key)
		}
		remaining := d.Swaps
		for dex, count := range d.SwapsByDEX {
			if dex == "" || count > remaining {
				return nil, fmt.Errorf("invalid DEX breakdown for %s", key)
			}
			remaining -= count
		}
		if remaining != 0 {
			return nil, fmt.Errorf("incomplete DEX breakdown for %s", key)
		}
		var err error
		if d.LiquidStakingAmount, err = canonicalAmount(d.LiquidStakingAmount, d.LiquidStakingDeposits); err != nil {
			return nil, fmt.Errorf("staking total for %s: %w", key, err)
		}
		pools := make(map[string]PoolDeposits, len(liquidPools))
		remaining = d.LiquidStakingDeposits
		sum := new(big.Int)
		unknown := false
		for _, pool := range liquidPools {
			p := d.LiquidStakingByPool[pool]
			if p.Count > remaining {
				return nil, fmt.Errorf("inconsistent staking counts for %s", key)
			}
			remaining -= p.Count
			if p.AmountNanoton, err = canonicalAmount(p.AmountNanoton, p.Count); err != nil {
				return nil, fmt.Errorf("staking pool %s for %s: %w", pool, key, err)
			}
			if p.AmountNanoton == nil {
				unknown = true
			} else {
				value, _ := new(big.Int).SetString(*p.AmountNanoton, 10)
				sum.Add(sum, value)
			}
			pools[pool] = p
		}
		for pool := range d.LiquidStakingByPool {
			if _, known := pools[pool]; !known {
				return nil, fmt.Errorf("unexpected liquid staking pool %q", pool)
			}
		}
		if remaining != 0 || unknown != (d.LiquidStakingAmount == nil) ||
			(!unknown && sum.String() != *d.LiquidStakingAmount) {
			return nil, fmt.Errorf("inconsistent staking total/breakdown for %s", key)
		}
		d.LiquidStakingByPool = pools
		byDay[key] = d
	}
	var rows []gramstat.DailyMetric
	for day := from; day.Before(snapshot.Through); day = day.AddDate(0, 0, 1) {
		d := byDay[day.Format(time.DateOnly)]
		values := []uint64{d.JettonSenders, d.JettonReceivers, d.JettonTransfers, d.NFTSenders, d.NFTReceivers, d.NFTTransfers,
			d.Swaps, d.SwapUsers, d.LiquidStakingDeposits, d.TonDNSMints}
		for _, pool := range liquidPools {
			values = append(values, d.LiquidStakingByPool[pool].Count)
		}
		through := day.AddDate(0, 0, 1)
		partial := uint8(0)
		if through.After(snapshot.Through) {
			through = snapshot.Through
			partial = 1
		}
		for i, metric := range Metrics {
			row := gramstat.DailyMetric{
				Metric: metric, Day: day.Format(time.DateOnly), Value: values[i],
				CalculatedAt:  snapshot.CalculatedAt.UTC().Format("2006-01-02 15:04:05.000000"),
				DataThrough:   through.UTC().Format("2006-01-02 15:04:05"),
				SourceMCSeqno: snapshot.SourceMC, IsPartial: partial,
			}
			switch metric {
			case "swaps_total":
				row.Breakdown = d.SwapsByDEX
			case "liquid_staking_deposits":
				row.AmountNanoton = d.LiquidStakingAmount
				if d.LiquidStakingDeposits == 0 {
					zero := "0"
					row.AmountNanoton = &zero
				}
			default:
				if pool, ok := strings.CutPrefix(metric, "liquid_staking_deposits_"); ok {
					p := d.LiquidStakingByPool[pool]
					row.AmountNanoton = p.AmountNanoton
					if p.Count == 0 {
						zero := "0"
						row.AmountNanoton = &zero
					}
				}
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// Keep nullable, exact Decimal(38,0) values all the way through JSONEachRow.
// A missing amount on an actual deposit is unknown; no deposits means zero.
func canonicalAmount(value *string, count uint64) (*string, error) {
	if value == nil {
		if count != 0 {
			return nil, nil
		}
		zero := "0"
		return &zero, nil
	}
	amount, ok := new(big.Rat).SetString(*value)
	if !ok || !amount.IsInt() || amount.Sign() < 0 || len(amount.Num().String()) > 38 || (count == 0 && amount.Sign() != 0) {
		return nil, fmt.Errorf("invalid exact nonnegative nanoTON amount")
	}
	canonical := amount.Num().String()
	return &canonical, nil
}
