package dexvolume

import (
	"fmt"
	"math/big"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

const Metric = "dex_volume_gram_v1"

// One ReplacingMergeTree row replaces the entire day including disappeared
// venues. Split GRAM and nanogram remainder for the UInt64 breakdown map;
// the exact independently reconciled total is Decimal(38,0).
func Summarize(d Day, b Book, now time.Time) (gramstat.DailyMetric, error) {
	m := gramstat.DailyMetric{Metric: Metric, Day: d.Date.Format(time.DateOnly), CalculatedAt: now.UTC().Format("2006-01-02 15:04:05.000000"), SourceMCSeqno: d.MC, DataThrough: d.Date.AddDate(0, 0, 1).Format(time.DateTime), Breakdown: map[string]uint64{"ambiguous": d.Ambiguous}}
	totals := map[string]*big.Int{}
	sum := new(big.Int)
	var unpriced uint64
	for _, r := range d.Routes {
		v := b.nanograms(r)
		if v == nil {
			m.Breakdown["unpriced:"+r.Dex]++
			unpriced++
			continue
		}
		m.Value++
		m.Breakdown["priced:"+r.Dex]++
		if totals[r.Dex] == nil {
			totals[r.Dex] = new(big.Int)
		}
		totals[r.Dex].Add(totals[r.Dex], v)
		sum.Add(sum, v)
	}
	for k, v := range totals {
		g, n := new(big.Int), new(big.Int)
		g.QuoRem(v, big.NewInt(1e9), n)
		if !g.IsUint64() {
			return m, fmt.Errorf("volume exceeds encoding for %s", k)
		}
		m.Breakdown["gram:"+k] = g.Uint64()
		m.Breakdown["nano:"+k] = n.Uint64()
	}
	if unpriced > 0 || d.Ambiguous > 0 {
		m.IsPartial = 1
	}
	if m.Value > 0 || m.IsPartial == 0 {
		v := sum.String()
		if len(v) > 38 {
			return m, fmt.Errorf("volume exceeds Decimal(38,0)")
		}
		m.AmountNanoton = &v
	}
	return m, nil
}
