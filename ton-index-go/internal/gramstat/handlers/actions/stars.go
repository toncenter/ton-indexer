package actions

import (
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

func starsRows(from time.Time, snapshot Snapshot) ([]gramstat.DailyStars, error) {
	byDay := make(map[string]DayCounts, len(snapshot.Days))
	for _, d := range snapshot.Days {
		byDay[d.Day.Format(time.DateOnly)] = d
	}
	var rows []gramstat.DailyStars
	for day := from; day.Before(snapshot.Through); day = day.AddDate(0, 0, 1) {
		d := byDay[day.Format(time.DateOnly)]
		row := gramstat.DailyStars{
			Day: day.Format(time.DateOnly), Purchases: d.StarsPurchases,
			Buckets:       append([]gramstat.StarsBucket{}, d.StarsBuckets...),
			CalculatedAt:  snapshot.CalculatedAt.UTC().Format("2006-01-02 15:04:05.000000"),
			SourceMCSeqno: snapshot.SourceMC,
		}
		through := day.AddDate(0, 0, 1)
		if through.After(snapshot.Through) {
			through = snapshot.Through
			row.IsPartial = 1
		}
		row.DataThrough = through.UTC().Format("2006-01-02 15:04:05")
		remaining := row.Purchases
		sum := new(big.Int)
		seen := make(map[string]bool)
		for i := range row.Buckets {
			b := &row.Buckets[i]
			if (b.PaymentAsset != "TON" && b.PaymentAsset != "USDT") ||
				(b.Product != "standard" && b.Product != "prepaid") || b.Purchases == 0 || b.Purchases > remaining {
				return nil, fmt.Errorf("invalid Stars bucket for %s", row.Day)
			}
			remaining -= b.Purchases
			quantity := "unknown"
			b.StarsTotal = nil
			switch b.QuantityStatus {
			case "known":
				if b.StarsPerPurchase == nil || *b.StarsPerPurchase == 0 {
					return nil, fmt.Errorf("invalid known Stars quantity for %s", row.Day)
				}
				value := new(big.Int).SetUint64(*b.StarsPerPurchase)
				quantity = value.String()
				value.Mul(value, new(big.Int).SetUint64(b.Purchases))
				total := value.String()
				b.StarsTotal = &total
				sum.Add(sum, value)
			case "missing_or_unrecognized_quantity", "zero_quantity", "out_of_range":
				if b.StarsPerPurchase != nil {
					return nil, fmt.Errorf("unknown Stars quantity has a value for %s", row.Day)
				}
				row.UnknownPurchases += b.Purchases
			default:
				return nil, fmt.Errorf("unexpected Stars quantity status %q", b.QuantityStatus)
			}
			key := b.PaymentAsset + "/" + b.Product + "/" + b.QuantityStatus + "/" + quantity
			if seen[key] {
				return nil, fmt.Errorf("duplicate Stars bucket for %s", row.Day)
			}
			seen[key] = true
		}
		row.KnownStarsTotal = sum.String()
		if remaining != 0 || len(row.KnownStarsTotal) > 38 {
			return nil, fmt.Errorf("inconsistent or overflowing Stars totals for %s", row.Day)
		}
		if row.UnknownPurchases == 0 {
			total := row.KnownStarsTotal
			row.StarsTotal = &total
		}
		sort.Slice(row.Buckets, func(i, j int) bool {
			a, b := row.Buckets[i], row.Buckets[j]
			if a.PaymentAsset != b.PaymentAsset {
				return a.PaymentAsset < b.PaymentAsset
			}
			if a.Product != b.Product {
				return a.Product < b.Product
			}
			if a.QuantityStatus != b.QuantityStatus {
				return a.QuantityStatus < b.QuantityStatus
			}
			return a.StarsPerPurchase != nil && b.StarsPerPurchase != nil && *a.StarsPerPurchase < *b.StarsPerPurchase
		})
		rows = append(rows, row)
	}
	return rows, nil
}
