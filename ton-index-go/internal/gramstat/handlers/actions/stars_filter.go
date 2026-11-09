package actions

import (
	"fmt"
	"math/big"
	"sort"
	"strconv"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

// One group contains identical payments, not a sum of differently priced ones.
// The legacy TON asset code denotes GRAM; keep the existing storage/API contract.
type starsPaymentGroup struct {
	gramstat.StarsBucket
	Hour int64   `json:"hour"`
	Paid *string `json:"paid"` // nanograms or USDT micro-units per payment
}

type starsPricePoint struct {
	rate   *big.Rat
	weight *big.Int
}

// filterStarsPayments uses a lower money-weighted median for each UTC hour of
// GRAM payments. Exactly-half ties choose the lower price deterministically.
// Standard and prepaid share a reference; USDT has its own fixed price floor.
// No dollar rate, floating point, rounded quotient or cross-hour fallback is used.
func filterStarsPayments(day *DayCounts, rawCount uint64, groups []starsPaymentGroup) error {
	remaining := rawCount
	amounts := make([]*big.Int, len(groups))
	byHour := make(map[int64][]starsPricePoint)
	for i, g := range groups {
		if g.Purchases == 0 || g.Purchases > remaining ||
			(g.PaymentAsset != "TON" && g.PaymentAsset != "USDT") ||
			(g.Product != "standard" && g.Product != "prepaid") ||
			g.Hour%3600 != 0 || g.Hour < day.Day.Unix() || g.Hour >= day.Day.Unix()+86400 {
			return fmt.Errorf("invalid hourly Stars payment group")
		}
		remaining -= g.Purchases
		switch g.QuantityStatus {
		case "known":
			if g.StarsPerPurchase == nil || *g.StarsPerPurchase == 0 {
				return fmt.Errorf("invalid known Stars quantity")
			}
		case "missing_or_unrecognized_quantity", "zero_quantity", "out_of_range":
			if g.StarsPerPurchase != nil {
				return fmt.Errorf("unknown Stars quantity has a value")
			}
		default:
			return fmt.Errorf("unexpected raw Stars quantity status %q", g.QuantityStatus)
		}
		if g.Paid == nil {
			continue
		}
		paid, ok := new(big.Rat).SetString(*g.Paid)
		if !ok || !paid.IsInt() || paid.Sign() < 0 {
			return fmt.Errorf("invalid integral Stars payment amount")
		}
		amounts[i] = new(big.Int).Set(paid.Num())
		if paid.Sign() > 0 && g.PaymentAsset == "TON" && g.QuantityStatus == "known" {
			qty := new(big.Int).SetUint64(*g.StarsPerPurchase)
			byHour[g.Hour] = append(byHour[g.Hour], starsPricePoint{
				rate:   new(big.Rat).SetFrac(amounts[i], qty),
				weight: new(big.Int).Mul(amounts[i], new(big.Int).SetUint64(g.Purchases)),
			})
		}
	}
	if remaining != 0 {
		return fmt.Errorf("incomplete Stars payment groups")
	}
	prices := make(map[int64]*big.Rat, len(byHour))
	for hour, points := range byHour {
		sort.Slice(points, func(i, j int) bool { return points[i].rate.Cmp(points[j].rate) < 0 })
		total, cumulative := new(big.Int), new(big.Int)
		for _, p := range points {
			total.Add(total, p.weight)
		}
		for _, p := range points {
			cumulative.Add(cumulative, p.weight)
			if new(big.Int).Lsh(cumulative, 1).Cmp(total) >= 0 {
				prices[hour] = p.rate
				break
			}
		}
	}
	day.StarsPurchases, day.StarsRejected, day.StarsBuckets = 0, 0, nil
	buckets := make(map[string]gramstat.StarsBucket)
	for i, g := range groups {
		paid := amounts[i]
		b := g.StarsBucket
		rejected := paid != nil && paid.Sign() == 0
		if paid == nil {
			// A parseable claim without a payment amount is not a known subtotal.
			b.StarsPerPurchase, b.QuantityStatus = nil, "missing_payment_amount"
		} else if !rejected && g.QuantityStatus == "known" {
			qty := new(big.Int).SetUint64(*g.StarsPerPurchase)
			if g.PaymentAsset == "USDT" {
				// $0.015 * 80% * 10^6 = 12,000 micro-USDT per Star.
				rejected = paid.Cmp(new(big.Int).Mul(qty, big.NewInt(12000))) < 0
			} else {
				ref := prices[g.Hour]
				if ref == nil {
					return fmt.Errorf("missing hourly price for positive known GRAM payment")
				}
				// paid >= quantity * reference / 2; exact at the boundary.
				lhs := new(big.Int).Mul(paid, ref.Denom())
				lhs.Lsh(lhs, 1)
				rejected = lhs.Cmp(new(big.Int).Mul(qty, ref.Num())) < 0
			}
		}
		if rejected {
			day.StarsRejected += g.Purchases
			continue
		}
		day.StarsPurchases += b.Purchases
		key := b.PaymentAsset + "/" + b.Product + "/" + b.QuantityStatus
		if b.StarsPerPurchase != nil {
			key += "/" + strconv.FormatUint(*b.StarsPerPurchase, 10)
		}
		b.Purchases += buckets[key].Purchases
		buckets[key] = b
	}
	keys := make([]string, 0, len(buckets))
	for key := range buckets {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		day.StarsBuckets = append(day.StarsBuckets, buckets[key])
	}
	return nil
}
