package actions

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

func TestStarsPaymentFilter(t *testing.T) {
	day := time.Date(2025, 8, 23, 0, 0, 0, 0, time.UTC)
	group := func(hour int64, asset string, qty, count uint64, paid string) starsPaymentGroup {
		return starsPaymentGroup{Hour: day.Unix() + hour*3600, Paid: &paid,
			StarsBucket: gramstat.StarsBucket{PaymentAsset: asset, Product: "standard",
				QuantityStatus: "known", StarsPerPurchase: &qty, Purchases: count}}
	}
	for _, test := range []struct {
		name           string
		groups         []starsPaymentGroup
		kept, rejected uint64
		total          string
	}{
		{"money not count or declared size", []starsPaymentGroup{
			group(0, "TON", 100, 1, "1000000000"),
			group(0, "TON", 1000000000000000000, 1000000, "1"),
		}, 1, 1000000, "100"},
		{"hourly prices and inclusive floor", []starsPaymentGroup{
			group(0, "TON", 100, 10, "1000"), group(0, "TON", 50, 1, "250"), group(0, "TON", 50, 1, "249"),
			group(1, "TON", 100, 10, "100"), group(1, "TON", 50, 1, "25"), group(1, "TON", 50, 1, "24"),
		}, 22, 2, "2100"},
		{"USDT fixed 80 percent even with hostile majority", []starsPaymentGroup{
			group(0, "USDT", 50, 1, "600000"), group(0, "USDT", 50, 1000, "599999"),
			group(1, "USDT", 100, 1, "1200000"), group(1, "USDT", 100, 1, "1199999"),
		}, 2, 1001, "150"},
		{"rational price boundary", []starsPaymentGroup{
			group(0, "TON", 3, 10, "10"), group(0, "TON", 9, 1, "15"), group(0, "TON", 9, 1, "14"),
		}, 11, 1, "39"},
		{"above float precision", []starsPaymentGroup{
			group(0, "TON", 9007199254740993, 10, "18014398509481986"),
			group(0, "TON", 9007199254740993, 1, "9007199254740993"),
			group(0, "TON", 9007199254740993, 1, "9007199254740992"),
		}, 11, 1, "99079191802150923"},
		{"zero is not a reference", []starsPaymentGroup{group(0, "TON", 100, 1, "0")}, 0, 1, "0"},
		{"no cross-hour fallback", []starsPaymentGroup{
			group(0, "TON", 100, 1, "1000"), group(1, "TON", 100, 1, "1"),
		}, 2, 0, "200"},
		{"lower median at exactly half money", []starsPaymentGroup{
			group(0, "TON", 100, 1, "100"), group(0, "TON", 1, 1, "100"),
		}, 2, 0, "101"},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := DayCounts{Day: day}
			var raw uint64
			for _, g := range test.groups {
				raw += g.Purchases
			}
			if err := filterStarsPayments(&d, raw, test.groups); err != nil {
				t.Fatal(err)
			}
			if d.StarsPurchases != test.kept || d.StarsRejected != test.rejected {
				t.Fatalf("counts: %+v", d)
			}
			rows, err := starsRows(day, Snapshot{Days: []DayCounts{d}, Through: day.Add(24 * time.Hour)})
			if err != nil {
				t.Fatal(err)
			}
			if rows[0].KnownStarsTotal != test.total {
				t.Fatalf("total %s want %s", rows[0].KnownStarsTotal, test.total)
			}
			// Source/JSON ordering must not change the median or output buckets.
			for i, j := 0, len(test.groups)-1; i < j; i, j = i+1, j-1 {
				test.groups[i], test.groups[j] = test.groups[j], test.groups[i]
			}
			reversed := DayCounts{Day: day}
			if err := filterStarsPayments(&reversed, raw, test.groups); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(d, reversed) {
				t.Fatal("order-dependent filter")
			}
		})
	}
	t.Run("unknown amounts and sizes", func(t *testing.T) {
		known := group(0, "TON", 50, 1, "100")
		unknown := group(0, "TON", 0, 1, "10000000000000000000000000000")
		unknown.StarsPerPurchase, unknown.QuantityStatus = nil, "missing_or_unrecognized_quantity"
		missing := group(0, "TON", math.MaxUint64, 1, "")
		missing.Paid = nil
		d := DayCounts{Day: day}
		if err := filterStarsPayments(&d, 3, []starsPaymentGroup{known, unknown, missing}); err != nil {
			t.Fatal(err)
		}
		rows, err := starsRows(day, Snapshot{Days: []DayCounts{d}, Through: day.Add(30 * time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
		r := rows[0]
		if r.Purchases != 3 || r.UnknownPurchases != 2 || r.KnownStarsTotal != "50" || r.StarsTotal != nil || r.IsPartial != 1 {
			t.Fatalf("unknown/partial lost: %+v", r)
		}
	})
	for _, amount := range []string{"-1", "0.5", "garbage"} {
		d := DayCounts{Day: day}
		if err := filterStarsPayments(&d, 1, []starsPaymentGroup{group(0, "TON", 50, 1, amount)}); err == nil {
			t.Fatalf("invalid amount accepted: %s", amount)
		}
	}
}
