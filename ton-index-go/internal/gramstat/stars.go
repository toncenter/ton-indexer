package gramstat

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
)

//go:embed stars.sql
var StarsSchema string

// StarsBucket counts payment actions, not blockchain transactions or wallets.
// A nullable quantity never means a zero-sized purchase.
type StarsBucket struct {
	PaymentAsset     string  `json:"payment_asset"`
	Product          string  `json:"product"`
	StarsPerPurchase *uint64 `json:"stars_per_purchase,string"`
	QuantityStatus   string  `json:"quantity_status"`
	Purchases        uint64  `json:"purchases"`
	StarsTotal       *string `json:"stars_total"`
}

// DailyStars replaces the entire distribution, including disappearing sizes.
// KnownStarsTotal is a subtotal; StarsTotal is NULL if any size is unknown.
type DailyStars struct {
	Day              string        `json:"day"`
	Purchases        uint64        `json:"purchases"`
	UnknownPurchases uint64        `json:"unknown_purchases"`
	KnownStarsTotal  string        `json:"known_stars_total"`
	StarsTotal       *string       `json:"stars_total"`
	Buckets          []StarsBucket `json:"buckets"`
	CalculatedAt     string        `json:"calculated_at"`
	DataThrough      string        `json:"data_through"`
	SourceMCSeqno    uint32        `json:"source_mc_seqno"`
	IsPartial        uint8         `json:"is_partial"`
}

func (c *ClickHouse) WriteStars(ctx context.Context, rows []DailyStars) error {
	if len(rows) == 0 {
		return nil
	}
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	return c.InsertJSON(ctx, "gramstat.telegram_stars_daily", body.Bytes())
}
