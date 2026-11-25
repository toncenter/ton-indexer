package codehashes

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

//go:embed schema.sql
var Schema string

const verifySQL = `SELECT toUInt32(count()) AS prefixes,toUInt32(uniqExact(prefix)) AS distinct_prefixes,
 toString(sum(length(counts))) AS hashes,
 toString(sum(arraySum(mapValues(counts)))) AS accounts,
 toUInt32(countIf(NOT arrayAll(k -> startsWith(k,prefix),mapKeys(counts)))) AS invalid_prefixes
 FROM gramstat.account_code_hash_counts
 WHERE source_id={source:String} AND snapshot_id={snapshot:UUID} AND observed_day={day:Date}
 FORMAT JSONEachRow`

type storeTransport interface {
	InsertJSON(context.Context, string, []byte) error
	Query(context.Context, string, url.Values) ([]byte, error)
}

type Store struct{ CH *gramstat.ClickHouse }

func (s Store) Publish(ctx context.Context, snapshot *Snapshot) error {
	return publish(ctx, s.CH, snapshot)
}

func publish(ctx context.Context, ch storeTransport, s *Snapshot) error {
	t, err := s.summary()
	if err != nil {
		return err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	day := s.StartedAt.UTC().Format(time.DateOnly)
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, p := range s.Prefixes {
		row := struct {
			SourceID   string `json:"source_id"`
			SnapshotID string `json:"snapshot_id"`
			Day        string `json:"observed_day"`
			Prefix
		}{SourceID, id.String(), day, p}
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	if err := ch.InsertJSON(ctx, "gramstat.account_code_hash_counts", body.Bytes()); err != nil {
		return fmt.Errorf("stage code-hash counts: %w", err)
	}
	params := url.Values{
		"param_source": {SourceID}, "param_snapshot": {id.String()}, "param_day": {day},
		"max_threads": {"2"}, "max_execution_time": {"30"},
	}
	data, err := ch.Query(ctx, verifySQL, params)
	if err != nil {
		return fmt.Errorf("verify code-hash snapshot: %w", err)
	}
	var check struct {
		Prefixes         uint64 `json:"prefixes"`
		DistinctPrefixes uint64 `json:"distinct_prefixes"`
		Hashes           string `json:"hashes"`
		Accounts         string `json:"accounts"`
		InvalidPrefixes  uint64 `json:"invalid_prefixes"`
	}
	if err := json.Unmarshal(data, &check); err != nil {
		return fmt.Errorf("decode code-hash verification: %w", err)
	}
	if check.Prefixes != 64 || check.DistinctPrefixes != 64 || check.InvalidPrefixes != 0 ||
		check.Hashes != fmt.Sprint(t.Hashes) || check.Accounts != fmt.Sprint(t.Accounts) {
		return fmt.Errorf("incomplete or inconsistent stored code-hash snapshot")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	const stamp = "2006-01-02 15:04:05.000000"
	marker := struct {
		SourceID        string `json:"source_id"`
		SnapshotID      string `json:"snapshot_id"`
		Day             string `json:"observed_day"`
		StartedAt       string `json:"started_at"`
		FinishedAt      string `json:"finished_at"`
		PublishedAt     string `json:"published_at"`
		HashCount       uint64 `json:"hash_count"`
		IndexedAccounts uint64 `json:"indexed_accounts"`
		MCMin           uint32 `json:"source_mc_min"`
		MCMax           uint32 `json:"source_mc_max"`
		Pages           uint64 `json:"pages"`
		ZeroHashes      uint64 `json:"zero_hashes"`
		Consistency     string `json:"consistency"`
	}{
		SourceID, id.String(), day, s.StartedAt.UTC().Format(stamp), s.FinishedAt.UTC().Format(stamp),
		time.Now().UTC().Format(stamp), t.Hashes, t.Accounts, t.MCMin, t.MCMax,
		s.Pages, s.ZeroHashes, "live-index-scan",
	}
	data, err = json.Marshal(marker)
	if err != nil {
		return err
	}
	// An interrupted insert/verification leaves only an invisible generation.
	// Never reuse its UUID or add its counts into the previous published run.
	return ch.InsertJSON(ctx, "gramstat.account_code_hash_snapshots", data)
}
