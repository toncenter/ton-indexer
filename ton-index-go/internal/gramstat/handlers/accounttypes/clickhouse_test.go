package accounttypes

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/handlers/codehashes"
)

type localStore struct {
	binary, dir string
	dropPrefix  bool
}

func (l localStore) Query(ctx context.Context, sql string, params url.Values) ([]byte, error) {
	args := []string{"local", "--path", l.dir, "--max_threads", "2", "--multiquery", "--query", sql}
	for key, values := range params {
		args = append(args, "--"+key, values[0])
	}
	b, err := exec.CommandContext(ctx, l.binary, args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, b)
	}
	return b, nil
}

func (l localStore) InsertJSON(ctx context.Context, table string, body []byte) error {
	if l.dropPrefix && table == "gramstat.account_type_features" && bytes.Contains(body, []byte(`"prefix":"A"`)) {
		return nil // Simulate an acknowledged incomplete stage.
	}
	cmd := exec.CommandContext(ctx, l.binary, "local", "--path", l.dir, "--max_threads", "2",
		"--query", "INSERT INTO "+table+" FORMAT JSONEachRow")
	cmd.Stdin = bytes.NewReader(body)
	b, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, b)
	}
	return nil
}

func TestRealPublicationAndIncompleteIsolation(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	store := localStore{binary: binary, dir: t.TempDir()}
	if _, err := store.Query(ctx, codehashes.Schema+Schema+`
CREATE DATABASE mainnet_gramstat_full;
CREATE TABLE mainnet_gramstat_full.contract_methods(code_hash FixedString(44),methods Array(UInt64))
ENGINE=ReplacingMergeTree ORDER BY code_hash;`, nil); err != nil {
		t.Fatal(err)
	}
	var rows bytes.Buffer
	const snapshot = "00000000-0000-4000-8000-000000000001"
	for _, prefix := range codehashes.Alphabet {
		b := make([]byte, 32)
		b[0] = byte(strings.IndexRune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/", prefix)) << 2
		hash := base64.StdEncoding.EncodeToString(b)
		_ = json.NewEncoder(&rows).Encode(map[string]any{
			"source_id": codehashes.SourceID, "snapshot_id": snapshot, "observed_day": "2026-11-15",
			"prefix": string(prefix), "mc_before": 100, "mc_after": 101, "counts": map[string]uint64{hash: 10},
		})
	}
	if err := store.InsertJSON(ctx, "gramstat.account_code_hash_counts", rows.Bytes()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Query(ctx, `INSERT INTO gramstat.account_code_hash_snapshots VALUES
('mainnet-kvrocks-code-hashes-v1','`+snapshot+`','2026-11-15','2026-11-15 12:00:00','2026-11-15 12:00:10','2026-11-15 12:00:11',64,640,100,101,64,0,'live-index-scan');
INSERT INTO mainnet_gramstat_full.contract_methods VALUES
('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',[97026]);`, nil); err != nil {
		t.Fatal(err)
	}
	broken := store
	broken.dropPrefix = true
	if err := (&Handler{CH: broken}).Run(ctx, gramstat.Request{}); err == nil {
		t.Fatal("incomplete features were published")
	}
	if b, err := store.Query(ctx, "SELECT count() FROM gramstat.account_type_snapshots", nil); err != nil || string(b) != "0\n" {
		t.Fatal(string(b), err)
	}
	h := &Handler{CH: store}
	if err := h.Run(ctx, gramstat.Request{}); err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx, gramstat.Request{}); err != nil {
		t.Fatal(err)
	}
	b, err := store.Query(ctx, `SELECT count(),sum(indexed_accounts) FROM gramstat.account_type_snapshots`, nil)
	if err != nil || string(b) != "1\t640\n" {
		t.Fatal(string(b), err)
	}
}
