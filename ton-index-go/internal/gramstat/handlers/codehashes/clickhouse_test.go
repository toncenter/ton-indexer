package codehashes

import (
	"bytes"
	"context"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"
)

type localStore struct {
	binary, dir string
}

func (l localStore) execute(ctx context.Context, sql string, p url.Values, data []byte) ([]byte, error) {
	args := []string{"local", "--path", l.dir, "--max_threads", "2", "--multiquery", "--query", sql}
	for k, values := range p {
		if strings.HasPrefix(k, "param_") {
			args = append(args, "--"+k, values[0])
		}
	}
	cmd := exec.CommandContext(ctx, l.binary, args...)
	cmd.Stdin = bytes.NewReader(data)
	return cmd.CombinedOutput()
}
func (l localStore) InsertJSON(ctx context.Context, table string, data []byte) error {
	_, err := l.execute(ctx, "INSERT INTO "+table+" FORMAT JSONEachRow", nil, data)
	return err
}
func (l localStore) Query(ctx context.Context, sql string, p url.Values) ([]byte, error) {
	return l.execute(ctx, sql, p, nil)
}

func TestRealClickHousePublicationAndStagedIsolation(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	store := localStore{binary, t.TempDir()}
	if b, err := store.execute(ctx, Schema+Schema, nil, nil); err != nil {
		t.Fatalf("%v: %s", err, b)
	}
	if err := publish(ctx, store, testSnapshot()); err != nil {
		t.Fatal(err)
	}
	// A later, incomplete generation must not replace the last complete one.
	if b, err := store.execute(ctx, `INSERT INTO gramstat.account_code_hash_counts VALUES
 ('mainnet-kvrocks-code-hashes-v1','00000000-0000-4000-8000-000000000001','2026-11-15','A',102,103,{'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=':999});
 SELECT hash_count,indexed_accounts,consistency FROM gramstat.account_code_hash_snapshots
 ORDER BY finished_at DESC LIMIT 1;`, nil, nil); err != nil || string(b) != "64\t640\tlive-index-scan\n" {
		t.Fatalf("%v: %s", err, b)
	}
}
