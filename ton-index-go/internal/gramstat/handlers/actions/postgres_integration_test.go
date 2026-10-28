package actions

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The supplied local test role must have CREATEDB. All fixture data lives in a
// fresh database, removed afterwards; never use the production source DSN here.
func TestPostgresDailyAggregation(t *testing.T) {
	dsn := os.Getenv("GRAMSTAT_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set GRAMSTAT_TEST_PG_DSN to a local PostgreSQL with CREATEDB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	name := fmt.Sprintf("gramstat_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	}()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Database = name
	fixture, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close(context.Background())
	_, err = fixture.Exec(ctx, `CREATE TABLE blocks(workchain integer, seqno integer, gen_utime bigint);
CREATE TABLE actions(trace_mc_seqno_end integer, start_utime bigint, source text, destination text, type text, success boolean, parent_action_id text);
INSERT INTO blocks VALUES (-1,99,1792540799), (-1,100,1792544400), (-1,101,1792630800), (-1,102,1792717200);
INSERT INTO actions VALUES
  (100,1792540800,'alice','carol','jetton_transfer',true,NULL),
  (100,1792540801,'alice','carol','jetton_transfer',true,'swap'),
  (102,1792540802,'bob','dave','jetton_transfer',true,'late_trace'),
  (100,1792540803,NULL,NULL,'jetton_transfer',true,NULL),
  (100,1792540804,'failed','failed_receiver','jetton_transfer',false,NULL),
  (101,1792627200,'alice','carol','jetton_transfer',true,NULL),
  (101,1792627201,'alice','collector','nft_transfer',true,NULL),
  (101,1792627202,NULL,'another_collector','nft_transfer',true,NULL),
  (101,1792627203,'alice',NULL,'nft_transfer',true,'sale'),
  (101,1792627204,'failed','failed_receiver','nft_transfer',false,NULL),
  (99,1792540799,'old','old_receiver','jetton_transfer',true,NULL),
  (102,1792713600,'future','future_receiver','jetton_transfer',true,NULL);`)
	if err != nil {
		t.Fatal(err)
	}
	// ConnString preserves a modified Database only for URL configs when rebuilt;
	// use the fixture's explicit connection parameters for this local-only test.
	testDSN := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable", cfg.Host, cfg.Port, cfg.User, name)
	if cfg.Password != "" {
		testDSN += " password='" + escapePassword(cfg.Password) + "'"
	}
	source := Postgres{DSN: testDSN}
	from := time.Unix(1792540800, 0).UTC()
	s, err := source.Collect(ctx, from, from.Add(48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Days) != 2 || s.SourceMC != 102 ||
		s.Days[0].JettonSenders != 2 || s.Days[0].JettonReceivers != 2 || s.Days[0].JettonTransfers != 4 ||
		s.Days[0].NFTSenders != 0 || s.Days[0].NFTReceivers != 0 || s.Days[0].NFTTransfers != 0 ||
		s.Days[1].JettonSenders != 1 || s.Days[1].JettonReceivers != 1 || s.Days[1].JettonTransfers != 1 ||
		s.Days[1].NFTSenders != 1 || s.Days[1].NFTReceivers != 2 || s.Days[1].NFTTransfers != 3 {
		t.Fatalf("incorrect distinct/null/nested/late-trace aggregation: %+v", s)
	}
	lagged, err := source.Collect(ctx, from, from.Add(72*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if !lagged.Through.Equal(time.Unix(1792717200, 0)) {
		t.Fatalf("source lag not retained: %v", lagged.Through)
	}
}

func escapePassword(s string) string {
	var result string
	for _, r := range s {
		if r == '\\' || r == '\'' {
			result += "\\"
		}
		result += string(r)
	}
	return result
}
