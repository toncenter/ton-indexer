package actions

import (
	"context"
	"fmt"
	"os"
	"strings"
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
CREATE TYPE staking_details AS (provider text);
CREATE TYPE jetton_swap_details AS (dex text);
CREATE TABLE actions(trace_mc_seqno_end integer, start_utime bigint, source text, destination text, type text, success boolean, parent_action_id text,
    amount numeric, asset text, staking_data staking_details, jetton_swap_data jetton_swap_details);
INSERT INTO blocks VALUES (-1,99,1792540799), (-1,100,1792544400), (-1,101,1792630800), (-1,102,1792717200);
INSERT INTO actions(trace_mc_seqno_end,start_utime,source,destination,type,success,parent_action_id) VALUES
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
	_, err = fixture.Exec(ctx, `
INSERT INTO actions(trace_mc_seqno_end,start_utime,source,destination,type,success,jetton_swap_data) VALUES
 (100,1792540800,'alice','receiver1','jetton_swap',true,ROW('stonfi_v2')),
 (100,1792540801,'alice','receiver2','jetton_swap',true,ROW('stonfi_v2')),
 (100,1792540802,'alice','receiver3','jetton_swap',true,ROW('dedust')),
 (102,1792540803,'bob','receiver4','jetton_swap',true,ROW('dedust')),
 (100,1792540804,NULL,'receiver5','jetton_swap',true,ROW(NULL)),
 (100,1792540805,'failed','receiver6','jetton_swap',false,ROW('stonfi_v2'));
INSERT INTO actions(trace_mc_seqno_end,start_utime,type,success,asset) VALUES
 (100,1792540800,'nft_mint',true,'0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF'),
 (100,1792540801,'nft_mint',true,'other_collection'),
 (100,1792540802,'nft_mint',true,NULL),
 (100,1792540803,'nft_mint',false,'0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF');
INSERT INTO actions(trace_mc_seqno_end,start_utime,type,success,destination,staking_data,amount) VALUES
 (100,1792540800,'stake_deposit',true,'0:A45B17F28409229B78360E3290420F13E4FE20F90D7E2BF8C4AC6703259E22FA',ROW('liquid_staking'),100000000000000000001),
 (100,1792540801,'stake_deposit',true,'0:A45B17F28409229B78360E3290420F13E4FE20F90D7E2BF8C4AC6703259E22FA',ROW('liquid_staking'),2),
 (100,1792540802,'stake_deposit',true,'0:F6FF877DD4CE1355B101572045F09D54C29309737EB52CA542CFA6C195F7CC5B',ROW('liquid_staking'),3),
 (100,1792540803,'stake_deposit',true,'0:3D1F01190FFB4E35F573EA5252D0F54A479BE2122106CA3F40B907B01905675C',ROW('liquid_staking'),4),
 (100,1792540804,'stake_deposit',true,'0:9FE32DD92875447A6B83D031D0230C539752152B660D4A056797BE9AD2E396D5',ROW('liquid_staking'),5),
 (100,1792540805,'stake_deposit',true,'other_pool',ROW('liquid_staking'),100),
 (100,1792540806,'stake_deposit',true,'0:A45B17F28409229B78360E3290420F13E4FE20F90D7E2BF8C4AC6703259E22FA',ROW('nominator'),100),
 (100,1792540807,'stake_deposit',true,'0:A45B17F28409229B78360E3290420F13E4FE20F90D7E2BF8C4AC6703259E22FA',ROW('ethena'),100),
 (100,1792540808,'stake_deposit',false,'0:A45B17F28409229B78360E3290420F13E4FE20F90D7E2BF8C4AC6703259E22FA',ROW('liquid_staking'),100),
 (101,1792627200,'stake_deposit',true,'0:A45B17F28409229B78360E3290420F13E4FE20F90D7E2BF8C4AC6703259E22FA',ROW('liquid_staking'),NULL),
 (101,1792627201,'stake_deposit',true,'0:F6FF877DD4CE1355B101572045F09D54C29309737EB52CA542CFA6C195F7CC5B',ROW('liquid_staking'),7);
`)
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
	if s.Days[0].Swaps != 5 || s.Days[0].SwapUsers != 2 || s.Days[0].SwapsByDEX["stonfi_v2"] != 2 || s.Days[0].SwapsByDEX["dedust"] != 2 || s.Days[0].SwapsByDEX["unknown"] != 1 || s.Days[0].TonDNSMints != 1 {
		t.Fatalf("incorrect swap distinct/breakdown or DNS collection filter: %+v", s.Days[0])
	}
	if s.Days[0].LiquidStakingDeposits != 5 || s.Days[0].LiquidStakingAmount == nil || *s.Days[0].LiquidStakingAmount != "100000000000000000015" ||
		s.Days[0].LiquidStakingByPool["tonstakers"].Count != 2 || s.Days[0].LiquidStakingByPool["sgram"].Count != 1 ||
		s.Days[1].LiquidStakingDeposits != 2 || s.Days[1].LiquidStakingAmount != nil || s.Days[1].LiquidStakingByPool["tonstakers"].AmountNanoton != nil {
		t.Fatalf("incorrect whitelist, exact/unknown staking amount: %+v", s.Days)
	}
	if _, err := dailyRows(from, from.Add(48*time.Hour), s); err != nil {
		t.Fatal(err)
	}
	// EXPLAIN the actual embedded SQL: adding dimensions must not introduce a
	// second scan of actions. The tiny fixture naturally uses a sequential scan.
	planRows, err := fixture.Query(ctx, "EXPLAIN "+dailySQL, int64(99), int64(102), from.Unix(), from.Add(48*time.Hour).Unix())
	if err != nil {
		t.Fatal(err)
	}
	scans := 0
	for planRows.Next() {
		var line string
		if err := planRows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(line, "Seq Scan on actions") {
			scans++
		}
	}
	planRows.Close()
	if err := planRows.Err(); err != nil {
		t.Fatal(err)
	}
	if scans != 1 {
		t.Fatalf("expected one source scan, got %d", scans)
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
