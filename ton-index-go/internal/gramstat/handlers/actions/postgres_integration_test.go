package actions

import (
	"context"
	"fmt"
	"os"
	"reflect"
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
CREATE TYPE ton_transfer_details AS (content text);
CREATE TYPE jetton_transfer_details AS (comment text);
CREATE TABLE actions(trace_mc_seqno_end integer, start_utime bigint, source text, destination text, type text, success boolean, parent_action_id text,
    amount numeric, value numeric, asset text, staking_data staking_details, jetton_swap_data jetton_swap_details,
    ton_transfer_data ton_transfer_details, jetton_transfer_data jetton_transfer_details);
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
	monthly, err := source.CollectMonthly(ctx, from, from.Add(48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(monthly.Days) != 1 || monthly.Days[0].JettonSenders != 2 || monthly.Days[0].JettonReceivers != 2 || monthly.Days[0].NFTSenders != 1 || monthly.Days[0].NFTReceivers != 2 || monthly.Days[0].SwapUsers != 2 {
		t.Fatalf("monthly uniques counted daily duplicates: %+v", monthly)
	}
	// Add payments after checking the pre-existing metrics. Repeated actions by
	// one sender (including nested/batched payments) must count individually.
	comments := []string{"50 Telegram Stars \n\nRef#a", "50.0 Telegram Stars", "00050.000 Telegram Stars",
		"500 Prepaid Telegram Stars", "Telegram Stars Ref#missing", "0 Telegram Stars", "50.5 Telegram Stars",
		"18446744073709551616 Telegram Stars", strings.Repeat("9", 100) + " Telegram Stars",
		"18446744073709551615 Telegram Stars"}
	const fragment = "0:852443F8599FE6A5DA34FE43049AC4E0BEB3071BB2BFB56635EA9421287C283A"
	const usdt = "0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE"
	for _, comment := range comments {
		_, err = fixture.Exec(ctx, `INSERT INTO actions(trace_mc_seqno_end,start_utime,source,destination,type,success,parent_action_id,ton_transfer_data,value)
VALUES(102,$1,'alice',$2,'ton_transfer',true,'batch',ROW($3::text),
 CASE WHEN $3='18446744073709551615 Telegram Stars' THEN 18446744073709551615000000
      WHEN $3='500 Prepaid Telegram Stars' THEN 500000000 ELSE 50000000 END)`, from.Unix()+10, fragment, comment)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		dest, asset, comment string
		success              bool
		utime                int64
	}{
		{fragment, usdt, "100 Telegram Stars", true, from.Unix() + 10},
		{fragment, usdt, "100 Telegram Stars", true, from.Unix() + 10},
		{fragment, "fake_usdt", "100 Telegram Stars", true, from.Unix() + 10},
		{"other", usdt, "100 Telegram Stars", true, from.Unix() + 10},
		{fragment, usdt, "100 Telegram Stars", false, from.Unix() + 10},
		{fragment, usdt, "12 months Telegram Premium", true, from.Unix() + 10},
		{fragment, usdt, "100 Telegram Stars", true, from.Unix() - 1},
		{fragment, usdt, "100 Telegram Stars", true, from.Add(48 * time.Hour).Unix()},
	} {
		_, err = fixture.Exec(ctx, `INSERT INTO actions(trace_mc_seqno_end,start_utime,source,destination,type,success,parent_action_id,asset,jetton_transfer_data,amount,value)
VALUES(102,$1,'alice',$2,'jetton_transfer',$3,'batch',$4,ROW($5::text),1500000,1)`, test.utime, test.dest, test.success, test.asset, test.comment)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Real payment fields differ: GRAM uses value, USDT uses amount. The spoofed
	// quantities are excluded even when the other field would have admitted them.
	_, err = fixture.Exec(ctx, `INSERT INTO actions(trace_mc_seqno_end,start_utime,type,success,destination,asset,ton_transfer_data,jetton_transfer_data,value,amount) VALUES
 (102,$1,'ton_transfer',true,$2,NULL,ROW('1000000000000000000 Telegram Stars'),NULL,1,1000000000000000000000000),
 (102,$1,'jetton_transfer',true,$2,$3,NULL,ROW('100 Telegram Stars'),1000000000,1199999)`, from.Unix()+10, fragment, usdt)
	if err != nil {
		t.Fatal(err)
	}
	s, err = source.Collect(ctx, from, from.Add(48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if s.Days[0].StarsPurchases != 12 || s.Days[0].StarsRejected != 2 || s.Days[1].StarsPurchases != 0 {
		t.Fatalf("Stars filter/bounds: %+v", s.Days)
	}
	stars, err := starsRows(from, s)
	if err != nil {
		t.Fatal(err)
	}
	if stars[0].UnknownPurchases != 5 || stars[0].KnownStarsTotal != "18446744073709552465" || stars[0].StarsTotal != nil || stars[1].KnownStarsTotal != "0" || stars[1].StarsTotal == nil || *stars[1].StarsTotal != "0" {
		t.Fatalf("wrong exact/unknown Stars totals: %+v", stars)
	}
	var fifty, hundred, prepaid uint64
	for _, b := range stars[0].Buckets {
		if b.StarsPerPurchase != nil && *b.StarsPerPurchase == 50 && b.PaymentAsset == "TON" {
			fifty = b.Purchases
		}
		if b.StarsPerPurchase != nil && *b.StarsPerPurchase == 100 && b.PaymentAsset == "USDT" {
			hundred = b.Purchases
		}
		if b.Product == "prepaid" {
			prepaid += b.Purchases
		}
	}
	if fifty != 3 || hundred != 2 || prepaid != 1 {
		t.Fatalf("decimal notation/batch/prepaid buckets: %+v", stars[0].Buckets)
	}
	// The Stars-only historical path must reproduce the regular scan, including
	// late traces. A stale account-index row must not duplicate an action.
	_, err = fixture.Exec(ctx, `ALTER TABLE actions ADD COLUMN trace_id text;
ALTER TABLE actions ADD COLUMN action_id text;
ALTER TABLE actions ADD COLUMN trace_end_lt bigint DEFAULT 42;
ALTER TABLE actions ADD COLUMN end_lt bigint DEFAULT 41;
UPDATE actions SET trace_id=ctid::text,action_id=ctid::text;
CREATE TABLE action_accounts AS SELECT destination AS account,trace_id,action_id,trace_mc_seqno_end,
 trace_end_lt,end_lt AS action_end_lt,start_utime+86400*10 AS trace_end_utime FROM actions;
INSERT INTO action_accounts SELECT account,trace_id,action_id,trace_mc_seqno_end,
 trace_end_lt+1,action_end_lt,trace_end_utime FROM action_accounts LIMIT 1;`)
	if err != nil {
		t.Fatal(err)
	}
	targetedSource := source
	targetedSource.StarsOnly = true
	targeted, err := targetedSource.Collect(ctx, from, from.Add(48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	targeted.CalculatedAt = s.CalculatedAt
	targetedRows, err := starsRows(from, targeted)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stars, targetedRows) {
		t.Fatalf("account-index Stars differs: %+v vs %+v", stars, targetedRows)
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
	// Combined history must preserve every daily value and calculate monthly
	// distinct users from the same source, excluding the unfinished trailing month.
	monthFrom := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	monthEnd := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	head := monthEnd.Add(24 * time.Hour)
	_, err = fixture.Exec(ctx, `INSERT INTO blocks VALUES (-1,98,$1),(-1,103,$2)`, monthFrom.Unix()-1, head.Unix())
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.Exec(ctx, `INSERT INTO actions(trace_mc_seqno_end,start_utime,type,success) VALUES (103,$1,'other',true)`, head.Unix())
	if err != nil {
		t.Fatal(err)
	}
	combinedSource := source
	combinedSource.IncludeMonthly = true
	combined, err := combinedSource.Collect(ctx, monthFrom, head)
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err := source.Collect(ctx, monthFrom, head)
	if err != nil {
		t.Fatal(err)
	}
	combined.CalculatedAt = ordinary.CalculatedAt
	combinedDaily, err := dailyRows(monthFrom, head, combined)
	if err != nil {
		t.Fatal(err)
	}
	ordinaryDaily, err := dailyRows(monthFrom, head, ordinary)
	if err != nil {
		t.Fatal(err)
	}
	combinedStars, err := starsRows(monthFrom, combined)
	if err != nil {
		t.Fatal(err)
	}
	ordinaryStars, err := starsRows(monthFrom, ordinary)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(combinedDaily, ordinaryDaily) || !reflect.DeepEqual(combinedStars, ordinaryStars) {
		t.Fatal("combined query changed daily counts, money or Stars")
	}
	monthly, err = source.CollectMonthly(ctx, monthFrom, monthEnd)
	if err != nil {
		t.Fatal(err)
	}
	if !combined.MonthlyIncluded || len(combined.Months) != 1 || !reflect.DeepEqual(combined.Months, monthly.Days) {
		t.Fatalf("monthly distinct results differ: %+v vs %+v", combined.Months, monthly.Days)
	}
	planRows, err = fixture.Query(ctx, "EXPLAIN "+combinedSQL(), int64(98), int64(103), monthFrom.Unix(), head.Unix())
	if err != nil {
		t.Fatal(err)
	}
	scans = 0
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
		t.Fatalf("combined query scans raw source %d times", scans)
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
