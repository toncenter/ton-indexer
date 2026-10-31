package validators

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPostgresRosterAndActivationAnchor(t *testing.T) {
	dsn := os.Getenv("GRAMSTAT_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set GRAMSTAT_TEST_PG_DSN to local PostgreSQL with CREATEDB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	name := fmt.Sprintf("gramstat_validators_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
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
	_, err = fixture.Exec(ctx, `CREATE TABLE blocks(workchain integer,seqno integer,gen_utime bigint,root_hash text,file_hash text,key_block boolean);
CREATE TABLE validator_cycles(utime_since integer PRIMARY KEY,utime_until integer,total integer,main integer,total_weight numeric,source_mc_seqno integer);
CREATE TABLE validator_cycle_members(utime_since integer,validator_index integer,validator_pubkey text,adnl_addr text,weight numeric,PRIMARY KEY(utime_since,validator_index));
INSERT INTO blocks VALUES(-1,9,99,'r9','f9',false),(-1,10,100,'r10','f10',true),(-1,11,100,'r11','f11',false),(-1,12,250,'r12','f12',false);
INSERT INTO validator_cycles VALUES(100,200,2,1,9007199254741000,12),(300,400,2,1,9007199254741000,12);`)
	if err != nil {
		t.Fatal(err)
	}
	r := fixtureRound()
	for _, m := range r.Members {
		if _, err = fixture.Exec(ctx, "INSERT INTO validator_cycle_members VALUES($1,$2,$3,$4,$5)", r.Since, m.Index, m.Pubkey, m.ADNL, fmt.Sprint(m.Weight)); err != nil {
			t.Fatal(err)
		}
	}
	testDSN := dsn + " dbname=" + name
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		u.Path = "/" + name
		testDSN = u.String()
	}
	source := Postgres{DSN: testDSN}
	rows, err := source.Collect(ctx, time.Unix(120, 0), time.Unix(500, 0), time.Unix(120, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].AnchorMC != 10 || rows[0].Through != 200 || rows[0].Partial != 0 || rows[0].TotalWeight != r.TotalWeight {
		t.Fatalf("wrong as-of/activation selection: %+v", rows)
	}
	if err = rows[0].validate(); err != nil {
		t.Fatal(err)
	}
	boc := configBOC(t, r)
	hash, err := verifyConfig(boc, rows[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.Exec(ctx, `CREATE TABLE validator_set_states(mc_seqno bigint PRIMARY KEY,gen_utime bigint,config_hash text,config_boc text)`); err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.Exec(ctx, `INSERT INTO validator_set_states VALUES(10,100,$1,$2)`, hash, base64.StdEncoding.EncodeToString(boc)); err != nil {
		t.Fatal(err)
	}
	verifier := IndexedConfig{DSN: testDSN}
	if got, err := verifier.Verify(ctx, rows[0]); err != nil || got != hash {
		t.Fatalf("native config: %s %v", got, err)
	}
	bad := rows[0]
	bad.AnchorRoot = "different"
	if _, err = verifier.Verify(ctx, bad); err == nil {
		t.Fatal("native config accepted wrong MC identity")
	}
	stored := map[uint32]Coverage{100: {Since: 100, Until: 200, Through: 200, Version: Version}}
	rows, err = source.Collect(ctx, time.Unix(120, 0), time.Unix(500, 0), time.Unix(220, 0), stored)
	if err != nil || len(rows) != 0 {
		t.Fatalf("old complete round not skipped: %+v %v", rows, err)
	}
	if _, err = fixture.Exec(ctx, "UPDATE blocks SET key_block=false WHERE seqno=10"); err != nil {
		t.Fatal(err)
	}
	rows, err = source.Collect(ctx, time.Unix(120, 0), time.Unix(150, 0), time.Unix(120, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].AnchorMC != 0 || rows[0].Through != 150 || rows[0].Partial != 1 {
		t.Fatalf("bad boundary accepted or partial lost: %+v", rows)
	}
}
