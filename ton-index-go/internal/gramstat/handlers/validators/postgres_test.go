package validators

import (
	"context"
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
	// A real network pause must not be mistaken for a missing activation block.
	if _, err = fixture.Exec(ctx, `UPDATE validator_cycles SET utime_until=2000 WHERE utime_since=100;
UPDATE blocks SET gen_utime=600,key_block=true WHERE seqno=10;
UPDATE blocks SET gen_utime=601 WHERE seqno=11;
UPDATE blocks SET gen_utime=2100 WHERE seqno=12;`); err != nil {
		t.Fatal(err)
	}
	rows, err = source.Collect(ctx, time.Unix(100, 0), time.Unix(250, 0), time.Time{}, nil)
	if err != nil || len(rows) != 1 || rows[0].AnchorMC != 10 || rows[0].AnchorTime != 600 {
		t.Fatalf("delayed activation lost: %+v %v", rows, err)
	}
	// Exact IDs must not read an intervening cycle, preserve money and missing
	// holders, and reject an ambiguous participant join as an invalid roster.
	_, err = fixture.Exec(ctx, `ALTER TABLE validator_cycles ADD COLUMN election_id integer;
ALTER TABLE validator_cycle_members ADD COLUMN stake numeric;
CREATE TABLE validator_election_participants(election_id integer,validator_pubkey text,stake_holder_address text);
UPDATE validator_cycles SET election_id=utime_since-10;
INSERT INTO validator_cycles SELECT 500,600,total,main,total_weight,source_mc_seqno,490 FROM validator_cycles WHERE utime_since=100;
INSERT INTO validator_cycle_members SELECT 300,validator_index,validator_pubkey,adnl_addr,weight,NULL FROM validator_cycle_members WHERE utime_since=100;
INSERT INTO validator_cycle_members SELECT 500,validator_index,validator_pubkey,adnl_addr,weight,0 FROM validator_cycle_members WHERE utime_since=100;
UPDATE validator_cycle_members SET stake=900719925474099312345 WHERE utime_since=100;
INSERT INTO validator_election_participants SELECT 90,validator_pubkey,'-1:holder' FROM validator_cycle_members WHERE utime_since=100 AND validator_index=0;`)
	if err != nil {
		t.Fatal(err)
	}
	members, err := source.CollectStakeMembersForRounds(ctx, []uint32{500, 100, 500})
	if err != nil || len(members) != 2 {
		t.Fatal(members, err)
	}
	for _, pos := range members[100].Positions {
		if pos.Stake.String() != "900719925474099312345" {
			t.Fatal(pos)
		}
	}
	for _, pos := range members[500].Positions {
		if pos.Stake.Sign() != 0 || pos.Holder != "" {
			t.Fatal(pos)
		}
	}
	if _, err = fixture.Exec(ctx, `INSERT INTO validator_election_participants SELECT election_id,validator_pubkey,'-1:ambiguous' FROM validator_election_participants`); err != nil {
		t.Fatal(err)
	}
	members, err = source.CollectStakeMembersForRounds(ctx, []uint32{100, 500})
	if err != nil || len(members) != 1 || members[100].Digest != "" || members[500].Digest == "" {
		t.Fatal("ambiguous holder accepted", members, err)
	}
}
