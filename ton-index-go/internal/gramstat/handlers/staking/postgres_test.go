package staking

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPostgresHistoryAsOfAndHolderUpgrade(t *testing.T) {
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
	name := fmt.Sprintf("gramstat_staking_%d", time.Now().UnixNano())
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
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	schema, err := os.ReadFile("../../../../../ton-index-worker/ton-index-postgres/src/ElectorSnapshotsSchema.h")
	if err != nil {
		t.Fatal(err)
	}
	ddl := strings.Split(strings.Split(string(schema), "R\"SQL(\n")[1], ")SQL\"")[0]
	if _, err = conn.Exec(ctx, `CREATE DOMAIN tonaddr AS text; CREATE TABLE blocks(workchain integer,seqno bigint,gen_utime bigint);`+ddl); err != nil {
		t.Fatal(err)
	}
	p := fixturePoint()
	origin := p.Origins["100:A"]
	base := int64(1788220800)
	write := func(mc uint32, at int64, s Snapshot) {
		t.Helper()
		raw, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = conn.Exec(ctx, `INSERT INTO elector_stake_states VALUES($1,$2,$3,$4)`, mc, at, s.Complete, raw); err != nil {
			t.Fatal(err)
		}
		for _, e := range s.Elections {
			if _, err = conn.Exec(ctx, `INSERT INTO elector_frozen_elections VALUES($1,$2,$3,$4,0,$5)`, mc, e.ID, e.Hash, e.UnfreezeAt, e.Total); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(1, base+1, origin.Previous.Snapshot)
	write(2, base+2, origin.First.Snapshot)
	write(3, base+3, p.State.Snapshot)
	unknown := Snapshot{Version: 1, Complete: false, Error: "getter failed"}
	write(5, base+86400+100, unknown)
	if _, err = conn.Exec(ctx, `INSERT INTO blocks VALUES(-1,4,$1),(-1,6,$2)`, base+86399, base+172799); err != nil {
		t.Fatal(err)
	}
	holder := Holder{Address: "-1:holder", Kind: "single_nominator"}
	raw, _ := json.Marshal(holder)
	if _, err = conn.Exec(ctx, `INSERT INTO stake_holder_states VALUES($1,3,$2,$3)`, holder.Address, base+3, raw); err != nil {
		t.Fatal(err)
	}
	pool := "pool"
	holder.Kind = "liquid_staking_controller"
	holder.Pool = &pool
	raw, _ = json.Marshal(holder)
	if _, err = conn.Exec(ctx, `INSERT INTO stake_holder_states VALUES($1,4,$2,$3)`, holder.Address, base+10, raw); err != nil {
		t.Fatal(err)
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
	points, err := (Postgres{DSN: testDSN}).Collect(ctx, time.Unix(base, 0).UTC(), time.Unix(base+172800, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 2 || points[0].AnchorMC != 4 || points[0].State.MC != 3 || points[1].State.Complete {
		t.Fatalf("bad as-of: %+v", points)
	}
	row, err := Calculate(points[0], time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if row.Selected == nil || *row.Selected != "100" || row.Submitted == nil || *row.Submitted != "150" || *row.Remaining != "95" || row.Breakdown["liquid_staking"] != "95" {
		t.Fatalf("lost origin or latest holder: %+v", row)
	}
	row, err = Calculate(points[1], time.Now())
	if err != nil || row.Remaining != nil {
		t.Fatal("carried through unknown", row, err)
	}
}
