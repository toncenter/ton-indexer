package validators

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestStakePostgresUnknownStagesAndAmbiguousElection(t *testing.T) {
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
	name := fmt.Sprintf("gramstat_stake_%d", time.Now().UnixNano())
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
	_, err = fixture.Exec(ctx, `CREATE TABLE validator_cycles(utime_since integer,utime_until integer,election_id integer,total_stake numeric,source_mc_seqno integer);
    CREATE TABLE validator_elections(election_id integer PRIMARY KEY,total_stake numeric,finished boolean,failed boolean,source_mc_seqno integer);
    INSERT INTO validator_cycles VALUES (100,200,90,800719925474099312345,1),(200,300,190,NULL,2),(300,400,290,0,3),(400,500,NULL,NULL,4),(500,600,490,100,5),(600,700,490,100,6);
    INSERT INTO validator_elections VALUES (90,900719925474099312345,true,false,1),(190,200,false,false,2),(290,0,true,false,3),(490,100,true,false,5);`)
	if err != nil {
		t.Fatal(err)
	}
	u := url.URL{Scheme: "postgres", User: url.UserPassword(cfg.User, cfg.Password), Host: net.JoinHostPort(cfg.Host, fmt.Sprint(cfg.Port)), Path: "/" + name, RawQuery: "sslmode=disable"}
	source := Postgres{DSN: u.String()}
	rows, err := source.CollectStake(ctx, time.Unix(150, 0), time.Unix(600, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 || *rows[0].Submitted != "900719925474099312345" || *rows[0].Selected != "800719925474099312345" {
		t.Fatalf("precision/overlap: %+v", rows)
	}
	if rows[1].Submitted != nil || rows[1].Selected != nil || *rows[2].Submitted != "0" || rows[3].ElectionID != nil {
		t.Fatal("unfinished/unknown/zero stages mixed")
	}
	if rows[4].ElectionLinks != 2 {
		t.Fatal("duplicate election outside selected interval was missed")
	}
}
