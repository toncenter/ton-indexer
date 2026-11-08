package wallets

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

type localStore struct {
	binary, path string
	fail         string
	memoryRetry  bool
	splits       int
}

func (s *localStore) Query(ctx context.Context, q string, p url.Values) ([]byte, error) {
	if s.memoryRetry && strings.Contains(q, "WITH senders AS") {
		a, _ := strconv.ParseUint(p.Get("param_from"), 10, 64)
		b, _ := strconv.ParseUint(p.Get("param_to"), 10, 64)
		if b-a > 43200 {
			s.splits++
			// Simulate an error after states have already reached the table.
			s.memoryRetry = false
			_, err := s.Query(ctx, q, p)
			s.memoryRetry = true
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("Code: 241. MEMORY_LIMIT_EXCEEDED")
		}
	}
	if s.fail != "" && strings.Contains(q, s.fail) {
		return nil, fmt.Errorf("injected insert failure")
	}
	args := []string{"local", "--path", s.path, "--multiquery", "--query", q}
	for k, v := range p {
		args = append(args, "--"+k, v[0])
	}
	b, err := exec.CommandContext(ctx, s.binary, args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, b)
	}
	return b, nil
}

func TestWindows(t *testing.T) {
	d := time.Date(2024, 10, 13, 0, 0, 0, 0, time.UTC)
	for _, req := range []gramstat.Request{
		{From: d, To: d.Add(25 * time.Hour)}, {From: d}, {From: d.Add(time.Minute), To: d.Add(24 * time.Hour)},
	} {
		if _, _, err := window(req, uint32(d.Add(24*time.Hour).Unix())); err == nil {
			t.Fatal("accepted unclosed/misaligned range")
		}
	}
	from, to, err := window(gramstat.Request{LookbackDays: 3}, uint32(d.Add(time.Hour).Unix()))
	if err != nil || !to.Equal(d) || !from.Equal(d.Add(-72*time.Hour)) {
		t.Fatal(from, to, err)
	}
}

func TestExactPublicationAndRetry(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := &localStore{binary: binary, path: t.TempDir()}
	run := func(q string, p url.Values) []byte {
		t.Helper()
		b, e := s.Query(ctx, q, p)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	fixture := `CREATE DATABASE raw; CREATE DATABASE metrics;
 CREATE TABLE raw.blocks(workchain Int32,shard Int64,seqno Int32,mc_block_seqno Int32,gen_utime UInt32,start_lt UInt64,end_lt UInt64)
 ENGINE=ReplacingMergeTree ORDER BY (workchain,shard,seqno);
 CREATE TABLE metrics.account_deltas(mc_seqno UInt32) ENGINE=ReplacingMergeTree ORDER BY mc_seqno;
 CREATE TABLE metrics.deployments(account String,lt UInt64,mc_seqno UInt32,gen_utime DateTime('UTC'),tx_hash String,init_code_hash Nullable(String))
 ENGINE=ReplacingMergeTree ORDER BY (account,lt,tx_hash);
 CREATE TABLE raw.messages(tx_account String,tx_lt UInt64,tx_now UInt32,mc_block_seqno Int32,direction String,source Nullable(String),bounced Nullable(Bool))
 ENGINE=MergeTree ORDER BY tx_lt;
 INSERT INTO metrics.account_deltas VALUES(3),(4),(5),(6),(7),(8);
 INSERT INTO raw.blocks VALUES
 (-1,-9223372036854775808,3,3,1728691200,1,20),
 (-1,-9223372036854775808,4,4,1728777600,20,60),
 (-1,-9223372036854775808,5,5,1728777660,60,100),
 (-1,-9223372036854775808,6,6,1728781200,100,200),
 (-1,-9223372036854775808,7,7,1728863999,200,300),
 (-1,-9223372036854775808,8,8,1728864000,300,400),
 (0,-9223372036854775808,1,8,1728863999,290,300);
 INSERT INTO metrics.deployments VALUES
 ('A',10,3,'2024-10-12 00:00:00','a','HASH'),
 ('A',10,3,'2024-10-12 00:00:00','a','HASH'),
 ('A',70,5,'2024-10-13 00:01:00','redeploy','HASH'),
 ('B',30,4,'2024-10-13 00:00:00','b','HASH'),
 ('C',5,3,'2024-10-12 00:00:00','old-nonwallet','excluded'),
 ('C',25,4,'2024-10-13 00:00:00','c','HASH'),
 ('E',15,3,'2024-10-12 00:00:00','e','excluded'),
 ('F',295,8,'2024-10-13 23:59:59','late-shard','HASH');
 INSERT INTO raw.messages VALUES
 ('A',40,1728777600,4,'out','A',false),
 ('A',40,1728777600,4,'out','A',false),
 ('A',41,1728777601,4,'out','A',NULL),
 ('B',20,1728777600,4,'out','B',false),
 ('C',35,1728777600,4,'out','C',false),
 ('E',40,1728777600,4,'out','E',false),
 ('A',42,1728777602,4,'out','A',true),
 ('A',43,1728777603,4,'in','A',false),
 ('B',65,1728777660,5,'out','B',false),
 ('A',75,1728777660,5,'out','A',false),
 ('A',110,1728781200,6,'out','A',false),
 ('F',296,1728863999,8,'out','F',false);
 `
	run("CREATE DATABASE gramstat;"+Schema+strings.ReplaceAll(fixture, "HASH", codes[0]), nil)
	run(`INSERT INTO gramstat.wallet_sources VALUES('fixture','raw','metrics',3,8,1728777600,1728864000,1,'fixture complete history');`, nil)
	h := &Handler{CH: s, SourceID: "fixture"}
	day := time.Date(2024, 10, 13, 0, 0, 0, 0, time.UTC)
	req := gramstat.Request{From: day, To: day.Add(24 * time.Hour)}
	s.fail = "wallet_first_daily\n"
	if err = h.Run(ctx, req); err == nil {
		t.Fatal("expected incomplete lookup failure")
	}
	s.fail = ""
	if got := string(run("SELECT count() FROM gramstat.wallet_lookup_versions", nil)); got != "0\n" {
		t.Fatal("partial lookup published", got)
	}
	s.fail = "wallet_counts\n"
	s.memoryRetry = true
	if err = h.Run(ctx, req); err == nil {
		t.Fatal("expected failed publication")
	}
	s.fail = ""
	if got := string(run("SELECT count() FROM gramstat.wallet_days", nil)); got != "0\n" {
		t.Fatal("failed run published", got)
	}
	for i := 0; i < 2; i++ {
		if err = h.Run(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	if got := string(run("SELECT count() FROM gramstat.wallet_lookup_versions", nil)); got != "1\n" {
		t.Fatal("completed lookup was rebuilt during retry", got)
	}
	if s.splits == 0 {
		t.Fatal("memory fallback was not exercised")
	}
	p := url.Values{"param_sink": {"gramstat"}, "param_source": {"fixture"}, "param_registry": {Registry}, "param_from": {"1728777600"}, "param_to": {"1728864000"}, "param_step": {"60"}}
	var result struct {
		Data []struct {
			Bucket uint32 `json:"bucket"`
			Active string `json:"active"`
		} `json:"data"`
	}
	if err = json.Unmarshal(run(ActivitySQL, p), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 1440 || result.Data[0].Active != "2" || result.Data[1].Active != "2" || result.Data[60].Active != "1" || result.Data[2].Active != "0" {
		t.Fatalf("incorrect exact minute counts: %+v", result.Data[:61])
	}
	p.Set("param_step", "3600")
	json.Unmarshal(run(ActivitySQL, p), &result)
	if len(result.Data) != 24 || result.Data[0].Active != "3" {
		t.Fatal("hour summed minute counts instead of union")
	}
	p.Set("param_step", "86400")
	json.Unmarshal(run(ActivitySQL, p), &result)
	if len(result.Data) != 1 || result.Data[0].Active != "4" {
		t.Fatal("daily distinct incorrect", result)
	}
	var mc struct {
		Data []struct {
			MC    uint32 `json:"mc_seqno"`
			Count string `json:"activated"`
		} `json:"data"`
	}
	p.Set("param_interval", "0")
	json.Unmarshal(run(ActivatedSQL, p), &mc)
	if len(mc.Data) != 4 || mc.Data[0].Count != "3" || mc.Data[3].Count != "3" {
		t.Fatal("redeploy or excluded-code history changed cumulative count", mc)
	}
	p.Set("param_interval", "86400")
	json.Unmarshal(run(ActivatedSQL, p), &mc)
	if len(mc.Data) != 1 || mc.Data[0].MC != 7 || mc.Data[0].Count != "3" {
		t.Fatal("invalid daily checkpoint", mc)
	}
	p.Set("param_from", "1728777630")
	p.Set("param_to", "1728777700")
	p.Set("param_interval", "3600")
	json.Unmarshal(run(ActivatedSQL, p), &mc)
	if len(mc.Data) != 1 || mc.Data[0].MC != 5 {
		t.Fatal("partial hour lost its MC endpoint", mc)
	}
	p.Set("param_from", "1728777600")
	p.Set("param_to", "1728864000")
	if got := string(run("SELECT DISTINCT activations,opening_activated FROM gramstat.wallet_days", nil)); got != "3\t1\n" {
		t.Fatal(got)
	}
	var daily struct {
		Data []struct {
			Activations string `json:"activations"`
		} `json:"data"`
	}
	if err := json.Unmarshal(run(ActivationsSQL, p), &daily); err != nil {
		t.Fatal(err)
	}
	if len(daily.Data) != 1 || daily.Data[0].Activations != "3" {
		t.Fatal("daily activation reader", daily)
	}
	// A fully computed but unverified source is never public, nor does a missing day become zero.
	p.Set("param_from", "1728691200")
	p.Set("param_to", "1728777600")
	json.Unmarshal(run(ActivitySQL, p), &result)
	if len(result.Data) != 0 {
		t.Fatal("invented missing day")
	}
	// A separately certified no-block day is a real zero, with the previous
	// cumulative count retained; absence of sparse events alone proves neither.
	run(`INSERT INTO metrics.account_deltas VALUES(9);
 INSERT INTO raw.blocks VALUES(-1,-9223372036854775808,9,9,1729036800,400,500);
 INSERT INTO gramstat.wallet_sources VALUES('fixture','raw','metrics',3,9,1728777600,1729036800,1,'fixture complete history');`, nil)
	pause := day.Add(48 * time.Hour)
	if err := h.Run(ctx, gramstat.Request{From: pause, To: pause.Add(24 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if got := string(run("SELECT activations,opening_activated FROM gramstat.wallet_days WHERE day='2024-10-15'", nil)); got != "0\t4\n" {
		t.Fatal("no-block day lost cumulative baseline", got)
	}
	run("ALTER TABLE gramstat.wallet_days UPDATE history_verified=0 WHERE 1 SETTINGS mutations_sync=1", nil)
	p.Set("param_from", "1728777600")
	p.Set("param_to", "1728864000")
	json.Unmarshal(run(ActivitySQL, p), &result)
	if len(result.Data) != 0 {
		t.Fatal("published unverified history")
	}
}
