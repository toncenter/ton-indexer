package staking

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func fixturePoint() Point {
	adnl := strings.Repeat("A", 64)
	a := Election{ID: 100, Hash: "a", Total: "100", Positions: []Position{{Pubkey: "a", Holder: "-1:holder", Stake: "100", ADNL: &adnl}}}
	before := Observation{MC: 1, Snapshot: Snapshot{Version: 1, Complete: true, Address: "elector", CodeHash: "code", DataHash: "before"}}
	first := Observation{MC: 2, Snapshot: Snapshot{Version: 1, Complete: true, Address: "elector", CodeHash: "code", DataHash: "first", PreviousHash: "before",
		Elections: []Election{a}, ApplicationsComplete: true, Applications: &Applications{ID: 100, Finished: true, Total: "150"}}}
	current := first
	current.MC = 3
	current.Time = 1792237000
	current.Elections = []Election{{ID: 100, Hash: "a", Total: "95", Positions: []Position{{Pubkey: "a", Holder: "-1:holder", Stake: "95", ADNL: &adnl}}}}
	current.Holders = []Holder{{Address: "-1:holder", Kind: "single_nominator"}}
	return Point{Day: "2026-10-17", AnchorMC: 4, AnchorTime: 1792238000, State: &current, Origins: map[string]Origin{electionKey(a): {First: first, Previous: &before}}}
}

func TestAcceptedSelectedRemainingAndUnfreeze(t *testing.T) {
	p := fixturePoint()
	r, err := Calculate(p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if *r.Submitted != "150" || *r.Selected != "100" || *r.Remaining != "95" || *r.Reduction != "5" || r.Breakdown["single_nominator"] != "95" {
		t.Fatalf("wrong amounts %+v", r)
	}
	// A second frozen election is independent even with the same holder/pubkey.
	e := p.State.Elections[0]
	e.ID = 200
	e.Hash = "b"
	p.State.Elections = append(p.State.Elections, e)
	r, err = Calculate(p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if *r.Remaining != "190" || r.Selected != nil || r.UnknownOrigins != 1 {
		t.Fatalf("overlapping rounds: %+v", r)
	}
	p.State.Elections = nil
	r, err = Calculate(p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if *r.Remaining != "0" || *r.Selected != "0" || *r.Reduction != "0" {
		t.Fatal("unfreeze treated as fine")
	}
	p.State.Complete = false
	r, err = Calculate(p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if r.Remaining != nil || r.TelegramUnknown != nil || r.Status != "unknown_elector_state" {
		t.Fatal("unknown turned into zero")
	}
}

func TestLateStartBrokenContinuityAndHolderUpgrade(t *testing.T) {
	for _, kind := range []string{"late_start", "gap", "unknown_previous", "changed_holder", "larger_stake"} {
		t.Run(kind, func(t *testing.T) {
			p := fixturePoint()
			o := p.Origins["100:A"]
			switch kind {
			case "late_start":
				o.Previous = nil
			case "gap":
				o.Previous.DataHash = "missing"
			case "unknown_previous":
				o.Previous.Complete = false
			case "changed_holder":
				p.State.Elections[0].Positions[0].Holder = "other"
			case "larger_stake":
				p.State.Elections[0].Total = "101"
				p.State.Elections[0].Positions[0].Stake = "101"
			}
			p.Origins["100:A"] = o
			r, err := Calculate(p, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if r.Selected != nil || r.Reduction != nil || r.Remaining == nil {
				t.Fatal("inferred unknown original")
			}
		})
	}
	p := fixturePoint()
	pool := "pool"
	p.State.Holders = []Holder{{Address: "-1:holder", Kind: "liquid_staking_controller", Pool: &pool}}
	r, err := Calculate(p, time.Now())
	if err != nil || r.Breakdown["liquid_staking"] != "95" {
		t.Fatal(r, err)
	}
	p.State.Holders[0].Error = "upgraded to unsupported code"
	r, err = Calculate(p, time.Now())
	if err != nil || r.Breakdown["unknown"] != "95" {
		t.Fatal(r, err)
	}
}

func TestExactAmountsAndMalformedSnapshots(t *testing.T) {
	p := fixturePoint()
	p.Origins = nil
	p.State.Elections[0].Total = "9007199254740993123456789"
	p.State.Elections[0].Positions[0].Stake = p.State.Elections[0].Total
	r, err := Calculate(p, time.Now())
	if err != nil || *r.Remaining != p.State.Elections[0].Total {
		t.Fatal(r, err)
	}
	p.State.Elections[0].Total = "0"
	if _, err = Calculate(p, time.Now()); err == nil {
		t.Fatal("accepted sum mismatch")
	}
	for _, s := range []string{"-1", "1.0", "01", "+1", ""} {
		if _, err := amount(s); err == nil {
			t.Fatalf("accepted %q", s)
		}
	}
}

func TestTelegramIsIndependentAndNullable(t *testing.T) {
	p := fixturePoint()
	row, err := Calculate(p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	missing := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("readonly") != "1" {
			t.Error("not readonly")
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
			return
		}
		defer r.MultipartForm.RemoveAll()
		f, _, err := r.FormFile("roster")
		if err != nil {
			t.Error(err)
			return
		}
		defer f.Close()
		data, _ := io.ReadAll(f)
		var input telegramInput
		if err = json.Unmarshal(bytesTrim(data), &input); err != nil {
			t.Error(err)
		}
		if input.To != p.AnchorTime+1 || input.From != p.AnchorTime-86400 {
			t.Error("wrong cutoff")
		}
		if missing {
			fmt.Fprintln(w, `{"id":0,"samples":1,"conflicts":0,"isp":null}`)
		} else {
			fmt.Fprintln(w, `{"id":0,"samples":1,"conflicts":0,"isp":"Telegram Messenger"}`)
		}
	}))
	defer server.Close()
	source := SessionStats{URL: server.URL}
	rows := []Row{row}
	if err = source.Attribute(context.Background(), []Point{p}, rows); err != nil {
		t.Fatal(err)
	}
	if rows[0].Telegram == nil || *rows[0].Telegram != "95" || rows[0].Breakdown["single_nominator"] != "95" {
		t.Fatal("overlap lost")
	}
	missing = true
	rows = []Row{row}
	if err = source.Attribute(context.Background(), []Point{p}, rows); err != nil {
		t.Fatal(err)
	}
	if rows[0].Telegram != nil || *rows[0].TelegramUnknown != "95" {
		t.Fatal("unknown ISP became non-Telegram")
	}
}
func bytesTrim(b []byte) []byte { return []byte(strings.TrimSpace(string(b))) }

func TestClickHouseSchemaAndLatestISP(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local not installed")
	}
	p := fixturePoint()
	r, err := Calculate(p, time.Date(2026, 10, 31, 1, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(r)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	dir := t.TempDir()
	run := func(query, input string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, "local", "--path", dir, "--multiquery", "--param_from", "100", "--param_to", "200", "--output_format_json_quote_64bit_integers", "0", "--query", query)
		cmd.Stdin = strings.NewReader(input)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		return string(out)
	}
	run(Schema, "")
	run("INSERT INTO gramstat.staking_daily FORMAT JSONEachRow", string(data))
	sql := `SELECT remaining_nanogram,selected_nanogram,frozen_principal_reduction_nanogram FROM gramstat.staking_daily FINAL;
CREATE DATABASE session_stats_mainnet_v2;
CREATE TABLE session_stats_mainnet_v2.node_addresses(adnl_addr String,created_timestamp DateTime,record_id String,geoip_isp Nullable(String)) ENGINE=Memory;
CREATE TABLE roster(id UInt32,adnl String,from_time UInt32,to_time UInt32) ENGINE=Memory;
INSERT INTO roster VALUES(0,'A',100,200),(1,'B',100,200);
INSERT INTO session_stats_mainnet_v2.node_addresses VALUES('A',100,'a','Telegram Messenger'),('A',199,'b',NULL),('A',200,'c','Telegram Messenger');
` + telegramSQL + ";\n"
	out := run(sql, "")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 || lines[0] != "95\t100\t5" {
		t.Fatalf("bad monetary output: %s", out)
	}
	var obs telegramResult
	if err = json.Unmarshal([]byte(lines[1]), &obs); err != nil {
		t.Fatal(err)
	}
	if obs.Samples != 2 || obs.ISP != nil {
		t.Fatal("nullable latest/boundary semantics", obs)
	}
}
