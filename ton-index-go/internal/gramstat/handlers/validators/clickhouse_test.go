package validators

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestSessionStatsExternalInputAndSourceLag(t *testing.T) {
	round := fixtureRound()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, _ := r.BasicAuth()
		if user != "reader" || pass != "secret" || strings.Contains(r.URL.String(), "secret") {
			t.Error("credential transport")
		}
		if r.URL.Query().Get("readonly") != "1" {
			t.Error("source must be read-only")
		}
		if strings.Contains(r.URL.Query().Get("query"), "maxOrNull") {
			io.WriteString(w, `{"head":149}`)
			return
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
			return
		}
		defer r.MultipartForm.RemoveAll()
		if r.FormValue("roster_format") != "JSONEachRow" {
			t.Error("external format")
		}
		f, _, err := r.FormFile("roster")
		if err != nil {
			t.Error(err)
			return
		}
		defer f.Close()
		data, _ := io.ReadAll(f)
		if !strings.Contains(string(data), `"cutoff":150`) || strings.Count(string(data), `"pubkey"`) != 2 {
			t.Errorf("wrong source-lag cutoff: %s", data)
		}
		if r.URL.Query().Get("param_to") != "150" {
			t.Error("upper bound must use source watermark")
		}
		fmt.Fprintf(w, "{\"utime_since\":100,\"idx\":0,\"pubkey\":%q,\"samples\":0}\n{\"utime_since\":100,\"idx\":1,\"pubkey\":%q,\"samples\":0}\n", round.Members[0].Pubkey, round.Members[1].Pubkey)
	}))
	defer server.Close()
	s := SessionStats{URL: server.URL, User: "reader", Password: "secret"}
	rows, err := s.Collect(context.Background(), []Round{round}, time.Unix(220, 0))
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Through != 150 || rows[0].Partial != 1 || rows[0].Countries != nil {
		t.Fatalf("source lag lost: %+v", rows[0])
	}
}

func TestClickHouseLatestNullableObservationAndAtomicReplacement(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local not installed")
	}
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	run := func(query, input string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, "local", "--path", dir, "--max_threads", "2", "--multiquery", "--param_from", "100", "--param_to", "200", "--output_format_json_quote_64bit_integers", "0", "--query", query)
		cmd.Stdin = strings.NewReader(input)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("ClickHouse: %v\n%s", err, out)
		}
		return string(out)
	}
	r := fixtureRound()
	r.validate()
	r.ConfigHash = strings.Repeat("c", 64)
	r.Version = Version
	r.CalculatedAt = "2026-10-30 12:00:00.000000"
	run(Schema+Schema+`
CREATE DATABASE session_stats_mainnet_v2;
CREATE TABLE session_stats_mainnet_v2.node_addresses (
 record_id String,created_timestamp DateTime,adnl_addr String,ipv4 IPv4,port Int32,
 geoip_country_iso Nullable(String),geoip_latitude Nullable(Float64),geoip_longitude Nullable(Float64),geoip_city Nullable(String)
) ENGINE=MergeTree ORDER BY (record_id,created_timestamp,adnl_addr);
CREATE TABLE roster(utime_since UInt32,cutoff UInt32,idx UInt16,pubkey String,adnl_addr String)
 ENGINE=MergeTree ORDER BY (utime_since,idx);
`, "")
	input, _ := encodeRows([]rosterRow{{100, 200, 0, r.Members[0].Pubkey, r.Members[0].ADNL}, {100, 200, 1, r.Members[1].Pubkey, r.Members[1].ADNL}})
	run("INSERT INTO roster FORMAT JSONEachRow", string(input))
	run(fmt.Sprintf(`INSERT INTO session_stats_mainnet_v2.node_addresses VALUES
 ('before',99,'%s','1.2.3.4',1,'US',40,-70,'Old'),
 ('a',100,'%s','1.2.3.4',1,'NL',52,4,'Amsterdam'),
 ('b',199,'%s','1.2.3.4',1,NULL,NULL,NULL,NULL),
 ('after',200,'%s','1.2.3.4',1,'US',40,-70,'New');`, r.Members[0].ADNL, r.Members[0].ADNL, r.Members[0].ADNL, r.Members[0].ADNL), "")
	text := run(locationsSQL, "")
	obs, err := decodeRows[observation]([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 2 || obs[0].Samples != 2 || obs[0].ObservedAt == nil || *obs[0].ObservedAt != 199 || obs[0].Latitude != nil || obs[0].Country != nil || obs[1].Samples != 0 {
		t.Fatalf("nullable tuple or half-open bounds broken: %s", text)
	}
	geo, err := geoRows([]Round{r}, obs, 210, time.Unix(220, 0), time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	input, _ = encodeRows([]Round{r})
	run("INSERT INTO gramstat.validator_rounds FORMAT JSONEachRow", string(input))
	if got := run("SELECT total_weight FROM gramstat.validator_rounds FINAL", ""); got != "9007199254741000\n" {
		t.Fatalf("weight precision: %s", got)
	}
	input, _ = encodeRows(geo)
	run("INSERT INTO gramstat.validator_geo_rounds FORMAT JSONEachRow", string(input))
	if got := run("SELECT count(),countIf(latitude IS NULL) FROM gramstat.validator_geo_members", ""); got != "2\t2\n" {
		t.Fatalf("map view: %s", got)
	}
	geo[0].Members = geo[0].Members[:1]
	geo[0].Total = 1
	geo[0].Unknown = 1
	geo[0].CalculatedAt = "2026-10-30 12:00:01.000000"
	input, _ = encodeRows(geo)
	run("INSERT INTO gramstat.validator_geo_rounds FORMAT JSONEachRow", string(input))
	if got := run("SELECT count() FROM gramstat.validator_geo_members", ""); got != "1\n" {
		t.Fatalf("removed validator survived replacement: %s", got)
	}
	// Exercise the actual round-reader wire representation, including named tuples.
	text = run("SELECT * FROM gramstat.validator_rounds FINAL SETTINGS output_format_json_named_tuples_as_objects=1 FORMAT JSONEachRow", "")
	roundTrip, err := decodeRows[Round]([]byte(text))
	if err != nil || len(roundTrip) != 1 || roundTrip[0].Members[0].Weight != r.Members[0].Weight {
		t.Fatalf("round JSON: %v %s", err, text)
	}
}
