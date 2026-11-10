package validators

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func lockedFixture() ([]StakeInput, map[uint32]Round, map[uint32]StakeMembers, map[uint32]GeoRound, Attribution) {
	inputs := []StakeInput{{Since: 100, Until: 200}, {Since: 200, Until: 300}}
	rounds := map[uint32]Round{}
	members := map[uint32]StakeMembers{}
	geos := map[uint32]GeoRound{}
	attrs := Attribution{Types: map[[2]uint32]ContractObservation{}, ISPs: map[string]ISPObservation{}}
	for i := range inputs {
		in := &inputs[i]
		in.ElectionID = stakePtr(in.Since)
		in.ElectionLinks = 1
		r := Round{Since: in.Since, Until: in.Until, Through: in.Until, Total: 3, Main: 3, TotalWeight: 3, ConfigHash: strings.Repeat("f", 64), Version: Version}
		g := GeoRound{Since: r.Since, Until: r.Until, Through: r.Until, ConfigHash: r.ConfigHash, Version: Version}
		m := StakeMembers{Positions: map[string]StakePosition{}}
		total := new(big.Int)
		for j := 0; j < 3; j++ {
			pubkey := fmt.Sprintf("%064X", j+1)
			adnl := fmt.Sprintf("%064X", j+11)
			holder := fmt.Sprintf("-1:%064X", j+21)
			r.Members = append(r.Members, Member{Index: uint16(j), Pubkey: pubkey, ADNL: adnl, Weight: 1})
			amount, _ := new(big.Int).SetString("9007199254740993", 10)
			amount.Add(amount, big.NewInt(int64(i*100+j)))
			m.Positions[pubkey] = StakePosition{Holder: holder, Stake: amount}
			total.Add(total, amount)
			typ := []string{"whales_proxy", "single_nominator_v1.1", "wallet_v1_r3"}[j]
			attrs.Types[[2]uint32{r.Since, uint32(j)}] = ContractObservation{Since: r.Since, Until: r.Until, Index: uint16(j), Pubkey: pubkey, ADNL: adnl, Weight: 1, Holder: holder, ContractType: &typ}
			id := fmt.Sprintf("%d-%d", r.Since, j)
			ts := r.Since + 1
			isp := []string{"Example ISP", "Telegram Messenger", "Example ISP"}[j]
			g.Members = append(g.Members, GeoMember{Pubkey: pubkey, ADNL: adnl, ObservedAt: &ts, RecordID: &id})
			attrs.ISPs[id] = ISPObservation{RecordID: id, Time: ts, ADNL: adnl, ISP: &isp}
		}
		if err := r.validate(); err != nil {
			panic(err)
		}
		in.Selected = amountString(total)
		m.Digest = r.RosterDigest
		g.RosterDigest = r.RosterDigest
		rounds[r.Since] = r
		members[r.Since] = m
		geos[r.Since] = g
	}
	return inputs, rounds, members, geos, attrs
}

func TestLockedExactTwoRoundsAndUnofficialPools(t *testing.T) {
	in, rounds, members, geos, attrs := lockedFixture()
	r := prepareLocked(in[1], in[0], rounds, members, geos, attrs, time.Unix(400, 0))
	if r.Status != "complete" || r.Positions != 6 || r.Official != 0 || *r.Total != "54043195528446264" || *r.Pools != "18014398509482086" || *r.Telegram != "18014398509482088" || *r.Others != "18014398509482090" || r.Breakdown["Whales-like"] != *r.Pools {
		t.Fatalf("bad exact pair: %+v", r)
	}
	// Same holder and key in adjacent rounds are two distinct elected positions.
	if len(r.Members) != 6 || r.Members[0].Holder != r.Members[3].Holder {
		t.Fatal("fixture/count")
	}
	sealLocked(&r, time.Unix(400, 0))
	hash := r.SnapshotHash
	sealLocked(&r, time.Unix(500, 0))
	if hash != r.SnapshotHash {
		t.Fatal("refresh clock changes content hash")
	}
	for _, typ := range []string{"liquid_staking_controller", "hipo_controller", "whales_proxy", "bemo_nominator"} {
		if poolFamily(typ) == "" {
			t.Fatal("family excluded", typ)
		}
	}
	for _, typ := range []string{"other", "nominator_pool_v1", "single_nominator_v1.1", "wallet_v1_r3"} {
		if poolFamily(typ) != "" {
			t.Fatal("unapproved family", typ)
		}
	}
}

func TestLockedGapsMissingSourcesAndOverlap(t *testing.T) {
	for _, mode := range []string{"gap", "missing_previous", "bad_principal", "missing_types", "missing_isp", "conflict", "wrong_holder", "future_isp"} {
		t.Run(mode, func(t *testing.T) {
			in, rounds, members, geos, attrs := lockedFixture()
			switch mode {
			case "gap":
				in[0].Until--
			case "missing_previous":
				delete(rounds, 100)
			case "bad_principal":
				in[0].Selected = stakePtr("1")
			case "missing_types":
				for key := range attrs.Types {
					if key[0] == 100 {
						delete(attrs.Types, key)
					}
				}
			case "missing_isp":
				delete(geos, 100)
			case "conflict":
				v := attrs.ISPs["100-0"]
				v.ISP = stakePtr("Telegram Messenger")
				attrs.ISPs["100-0"] = v
			case "wrong_holder":
				for key, v := range attrs.Types {
					if key[0] == 100 {
						v.Holder = "-1:" + strings.Repeat("f", 64)
						attrs.Types[key] = v
					}
				}
			case "future_isp":
				g := geos[100]
				for i := range g.Members {
					g.Members[i].ObservedAt = stakePtr(uint32(201))
				}
				geos[100] = g
			}
			r := prepareLocked(in[1], in[0], rounds, members, geos, attrs, time.Unix(400, 0))
			if r.Telegram != nil || r.Pools != nil || r.Others != nil {
				t.Fatalf("unknown became zero/partial pair: %+v", r)
			}
			if mode == "conflict" && (r.Overlaps != 1 || r.Status != "overlapping_attribution") {
				t.Fatal(r)
			}
		})
	}
}

func TestLockedAffiliationFollowsHolderAcrossRounds(t *testing.T) {
	holder := rawHolder("Ef8S77rXFmgqImDq0Om9jiyFEXVX6nAgPiqstBOhGKMxVyk6")
	in, rounds, members, geos, attrs := lockedFixture()
	for since, round := range rounds {
		key := round.Members[0].Pubkey
		pos := members[since].Positions[key]
		pos.Holder = holder
		members[since].Positions[key] = pos
		typ := attrs.Types[[2]uint32{since, 0}]
		typ.Holder = holder
		typ.ContractType = stakePtr("liquid_staking_controller")
		attrs.Types[[2]uint32{since, 0}] = typ
	}
	r := prepareLocked(in[1], in[0], rounds, members, geos, attrs, time.Unix(400, 0))
	if r.Official != 2 || r.Breakdown["Stakee"] != *r.Pools || r.Breakdown["Tonstakers-like"] != "" {
		t.Fatalf("identity changed at a round boundary: %+v", r)
	}
	if name, _ := affiliationFor(holder, "whales_proxy", nil); name != "" {
		t.Fatal("wrong implementation accepted")
	}
}

func TestLockedTelegramHistoryBoundariesAndTelemetryGaps(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first uint32
		other *uint32
		want  bool
	}{
		{"known", 99, nil, true}, {"future", 200, nil, false}, {"conflicted", 99, stakePtr(uint32(150)), false}, {"future_conflict", 99, stakePtr(uint32(250)), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in, rs, ms, gs, a := lockedFixture()
			adnl := rs[100].Members[1].ADNL
			delete(a.ISPs, "100-1")
			a.Telegram = map[string]TelegramEvidence{adnl: {ADNL: adnl, First: tc.first, FirstOther: tc.other, RecordID: "first"}}
			r := prepareLocked(in[1], in[0], rs, ms, gs, a, time.Unix(400, 0))
			if (r.Members[1].ISPStatus == "historical_telegram") != tc.want {
				t.Fatalf("history extrapolated/lost: %+v", r.Members[1])
			}
		})
	}
}

func TestLockedSourceDuplicateTypesAndExactISP(t *testing.T) {
	in, rs, _, gs, _ := lockedFixture()
	r := rs[in[0].Since]
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Query().Get("readonly") != "1" {
			t.Error("source is not read-only")
		}
		if strings.Contains(req.URL.Query().Get("query"), "session_stats_mainnet_v2.validators") {
			fmt.Fprintf(w, `{"utime_since":100,"utime_until":200,"idx":0,"pubkey":%q,"adnl_addr":%q,"weight":1,"holder":%q,"contract_type":"whales_proxy"}`+"\n", r.Members[0].Pubkey, r.Members[0].ADNL, "-1:"+fmt.Sprintf("%064X", 21))
			fmt.Fprintf(w, `{"utime_since":100,"utime_until":200,"idx":0,"pubkey":%q,"adnl_addr":%q,"weight":1,"holder":%q,"contract_type":"wallet_v1_r3"}`+"\n", r.Members[0].Pubkey, r.Members[0].ADNL, "-1:"+fmt.Sprintf("%064X", 21))
			return
		}
		if err := req.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
			return
		}
		defer req.MultipartForm.RemoveAll()
		f, _, err := req.FormFile("selected_records")
		if err != nil {
			t.Error(err)
			return
		}
		defer f.Close()
		b, _ := io.ReadAll(f)
		if strings.Count(string(b), "record_id") != 3 {
			t.Error("wrong record selection", string(b))
		}
		io.WriteString(w, `{"record_id":"100-0","observed_at":101,"adnl_addr":"A","isp":null}`+"\n"+`{"record_id":"100-1","observed_at":101,"adnl_addr":"B","isp":"Telegram Messenger"}`)
	}))
	defer server.Close()
	a, err := (SessionStats{URL: server.URL}).CollectAttribution(context.Background(), []Round{r}, gs)
	if err != nil {
		t.Fatal(err)
	}
	if !a.Types[[2]uint32{100, 0}].Conflict || a.ISPs["100-0"].ISP != nil || *a.ISPs["100-1"].ISP != "Telegram Messenger" {
		t.Fatal("nullable/conflicting source changed", a)
	}
}

func TestLockedClickHouseSchemaAndISPSelection(t *testing.T) {
	binary, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse-local not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	dir := t.TempDir()
	run := func(q, input string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, "local", "--path", dir, "--max_threads", "2", "--multiquery", "--query", q)
		cmd.Stdin = strings.NewReader(input)
		b, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("ClickHouse: %v\n%s", err, b)
		}
		return string(b)
	}
	run("CREATE DATABASE gramstat;"+LockedSchema+LockedSchema+`
 CREATE DATABASE session_stats_mainnet_v2;
 CREATE TABLE session_stats_mainnet_v2.node_addresses(record_id String,created_timestamp DateTime,adnl_addr String,geoip_isp Nullable(String)) ENGINE=MergeTree ORDER BY(record_id,created_timestamp,adnl_addr);
 CREATE TABLE selected_records(record_id String) ENGINE=MergeTree ORDER BY record_id;
 INSERT INTO selected_records VALUES ('chosen');
 INSERT INTO session_stats_mainnet_v2.node_addresses VALUES ('old',99,'A','Telegram Messenger'),('chosen',100,'A',NULL),('new',101,'A','Telegram Messenger');`, "")
	observations, err := decodeRows[ISPObservation]([]byte(run(stakeISPQuery, "")))
	if err != nil || len(observations) != 1 || observations[0].Time != 100 || observations[0].ISP != nil {
		t.Fatal("wrong/null record replaced", observations, err)
	}
	in, rs, ms, gs, attrs := lockedFixture()
	row := prepareLocked(in[1], in[0], rs, ms, gs, attrs, time.Unix(400, 0))
	sealLocked(&row, time.Unix(400, 0))
	data, _ := encodeRows([]LockedRound{row})
	run("INSERT INTO gramstat.validator_stake_locked_rounds FORMAT JSONEachRow", string(data))
	if got := run("SELECT toString(total_nanogram),toString(telegram_nanogram+pools_nanogram+others_nanogram),length(members) FROM gramstat.validator_stake_locked_rounds FINAL", ""); got != "54043195528446264\t54043195528446264\t6\n" {
		t.Fatal("exact sums failed", got)
	}
	row = prepareLocked(in[1], StakeInput{}, rs, ms, gs, attrs, time.Unix(400, 0))
	sealLocked(&row, time.Unix(401, 0))
	data, _ = encodeRows([]LockedRound{row})
	run("INSERT INTO gramstat.validator_stake_locked_rounds FORMAT JSONEachRow", string(data))
	if got := run("SELECT isNull(total_nanogram),length(members) FROM gramstat.validator_stake_locked_rounds FINAL", ""); got != "1\t0\n" {
		t.Fatal("stale pair survived replacement", got)
	}
}
