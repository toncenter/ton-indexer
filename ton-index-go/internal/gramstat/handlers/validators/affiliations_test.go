package validators

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func affiliationFixture(t *testing.T, name string) (Round, string, string, []byte) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "affiliations", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Since  uint32          `json:"utime_since"`
		MC     uint32          `json:"anchor_mc_seqno"`
		Root   string          `json:"anchor_root_hash"`
		File   string          `json:"anchor_file_hash"`
		Holder string          `json:"holder"`
		Family string          `json:"family"`
		Body   json.RawMessage `json:"body"`
	}
	if err = json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	return Round{Since: f.Since, AnchorMC: f.MC, AnchorRoot: f.Root, AnchorFile: f.File}, rawHolder(f.Holder), f.Family, f.Body
}

func TestAffiliationAnchoredCodeAndStaticPoolIdentity(t *testing.T) {
	for _, name := range []string{"stakee", "hipo", "whales-wston"} {
		t.Run(name, func(t *testing.T) {
			r, holder, family, body := affiliationFixture(t, name)
			a, err := InspectAffiliation(body, r, holder, family)
			if err != nil || a.Protocol == "" || a.Pool == "" || len(a.CodeHash) != 64 || len(a.DataHash) != 64 {
				t.Fatal(a, err)
			}
			bad := r
			bad.AnchorMC++
			if _, err = InspectAffiliation(body, bad, holder, family); err == nil {
				t.Fatal("wrong MC accepted")
			}
			bad = r
			bad.AnchorRoot = "different"
			if _, err = InspectAffiliation(body, bad, holder, family); err == nil {
				t.Fatal("wrong block identity accepted")
			}
			if family == "whales_proxy" {
				other, err := InspectAffiliation(body, r, "-1:"+strings.Repeat("a", 64), family)
				if err != nil || other.Protocol != "" {
					t.Fatal("all Whales proxies labelled as official", other, err)
				}
			} else {
				var changed map[string]any
				json.Unmarshal(body, &changed)
				changed["result"].(map[string]any)["code"] = "te6ccgEBAQEAAgAAAA=="
				b, _ := json.Marshal(changed)
				if _, err = InspectAffiliation(b, r, holder, family); err == nil {
					t.Fatal("unreviewed code accepted")
				}
			}
		})
	}
}

func TestAffiliationResolverCachesNewHolderAndPreservesHistoricalAnchor(t *testing.T) {
	r, _, family, body := affiliationFixture(t, "stakee")
	fixture := fixtureRound()
	fixture.Since = r.Since
	fixture.Until = r.Since + 65536
	fixture.AnchorMC, fixture.AnchorRoot, fixture.AnchorFile = r.AnchorMC, r.AnchorRoot, r.AnchorFile
	if err := fixture.validate(); err != nil {
		t.Fatal(err)
	}
	holder := "-1:" + strings.Repeat("A", 64)
	m := fixture.Members[0]
	members := map[uint32]StakeMembers{fixture.Since: {Digest: fixture.RosterDigest, Positions: map[string]StakePosition{m.Pubkey: {Holder: holder}}}}
	attrs := Attribution{Types: map[[2]uint32]ContractObservation{{fixture.Since, uint32(m.Index)}: {Since: fixture.Since, Until: fixture.Until, Index: m.Index, Pubkey: m.Pubkey, ADNL: m.ADNL, Weight: m.Weight, Holder: holder, ContractType: &family}}}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls++
		if req.Method != "GET" || req.URL.Path != "/getAddressInformation" || req.URL.Query().Get("seqno") != fmt.Sprint(r.AnchorMC) || req.URL.Query().Get("address") != holder || req.Header.Get("X-API-Key") != "test-key" || strings.Contains(req.URL.String(), "test-key") {
			t.Error("invalid historical read", req.URL)
		}
		w.Write(body)
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "affiliations.json")
	resolver := AffiliationResolver{RPC: ConfigRPC{URL: server.URL, APIKey: "test-key"}, CachePath: path}
	resolved, err := resolver.Resolve(context.Background(), []Round{fixture}, members, attrs)
	if err != nil {
		t.Fatal(err)
	}
	if name, evidence := affiliationFor(holder, family, resolved); name != "Stakee" || !strings.Contains(evidence, "account-identity-v2") {
		t.Fatal(name, evidence)
	}
	second := AffiliationResolver{RPC: resolver.RPC, CachePath: path}
	resolved, err = second.Resolve(context.Background(), []Round{fixture}, members, attrs)
	if err != nil || calls != 1 {
		t.Fatal("cache not reused", calls, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("cache permissions", info, err)
	}
	if name, _ := affiliationFor(holder, "whales_proxy", resolved); name != "" {
		t.Fatal("cached identity leaked into another family")
	}
}
