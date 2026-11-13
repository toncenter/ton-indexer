package validators

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tvm/cell"
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

type memoryAffiliations struct {
	rows     []Affiliation
	writeErr error
}

func (s *memoryAffiliations) LoadAffiliations(context.Context) ([]Affiliation, error) {
	return s.rows, nil
}
func (s *memoryAffiliations) WriteAffiliations(_ context.Context, rows []Affiliation) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	s.rows = append(s.rows, rows...)
	return nil
}

func TestAffiliationResolverPersistsNewHolderAndPreservesHistoricalAnchor(t *testing.T) {
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
	store := &memoryAffiliations{}
	resolver := AffiliationResolver{RPC: ConfigRPC{URL: server.URL, APIKey: "test-key"}, Store: store}
	resolved, err := resolver.Resolve(context.Background(), []Round{fixture}, members, attrs)
	if err != nil {
		t.Fatal(err)
	}
	if name, evidence := affiliationFor(fixture.Since, holder, family, resolved); name != "Stakee" || !strings.Contains(evidence, AffiliationVersion) {
		t.Fatal(name, evidence)
	}
	second := AffiliationResolver{RPC: resolver.RPC, Store: store}
	resolved, err = second.Resolve(context.Background(), []Round{fixture}, members, attrs)
	if err != nil || calls != 1 {
		t.Fatal("cache not reused", calls, err)
	}
	if name, _ := affiliationFor(fixture.Since, holder, "whales_proxy", resolved); name != "" {
		t.Fatal("cached identity leaked into another family")
	}
	store.rows[0].Version = "account-identity-v2"
	store.writeErr = errors.New("storage unavailable")
	third := AffiliationResolver{RPC: resolver.RPC, Store: store}
	if _, err := third.Resolve(context.Background(), []Round{fixture}, members, attrs); err == nil || calls != 2 || len(third.cache) != 0 {
		t.Fatal("old verification reused or failed persistence silently accepted", err, calls, third.cache)
	}
}

func TestHipoStateInitBindsHolderBorrowerTreasuryAndRound(t *testing.T) {
	r, holder, family, body := affiliationFixture(t, "hipo")
	a, err := InspectAffiliation(body, r, holder, family)
	if err != nil {
		t.Fatal(err)
	}
	derived, err := hipoLoanAddress(hipoTreasury, a.Borrower, a.LoanSince)
	if err != nil || derived != holder {
		t.Fatal(derived, holder, err)
	}
	if _, err := InspectAffiliation(body, r, "-1:"+strings.Repeat("A", 64), family); err == nil {
		t.Fatal("a self-reported treasury without the correct StateInit address was accepted")
	}
	badRound := r
	badRound.Since++
	if _, err := InspectAffiliation(body, badRound, holder, family); err == nil {
		t.Fatal("wrong round accepted")
	}
	resolved := map[string]Affiliation{affiliationKey(holder, family): a}
	if name, _ := affiliationFor(r.Since, holder, family, resolved); name != "Hipo" {
		t.Fatal(name)
	}
	if name, _ := affiliationFor(r.Since+1, holder, family, resolved); name != "" {
		t.Fatal("loan leaked into another round")
	}
	for _, mutate := range []func(*Affiliation){
		func(a *Affiliation) { a.Borrower = "0:" + strings.Repeat("1", 64) },
		func(a *Affiliation) { a.Pool = "0:" + strings.Repeat("2", 64) },
		func(a *Affiliation) { a.LoanSince++ },
		func(a *Affiliation) { a.CodeHash = strings.Repeat("0", 64) },
	} {
		changed := a
		mutate(&changed)
		if protocolFor(changed) != "" {
			t.Fatal("changed loan identity accepted", changed)
		}
	}
	// A new borrower/round is recognized without adding any holder to a registry.
	borrower := "0:" + strings.Repeat("4", 64)
	derived, err = hipoLoanAddress(hipoTreasury, borrower, r.Since)
	if err != nil {
		t.Fatal(err)
	}
	elector, _ := address.ParseRawAddr("-1:" + strings.Repeat("3", 64))
	pool, _ := address.ParseRawAddr(hipoTreasury)
	b, _ := address.ParseRawAddr(borrower)
	data := cell.BeginCell().MustStoreAddr(elector).MustStoreAddr(pool).MustStoreAddr(b).MustStoreUInt(uint64(r.Since), 32).EndCell()
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	response["result"].(map[string]any)["data"] = data.ToBOC()
	changedBody, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if a, err := InspectAffiliation(changedBody, r, derived, family); err != nil || a.Protocol != "Hipo" {
		t.Fatal(a, err)
	}
}
