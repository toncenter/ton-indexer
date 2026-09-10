package crud

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ton-blockchain/acton/packages/abi-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/acton/catalog"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

// Pinned from acton-abi-catalog/data/data-abis.json, not a synthetic catalog entry.
const vestingCodeHash = "b360d6da4bb86554459ca38c3ec6f26e932370743eacf1219cad54ee006b5ca6"
const walletCodeHash = "IINLe3KxEhR+Gy+0V7hOdNGjDwT3N9T2KmaOlVLSty8="

func traceState(codeHash string) *models.AccountState {
	return &models.AccountState{CodeHash: new(models.HashType(codeHash))}
}

func TestEnrichTraceContractsCatalogAndHistory(t *testing.T) {
	account := models.AccountAddress("0:" + strings.Repeat("1", 64))
	unknown := strings.Repeat("0", 64)
	first := &models.Transaction{
		Account: account, Hash: "first", Lt: 1,
		AccountStateBefore: traceState(vestingCodeHash), AccountStateAfter: traceState(walletCodeHash),
	}
	second := &models.Transaction{
		Account: account, Hash: "second", Lt: 2,
		AccountStateBefore: traceState(walletCodeHash), AccountStateAfter: traceState(unknown),
	}
	response := models.TracesResponse{
		Traces: []models.Trace{{Transactions: map[models.HashType]*models.Transaction{"first": first, "second": second}}},
		// The current account's label must not be substituted for a historical version.
		AddressBook: models.AddressBook{account: {Interfaces: new([]string{"latest_only"})}},
		Metadata:    models.Metadata{account: {IsIndexed: true}},
	}
	before, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	EnrichTraceContracts(response.Traces)
	info := response.Traces[0].ContractInfo
	if info == nil || info.CatalogRevision != catalog.Revision {
		t.Fatalf("missing catalog revision: %+v", info)
	}
	vestingHash := models.MustParseHashType(vestingCodeHash)
	vesting := info.ByCodeHash[vestingHash]
	if vesting == nil || !slices.ContainsFunc(vesting.Candidates, func(c models.ContractCandidate) bool {
		return c.ID == "Jetton Vesting.JettonVesting" && c.DisplayName == "Jetton Vesting"
	}) {
		t.Fatalf("missing known catalog candidate: %+v", vesting)
	}
	wallet := info.ByCodeHash[models.HashType(walletCodeHash)]
	if wallet == nil || !slices.Contains(wallet.Interfaces, "wallet_v5r1") || wallet.Match != "code_hash" {
		t.Fatalf("missing exact interface match: %+v", wallet)
	}
	want := []models.HashType{vestingHash, models.HashType(walletCodeHash), models.MustParseHashType(unknown)}
	slices.Sort(want)
	if !slices.Equal(info.Accounts[account], want) || len(info.ByCodeHash) != 3 {
		t.Fatalf("code changes lost or duplicated: %+v", info)
	}
	if first.AccountStateBefore.ContractInfoKey == nil || *first.AccountStateBefore.ContractInfoKey != vestingHash {
		t.Fatal("before-state does not link to its own code version")
	}
	if first.AccountStateAfter.ContractInfoKey == nil || *first.AccountStateAfter.ContractInfoKey != models.HashType(walletCodeHash) {
		t.Fatal("after-state does not link to its own code version")
	}
	if raw, err := json.Marshal(info.ByCodeHash[models.MustParseHashType(unknown)]); err != nil || string(raw) != "{}" {
		t.Fatalf("unknown code must have an explicit empty summary: %s, %v", raw, err)
	}
	// Removing only the new fields must recover the exact original response.
	response.Traces[0].ContractInfo = nil
	for _, tx := range response.Traces[0].Transactions {
		tx.AccountStateBefore.ContractInfoKey = nil
		tx.AccountStateAfter.ContractInfoKey = nil
	}
	after, err := json.Marshal(response)
	if err != nil || string(before) != string(after) {
		t.Fatalf("existing response fields changed: %s -> %s (%v)", before, after, err)
	}
}

func TestEnrichTraceContractsNormalizationAndAmbiguity(t *testing.T) {
	bytes, err := models.ParseHashBytes(vestingCodeHash)
	if err != nil {
		t.Fatal(err)
	}
	encodings := []string{vestingCodeHash, strings.ToUpper(vestingCodeHash), "0x" + vestingCodeHash}
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		encodings = append(encodings, encoding.EncodeToString(bytes))
	}
	var traces []models.Trace
	for _, hash := range encodings {
		traces = append(traces, models.Trace{Transactions: map[models.HashType]*models.Transaction{
			"tx":      {Account: "same-account", AccountStateBefore: traceState(hash), AccountStateAfter: traceState(hash)},
			"missing": nil,
		}})
	}
	candidates := []*acton.Contract{
		{ID: "b", DisplayName: "Candidate B", ABI: json.RawMessage(`{"large":"abi"}`), Storage: &acton.Binding{}},
		{ID: "a", DisplayName: "Candidate A", Links: []acton.Link{{Kind: "source", Title: "Source", URL: "https://example.com/source"}}},
	}
	calls := 0
	lookup := func(hash string) []*acton.Contract {
		calls++
		if hash != base64.StdEncoding.EncodeToString(bytes) {
			t.Fatalf("lookup hash not normalized: %s", hash)
		}
		return candidates
	}
	enrichTraceContracts(traces, lookup)
	if calls != 1 {
		t.Fatalf("expected one lookup across all transactions and traces, got %d", calls)
	}
	key := models.MustParseHashType(vestingCodeHash)
	for i, trace := range traces {
		info := trace.ContractInfo
		if len(info.ByCodeHash) != 1 || !slices.Equal(info.Accounts["same-account"], []models.HashType{key}) {
			t.Fatalf("trace %d is not deduplicated: %+v", i, info)
		}
		summary := info.ByCodeHash[key]
		if len(summary.Candidates) != 2 || summary.Candidates[0].ID != "a" || summary.Candidates[1].ID != "b" {
			t.Fatalf("ambiguous candidates lost or not sorted: %+v", summary)
		}
		if *trace.Transactions["tx"].AccountStateBefore.CodeHash != models.HashType(encodings[i]) {
			t.Fatal("original code_hash encoding changed")
		}
		raw, err := json.Marshal(summary)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{`"abi"`, `"storage"`, `"get_methods"`, `"verified"`, `"known_addresses"`} {
			if strings.Contains(string(raw), forbidden) {
				t.Fatalf("heavy or misleading field in summary: %s", raw)
			}
		}
	}
	candidates[1].Links[0].Title = "changed"
	if traces[0].ContractInfo.ByCodeHash[key].Candidates[0].Links[0].Title != "Source" {
		t.Fatal("response shares mutable links with catalog")
	}
}

func TestEnrichTraceContractsMissingAndUnknown(t *testing.T) {
	states := []*models.AccountState{nil, {}, traceState(""), traceState("invalid")}
	for _, state := range states {
		traces := []models.Trace{{Transactions: map[models.HashType]*models.Transaction{
			"tx": {Account: "EQAFpr-4AamlDrWSYyLuHf0Ndv3EKMXpYSrT42YZYfGXpSA_", AccountStateBefore: state},
		}}}
		enrichTraceContracts(traces, func(string) []*acton.Contract {
			t.Fatal("missing/invalid hash must not query the catalog or fall back to address")
			return nil
		})
		raw, err := json.Marshal(traces)
		if err != nil || strings.Contains(string(raw), "contract_info") {
			t.Fatalf("missing hash should omit optional metadata: %s, %v", raw, err)
		}
	}
	unknown := traceState(strings.Repeat("0", 64))
	traces := []models.Trace{{Transactions: map[models.HashType]*models.Transaction{
		"tx": {Account: "account", AccountStateBefore: unknown, AccountStateAfter: unknown},
	}}}
	calls := 0
	enrichTraceContracts(traces, func(string) []*acton.Contract { calls++; return nil })
	if calls != 1 {
		t.Fatalf("unknown lookups must also be cached, got %d", calls)
	}
	for _, tx := range traces[0].Transactions {
		tx.AccountStateBefore.CodeHash = nil
		tx.AccountStateAfter.CodeHash = nil
	}
	EnrichTraceContracts(traces)
	if traces[0].ContractInfo != nil || traces[0].Transactions["tx"].AccountStateAfter.ContractInfoKey != nil {
		t.Fatal("re-enrichment retained metadata for a now-missing state")
	}
	EnrichTraceContracts(nil)
}

func TestEnrichTraceContractsPendingAndEmbedded(t *testing.T) {
	for _, populated := range []bool{false, true} {
		context := NewEmptyContext(true)
		tx := &models.Transaction{
			Account: "pending-account", Hash: "tx", Emulated: true,
			AccountStateBefore: &models.AccountState{Hash: "before"},
			AccountStateAfter:  &models.AccountState{Hash: "after"},
		}
		if populated {
			tx.AccountStateAfter.CodeHash = new(models.HashType(vestingCodeHash))
		}
		context.emulatedTransactions["trace"] = []*models.Transaction{tx}
		// No database connection: this path and contract enrichment must not do I/O.
		txs, err := QueryPendingTransactionsImpl(context, nil, models.RequestSettings{}, false)
		if err != nil || len(txs) != 1 {
			t.Fatalf("pending transaction assembly: %v, %v", txs, err)
		}
		traces := []models.Trace{{TraceMeta: models.TraceMeta{TraceState: "pending"}, Trace: &models.TraceNode{
			Children: []*models.TraceNode{nil, {Transaction: &txs[0]}},
		}}}
		EnrichTraceContracts(traces)
		if tx.AccountStateBefore.ContractInfoKey != nil || tx.AccountStateAfter.ContractInfoKey != nil {
			t.Fatal("trace-only links leaked into the pending context's shared account states")
		}
		if (traces[0].ContractInfo != nil) != populated || !txs[0].Emulated || txs[0].AccountStateBefore.ContractInfoKey != nil {
			t.Fatalf("pending states were inferred or dropped: %+v", traces[0])
		}
		first, err := json.Marshal(traces)
		if err != nil {
			t.Fatal(err)
		}
		EnrichTraceContracts(traces)
		second, err := json.Marshal(traces)
		if err != nil || !reflect.DeepEqual(first, second) {
			t.Fatal("enrichment is not idempotent")
		}
	}
}

func TestEnrichTraceContractsRebuildsFilteredClone(t *testing.T) {
	traces := []models.Trace{{Transactions: map[models.HashType]*models.Transaction{
		"old": {Account: "account", AccountStateAfter: traceState(vestingCodeHash)},
		"new": {Account: "account", AccountStateAfter: traceState(walletCodeHash)},
	}}}
	EnrichTraceContracts(traces)
	clone := traces[0]
	clone.Transactions = map[models.HashType]*models.Transaction{"new": traces[0].Transactions["new"]}
	filtered := []models.Trace{clone}
	EnrichTraceContracts(filtered)
	if len(filtered[0].ContractInfo.ByCodeHash) != 1 || !slices.Equal(filtered[0].ContractInfo.Accounts["account"], []models.HashType{walletCodeHash}) {
		t.Fatal("filtered clone retained a code version from the original trace")
	}
	if len(traces[0].ContractInfo.ByCodeHash) != 2 || len(traces[0].ContractInfo.Accounts["account"]) != 2 {
		t.Fatal("re-enriching the clone mutated the original trace's metadata")
	}
}

func TestEnrichTraceContractsInterfaceEncodings(t *testing.T) {
	bytes, err := models.ParseHashBytes(walletCodeHash)
	if err != nil {
		t.Fatal(err)
	}
	encodings := []string{fmt.Sprintf("%x", bytes)}
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		encodings = append(encodings, encoding.EncodeToString(bytes))
	}
	for _, hash := range encodings {
		t.Run(hash, func(t *testing.T) {
			traces := []models.Trace{{Transactions: map[models.HashType]*models.Transaction{
				"tx": {Account: "account", AccountStateAfter: traceState(hash)},
			}}}
			EnrichTraceContracts(traces)
			summary := traces[0].ContractInfo.ByCodeHash[walletCodeHash]
			if summary == nil || !slices.Contains(summary.Interfaces, "wallet_v5r1") {
				t.Fatalf("%s did not match the existing base64 interface table: %+v", hash, summary)
			}
		})
	}
}
