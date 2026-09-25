package acton

import (
	"encoding/base64"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/ton-blockchain/tolk-abi-to-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

// Pinned from the abis catalog release, not a synthetic catalog entry.
const vestingCodeHash = "b360d6da4bb86554459ca38c3ec6f26e932370743eacf1219cad54ee006b5ca6"
const walletCodeHash = "IINLe3KxEhR+Gy+0V7hOdNGjDwT3N9T2KmaOlVLSty8="

func traceState(codeHash string) *models.AccountState {
	return &models.AccountState{CodeHash: new(models.HashType(codeHash))}
}

func TestCodeBookResolvesEverySpellingFromTheCatalog(t *testing.T) {
	unknown := strings.Repeat("0", 64)
	// The catalog holds walletCodeHash as hex. No other spelling of it may appear
	// here: codeBook resolves a code once for all its spellings, so an unnormalized
	// lookup would show up as a miss.
	rawURLWallet := strings.NewReplacer("+", "-", "/", "_", "=", "").Replace(walletCodeHash)
	traces := []models.Trace{{Transactions: map[models.HashType]*models.Transaction{
		"first": {AccountStateBefore: traceState(vestingCodeHash), AccountStateAfter: traceState(rawURLWallet)},
		// Emulated states carry no code hash.
		"second": {AccountStateBefore: traceState(unknown), AccountStateAfter: &models.AccountState{}},
	}}}
	before, err := json.Marshal(traces)
	if err != nil {
		t.Fatal(err)
	}
	book := TraceCodeBook(traces)
	vesting := book[vestingCodeHash]
	if !slices.ContainsFunc(vesting.Contracts, func(c models.CodeContract) bool {
		return c.CatalogID == "Jetton Vesting.JettonVesting" && c.DisplayName == "Jetton Vesting"
	}) {
		t.Fatalf("missing known catalog entry: %+v", vesting)
	}
	wallet := book[models.HashType(rawURLWallet)]
	if !slices.ContainsFunc(wallet.Contracts, func(c models.CodeContract) bool {
		return c.CatalogID == "wallets.WalletV5r1"
	}) {
		t.Fatalf("missing catalog entry for a non-standard spelling: %+v", wallet)
	}
	if _, present := book[models.HashType(unknown)]; present || len(book) != 2 {
		t.Fatalf("unknown code must be absent: %+v", book)
	}
	// Building the book must leave the response it describes untouched.
	after, err := json.Marshal(traces)
	if err != nil || string(before) != string(after) {
		t.Fatalf("existing response fields changed: %s -> %s (%v)", before, after, err)
	}
}

func TestCodeBookKeysEverySpellingAndResolvesEachCodeOnce(t *testing.T) {
	bytes, err := models.ParseHashBytes(vestingCodeHash)
	if err != nil {
		t.Fatal(err)
	}
	hashes := []models.HashType{vestingCodeHash, models.HashType(strings.ToUpper(vestingCodeHash)), "0x" + vestingCodeHash}
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		hashes = append(hashes, models.HashType(encoding.EncodeToString(bytes)))
	}
	// B sorts after A by ID but declares a getter, so the most specific order puts it first.
	candidates := []*tolkabi.Contract{
		{ID: "a", DisplayName: "Candidate A", Links: []tolkabi.Link{{Kind: "source", Title: "Source", URL: "https://example.com/source"}}},
		{ID: "b", DisplayName: "Candidate B", ABI: json.RawMessage(`{"large":"abi"}`), Storage: &tolkabi.Binding{},
			GetMethods: make([]tolkabi.GetMethod, 1)},
	}
	calls := 0
	book := codeBook(hashes, func(string) []*tolkabi.Contract { calls++; return candidates })
	if calls != 1 || len(book) != len(hashes) {
		t.Fatalf("expected one lookup and a key per spelling, got %d lookups: %+v", calls, book)
	}
	// An exact row also rules out heavy fields such as abi, storage and get_methods.
	const want = `{"contracts":[{"catalog_id":"b","display_name":"Candidate B"},` +
		`{"catalog_id":"a","display_name":"Candidate A","links":[{"kind":"source","title":"Source","url":"https://example.com/source"}]}]}`
	for _, hash := range hashes {
		raw, err := json.Marshal(book[hash])
		if err != nil || string(raw) != want {
			t.Fatalf("%s: candidates lost, not ordered or carrying heavy fields: %s (%v)", hash, raw, err)
		}
	}
}
