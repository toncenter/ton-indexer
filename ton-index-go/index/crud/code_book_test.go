package crud

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

func TestCodeBookCatalogAndInterfaces(t *testing.T) {
	account := models.AccountAddress("0:" + strings.Repeat("1", 64))
	unknown := strings.Repeat("0", 64)
	response := models.TracesResponse{
		Traces: []models.Trace{{Transactions: map[models.HashType]*models.Transaction{
			"first": {Account: account, Hash: "first", Lt: 1,
				AccountStateBefore: traceState(vestingCodeHash), AccountStateAfter: traceState(walletCodeHash)},
			"second": {Account: account, Hash: "second", Lt: 2,
				AccountStateBefore: traceState(walletCodeHash), AccountStateAfter: traceState(unknown)},
		}}},
		// The current account's label must not be substituted for a historical version.
		AddressBook: models.AddressBook{account: {Interfaces: new([]string{"latest_only"})}},
		Metadata:    models.Metadata{account: {IsIndexed: true}},
	}
	before, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	book := TraceCodeBook(response.Traces)
	vesting := book[vestingCodeHash]
	if !slices.ContainsFunc(vesting.Contracts, func(c models.CodeContract) bool {
		return c.CatalogID == "Jetton Vesting.JettonVesting" && c.DisplayName == "Jetton Vesting"
	}) {
		t.Fatalf("missing known catalog entry: %+v", vesting)
	}
	if wallet := book[walletCodeHash]; !slices.Contains(wallet.Interfaces, "wallet_v5r1") {
		t.Fatalf("missing exact interface match: %+v", wallet)
	}
	if _, present := book[models.HashType(unknown)]; present || len(book) != 2 {
		t.Fatalf("unknown code must be absent: %+v", book)
	}
	// Building the book must leave the response it describes untouched.
	after, err := json.Marshal(response)
	if err != nil || string(before) != string(after) {
		t.Fatalf("existing response fields changed: %s -> %s (%v)", before, after, err)
	}
}

func TestCodeBookKeysEverySpellingAndResolvesEachCodeOnce(t *testing.T) {
	bytes, err := models.ParseHashBytes(vestingCodeHash)
	if err != nil {
		t.Fatal(err)
	}
	spellings := []string{vestingCodeHash, strings.ToUpper(vestingCodeHash), "0x" + vestingCodeHash}
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		spellings = append(spellings, encoding.EncodeToString(bytes))
	}
	var hashes []models.HashType
	for _, spelling := range spellings {
		hashes = append(hashes, models.HashType(spelling), models.HashType(spelling))
	}
	candidates := []*tolkabi.Contract{
		{ID: "b", DisplayName: "Candidate B", ABI: json.RawMessage(`{"large":"abi"}`), Storage: &tolkabi.Binding{}},
		{ID: "a", DisplayName: "Candidate A", Links: []tolkabi.Link{{Kind: "source", Title: "Source", URL: "https://example.com/source"}}},
	}
	calls := 0
	lookup := func(hash string) []*tolkabi.Contract {
		calls++
		if hash != base64.StdEncoding.EncodeToString(bytes) {
			t.Fatalf("lookup hash not normalized: %s", hash)
		}
		return candidates
	}
	book := codeBook(hashes, lookup)
	if calls != 1 {
		t.Fatalf("expected one lookup for one code, got %d", calls)
	}
	if len(book) != len(spellings) {
		t.Fatalf("every spelling needs its own key: %+v", book)
	}
	for _, spelling := range spellings {
		row := book[models.HashType(spelling)]
		if len(row.Contracts) != 2 || row.Contracts[0].CatalogID != "a" || row.Contracts[1].CatalogID != "b" {
			t.Fatalf("%s: candidates lost or not ordered: %+v", spelling, row)
		}
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{`"abi"`, `"storage"`, `"get_methods"`, `"verified"`, `"known_addresses"`} {
			if strings.Contains(string(raw), forbidden) {
				t.Fatalf("heavy or misleading field in row: %s", raw)
			}
		}
	}
	candidates[1].Links[0].Title = "changed"
	if book[models.HashType(vestingCodeHash)].Contracts[0].Links[0].Title != "Source" {
		t.Fatal("response shares mutable links with catalog")
	}
}

func TestCodeBookOrdersMostSpecificFirst(t *testing.T) {
	generic := &tolkabi.Contract{ID: "a.Generic", GetMethods: make([]tolkabi.GetMethod, 1)}
	specific := &tolkabi.Contract{ID: "z.Specific", GetMethods: make([]tolkabi.GetMethod, 2)}
	book := codeBook([]models.HashType{vestingCodeHash}, func(string) []*tolkabi.Contract {
		return []*tolkabi.Contract{generic, specific}
	})
	contracts := book[vestingCodeHash].Contracts
	if len(contracts) != 2 || contracts[0].CatalogID != "z.Specific" {
		t.Fatalf("the entry decoding more of the code must come first: %+v", contracts)
	}
}

func TestCodeBookSkipsMissingAndUnknownCodes(t *testing.T) {
	for _, state := range []*models.AccountState{nil, {}, traceState(""), traceState("invalid")} {
		traces := []models.Trace{{Transactions: map[models.HashType]*models.Transaction{
			"tx": {Account: "EQAFpr-4AamlDrWSYyLuHf0Ndv3EKMXpYSrT42YZYfGXpSA_", AccountStateBefore: state},
		}}}
		if book := TraceCodeBook(traces); len(book) != 0 {
			t.Fatalf("missing or invalid hash must not reach the catalog: %+v", book)
		}
	}
	if book := TraceCodeBook(nil); book != nil {
		t.Fatalf("no traces must produce no book: %+v", book)
	}
}

func TestCodeBookWalksEmbeddedTransactions(t *testing.T) {
	transaction := &models.Transaction{Account: "account", Emulated: true, AccountStateAfter: traceState(vestingCodeHash)}
	traces := []models.Trace{{TraceMeta: models.TraceMeta{TraceState: "pending"}, Trace: &models.TraceNode{
		Children: []*models.TraceNode{nil, {Transaction: transaction}},
	}}}
	if book := TraceCodeBook(traces); len(book) != 1 {
		t.Fatalf("transactions embedded in the node tree were skipped: %+v", book)
	}
	transaction.AccountStateAfter.CodeHash = nil
	if book := TraceCodeBook(traces); len(book) != 0 {
		t.Fatalf("a now-missing state still produced a row: %+v", book)
	}
}

func TestCodeBookInterfaceEncodings(t *testing.T) {
	bytes, err := models.ParseHashBytes(walletCodeHash)
	if err != nil {
		t.Fatal(err)
	}
	var spellings []string
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		spellings = append(spellings, encoding.EncodeToString(bytes))
	}
	for _, spelling := range spellings {
		t.Run(spelling, func(t *testing.T) {
			book := CodeBook([]models.HashType{models.HashType(spelling)})
			if row := book[models.HashType(spelling)]; !slices.Contains(row.Interfaces, "wallet_v5r1") {
				t.Fatalf("%s did not match the existing base64 interface table: %+v", spelling, row)
			}
		})
	}
}
