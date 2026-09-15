package actonapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/ton-blockchain/tolk-abi-to-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/acton/catalog"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func catalogMethod(t *testing.T, id, name string) (*tolkabi.Contract, tolkabi.GetMethod) {
	t.Helper()
	c := catalog.ByID(id)
	if c == nil {
		t.Fatalf("missing real catalog contract %s", id)
	}
	for _, m := range c.GetMethods {
		if m.Name == name {
			return c, m
		}
	}
	t.Fatalf("missing real catalog method %s/%s", id, name)
	return nil, tolkabi.GetMethod{}
}

func TestRealCatalogAddressGetter(t *testing.T) {
	contract, method := catalogMethod(t, "coffee.CoffeeStakingMaster", "get_nft_address_by_index")
	boc := base64.StdEncoding.EncodeToString(cell.BeginCell().MustStoreAddr(address.MustParseRawAddr(testAddress)).EndCell().ToBOC())
	raw := json.RawMessage(`[{"@type":"tvm.stackEntrySlice","slice":{"@type":"tvm.slice","bytes":"` + boc + `"}}]`)
	stack, err := DecodeStandardStack(raw)
	if err != nil {
		t.Fatal(err)
	}
	seqno := int32(91668427)
	e := &fakeExecutor{t: t, snapshot: Snapshot{Address: testAddress, CodeHash: &contract.CodeHashes[0], Seqno: &seqno}, execution: Execution{Stack: stack, RawStack: raw}}
	api := New(catalog.Contracts, catalog.Revision, Dependencies{Executor: func(*fiber.Ctx) GetterExecutor { return e }})
	var result RunResponse
	call(t, testApp(api), "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_nft_address_by_index","args":{"itemIndex":1},"seqno":91668427}`, 200, &result)
	if !result.Success || result.DecodeError != "" || result.Decoded == nil || e.method != method.ID {
		t.Fatalf("real codec integration failed: %+v", result)
	}
	if len(e.stack) != 1 || e.stack[0].Type != "int" || e.stack[0].Value != "1" {
		t.Fatalf("native arguments not normalized: %+v", e.stack)
	}
}

func TestRealCatalogPluginLists(t *testing.T) {
	_, method := catalogMethod(t, "wallets/w4r2.WalletV4r2", "get_plugin_list")
	for _, tc := range []struct {
		name, raw string
		length    int
	}{
		{"standard_empty", `[{"@type":"tvm.stackEntryList","list":{"@type":"tvm.list","elements":[]}}]`, 0},
		{"standard_one", `[{"@type":"tvm.stackEntryList","list":{"@type":"tvm.list","elements":[{"@type":"tvm.stackEntryTuple","tuple":{"@type":"tvm.tuple","elements":[{"@type":"tvm.stackEntryNumber","number":{"@type":"tvm.numberDecimal","number":"0"}},{"@type":"tvm.stackEntryNumber","number":{"@type":"tvm.numberDecimal","number":"1"}}]}}]}}]`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stack, err := DecodeStandardStack(json.RawMessage(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			result, err := method.DecodeResult(stack)
			if err != nil {
				t.Fatalf("real Lisp-list codec failed: %v", err)
			}
			if items, ok := result.([]any); !ok || len(items) != tc.length {
				t.Fatalf("incorrect plugin list: %#v", result)
			}
		})
	}
}

// Selectors are bounded in number and in the size of the response they select.
func TestCatalogSelectorsResistAmplification(t *testing.T) {
	contract := testContract()
	app := testApp(New([]*tolkabi.Contract{contract}, "revision", Dependencies{}))
	selectors := strings.Repeat("catalog_id=counter&", MaxSelectors)
	call(t, app, "GET", "/contracts?"+selectors, "", 200, nil)
	call(t, app, "GET", "/contracts?"+selectors+"catalog_id=counter", "", 422, nil)
	call(t, app, "GET", "/contracts?code_hash="+url.QueryEscape(testHash+" "), "", 422, nil)

	// A single oversized catalog ABI must fail before it reaches the wire.
	contract.ABI = json.RawMessage(`{"description":"` + strings.Repeat("a", MaxMetadataBytes) + `"}`)
	if raw := call(t, app, "GET", "/contracts?catalog_id=counter", "", 413, nil); len(raw) > 1024 {
		t.Fatalf("amplified body returned instead of an error: %d bytes", len(raw))
	}
}

func TestCanonicalAddressFormsAndTags(t *testing.T) {
	expected := "0:" + strings.Repeat("FF", 32)
	for _, value := range []string{"EQD__________________________________________0vo", "EQD//////////////////////////////////////////0vo", expected, strings.ToLower(expected)} {
		actual, err := CanonicalAddress(value)
		if err != nil || actual != expected {
			t.Fatalf("%s: got %s, %v", value, actual, err)
		}
	}
	for _, value := range []string{"EgD___________________________________________-m", "EQD__________________________________________0vp", "EQD__________________________________________0vo=", "128:" + strings.Repeat("ff", 32), "-129:" + strings.Repeat("ff", 32)} {
		if _, err := CanonicalAddress(value); err == nil {
			t.Fatalf("accepted invalid address: %s", value)
		}
	}
	for _, wc := range []int{-128, -1, 0, 127} {
		addr := address.MustParseRawAddr(fmt.Sprintf("%d:%s", wc, strings.Repeat("ab", 32)))
		for _, bounce := range []bool{false, true} {
			for _, testnet := range []bool{false, true} {
				value := addr.Bounce(bounce).Testnet(testnet).String()
				if actual, err := CanonicalAddress(value); err != nil || actual != strings.ToUpper(addr.StringRaw()) {
					t.Fatalf("valid flags/workchain rejected: %s %v", value, err)
				}
			}
		}
	}
}

func TestLibraryImplementationCatalogSelection(t *testing.T) {
	app, executor, contract := runFixture(t)
	actual := strings.Repeat("ff", 32)
	executor.snapshot.CodeHash = &actual
	executor.snapshot.ImplementationHash = &contract.CodeHashes[0]
	var result RunResponse
	call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter"}`, 200, &result)
	if *result.Snapshot.CodeHash != actual || *result.Snapshot.ImplementationHash != testHash || result.Identification != "library_reference" {
		t.Fatalf("confused code and implementation identity: %+v", result)
	}
	// The code cell's own entry wins over the library implementation's, which is a
	// different contract rather than another name for the same one.
	other := testContract()
	other.ID, other.CodeHashes = "different", []string{actual}
	api := New([]*tolkabi.Contract{contract, other}, "revision", Dependencies{Executor: func(*fiber.Ctx) GetterExecutor { return executor }})
	call(t, testApp(api), "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter"}`, 200, &result)
	if result.CatalogID != "different" || result.Identification != "exact_code_hash" {
		t.Fatalf("library implementation displaced the code cell's own entry: %+v", result)
	}
}

// The index is served whole, without paging, so its size is a design assumption
// rather than a runtime accident: the catalog is compiled in, which makes this a
// quantity known at build time. A failure here is not a reason to raise the bound
// but to introduce paging deliberately, which for a client that never paged is a
// silent truncation of the catalog.
func TestCatalogIndexFitsOneResponse(t *testing.T) {
	const bound = 2 << 20
	contracts := make([]ActonContract, 0, len(catalog.Contracts))
	for _, contract := range catalog.Contracts {
		contracts = append(contracts, contractInfo(contract, false))
	}
	body, err := json.Marshal(ActonContractsResponse{Contracts: contracts, Total: len(contracts), Limit: len(contracts)})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("index is %d bytes for %d contracts", len(body), len(contracts))
	if len(body) > bound {
		t.Fatalf("index is %d bytes, over the %d-byte unpaged bound", len(body), bound)
	}
}
