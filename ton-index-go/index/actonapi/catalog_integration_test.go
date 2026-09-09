package actonapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/ton-blockchain/acton/packages/abi-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/acton/catalog"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func catalogMethod(t *testing.T, id, name string) (*acton.Contract, acton.GetMethod) {
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
	return nil, acton.GetMethod{}
}

func TestRealCatalogIntegerGetters(t *testing.T) {
	for _, id := range []string{"coffee.CoffeeStakingMaster", "system.Elector"} {
		t.Run(id, func(t *testing.T) {
			name, args := "active_election_id", `{}`
			raw := json.RawMessage(`[{"@type":"tvm.stackEntryNumber","number":{"@type":"tvm.numberDecimal","number":"0"}}]`)
			if id == "coffee.CoffeeStakingMaster" {
				name, args = "get_nft_address_by_index", `{"itemIndex":1}`
				boc := base64.StdEncoding.EncodeToString(cell.BeginCell().MustStoreAddr(address.MustParseRawAddr(testAddress)).EndCell().ToBOC())
				raw = json.RawMessage(`[{"@type":"tvm.stackEntrySlice","slice":{"@type":"tvm.slice","bytes":"` + boc + `"}}]`)
			}
			contract, method := catalogMethod(t, id, name)
			stack, err := DecodeStandardStack(raw)
			if err != nil {
				t.Fatal(err)
			}
			seqno := int32(91668427)
			e := &fakeExecutor{t: t, snapshot: Snapshot{Address: testAddress, CodeHash: &contract.CodeHashes[0], Seqno: &seqno}, execution: Execution{Stack: stack, RawStack: raw, ExitCode: 0, GasUsed: "1292"}}
			api := New(catalog.Contracts, catalog.Revision, Dependencies{Executor: func(*fiber.Ctx) GetterExecutor { return e }})
			var result RunResponse
			call(t, testApp(api), "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"`+name+`","args":`+args+`,"seqno":91668427}`, 200, &result)
			if !result.Success || result.DecodeError != "" || result.Decoded == nil || result.GasUsed != "1292" || e.method != method.ID {
				t.Fatalf("real codec integration failed: %+v", result)
			}
			if id == "coffee.CoffeeStakingMaster" {
				if len(e.stack) != 1 || e.stack[0].Type != "int" || e.stack[0].Value != "1" {
					t.Fatalf("native arguments not normalized: %+v", e.stack)
				}
				wire, err := EncodeStandardStack(e.stack)
				if err != nil || wire[0].(map[string]any)["@type"] != "tvm.stackEntryNumber" {
					t.Fatalf("native int cannot cross wire: %v %+v", err, wire)
				}
			} else if result.Decoded != "0" {
				t.Fatalf("Elector result: %#v", result.Decoded)
			}
		})
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
		{"standard_pair_tail", `[{"@type":"tvm.stackEntryTuple","tuple":{"@type":"tvm.tuple","elements":[{"@type":"tvm.stackEntryTuple","tuple":{"@type":"tvm.tuple","elements":[{"@type":"tvm.stackEntryNumber","number":{"@type":"tvm.numberDecimal","number":"0"}},{"@type":"tvm.stackEntryNumber","number":{"@type":"tvm.numberDecimal","number":"1"}}]}},{"@type":"tvm.stackEntryList","list":{"@type":"tvm.list","elements":[]}}]}}]`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stack, err := DecodeStandardStack(json.RawMessage(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			if stack[0].Type != "tuple" && stack[0].Type != "null" {
				t.Fatalf("non-core list type: %+v", stack)
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
	if _, err := DecodeStandardStack(json.RawMessage(`[{"@type":"tvm.stackEntryUnsupported"}]`)); err == nil {
		t.Fatal("unsupported output guessed to be null")
	}
}

func TestMetadataCapabilitiesAndAmplificationBudget(t *testing.T) {
	contract := testContract()
	contract.CodeHashes = []string{strings.Repeat("ab", 32)}
	contract.ABI = json.RawMessage(`{"description":"` + strings.Repeat("a", 77000) + `"}`)
	app := testApp(New([]*acton.Contract{contract}, "revision", Dependencies{}))
	for _, count := range []int{10, 1000} {
		values := url.Values{}
		for i := 0; i < count; i++ {
			variant := []byte(contract.CodeHashes[0])
			for bit := 0; bit < 10; bit++ {
				if i&(1<<bit) != 0 {
					variant[bit] -= 'a' - 'A'
				}
			}
			values.Add("code_hash", string(variant))
		}
		status := 200
		if count == 1000 {
			status = 413
		}
		raw := call(t, app, "GET", "/abi?"+values.Encode(), "", status, nil)
		if len(raw) > MaxMetadataBytes {
			t.Fatalf("metadata cap exceeded: %d bytes", len(raw))
		}
		if count == 10 {
			var entries map[string]json.RawMessage
			if err := json.Unmarshal(raw, &entries); err != nil || len(entries) != 10 {
				t.Fatal("input spellings were not preserved")
			}
		} else if len(raw) > 1024 {
			t.Fatal("amplified body allocated and returned instead of an error")
		}
	}
	call(t, app, "GET", "/abi?code_hash="+url.QueryEscape(contract.CodeHashes[0]+" "), "", 422, nil)
	var methods GetMethodsResponse
	call(t, app, "GET", "/getMethods?contract_type=counter", "", 200, &methods)
	if len(methods.TransportCapabilities) != 1 || methods.TransportCapabilities[0].Endpoint != "runGetMethodStd" || len(methods.TransportCapabilities[0].Warnings) == 0 {
		t.Fatal("transport limitations missing from getter metadata")
	}
	// A single oversized catalog ABI must fail before JSON marshaling as well.
	contract.ABI = json.RawMessage(`{"description":"` + strings.Repeat("a", MaxMetadataBytes) + `"}`)
	call(t, app, "GET", "/getMethods?contract_type=counter", "", 413, nil)
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
	for _, selector := range []string{"", `,"contract_type":"counter"`, `,"code_hash":"` + testHash + `"`, `,"code_hash":"` + actual + `"`} {
		var result RunResponse
		call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter"`+selector+`}`, 200, &result)
		if *result.Snapshot.CodeHash != actual || *result.Snapshot.ImplementationHash != testHash || result.Identification != "library_reference" {
			t.Fatalf("confused code and implementation identity: %+v", result)
		}
	}
	other := testContract()
	other.ID = "different"
	other.CodeHashes = []string{strings.Repeat("cd", 32)}
	api := New([]*acton.Contract{contract, other}, "revision", Dependencies{Executor: func(*fiber.Ctx) GetterExecutor { return executor }})
	call(t, testApp(api), "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter","contract_type":"different"}`, 409, nil)
	// Both code-cell and implementation matches must be retained, not first-win.
	other.CodeHashes = []string{actual}
	api = New([]*acton.Contract{contract, other}, "revision", Dependencies{Executor: func(*fiber.Ctx) GetterExecutor { return executor }})
	call(t, testApp(api), "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter"}`, 409, nil)
}
