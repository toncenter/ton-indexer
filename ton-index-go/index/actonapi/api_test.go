package actonapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/toncenter/ton-indexer/ton-index-go/index/acton"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var testAddress = "0:" + strings.Repeat("AB", 32)
var testHash = strings.Repeat("12", 32)

func testContract() *acton.Contract {
	return &acton.Contract{ID: "counter", DisplayName: "Counter", CodeHashes: []string{testHash},
		ABI:   json.RawMessage(`{"types":[{"name":"int"}]}`),
		Links: []acton.Link{{Kind: "source", Title: "Source", URL: "https://example.org/source"}},
		GetMethods: []acton.GetMethod{{Name: "get_counter", ID: 76543, Return: acton.TypeInfo{Index: 0, Name: "int"},
			Parameters: []acton.Parameter{{Name: "increment", Type: acton.TypeInfo{Index: 0, Name: "int"}}},
			EncodeArgs: func(args map[string]any) ([]acton.StackValue, error) {
				value, ok := args["increment"]
				if !ok {
					value = "0"
				}
				return []acton.StackValue{{Type: "int", Value: value}}, nil
			},
			DecodeResult: func(stack []acton.StackValue) (any, error) {
				if len(stack) != 1 || stack[0].Type != "int" {
					return nil, errors.New("expected one integer")
				}
				return stack[0].Value, nil
			},
		}},
	}
}

func testApp(api *API) *fiber.App {
	app := fiber.New(fiber.Config{ReadBufferSize: MaxBodyBytes, ErrorHandler: func(c *fiber.Ctx, err error) error {
		var apiError *Error
		if errors.As(err, &apiError) {
			return c.Status(apiError.Code).JSON(apiError)
		}
		return c.Status(500).JSON(map[string]string{"error": err.Error()})
	}})
	app.Get("/contracts", api.Contracts)
	app.Get("/abi", api.ABI)
	app.Get("/accounts", api.Accounts)
	app.Post("/accounts", api.PostAccounts)
	app.Get("/getMethods", api.GetMethods)
	app.Post("/decode", api.Decode)
	app.Post("/runGetMethod", api.RunGetMethod)
	return app
}

func call(t *testing.T, app *fiber.App, method, path, body string, status int, dst any) []byte {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != status {
		t.Fatalf("%s %s: status %d, want %d: %s", method, path, resp.StatusCode, status, raw)
	}
	if dst != nil {
		d := json.NewDecoder(strings.NewReader(string(raw)))
		d.UseNumber()
		if err := d.Decode(dst); err != nil {
			t.Fatalf("decode response: %v: %s", err, raw)
		}
	}
	return raw
}

func TestCatalogAndABI(t *testing.T) {
	contract := testContract()
	app := testApp(New([]*acton.Contract{contract}, "revision", Dependencies{}))
	var page ContractsResponse
	call(t, app, "GET", "/contracts?limit=1", "", 200, &page)
	if page.Total != 1 || len(page.Contracts) != 1 || page.Revision != "revision" {
		t.Fatalf("bad page: %+v", page)
	}
	if page.Contracts[0].SourceVerified || page.Contracts[0].LinksProvenance != "catalog_asserted" {
		t.Fatal("misleading source provenance")
	}
	call(t, app, "GET", "/contracts?offset=9223372036854775807", "", 200, &page)
	if len(page.Contracts) != 0 {
		t.Fatal("expected empty page")
	}
	for _, query := range []string{"limit=0", "limit=1001", "offset=-1", "limit=oops"} {
		call(t, app, "GET", "/contracts?"+query, "", 422, nil)
	}
	b64 := base64.StdEncoding.EncodeToString(bytesOf(0x12, 32))
	unknown := strings.Repeat("00", 32)
	var abis map[string]*ExtendedContractABI
	call(t, app, "GET", "/abi?code_hash="+testHash+"&code_hash="+url.QueryEscape(b64)+"&code_hash="+unknown, "", 200, &abis)
	if len(abis) != 3 || abis[testHash] == nil || abis[b64] == nil || abis[unknown] != nil {
		t.Fatalf("input keys or null missing: %+v", abis)
	}
	if len(abis[b64].CompilerABI) == 0 {
		t.Fatal("missing compiler ABI")
	}
	call(t, app, "GET", "/abi?code_hash=bad", "", 422, nil)
	other := testContract()
	other.ID = "conflict"
	app = testApp(New([]*acton.Contract{contract, other}, "revision", Dependencies{}))
	call(t, app, "GET", "/abi?code_hash="+testHash, "", 409, nil)
	var methods GetMethodsResponse
	call(t, app, "GET", "/getMethods?code_hash="+testHash, "", 200, &methods)
	if len(methods.Contracts) != 2 {
		t.Fatal("getter catalog hid a conflict")
	}
}

func bytesOf(value byte, count int) []byte {
	result := make([]byte, count)
	for i := range result {
		result[i] = value
	}
	return result
}

func TestAccountsBatchSnapshotAndStorage(t *testing.T) {
	contract := testContract()
	boc := base64.StdEncoding.EncodeToString(cell.BeginCell().MustStoreUInt(7, 8).EndCell().ToBOC())
	decodeCalls, queries := 0, 0
	contract.Storage = &acton.Binding{Type: acton.TypeInfo{Index: 1, Name: "Storage"}, Decode: func(c *cell.Cell) (any, error) {
		decodeCalls++
		v, err := c.BeginParse().LoadUInt(8)
		return fmt.Sprint(v), err
	}}
	unknown := "0:" + strings.Repeat("CD", 32)
	missing := "0:" + strings.Repeat("EF", 32)
	failed := "0:" + strings.Repeat("01", 32)
	stateHash, dataHash, txHash, lt := "state", "data", "transaction", "9007199254740993"
	storageRequested := false
	app := testApp(New([]*acton.Contract{contract}, "revision", Dependencies{QueryAccounts: func(c *fiber.Ctx, addresses []string, storage bool) ([]AccountState, error) {
		queries++
		if len(addresses) != 4 || addresses[0] != testAddress {
			t.Fatalf("not canonically deduplicated: %v", addresses)
		}
		storageRequested = storage
		row := AccountState{Address: testAddress, Status: "active", CodeHash: &testHash, StateHash: &stateHash, DataHash: &dataHash, LastTransactionHash: &txHash, LastTransactionLT: &lt, Interfaces: []string{"counter_interface"}}
		if storage {
			row.DataBOC = &boc
		}
		return []AccountState{{Address: unknown, Status: "uninit"}, row, {Address: failed, Error: "row failed"}}, nil
	}}))
	friendly := address.MustParseRawAddr(testAddress).String()
	values := url.Values{"address": {testAddress, strings.ToLower(testAddress), friendly, unknown, missing, failed}}
	var response AccountsResponse
	call(t, app, "GET", "/accounts?"+values.Encode(), "", 200, &response)
	if queries != 1 || storageRequested || decodeCalls != 0 {
		t.Fatal("default listing fetched storage or decoded it")
	}
	if len(response.Accounts) != 4 {
		t.Fatalf("wrong batch size: %+v", response)
	}
	account := response.Accounts[0]
	if account.Status != "identified" || account.AccountStateHash == nil || *account.LastTransactionLT != lt || len(account.Types) != 2 || account.Storage != nil {
		t.Fatalf("bad snapshot: %+v", account)
	}
	if account.Types[0].Provenance != "exact_code_hash" || account.Types[1].Provenance != "public_interface" {
		t.Fatal("bad identification provenance")
	}
	if response.Accounts[1].Status != "unknown" || response.Accounts[2].Status != "not_found" || response.Accounts[3].Status != "error" {
		t.Fatalf("bad per-address statuses: %+v", response)
	}
	body, _ := json.Marshal(AccountsRequest{Addresses: []string{friendly, unknown, missing, failed}, IncludeStorage: true})
	call(t, app, "POST", "/accounts", string(body), 200, &response)
	if queries != 2 || !storageRequested || decodeCalls != 1 || response.Accounts[0].Storage[contract.ID].Decoded != "7" {
		t.Fatalf("storage not decoded once: %+v", response)
	}
}

func TestAccountsValidationAndNoExecution(t *testing.T) {
	queries := 0
	app := testApp(New(nil, "revision", Dependencies{QueryAccounts: func(*fiber.Ctx, []string, bool) ([]AccountState, error) { queries++; return nil, nil }, Executor: func(*fiber.Ctx) GetterExecutor { t.Fatal("listing executed a getter"); return nil }}))
	call(t, app, "GET", "/accounts", "", 422, nil)
	call(t, app, "GET", "/accounts?address=invalid", "", 422, nil)
	call(t, app, "GET", "/accounts?address="+testAddress+"&include_storage=nope", "", 422, nil)
	tooMany := make([]string, MaxBatch+1)
	for i := range tooMany {
		tooMany[i] = testAddress
	}
	body, _ := json.Marshal(AccountsRequest{Addresses: tooMany})
	call(t, app, "POST", "/accounts", string(body), 422, nil)
	if queries != 0 {
		t.Fatal("invalid requests queried DB")
	}
	for _, selector := range []string{"", "address=" + testAddress + "&contract_type=counter", "contract_type=counter&contract_type=other", "code_hash="} {
		call(t, app, "GET", "/getMethods?"+selector, "", 422, nil)
	}
	call(t, app, "GET", "/getMethods?address="+testAddress, "", 200, nil)
	if queries != 1 {
		t.Fatal("getter enumeration must make one state query")
	}
}

func TestNativeDecode(t *testing.T) {
	contract := testContract()
	decode := func(c *cell.Cell) (any, error) { return c.BeginParse().LoadUInt(8) }
	binding := acton.Binding{Type: acton.TypeInfo{Index: 1, Name: "Message"}, Decode: decode}
	contract.Messages = map[string][]acton.Binding{"incoming_messages": {binding}}
	contract.Storage = &binding
	app := testApp(New([]*acton.Contract{contract}, "revision", Dependencies{}))
	boc := base64.StdEncoding.EncodeToString(cell.BeginCell().MustStoreUInt(17, 8).EndCell().ToBOC())
	for _, direction := range []string{"storage", "incoming_messages"} {
		body, _ := json.Marshal(DecodeRequest{ContractType: contract.ID, Direction: direction, Body: boc})
		var response DecodeResponse
		call(t, app, "POST", "/decode", string(body), 200, &response)
		if response.Type.Name != "Message" || response.Decoded != json.Number("17") {
			t.Fatalf("bad decoded message: %+v", response)
		}
	}
	for _, body := range []string{`{}`, `{"contract_type":"counter","direction":"storage","body":"bad"}`, `{"contract_type":"counter","direction":"bad","body":"` + boc + `"}`, `{"contract_type":"counter","code_hash":"` + testHash + `"}`} {
		call(t, app, "POST", "/decode", body, 422, nil)
	}
	call(t, app, "POST", "/decode", strings.Repeat(" ", MaxBodyBytes+1), 413, nil)
}

type fakeExecutor struct {
	t               *testing.T
	snapshot        Snapshot
	execution       Execution
	snapshots, runs int
	method          int64
	stack           []acton.StackValue
}

func (e *fakeExecutor) Snapshot(_ context.Context, addr string, seqno *int32) (*Snapshot, error) {
	e.snapshots++
	if addr != testAddress {
		e.t.Fatalf("noncanonical address: %s", addr)
	}
	if seqno != nil && *seqno != *e.snapshot.Seqno {
		e.t.Fatal("wrong requested seqno")
	}
	return &e.snapshot, nil
}

func (e *fakeExecutor) Run(_ context.Context, snapshot *Snapshot, method int64, stack []acton.StackValue) (*Execution, error) {
	e.runs++
	if snapshot != &e.snapshot {
		e.t.Fatal("snapshot not passed through")
	}
	e.method, e.stack = method, stack
	e.execution.Transport = "standard"
	return &e.execution, nil
}

func runFixture(t *testing.T) (*fiber.App, *fakeExecutor, *acton.Contract) {
	contract := testContract()
	seqno := int32(123)
	executor := &fakeExecutor{t: t, snapshot: Snapshot{Address: testAddress, CodeHash: &testHash, Seqno: &seqno, Pinning: "upstream_seqno"}, execution: Execution{Stack: []acton.StackValue{{Type: "num", Value: "9007199254740993"}}, RawStack: json.RawMessage(`[{"@type":"tvm.stackEntryNumber","number":{"@type":"tvm.numberDecimal","number":"9007199254740993"}}]`), GasUsed: "9007199254740993", ExitCode: 0}}
	app := testApp(New([]*acton.Contract{contract}, "revision", Dependencies{Executor: func(*fiber.Ctx) GetterExecutor { return executor }, QueryAccounts: func(*fiber.Ctx, []string, bool) ([]AccountState, error) {
		t.Fatal("execution used latest indexed state")
		return nil, nil
	}}))
	return app, executor, contract
}

func TestRunPinnedNamedArgsAndIDs(t *testing.T) {
	app, executor, _ := runFixture(t)
	var response RunResponse
	call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter","args":{"increment":9007199254740993},"seqno":123}`, 200, &response)
	if executor.method != 76543 || executor.stack[0].Type != "int" || executor.stack[0].Value != "9007199254740993" {
		t.Fatalf("method ID or precision lost: %d %+v", executor.method, executor.stack)
	}
	if !response.Success || response.Decoded != "9007199254740993" || response.Snapshot.Pinning != "upstream_seqno" || response.Snapshot.ProofVerified {
		t.Fatalf("bad response: %+v", response)
	}
	for _, method := range []string{`76543`, `"76543"`} {
		call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":`+method+`,"stack":[{"type":"num","value":"-0xabcdef"}]}`, 200, nil)
	}
	if executor.snapshots != 3 || executor.runs != 3 {
		t.Fatal("unexpected execution count")
	}
}

func TestRunPreservesVMAndDecodeFailures(t *testing.T) {
	for _, mode := range []string{"vm", "abi", "wire", "alternative_success"} {
		t.Run(mode, func(t *testing.T) {
			app, executor, contract := runFixture(t)
			switch mode {
			case "vm":
				executor.execution.ExitCode = 11
			case "abi":
				contract.GetMethods[0].DecodeResult = func([]acton.StackValue) (any, error) { return nil, errors.New("wrong result type") }
			case "wire":
				executor.execution.StackError = "unsupported wire value"
			case "alternative_success":
				executor.execution.ExitCode = 1
			}
			var response RunResponse
			call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter"}`, 200, &response)
			if string(response.RawStack) != string(executor.execution.RawStack) || response.GasUsed != executor.execution.GasUsed || response.ExitCode != executor.execution.ExitCode {
				t.Fatal("lost raw VM result")
			}
			if mode != "alternative_success" && (response.DecodeError == "" || response.Decoded != nil) {
				t.Fatal("decoding error not independent")
			}
			if mode == "alternative_success" && (!response.Success || response.DecodeError != "") {
				t.Fatal("exit 1 is successful")
			}
		})
	}
}

func TestRunValidationAndCodeMismatch(t *testing.T) {
	app, executor, _ := runFixture(t)
	for _, fields := range []string{
		`"args":{},"stack":[]`, `"args":null`, `"args":[]`, `"stack":null`,
		`"stack":[{"type":"num","value":1.5}]`, `"stack":[{"type":"tuple","value":[{}]}]`,
		`"seqno":-1`, `"seqno":0`, `"transport":"legacy"`, `"unknown":true`, `"contract_type":"counter","code_hash":"` + testHash + `"`,
	} {
		call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter",`+fields+`}`, 422, nil)
	}
	for _, method := range []string{`1.5`, `2147483648`, `null`, `{}`} {
		call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":`+method+`}`, 422, nil)
	}
	if executor.snapshots != 0 {
		t.Fatal("invalid request reached upstream")
	}
	call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter","args":{"unknown":1}}`, 422, nil)
	otherHash := strings.Repeat("34", 32)
	executor.snapshot.CodeHash = &otherHash
	call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter","contract_type":"counter"}`, 409, nil)
	if executor.runs != 0 {
		t.Fatal("mismatched code or invalid args were executed")
	}
}

func TestRunCatalogConflictsAndSnapshotMismatch(t *testing.T) {
	t.Run("duplicate_method_id", func(t *testing.T) {
		app, executor, contract := runFixture(t)
		contract.GetMethods = append(contract.GetMethods, acton.GetMethod{Name: "different_getter", ID: 76543})
		call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter"}`, 409, nil)
		if executor.runs != 0 {
			t.Fatal("ambiguous method ID executed")
		}
	})
	t.Run("snapshot_address", func(t *testing.T) {
		app, executor, _ := runFixture(t)
		executor.snapshot.Address = "0:" + strings.Repeat("11", 32)
		call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter"}`, 502, nil)
		if executor.runs != 0 {
			t.Fatal("mismatched snapshot executed")
		}
	})
	t.Run("invalid_explicit_hash", func(t *testing.T) {
		app, executor, _ := runFixture(t)
		call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter","code_hash":"invalid"}`, 422, nil)
		if executor.snapshots != 0 {
			t.Fatal("invalid explicit hash reached upstream")
		}
	})
}

func TestAccountStorageFailureKeepsIdentification(t *testing.T) {
	contract := testContract()
	contract.Storage = &acton.Binding{Type: acton.TypeInfo{Index: 1, Name: "Storage"}, Unsupported: "unsupported binding"}
	boc := base64.StdEncoding.EncodeToString(cell.BeginCell().EndCell().ToBOC())
	app := testApp(New([]*acton.Contract{contract}, "revision", Dependencies{QueryAccounts: func(*fiber.Ctx, []string, bool) ([]AccountState, error) {
		return []AccountState{{Address: testAddress, CodeHash: &testHash, DataBOC: &boc}}, nil
	}}))
	var response AccountsResponse
	call(t, app, "GET", "/accounts?address="+testAddress+"&include_storage=true", "", 200, &response)
	account := response.Accounts[0]
	if account.Status != "identified" || len(account.Types) != 1 || account.Storage[contract.ID].Error == "" {
		t.Fatalf("storage failure erased identification: %+v", account)
	}
}
