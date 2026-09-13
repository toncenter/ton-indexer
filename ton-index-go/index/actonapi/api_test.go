package actonapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/ton-blockchain/acton/packages/abi-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
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
		// Mirror main.go: the shared handler renders models.IndexError.
		var apiError models.IndexError
		if errors.As(err, &apiError) {
			return c.Status(apiError.Code).JSON(apiError)
		}
		return c.Status(500).JSON(map[string]string{"error": err.Error()})
	}})
	app.Get("/contracts", api.Contracts)
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

func TestCatalogIndexAndSelectors(t *testing.T) {
	contract := testContract()
	app := testApp(New([]*acton.Contract{contract}, "revision", Dependencies{
		Executor: func(*fiber.Ctx) GetterExecutor { t.Fatal("listing executed a getter"); return nil }}))
	// Without a selector the whole catalog is returned, getters included and type
	// tables left out.
	var page ActonContractsResponse
	call(t, app, "GET", "/contracts", "", 200, &page)
	if page.Total != 1 || page.Limit != 1 || len(page.Contracts) != 1 {
		t.Fatalf("bad index: %+v", page)
	}
	if len(page.Contracts[0].GetMethods) != 1 || page.Contracts[0].GetMethods[0].Return != "int" || page.Contracts[0].ABI != nil {
		t.Fatalf("index must carry rendered getters and no ABI: %+v", page.Contracts[0])
	}
	call(t, app, "GET", "/contracts?offset=9223372036854775807", "", 200, &page)
	if len(page.Contracts) != 0 || page.Total != 1 {
		t.Fatalf("expected an empty page of a known total: %+v", page)
	}
	for _, query := range []string{"limit=0", "limit=1001", "offset=-1", "limit=oops", "code_hash=bad"} {
		call(t, app, "GET", "/contracts?"+query, "", 422, nil)
	}
	tooMany := make([]string, MaxSelectors+1)
	for i := range tooMany {
		tooMany[i] = "catalog_id=counter"
	}
	call(t, app, "GET", "/contracts?"+strings.Join(tooMany, "&"), "", 422, nil)

	// A selector narrows the result and attaches the compiler ABI.
	b64 := base64.StdEncoding.EncodeToString(bytesOf(0x12, 32))
	unknown := strings.Repeat("00", 32)
	call(t, app, "GET", "/contracts?code_hash="+url.QueryEscape(b64)+"&code_hash="+unknown+"&catalog_id=counter", "", 200, &page)
	if page.Total != 1 || len(page.Contracts) != 1 || len(page.Contracts[0].ABI) == 0 {
		t.Fatalf("selected entry missing its ABI or duplicated: %+v", page)
	}
	call(t, app, "GET", "/contracts?code_hash="+unknown, "", 200, &page)
	if page.Total != 0 {
		t.Fatalf("an unknown selector must match nothing, not fail: %+v", page)
	}

	// An ambiguous hash returns every candidate, most specific first.
	other := testContract()
	other.ID, other.GetMethods = "conflict", nil
	app = testApp(New([]*acton.Contract{other, contract}, "revision", Dependencies{}))
	call(t, app, "GET", "/contracts?code_hash="+testHash, "", 200, &page)
	if page.Total != 2 || page.Contracts[0].CatalogID != "counter" {
		t.Fatalf("ambiguous hash collapsed or misordered: %+v", page)
	}
}

// The index is a pure function of the pinned catalog, so a client that already
// holds it revalidates without transferring it again.
func TestCatalogIndexRevalidates(t *testing.T) {
	app := testApp(New([]*acton.Contract{testContract()}, "revision", Dependencies{}))
	request := httptest.NewRequest("GET", "/contracts", nil)
	response, err := app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	tag := response.Header.Get("ETag")
	if tag == "" || response.Header.Get("X-Acton-Catalog-Revision") != "revision" {
		t.Fatalf("missing validator or revision: %+v", response.Header)
	}
	request = httptest.NewRequest("GET", "/contracts", nil)
	request.Header.Set("If-None-Match", tag)
	response, err = app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 304 {
		t.Fatalf("unchanged catalog was sent again: %d", response.StatusCode)
	}
}

func bytesOf(value byte, count int) []byte {
	out := make([]byte, count)
	for i := range out {
		out[i] = value
	}
	return out
}

func TestNativeDecode(t *testing.T) {
	contract := testContract()
	decode := func(c *cell.Cell) (any, error) { return c.BeginParse().LoadUInt(8) }
	binding := acton.Binding{Type: acton.TypeInfo{Index: 1, Name: "Message"}, Decode: decode,
		DecodeWith: func(_ *acton.Context, c *cell.Cell) (any, error) { return decode(c) }}
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
	return &e.execution, nil
}

func runFixture(t *testing.T) (*fiber.App, *fakeExecutor, *acton.Contract) {
	contract := testContract()
	seqno := int32(123)
	executor := &fakeExecutor{t: t, snapshot: Snapshot{Address: testAddress, CodeHash: &testHash, Seqno: &seqno, Pinning: "upstream_seqno"}, execution: Execution{Stack: []acton.StackValue{{Type: "num", Value: "9007199254740993"}}, RawStack: json.RawMessage(`[{"@type":"tvm.stackEntryNumber","number":{"@type":"tvm.numberDecimal","number":"9007199254740993"}}]`), GasUsed: "9007199254740993", ExitCode: 0}}
	app := testApp(New([]*acton.Contract{contract}, "revision", Dependencies{Executor: func(*fiber.Ctx) GetterExecutor { return executor }}))
	return app, executor, contract
}

func TestRunPinnedNamedArgsAndIDs(t *testing.T) {
	app, executor, _ := runFixture(t)
	var response RunResponse
	call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter","args":{"increment":9007199254740993},"seqno":123}`, 200, &response)
	if executor.method != 76543 || executor.stack[0].Type != "int" || executor.stack[0].Value != "9007199254740993" {
		t.Fatalf("method ID or precision lost: %d %+v", executor.method, executor.stack)
	}
	if !response.Success || response.Decoded != "9007199254740993" || response.Snapshot.Pinning != "upstream_seqno" {
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
