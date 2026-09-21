package actonapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/ton-blockchain/tolk-abi-to-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var testAddress = "0:" + strings.Repeat("AB", 32)
var testHash = strings.Repeat("12", 32)

func testContract() *tolkabi.Contract {
	return &tolkabi.Contract{ID: "counter", DisplayName: "Counter", CodeHashes: []string{testHash},
		ABI:   json.RawMessage(`{"types":[{"name":"int"}]}`),
		Links: []tolkabi.Link{{Kind: "source", Title: "Source", URL: "https://example.org/source"}},
		GetMethods: []tolkabi.GetMethod{{Name: "get_counter", ID: 76543, Return: tolkabi.TypeInfo{Index: 0, Name: "int"},
			Parameters: []tolkabi.Parameter{{Name: "increment", Type: tolkabi.TypeInfo{Index: 0, Name: "int"}}},
			EncodeArgs: func(args map[string]any) ([]tolkabi.StackValue, error) {
				value, ok := args["increment"]
				if !ok {
					value = "0"
				}
				return []tolkabi.StackValue{{Type: "int", Value: value}}, nil
			},
			DecodeResult: func(stack []tolkabi.StackValue) (any, error) {
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
	app := testApp(New([]*tolkabi.Contract{contract}, "revision", Dependencies{}))
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

	// A selector narrows the result and attaches the compiler ABI.
	b64 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x12}, 32))
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
	app = testApp(New([]*tolkabi.Contract{other, contract}, "revision", Dependencies{}))
	call(t, app, "GET", "/contracts?code_hash="+testHash, "", 200, &page)
	if page.Total != 2 || page.Contracts[0].CatalogID != "counter" {
		t.Fatalf("ambiguous hash collapsed or misordered: %+v", page)
	}
}

// Candidates sharing a code hash rank by getters, then message bindings, then ID, so every
// response that names one contract for that code names the same one.
func TestOrderCandidates(t *testing.T) {
	message := map[string][]tolkabi.Binding{"incoming_messages": {{}}}
	ordered := OrderCandidates([]*tolkabi.Contract{{ID: "b"}, {ID: "a"}, {ID: "c", Messages: message}, {ID: "d", GetMethods: make([]tolkabi.GetMethod, 1)}})
	ids := make([]string, 0, len(ordered))
	for _, contract := range ordered {
		ids = append(ids, contract.ID)
	}
	if got := strings.Join(ids, ","); got != "d,c,a,b" {
		t.Fatalf("candidates ranked %s, want getters, then message bindings, then ID", got)
	}
}

// The index is a pure function of the pinned catalog, so a client that already
// holds it revalidates without transferring it again.
func TestCatalogIndexRevalidates(t *testing.T) {
	app := testApp(New([]*tolkabi.Contract{testContract()}, "revision", Dependencies{}))
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

func TestNativeDecode(t *testing.T) {
	contract := testContract()
	decode := func(c *cell.Cell) (any, error) {
		s, err := c.BeginParse()
		if err != nil {
			return nil, err
		}
		return s.LoadUInt(8)
	}
	binding := tolkabi.Binding{Type: tolkabi.TypeInfo{Index: 1, Name: "Message"}, Decode: decode,
		DecodeWith: func(_ *tolkabi.Context, c *cell.Cell) (any, error) { return decode(c) }}
	contract.Messages = map[string][]tolkabi.Binding{"incoming_messages": {binding}}
	contract.Storage = &binding
	app := testApp(New([]*tolkabi.Contract{contract}, "revision", Dependencies{}))
	boc := base64.StdEncoding.EncodeToString(cell.BeginCell().MustStoreUInt(17, 8).EndCell().ToBOC())
	for _, direction := range []string{"storage", "incoming_messages"} {
		body, _ := json.Marshal(DecodeRequest{CatalogID: contract.ID, Direction: direction, Body: boc})
		var response DecodeResponse
		call(t, app, "POST", "/decode", string(body), 200, &response)
		if response.Type.Name != "Message" || response.Decoded != json.Number("17") {
			t.Fatalf("bad decoded message: %+v", response)
		}
	}
	for _, body := range []string{`{}`, `{"catalog_id":"counter","direction":"storage","body":"bad"}`, `{"catalog_id":"counter","direction":"bad","body":"` + boc + `"}`, `{"catalog_id":"counter","code_hash":"` + testHash + `"}`} {
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
	stack           []tolkabi.StackValue
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

func (e *fakeExecutor) Run(_ context.Context, snapshot *Snapshot, method int64, stack []tolkabi.StackValue) (*Execution, error) {
	e.runs++
	if snapshot != &e.snapshot {
		e.t.Fatal("snapshot not passed through")
	}
	e.method, e.stack = method, stack
	return &e.execution, nil
}

func runFixture(t *testing.T) (*fiber.App, *fakeExecutor, *tolkabi.Contract) {
	contract := testContract()
	seqno := int32(123)
	executor := &fakeExecutor{t: t, snapshot: Snapshot{Address: testAddress, CodeHash: &testHash, Seqno: &seqno}, execution: Execution{
		Stack:   []models.V2StackEntity{{Type: "num", Value: "0x20000000000001"}},
		Native:  []tolkabi.StackValue{{Type: "num", Value: "9007199254740993"}},
		GasUsed: 9007199254740993, ExitCode: 0}}
	app := testApp(New([]*tolkabi.Contract{contract}, "revision", Dependencies{Executor: func(*fiber.Ctx) GetterExecutor { return executor }}))
	return app, executor, contract
}

func TestRunPinnedNamedArgsAndIDs(t *testing.T) {
	app, executor, _ := runFixture(t)
	var response RunResponse
	call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter","args":{"increment":9007199254740993},"seqno":123}`, 200, &response)
	if executor.method != 76543 || executor.stack[0].Type != "int" || executor.stack[0].Value != "9007199254740993" {
		t.Fatalf("method ID or precision lost: %d %+v", executor.method, executor.stack)
	}
	if !response.Success || response.Decoded != "9007199254740993" {
		t.Fatalf("bad response: %+v", response)
	}
	// A getter is named either way: a decimal string is its TVM ID.
	for _, method := range []string{`"76543"`, `"get_counter"`} {
		call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":`+method+`}`, 200, nil)
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
				contract.GetMethods[0].DecodeResult = func([]tolkabi.StackValue) (any, error) { return nil, errors.New("wrong result type") }
			case "wire":
				executor.execution.StackError = "unsupported wire value"
			case "alternative_success":
				executor.execution.ExitCode = 1
			}
			var response RunResponse
			call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter"}`, 200, &response)
			if !reflect.DeepEqual(response.Stack, executor.execution.Stack) || response.GasUsed != executor.execution.GasUsed || response.ExitCode != executor.execution.ExitCode {
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
	// The request names a getter and nothing about its ABI; anything else is refused.
	for _, fields := range []string{
		`"args":null`, `"args":[]`, `"seqno":-1`, `"seqno":0`, `"transport":"legacy"`, `"unknown":true`,
		`"stack":[]`, `"catalog_id":"counter"`, `"code_hash":"` + testHash + `"`,
	} {
		call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter",`+fields+`}`, 422, nil)
	}
	for _, method := range []string{`1.5`, `76543`, `null`, `{}`, `""`, `"2147483648"`} {
		call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":`+method+`}`, 422, nil)
	}
	if executor.snapshots != 0 {
		t.Fatal("invalid request reached upstream")
	}
	call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter","args":{"unknown":1}}`, 422, nil)
	// Code the catalog does not know is never executed against a neighbour's ABI.
	otherHash := strings.Repeat("34", 32)
	executor.snapshot.CodeHash = &otherHash
	call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter"}`, 404, nil)
	if executor.runs != 0 {
		t.Fatal("mismatched code or invalid args were executed")
	}
}

// A snapshot of another account is refused rather than executed: the code that
// selected the ABI must belong to the requested address.
func TestRunSnapshotAddressMismatch(t *testing.T) {
	app, executor, _ := runFixture(t)
	executor.snapshot.Address = "0:" + strings.Repeat("11", 32)
	call(t, app, "POST", "/runGetMethod", `{"address":"`+testAddress+`","method":"get_counter"}`, 502, nil)
	if executor.runs != 0 {
		t.Fatal("mismatched snapshot executed")
	}
}

// A codec renders an address as lowercase raw, which v3 does not: it spells every
// address with uppercase hex so a client can index address_book with what it sees.
// The rewrite reaches nested values and leaves everything that merely looks close
// alone, since decoded values also carry BOCs, decimals and bit strings.
func TestCanonicalizeDecodedRewritesOnlyAddresses(t *testing.T) {
	const lower = "0:abababababababababababababababababababababababababababababababab"
	const upper = "0:ABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABAB"
	decoded := map[string]any{
		"owner":    lower,
		"master":   "-1:" + strings.Repeat("f", 64),
		"children": []any{map[string]any{"wallet": lower}, "te6cckEBAQEAAgAAAEysuc0=", "12345"},
		"bits":     map[string]any{"bits": 64, "hex": strings.Repeat("ab", 32)},
		"short":    "0:abab",
	}
	CanonicalizeDecoded(decoded)
	children := decoded["children"].([]any)
	if decoded["owner"] != upper || children[0].(map[string]any)["wallet"] != upper {
		t.Fatalf("addresses were not rewritten: %v", decoded)
	}
	if decoded["master"] != "-1:"+strings.Repeat("F", 64) {
		t.Fatalf("masterchain address was not rewritten: %v", decoded["master"])
	}
	if children[1] != "te6cckEBAQEAAgAAAEysuc0=" || children[2] != "12345" || decoded["short"] != "0:abab" {
		t.Fatalf("a non-address value was rewritten: %v", decoded)
	}
	if decoded["bits"].(map[string]any)["hex"] != strings.Repeat("ab", 32) {
		t.Fatalf("a bit string was rewritten: %v", decoded["bits"])
	}
}
