package index

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ton-blockchain/tolk-abi-to-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/actonapi"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// No external network or chain mutations: requests use an in-memory listener.
// Tests are deliberately nonparallel because the isolated pool is package-global.
func actonUpstream(t *testing.T, handler fasthttp.RequestHandler) {
	t.Helper()
	listener := fasthttputil.NewInmemoryListener()
	server := &fasthttp.Server{Handler: handler}
	stopped := make(chan struct{})
	go func() { _ = server.Serve(listener); close(stopped) }()
	previous := actonV2HTTPClient
	actonV2HTTPClient = &fasthttp.Client{
		MaxConnsPerHost:     previous.MaxConnsPerHost,
		MaxConnWaitTimeout:  previous.MaxConnWaitTimeout,
		MaxIdleConnDuration: previous.MaxIdleConnDuration,
		MaxResponseBodySize: previous.MaxResponseBodySize,
		Dial:                func(string) (net.Conn, error) { return listener.Dial() },
	}
	t.Cleanup(func() {
		actonV2HTTPClient.CloseIdleConnections()
		actonV2HTTPClient = previous
		_ = listener.Close()
		<-stopped
	})
}

func actonSettings() models.RequestSettings {
	return models.RequestSettings{V2Endpoint: "http://v2.test/api/v2/", V2ApiKey: "private-key", Timeout: time.Second}
}

func TestActonProxyPinsDiscoveryStateAndExecution(t *testing.T) {
	code := cell.BeginCell().MustStoreUInt(99, 8).EndCell()
	data := cell.BeginCell().MustStoreUInt(7, 8).EndCell()
	codeBOC := base64.StdEncoding.EncodeToString(code.ToBOC())
	dataBOC := base64.StdEncoding.EncodeToString(data.ToBOC())
	address := "0:" + strings.Repeat("AB", 32)
	var mu sync.Mutex
	var paths []string
	var violations []string
	var executionRequest map[string]any
	actonUpstream(t, func(c *fasthttp.RequestCtx) {
		mu.Lock()
		defer mu.Unlock()
		path := string(c.Path())
		paths = append(paths, path)
		if string(c.QueryArgs().Peek("api_key")) != "private-key" {
			violations = append(violations, "API key not forwarded")
		}
		var result any
		switch path {
		case "/api/v2/getMasterchainInfo":
			if string(c.Method()) != "GET" {
				violations = append(violations, "masterchain info was not GET")
			}
			result = map[string]any{"last": map[string]any{"seqno": 54321}}
		case "/api/v2/getAddressInformation":
			if string(c.QueryArgs().Peek("seqno")) != "54321" || string(c.QueryArgs().Peek("address")) != address {
				violations = append(violations, "state selector mismatch")
			}
			result = map[string]any{"state": "active", "code": codeBOC, "data": dataBOC, "block_id": map[string]any{"workchain": 0, "seqno": 333}, "last_transaction_id": map[string]any{"lt": "9007199254740993", "hash": "transaction"}}
		case "/api/v2/runGetMethodStd":
			if string(c.Method()) != "POST" {
				violations = append(violations, "getter was not POST")
			}
			d := json.NewDecoder(strings.NewReader(string(c.PostBody())))
			d.UseNumber()
			if err := d.Decode(&executionRequest); err != nil {
				violations = append(violations, err.Error())
			}
			result = map[string]any{"gas_used": json.Number("9007199254740993"), "exit_code": 1, "stack": []any{map[string]any{"@type": "tvm.stackEntryNumber", "number": map[string]any{"@type": "tvm.numberDecimal", "number": "9007199254740993"}}}}
		default:
			violations = append(violations, "unexpected endpoint: "+path)
		}
		body, _ := json.Marshal(map[string]any{"ok": true, "result": result})
		c.SetBody(body)
	})
	executor := NewActonExecutor(actonSettings())
	snapshot, err := executor.Snapshot(context.Background(), strings.ToLower(address), nil)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.McSeqno == nil || *snapshot.McSeqno != 54321 {
		t.Fatalf("bad pinning: %+v", snapshot)
	}
	if *snapshot.CodeHash != base64.StdEncoding.EncodeToString(code.Hash()) || *snapshot.DataHash != base64.StdEncoding.EncodeToString(data.Hash()) || *snapshot.LastTransactionLT != "9007199254740993" {
		t.Fatal("incorrect snapshot hashes or LT")
	}
	stack := []tolkabi.StackValue{{Type: "tuple", Value: []tolkabi.StackValue{{Type: "num", Value: json.Number("9007199254740993")}, {Type: "slice", Value: dataBOC}, {Type: "null"}}}}
	result, err := executor.Run(context.Background(), snapshot, 76543, stack)
	if err != nil {
		t.Fatal(err)
	}
	// gas above 2^53 must survive, and the stack must be spelled like
	// /runGetMethod: type "num", hexadecimal value.
	if result.ExitCode != 1 || result.GasUsed != 9007199254740993 {
		t.Fatalf("lost exact VM results: %+v", result)
	}
	if len(result.Stack) != 1 || result.Stack[0].Type != "num" || result.Stack[0].Value != "0x20000000000001" {
		t.Fatalf("stack is not spelled like /runGetMethod: %+v", result.Stack)
	}
	if len(result.Native) != 1 || result.Native[0].Type != "int" || result.Native[0].Value != "9007199254740993" {
		t.Fatalf("codec stack lost its exact decimal: %+v", result.Native)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(violations) != 0 {
		t.Fatal(violations)
	}
	if !reflect.DeepEqual(paths, []string{"/api/v2/getMasterchainInfo", "/api/v2/getAddressInformation", "/api/v2/runGetMethodStd"}) {
		t.Fatal(paths)
	}
	if executionRequest["seqno"] != json.Number("54321") || executionRequest["method"] != json.Number("76543") || executionRequest["address"] != address {
		t.Fatalf("execution not pinned or numeric ID lost: %+v", executionRequest)
	}
	if len(executionRequest) != 4 {
		t.Fatalf("unexpected request fields: %+v", executionRequest)
	}
	wire := executionRequest["stack"].([]any)[0].(map[string]any)
	if wire["@type"] != "tvm.stackEntryTuple" {
		t.Fatalf("legacy stack used: %+v", wire)
	}
	// Tonlib has no null entry, so null must travel as an empty list.
	entry := wire["tuple"].(map[string]any)["elements"].([]any)[2].(map[string]any)
	if entry["@type"] != "tvm.stackEntryList" || len(entry["list"].(map[string]any)["elements"].([]any)) != 0 {
		t.Fatalf("null not encoded as empty Tonlib list: %+v", entry)
	}
}

func TestActonProxyHistoricalSkipsDiscovery(t *testing.T) {
	boc := base64.StdEncoding.EncodeToString(cell.BeginCell().EndCell().ToBOC())
	var mu sync.Mutex
	var paths []string
	actonUpstream(t, func(c *fasthttp.RequestCtx) {
		mu.Lock()
		paths = append(paths, string(c.Path())+"?"+string(c.QueryArgs().Peek("seqno")))
		mu.Unlock()
		body, _ := json.Marshal(map[string]any{"ok": true, "result": map[string]any{"state": "active", "code": boc, "data": boc}})
		c.SetBody(body)
	})
	seqno := int32(123)
	snapshot, err := NewActonExecutor(actonSettings()).Snapshot(context.Background(), "0:"+strings.Repeat("00", 32), &seqno)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 || paths[0] != "/api/v2/getAddressInformation?123" || *snapshot.McSeqno != seqno {
		t.Fatalf("historical selector not forwarded: %v %+v", paths, snapshot)
	}
}

func TestActonProxyPreservesUnsupportedAndFailedVMResults(t *testing.T) {
	actonUpstream(t, func(c *fasthttp.RequestCtx) {
		c.SetBodyString(`{"ok":true,"result":{"exit_code":11,"gas_used":"456","stack":[{"@type":"tvm.stackEntryUnsupported"}]}}`)
	})
	seqno := int32(42)
	result, err := NewActonExecutor(actonSettings()).Run(context.Background(), &actonapi.Snapshot{Address: "0:" + strings.Repeat("00", 32), McSeqno: &seqno}, 76543, nil)
	if err != nil {
		t.Fatal(err)
	}
	// an unreadable entry must not cost the caller the VM result.
	if result.ExitCode != 11 || result.GasUsed != 456 || result.StackError == "" || result.Stack != nil {
		t.Fatalf("VM failure was lost: %+v", result)
	}
}

func TestActonProxyRejectsIncompatibleUpstreamWithoutFallback(t *testing.T) {
	okBody := `{"ok":true,"result":{"gas_used":1,"exit_code":0,"stack":[]}}`
	for _, tc := range []struct {
		name, location, body string
		status               int
	}{
		{"not_json", "", `not json`, 200},
		{"ok_false_with_upstream_text", "", `{"ok":false,"error":"private-key internal detail"}`, 200},
		{"null_result", "", `{"ok":true,"result":null}`, 200},
		{"missing_exit_code", "", `{"ok":true,"result":{"gas_used":1,"stack":[]}}`, 200},
		{"float_gas_used", "", `{"ok":true,"result":{"gas_used":1.5,"exit_code":0,"stack":[]}}`, 200},
		{"status_503_even_ok_true", "", okBody, 503},
		{"redirect_not_followed", "http://must-not-follow.invalid/private-key", "", 302},
		// A valid body, so only the production response-size cap can reject it.
		{"oversized_body", "", okBody + strings.Repeat(" ", actonMaxUpstreamBytes), 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			actonUpstream(t, func(c *fasthttp.RequestCtx) {
				calls.Add(1)
				if tc.location != "" {
					c.Response.Header.Set("Location", tc.location)
				}
				c.SetStatusCode(tc.status)
				c.SetBodyString(tc.body)
			})
			snapshot := &actonapi.Snapshot{Address: "0:" + strings.Repeat("00", 32), McSeqno: new(int32(1))}
			result, err := NewActonExecutor(actonSettings()).Run(context.Background(), snapshot, 76543, nil)
			var apiError models.IndexError
			if result != nil || !errors.As(err, &apiError) || apiError.Code != 502 || strings.Contains(err.Error(), "private-key") || strings.Contains(err.Error(), "v2.test") || calls.Load() != 1 {
				t.Fatalf("bad upstream not rejected, leaked detail or fell back: calls=%d err=%v", calls.Load(), err)
			}
		})
	}
}

func TestActonProxyDeadlineIsBoundedAndShared(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second, 24 * time.Hour} {
		settings := actonSettings()
		settings.Timeout = timeout
		executor := NewActonExecutor(settings).(*actonExecutor)
		if remaining := time.Until(executor.deadline); remaining <= 0 || remaining > 3*time.Second {
			t.Fatalf("unbounded deadline: %v", remaining)
		}
	}
	executor := NewActonExecutor(actonSettings()).(*actonExecutor)
	executor.deadline = time.Now().Add(-time.Millisecond)
	seqno := int32(1)
	_, err := executor.Snapshot(context.Background(), "0:"+strings.Repeat("00", 32), &seqno)
	var apiError models.IndexError
	if !errors.As(err, &apiError) || apiError.Code != 504 {
		t.Fatalf("expired deadline allowed request: %v", err)
	}
}

func TestActonProxyLibraryReferenceSnapshot(t *testing.T) {
	// Any 32 bytes serve as the embedded library hash; no catalog lookup happens here.
	implementation := bytes.Repeat([]byte{0xab}, 32)
	boc := base64.StdEncoding.EncodeToString(append([]byte{0xb5, 0xee, 0x9c, 0x72, 1, 1, 1, 1, 0, 35, 0, 8, 66, 2}, implementation...))
	code, err := tolkabi.DecodeOpaqueBOC(boc)
	if err != nil {
		t.Fatal(err)
	}
	actonUpstream(t, func(c *fasthttp.RequestCtx) {
		body, _ := json.Marshal(map[string]any{"ok": true, "result": map[string]any{"state": "active", "code": boc, "data": boc}})
		c.SetBody(body)
	})
	seqno := int32(123)
	snapshot, err := NewActonExecutor(actonSettings()).Snapshot(context.Background(), "0:"+strings.Repeat("00", 32), &seqno)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ImplementationHash == nil || *snapshot.CodeHash != base64.StdEncoding.EncodeToString(code.Hash()) || *snapshot.ImplementationHash != base64.StdEncoding.EncodeToString(implementation) || *snapshot.CodeHash == *snapshot.ImplementationHash {
		t.Fatalf("library code identity lost: %+v", snapshot)
	}
	wire, err := actonapi.EncodeStandardStack([]tolkabi.StackValue{{Type: "cell", Value: boc}})
	if err != nil {
		t.Fatalf("opaque stack cell rejected: %v", err)
	}
	encoded, _ := json.Marshal(wire)
	if _, err := actonapi.DecodeStandardStack(encoded); err != nil {
		t.Fatalf("opaque stack result rejected: %v", err)
	}
}

func TestActonProxyDeadlineCoversChainAndBody(t *testing.T) {
	// fasthttp serves each request on its own goroutine, so the counter the test
	// asserts on must be written and read atomically.
	var calls atomic.Int32
	actonUpstream(t, func(c *fasthttp.RequestCtx) {
		if calls.Add(1) == 1 {
			c.SetBodyString(`{"ok":true,"result":{"last":{"seqno":1}}}`)
			return
		}
		// Outlive the executor's whole-chain deadline, not just this step's.
		time.Sleep(300 * time.Millisecond)
		c.SetBodyString(`{"ok":true,"result":{}}`)
	})
	settings := actonSettings()
	settings.Timeout = 50 * time.Millisecond
	_, err := NewActonExecutor(settings).Snapshot(context.Background(), "0:"+strings.Repeat("00", 32), nil)
	var apiError models.IndexError
	if !errors.As(err, &apiError) || apiError.Code != 504 || calls.Load() != 2 {
		t.Fatalf("chain deadline lost: calls=%d err=%v", calls.Load(), err)
	}
}
