package index

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ton-blockchain/acton/packages/abi-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/acton/catalog"
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
		MaxConnsPerHost:     v2MaxConnections,
		MaxConnWaitTimeout:  v2ConnectionWaitLimit,
		MaxIdleConnDuration: v2IdleConnectionLimit,
		MaxResponseBodySize: actonMaxUpstreamBytes,
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
	if snapshot.Seqno == nil || *snapshot.Seqno != 54321 || snapshot.Pinning != "upstream_seqno" {
		t.Fatalf("bad pinning: %+v", snapshot)
	}
	if snapshot.AccountStateHash != nil {
		t.Fatal("fabricated unavailable state hash")
	}
	if *snapshot.CodeHash != base64.StdEncoding.EncodeToString(code.Hash()) || *snapshot.DataHash != base64.StdEncoding.EncodeToString(data.Hash()) || *snapshot.LastTransactionLT != "9007199254740993" {
		t.Fatal("incorrect snapshot hashes or LT")
	}
	stack := []acton.StackValue{{Type: "tuple", Value: []acton.StackValue{{Type: "num", Value: json.Number("9007199254740993")}, {Type: "slice", Value: dataBOC}}}}
	result, err := executor.Run(context.Background(), snapshot, 76543, stack)
	if err != nil {
		t.Fatal(err)
	}
	if result.ExitCode != 1 || result.GasUsed != "9007199254740993" || result.Stack[0].Value != "9007199254740993" {
		t.Fatalf("lost exact VM results: %+v", result)
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
	if len(paths) != 1 || paths[0] != "/api/v2/getAddressInformation?123" || *snapshot.Seqno != seqno {
		t.Fatalf("historical selector not forwarded: %v %+v", paths, snapshot)
	}
}

func TestActonProxyPreservesUnsupportedAndFailedVMResults(t *testing.T) {
	actonUpstream(t, func(c *fasthttp.RequestCtx) {
		c.SetBodyString(`{"ok":true,"result":{"exit_code":11,"gas_used":"456","stack":[{"@type":"tvm.stackEntryUnsupported"}]}}`)
	})
	seqno := int32(42)
	result, err := NewActonExecutor(actonSettings()).Run(context.Background(), &actonapi.Snapshot{Address: "0:" + strings.Repeat("00", 32), Seqno: &seqno}, 76543, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.ExitCode != 11 || result.GasUsed != "456" || result.StackError == "" || !strings.Contains(string(result.RawStack), "Unsupported") {
		t.Fatalf("raw VM failure was lost: %+v", result)
	}
}

func TestActonProxyRejectsIncompatibleUpstreamWithoutFallback(t *testing.T) {
	for _, body := range []string{
		`not json`, `{"ok":false,"error":"private-key internal detail"}`, `{"ok":true,"result":null}`,
		`{"ok":true,"result":{"gas_used":1,"stack":[]}}`,
		`{"ok":true,"result":{"gas_used":1.5,"exit_code":0,"stack":[]}}`,
	} {
		t.Run(body, func(t *testing.T) {
			var mu sync.Mutex
			calls := 0
			actonUpstream(t, func(c *fasthttp.RequestCtx) {
				mu.Lock()
				calls++
				mu.Unlock()
				c.SetBodyString(body)
			})
			seqno := int32(1)
			_, err := NewActonExecutor(actonSettings()).Run(context.Background(), &actonapi.Snapshot{Address: "0:" + strings.Repeat("00", 32), Seqno: &seqno}, 76543, nil)
			var apiError models.IndexError
			if !errors.As(err, &apiError) || apiError.Code != 502 || strings.Contains(err.Error(), "private-key") {
				t.Fatalf("unexpected upstream error: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != 1 {
				t.Fatalf("unexpected fallback: %d requests", calls)
			}
		})
	}
}

func TestActonProxyRejectsInvalidStackBeforeRequest(t *testing.T) {
	seqno := int32(1)
	snapshot := &actonapi.Snapshot{Address: "0:" + strings.Repeat("00", 32), Seqno: &seqno}
	executor := NewActonExecutor(models.RequestSettings{})
	for _, stack := range [][]acton.StackValue{{{Type: "null", Value: "invalid"}}, {{Type: "num", Value: "1.5"}}, {{Type: "cell", Value: "bad"}}} {
		_, err := executor.Run(context.Background(), snapshot, 76543, stack)
		var apiError models.IndexError
		if !errors.As(err, &apiError) || apiError.Code != 422 {
			t.Fatalf("bad input reached transport: %v", err)
		}
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
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = NewActonExecutor(actonSettings()).Snapshot(ctx, "0:"+strings.Repeat("00", 32), &seqno)
	if !errors.As(err, &apiError) || apiError.Code != 504 {
		t.Fatalf("cancelled context allowed request: %v", err)
	}
}

func TestActonProxyLibraryReferenceSnapshot(t *testing.T) {
	contract := catalog.ByID("wallets/w4r2.WalletV4r2")
	implementation, err := hex.DecodeString(contract.CodeHashes[0])
	if err != nil {
		t.Fatal(err)
	}
	raw := append([]byte{0xb5, 0xee, 0x9c, 0x72, 1, 1, 1, 1, 0, 35, 0, 8, 66, 2}, implementation...)
	boc := base64.StdEncoding.EncodeToString(raw)
	code, err := acton.DecodeOpaqueBOC(boc)
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
	wire, err := actonapi.EncodeStandardStack([]acton.StackValue{{Type: "cell", Value: boc}})
	if err != nil {
		t.Fatalf("opaque stack cell rejected: %v", err)
	}
	encoded, _ := json.Marshal(wire)
	if _, err := actonapi.DecodeStandardStack(encoded); err != nil {
		t.Fatalf("opaque stack result rejected: %v", err)
	}
}

func TestActonProxyStandardNullAndBuilderRejection(t *testing.T) {
	boc := base64.StdEncoding.EncodeToString(cell.BeginCell().EndCell().ToBOC())
	var mu sync.Mutex
	var paths []string
	var received map[string]any
	actonUpstream(t, func(c *fasthttp.RequestCtx) {
		mu.Lock()
		paths = append(paths, string(c.Path()))
		_ = json.Unmarshal(c.PostBody(), &received)
		mu.Unlock()
		c.SetBodyString(`{"ok":true,"result":{"gas_used":1,"exit_code":0,"stack":[{"@type":"tvm.stackEntryList","list":{"@type":"tvm.list","elements":[]}}]}}`)
	})
	seqno := int32(42)
	snapshot := &actonapi.Snapshot{Address: "0:" + strings.Repeat("00", 32), Seqno: &seqno}
	stack := []acton.StackValue{{Type: "null"}}
	result, err := NewActonExecutor(actonSettings()).Run(context.Background(), snapshot, 123, stack)
	if err != nil {
		t.Fatal(err)
	}
	if result.StackError != "" || result.Stack[0].Type != "null" {
		t.Fatalf("standard null result lost: %+v", result)
	}
	_, err = NewActonExecutor(actonSettings()).Run(context.Background(), snapshot, 123, []acton.StackValue{{Type: "builder", Value: boc}})
	var apiError models.IndexError
	if !errors.As(err, &apiError) || apiError.Code != 422 {
		t.Fatalf("builder input not rejected: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 || paths[0] != "/api/v2/runGetMethodStd" || received["seqno"] != float64(42) {
		t.Fatalf("unexpected transport/selector: %v %+v", paths, received)
	}
	entry := received["stack"].([]any)[0].(map[string]any)
	if entry["@type"] != "tvm.stackEntryList" || len(entry["list"].(map[string]any)["elements"].([]any)) != 0 {
		t.Fatalf("null not encoded as empty Tonlib list: %+v", entry)
	}
}

func TestActonProxyBoundsHTTPStatusAndResponseBody(t *testing.T) {
	for _, tc := range []struct {
		name    string
		respond func(*fasthttp.RequestCtx)
	}{
		{"status_503_even_ok_true", func(c *fasthttp.RequestCtx) {
			c.SetStatusCode(503)
			c.SetBodyString(`{"ok":true,"result":{"gas_used":1,"exit_code":0,"stack":[]}}`)
		}},
		{"oversized_body", func(c *fasthttp.RequestCtx) {
			c.SetBodyString(strings.Repeat(" ", actonMaxUpstreamBytes+1))
		}},
		{"redirect_not_followed", func(c *fasthttp.RequestCtx) {
			c.Response.Header.Set("Location", "http://must-not-follow.invalid/private-key")
			c.SetStatusCode(302)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			actonUpstream(t, func(c *fasthttp.RequestCtx) { calls++; tc.respond(c) })
			seqno := int32(1)
			result, err := NewActonExecutor(actonSettings()).Run(context.Background(), &actonapi.Snapshot{Address: "0:" + strings.Repeat("00", 32), Seqno: &seqno}, 85143, nil)
			var apiError models.IndexError
			if result != nil || !errors.As(err, &apiError) || apiError.Code != 502 {
				t.Fatalf("bad upstream not rejected: %v", err)
			}
			if strings.Contains(err.Error(), "private-key") || strings.Contains(err.Error(), "v2.test") {
				t.Fatalf("upstream detail leaked into error: %v", err)
			}
			if calls != 1 {
				t.Fatalf("retried or fell back: calls=%d", calls)
			}
		})
	}
}

func TestActonProxyPositiveSeqnoBeforeUpstream(t *testing.T) {
	for _, seqno := range []int32{0, -1} {
		executor := NewActonExecutor(models.RequestSettings{})
		addr := "0:" + strings.Repeat("00", 32)
		_, snapshotErr := executor.Snapshot(context.Background(), addr, &seqno)
		_, runErr := executor.Run(context.Background(), &actonapi.Snapshot{Address: addr, Seqno: &seqno}, 85143, nil)
		for _, err := range []error{snapshotErr, runErr} {
			var apiError models.IndexError
			if !errors.As(err, &apiError) || apiError.Code != 422 {
				t.Fatalf("seqno=%d yielded %v, not client validation", seqno, err)
			}
		}
	}
}

func TestActonProxyDeadlineCoversChainAndBody(t *testing.T) {
	calls := 0
	actonUpstream(t, func(c *fasthttp.RequestCtx) {
		calls++
		if calls == 1 {
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
	if !errors.As(err, &apiError) || apiError.Code != 504 || calls != 2 {
		t.Fatalf("chain deadline lost: calls=%d err=%v", calls, err)
	}
}
