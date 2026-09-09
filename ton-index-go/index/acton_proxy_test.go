package index

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
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
	client := newActonHTTPClient()
	client.Transport.(*http.Transport).DialContext = func(context.Context, string, string) (net.Conn, error) { return listener.Dial() }
	previous := actonHTTPClient
	actonHTTPClient = client
	t.Cleanup(func() {
		actonHTTPClient = previous
		client.CloseIdleConnections()
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
	if snapshot.Seqno == nil || *snapshot.Seqno != 54321 || snapshot.Pinning != "upstream_seqno" || snapshot.ProofVerified {
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
			var apiError *actonapi.Error
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
		var apiError *actonapi.Error
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
	var apiError *actonapi.Error
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
	if result.StackError != "" || result.Transport != "standard" || result.Stack[0].Type != "null" {
		t.Fatalf("standard null result lost: %+v", result)
	}
	_, err = NewActonExecutor(actonSettings()).Run(context.Background(), snapshot, 123, []acton.StackValue{{Type: "builder", Value: boc}})
	var apiError *actonapi.Error
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

type actonRoundTripFunc func(*http.Request) (*http.Response, error)

func (f actonRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type actonCountingBody struct {
	bytes  int
	closed bool
}

func (b *actonCountingBody) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = ' '
	}
	b.bytes += len(p)
	return len(p), nil
}
func (b *actonCountingBody) Close() error { b.closed = true; return nil }

func TestActonProxyBoundsHTTPStatusAndResponseBody(t *testing.T) {
	for _, tc := range []struct {
		name          string
		status        int
		contentLength int64
		maxRead       int
	}{
		{"status_503_even_ok_true", 503, -1, 0},
		{"known_oversized", 200, actonMaxUpstreamBytes + 1, 0},
		{"unknown_length_bounded", 200, -1, actonMaxUpstreamBytes + 1},
		{"redirect_not_followed", 302, -1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := actonHTTPClient
			defer func() { actonHTTPClient = previous }()
			body := &actonCountingBody{}
			calls := 0
			actonHTTPClient = newActonHTTPClient()
			actonHTTPClient.Transport = actonRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				var reader io.ReadCloser = body
				if tc.status == 503 {
					reader = io.NopCloser(strings.NewReader(`{"ok":true,"result":{"gas_used":1,"exit_code":0,"stack":[]}}`))
				}
				return &http.Response{StatusCode: tc.status, Body: reader, ContentLength: tc.contentLength, Header: http.Header{"Location": []string{"http://must-not-follow.invalid/private-key"}}, Request: r}, nil
			})
			seqno := int32(1)
			result, err := NewActonExecutor(actonSettings()).Run(context.Background(), &actonapi.Snapshot{Address: "0:" + strings.Repeat("00", 32), Seqno: &seqno}, 85143, nil)
			var apiError *actonapi.Error
			if result != nil || !errors.As(err, &apiError) || apiError.Code != 502 || strings.Contains(err.Error(), "private-key") || strings.Contains(err.Error(), "v2.test") {
				t.Fatalf("bad upstream failure: %v", err)
			}
			if calls != 1 || body.bytes > tc.maxRead {
				t.Fatalf("unbounded body or fallback: reads=%d calls=%d", body.bytes, calls)
			}
			if tc.status != 503 && !body.closed {
				t.Fatal("upstream body not closed")
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
			var apiError *actonapi.Error
			if !errors.As(err, &apiError) || apiError.Code != 422 {
				t.Fatalf("seqno=%d yielded %v, not client validation", seqno, err)
			}
		}
	}
}

func TestActonProxyReusesIsolatedPool(t *testing.T) {
	actonUpstream(t, func(c *fasthttp.RequestCtx) {
		c.SetBodyString(`{"ok":true,"result":{"gas_used":1,"exit_code":0,"stack":[]}}`)
	})
	transport := actonHTTPClient.Transport.(*http.Transport)
	dial := transport.DialContext
	var connections atomic.Int32
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		connections.Add(1)
		return dial(ctx, network, addr)
	}
	seqno := int32(1)
	for i := 0; i < 2; i++ {
		_, err := NewActonExecutor(actonSettings()).Run(context.Background(), &actonapi.Snapshot{Address: "0:" + strings.Repeat("00", 32), Seqno: &seqno}, 85143, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if connections.Load() != 1 {
		t.Fatalf("connection pool not reused: %d", connections.Load())
	}
}

type actonDeadlineBody struct {
	ctx    context.Context
	closed bool
}

func (b *actonDeadlineBody) Read([]byte) (int, error) { <-b.ctx.Done(); return 0, b.ctx.Err() }
func (b *actonDeadlineBody) Close() error             { b.closed = true; return nil }

func TestActonProxyDeadlineCoversChainAndBody(t *testing.T) {
	previous := actonHTTPClient
	defer func() { actonHTTPClient = previous }()
	settings := actonSettings()
	settings.Timeout = 50 * time.Millisecond
	executor := NewActonExecutor(settings).(*actonExecutor)
	var body *actonDeadlineBody
	calls := 0
	actonHTTPClient = &http.Client{Transport: actonRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		deadline, ok := r.Context().Deadline()
		if !ok || !deadline.Equal(executor.deadline) {
			t.Fatal("per-step request reset the chain deadline")
		}
		if calls == 1 {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":{"last":{"seqno":1}}}`)), Header: http.Header{}}, nil
		}
		body = &actonDeadlineBody{ctx: r.Context()}
		return &http.Response{StatusCode: 200, Body: body, Header: http.Header{}}, nil
	})}
	_, err := executor.Snapshot(context.Background(), "0:"+strings.Repeat("00", 32), nil)
	var apiError *actonapi.Error
	if !errors.As(err, &apiError) || apiError.Code != 504 || calls != 2 || body == nil || !body.closed {
		t.Fatalf("body timeout lost: calls=%d body=%+v err=%v", calls, body, err)
	}
}

// Opt-in public read-only smoke; ordinary tests never access the network. Each
// complete state/execution chain retains the production three-second deadline.
func TestActonLiveReadOnlySmoke(t *testing.T) {
	if os.Getenv("ACTON_LIVE_SMOKE") != "1" {
		t.Skip("set ACTON_LIVE_SMOKE=1 for public read-only smoke")
	}
	for _, tc := range []struct {
		contract, method string
		seqno            int32
	}{
		{"system.Elector", "active_election_id", 91668427},
		{"wallets/w4r2.WalletV4r2", "get_plugin_list", 0},
	} {
		t.Run(tc.contract, func(t *testing.T) {
			contract := catalog.ByID(tc.contract)
			if contract == nil || len(contract.KnownAddresses) == 0 {
				t.Fatal("missing real catalog fixture")
			}
			var method *acton.GetMethod
			for i := range contract.GetMethods {
				if contract.GetMethods[i].Name == tc.method {
					method = &contract.GetMethods[i]
					break
				}
			}
			if method == nil {
				t.Fatal("missing real catalog getter")
			}
			executor := NewActonExecutor(models.RequestSettings{V2Endpoint: "https://toncenter.com/api/v2", Timeout: 3 * time.Second})
			var seqno *int32
			if tc.seqno > 0 {
				seqno = &tc.seqno
			}
			snapshot, err := executor.Snapshot(context.Background(), contract.KnownAddresses[0], seqno)
			if err != nil {
				t.Skipf("public snapshot unavailable within bounded chain: %v", err)
			}
			matched := false
			for _, hash := range []*string{snapshot.CodeHash, snapshot.ImplementationHash} {
				if hash == nil {
					continue
				}
				for _, candidate := range catalog.ByCodeHash(*hash) {
					if candidate == contract {
						matched = true
					}
				}
			}
			if !matched {
				t.Skip("live code no longer matches this catalog fixture; refusing unrelated decoding")
			}
			stack, err := method.EncodeArgs(map[string]any{})
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.Run(context.Background(), snapshot, method.ID, stack)
			if err != nil {
				t.Skipf("public execution unavailable within bounded chain: %v", err)
			}
			if result.ExitCode != 0 && result.ExitCode != 1 {
				t.Fatalf("VM exit %d, gas=%s", result.ExitCode, result.GasUsed)
			}
			if result.StackError != "" {
				t.Fatal(result.StackError)
			}
			decoded, err := method.DecodeResult(result.Stack)
			if err != nil {
				t.Fatal(err)
			}
			if tc.contract == "system.Elector" && decoded != "0" {
				t.Fatalf("historical election id: %#v", decoded)
			}
			if list, ok := decoded.([]any); ok {
				decoded = fmt.Sprintf("list length %d", len(list))
			}
			t.Logf("%s/%s seqno=%d exit=%d gas=%s decoded=%v", tc.contract, tc.method, *snapshot.Seqno, result.ExitCode, result.GasUsed, decoded)
		})
	}
}
