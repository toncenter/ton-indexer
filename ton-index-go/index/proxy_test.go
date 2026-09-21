package index

import (
	"encoding/json"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/index/models"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

func TestV2HTTPClientHasBoundedConnectionPool(t *testing.T) {
	client := newV2HTTPClient()
	if client.MaxConnsPerHost != v2MaxConnections {
		t.Fatalf("connection limit = %d, want %d", client.MaxConnsPerHost, v2MaxConnections)
	}
	if client.MaxConnWaitTimeout != v2ConnectionWaitLimit {
		t.Fatalf("connection wait limit = %s, want %s", client.MaxConnWaitTimeout, v2ConnectionWaitLimit)
	}
}

func TestV2HTTPClientReusesConnection(t *testing.T) {
	var openedConnections atomic.Int32
	listener := fasthttputil.NewInmemoryListener()
	server := &fasthttp.Server{Handler: func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("ok")
	}}
	serverStopped := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(serverStopped)
	}()

	client := newV2HTTPClient()
	client.Dial = func(string) (net.Conn, error) {
		openedConnections.Add(1)
		return listener.Dial()
	}
	defer func() {
		client.CloseIdleConnections()
		_ = listener.Close()
		<-serverStopped
	}()

	for i := 0; i < 2; i++ {
		body, err := executeV2Request(client, fasthttp.MethodGet, "http://api-v2.test/", nil, time.Second)
		if err != nil {
			t.Fatalf("request %d failed: %v", i+1, err)
		}
		if string(body) != "ok" {
			t.Fatalf("request %d returned %q, want %q", i+1, body, "ok")
		}
	}

	if got := openedConnections.Load(); got != 1 {
		t.Fatalf("opened %d connections for two sequential requests, want 1", got)
	}
}

// entries come from an upstream response, so an unexpected shape has to reach
// the handler as an error rather than take the worker down with it.
func TestDecodeStackRejectsMalformedUpstreamShapes(t *testing.T) {
	deep := `{"@type":"tvm.stackEntryTuple","tuple":{"@type":"tvm.tuple","elements":[`
	for i := 0; i < maxStackDecodeDepth+8; i++ {
		deep += `{"@type":"tvm.stackEntryTuple","tuple":{"@type":"tvm.tuple","elements":[`
	}
	deep += `]}}` + strings.Repeat(`]}}`, maxStackDecodeDepth+8)
	for name, body := range map[string]string{
		"not_an_array":        `{"stack":[]}`,
		"null_entry":          `[null]`,
		"empty_legacy_pair":   `[[]]`,
		"short_legacy_pair":   `[["num"]]`,
		"numeric_legacy_type": `[[7,"0x1"]]`,
		"legacy_cell_scalar":  `[["cell","not-an-object"]]`,
		"legacy_tuple_scalar": `[["tuple",{"elements":"not-an-array"}]]`,
		"missing_marker":      `[{}]`,
		"numeric_marker":      `[{"@type":7}]`,
		"number_scalar":       `[{"@type":"tvm.stackEntryNumber","number":"not-an-object"}]`,
		"number_not_string":   `[{"@type":"tvm.stackEntryNumber","number":{"number":7}}]`,
		"number_not_decimal":  `[{"@type":"tvm.stackEntryNumber","number":{"number":"zz"}}]`,
		"cell_without_bytes":  `[{"@type":"tvm.stackEntryCell","cell":{}}]`,
		"tuple_scalar":        `[{"@type":"tvm.stackEntryTuple","tuple":{"elements":"not-an-array"}}]`,
		"unknown_marker":      `[{"@type":"tvm.stackEntryUnsupported"}]`,
		"nested_past_bound":   `[` + deep + `]`,
	} {
		t.Run(name, func(t *testing.T) {
			var upstream any
			if err := json.Unmarshal([]byte(body), &upstream); err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeStack(upstream); err == nil {
				t.Fatal("malformed upstream stack accepted")
			}
		})
	}
}

// both spellings the upstream sends must decode, with hexadecimal integers.
func TestDecodeStackReadsBothUpstreamSpellings(t *testing.T) {
	for name, body := range map[string]string{
		"legacy":   `[["num","0x2a"],["cell",{"bytes":"te6cckEBAQEAAgAAAEysuc0="}],["tuple",{"elements":[["num","0x1"]]}]]`,
		"standard": `[{"@type":"tvm.stackEntryNumber","number":{"@type":"tvm.numberDecimal","number":"42"}},{"@type":"tvm.stackEntryCell","cell":{"@type":"tvm.cell","bytes":"te6cckEBAQEAAgAAAEysuc0="}},{"@type":"tvm.stackEntryTuple","tuple":{"@type":"tvm.tuple","elements":[{"@type":"tvm.stackEntryNumber","number":{"@type":"tvm.numberDecimal","number":"1"}}]}}]`,
	} {
		t.Run(name, func(t *testing.T) {
			var upstream any
			if err := json.Unmarshal([]byte(body), &upstream); err != nil {
				t.Fatal(err)
			}
			stack, err := DecodeStack(upstream)
			if err != nil {
				t.Fatal(err)
			}
			if len(stack) != 3 || stack[0].Type != "num" || stack[0].Value != "0x2a" || stack[1].Type != "cell" {
				t.Fatalf("unexpected stack: %+v", stack)
			}
			nested, ok := stack[2].Value.([]interface{})
			if !ok || len(nested) != 1 || nested[0].(models.V2StackEntity).Value != "0x1" {
				t.Fatalf("nested entry lost: %+v", stack[2])
			}
		})
	}
}
