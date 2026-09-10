package index

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ton-blockchain/acton/packages/abi-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/actonapi"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/valyala/fasthttp"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const actonMaxUpstreamBytes = 4 << 20

// Acton getters use their own pool so a slow upstream cannot starve the legacy
// v2 routes, but the same tuning and a hard response-body cap.
var actonV2HTTPClient = &fasthttp.Client{
	MaxConnsPerHost:     v2MaxConnections,
	MaxConnWaitTimeout:  v2ConnectionWaitLimit,
	MaxIdleConnDuration: v2IdleConnectionLimit,
	MaxResponseBodySize: actonMaxUpstreamBytes,
}

type actonExecutor struct {
	settings models.RequestSettings
	deadline time.Time
}

// NewActonExecutor pins one deadline across discovery, state loading and
// execution, so a chain of upstream calls cannot outlive the client's request.
func NewActonExecutor(settings models.RequestSettings) actonapi.GetterExecutor {
	timeout := settings.Timeout
	if timeout <= 0 || timeout > 3*time.Second {
		timeout = 3 * time.Second
	}
	return &actonExecutor{settings: settings, deadline: time.Now().Add(timeout)}
}

func (e *actonExecutor) request(ctx context.Context, method, endpoint string, query url.Values, payload any, result any) error {
	requestURL, err := v2RequestURL(e.settings, endpoint, query)
	if err != nil {
		return actonapi.Fail(503, "configured v2 endpoint is unavailable")
	}
	var data []byte
	if payload != nil {
		if data, err = json.Marshal(payload); err != nil || len(data) > actonapi.MaxBodyBytes {
			return actonapi.Fail(422, "invalid or oversized v2 request")
		}
	}
	deadline := e.deadline
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if ctx.Err() != nil || time.Until(deadline) <= 0 {
		return actonapi.Fail(504, "Acton execution deadline exceeded")
	}

	req, resp := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.Header.SetMethod(method)
	req.SetRequestURI(requestURL)
	if payload != nil {
		req.Header.SetContentType("application/json")
		req.SetBody(data)
	}
	// Do (unlike DoRedirects) never follows redirects, and the client's
	// MaxResponseBodySize bounds the body for chunked responses too.
	if err := actonV2HTTPClient.DoDeadline(req, resp, deadline); err != nil {
		if ctx.Err() != nil || time.Until(deadline) <= 0 {
			return actonapi.Fail(504, "Acton upstream timeout")
		}
		return actonapi.Fail(502, "Acton v2 upstream request failed")
	}
	if status := resp.StatusCode(); status < 200 || status >= 300 {
		return actonapi.Fail(502, fmt.Sprintf("Acton v2 %s returned HTTP %d; no transport fallback", endpoint, status))
	}
	var envelope struct {
		OK     bool            `json:"ok"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(resp.Body(), &envelope); err != nil || !envelope.OK || len(envelope.Result) == 0 || bytes.Equal(envelope.Result, []byte("null")) {
		// Do not relay upstream error text, which can contain credentials or URLs.
		return actonapi.Fail(502, "v2 "+endpoint+" failed or returned an incompatible response; no legacy fallback")
	}
	d := json.NewDecoder(bytes.NewReader(envelope.Result))
	d.UseNumber()
	if err := d.Decode(result); err != nil {
		return actonapi.Fail(502, "invalid v2 "+endpoint+" result")
	}
	return nil
}

func (e *actonExecutor) Snapshot(ctx context.Context, address string, seqno *int32) (*actonapi.Snapshot, error) {
	canonical, err := actonapi.CanonicalAddress(address)
	if err != nil {
		return nil, err
	}
	var pinned int32
	if seqno == nil {
		var info struct {
			Last struct {
				Seqno *int32 `json:"seqno"`
			} `json:"last"`
		}
		if err := e.request(ctx, "GET", "getMasterchainInfo", nil, nil, &info); err != nil {
			return nil, err
		}
		if info.Last.Seqno == nil || *info.Last.Seqno <= 0 {
			return nil, actonapi.Fail(502, "upstream did not return a masterchain seqno")
		}
		pinned = *info.Last.Seqno
	} else {
		if *seqno <= 0 {
			return nil, actonapi.Fail(422, "seqno must be positive")
		}
		pinned = *seqno
	}
	var state struct {
		Code              string          `json:"code"`
		Data              string          `json:"data"`
		State             string          `json:"state"`
		BlockID           json.RawMessage `json:"block_id"`
		LastTransactionID struct {
			Hash *string `json:"hash"`
			LT   any     `json:"lt"`
		} `json:"last_transaction_id"`
	}
	query := url.Values{"address": {canonical}, "seqno": {strconv.FormatInt(int64(pinned), 10)}}
	if err := e.request(ctx, "GET", "getAddressInformation", query, nil, &state); err != nil {
		return nil, err
	}
	if state.State == "" {
		return nil, actonapi.Fail(502, "missing upstream account status")
	}
	if state.State != "active" || state.Code == "" {
		return nil, actonapi.Fail(409, "account has no active code at execution seqno")
	}
	if len(state.Code) > actonapi.MaxBodyBytes || len(state.Data) > actonapi.MaxBodyBytes {
		return nil, actonapi.Fail(502, "upstream account BOC exceeds size limit")
	}
	code, err := acton.DecodeOpaqueBOC(state.Code)
	if err != nil {
		return nil, actonapi.Fail(502, "invalid upstream account code BOC")
	}
	hash := base64.StdEncoding.EncodeToString(code.Hash())
	snapshot := &actonapi.Snapshot{Address: canonical, AccountStatus: state.State, CodeHash: &hash, Seqno: &pinned, BlockID: state.BlockID, LastTransactionHash: state.LastTransactionID.Hash, Pinning: "upstream_seqno"}
	if code.GetType() == cell.LibraryCellType {
		// ActonScan codeCell.ts uses the embedded hash for catalog lookup, but
		// it is not the account's code-cell hash. Preserve both identities.
		slice := code.BeginParse()
		if _, err := slice.LoadUInt(8); err != nil {
			return nil, actonapi.Fail(502, "invalid library reference")
		}
		implementation, err := slice.LoadSlice(256)
		if err != nil {
			return nil, actonapi.Fail(502, "invalid library reference hash")
		}
		snapshot.ImplementationHash = new(base64.StdEncoding.EncodeToString(implementation))
	}
	if state.LastTransactionID.LT != nil {
		lt, err := actonapi.Decimal(state.LastTransactionID.LT)
		if err != nil || strings.HasPrefix(lt, "-") {
			return nil, actonapi.Fail(502, "invalid upstream last transaction LT")
		}
		snapshot.LastTransactionLT = &lt
	}
	if state.Data != "" {
		data, err := acton.DecodeOpaqueBOC(state.Data)
		if err != nil {
			return nil, actonapi.Fail(502, "invalid upstream account data BOC")
		}
		hash := base64.StdEncoding.EncodeToString(data.Hash())
		snapshot.DataHash = &hash
	}
	// raw.fullAccountState has no account-state hash; never synthesize one from
	// code+data. block_id may be a shard block, so it is not compared to MC seqno.
	return snapshot, nil
}

func (e *actonExecutor) Run(ctx context.Context, snapshot *actonapi.Snapshot, method int64, stack []acton.StackValue) (*actonapi.Execution, error) {
	if snapshot == nil || snapshot.Seqno == nil || *snapshot.Seqno <= 0 {
		return nil, actonapi.Fail(422, "pinned snapshot with positive seqno is required")
	}
	address, err := actonapi.CanonicalAddress(snapshot.Address)
	if err != nil {
		return nil, err
	}
	if method < math.MinInt32 || method > math.MaxInt32 {
		return nil, actonapi.Fail(422, "method ID must be an int32")
	}
	endpoint := "runGetMethodStd"
	wire, err := actonapi.EncodeStandardStack(stack)
	if err != nil {
		return nil, actonapi.Fail(422, err.Error())
	}
	request := struct {
		Address string `json:"address"`
		Method  int64  `json:"method"`
		Seqno   int32  `json:"seqno"`
		Stack   []any  `json:"stack"`
	}{address, method, *snapshot.Seqno, wire}
	var response struct {
		GasUsed  any             `json:"gas_used"`
		ExitCode *int32          `json:"exit_code"`
		Stack    json.RawMessage `json:"stack"`
	}
	if err := e.request(ctx, "POST", endpoint, nil, request, &response); err != nil {
		return nil, err
	}
	if response.ExitCode == nil || len(response.Stack) == 0 {
		return nil, actonapi.Fail(502, "incomplete "+endpoint+" result")
	}
	gas, err := actonapi.Decimal(response.GasUsed)
	if err != nil || strings.HasPrefix(gas, "-") {
		return nil, actonapi.Fail(502, "invalid "+endpoint+" gas_used")
	}
	result := &actonapi.Execution{GasUsed: gas, ExitCode: *response.ExitCode, RawStack: response.Stack}
	result.Stack, err = actonapi.DecodeStandardStack(response.Stack)
	if err != nil {
		result.StackError = err.Error()
	}
	return result, nil
}
