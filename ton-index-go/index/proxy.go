package index

import (
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/valyala/fasthttp"
)

const (
	v2MaxConnections      = 256
	v2ConnectionWaitLimit = 3 * time.Second
	v2IdleConnectionLimit = 30 * time.Second
)

var v2HTTPClient = newV2HTTPClient()

func newV2HTTPClient() *fasthttp.Client {
	return &fasthttp.Client{
		MaxConnsPerHost:     v2MaxConnections,
		MaxConnWaitTimeout:  v2ConnectionWaitLimit,
		MaxIdleConnDuration: v2IdleConnectionLimit,
	}
}

func doV2Request(method string, requestURL string, requestBody []byte, timeout time.Duration) ([]byte, error) {
	return executeV2Request(v2HTTPClient, method, requestURL, requestBody, timeout)
}

// v2RequestURL builds an upstream v2 URL, appending the configured API key.
func v2RequestURL(settings models.RequestSettings, endpoint string, params url.Values) (string, error) {
	if len(settings.V2Endpoint) == 0 {
		return "", models.IndexError{Code: 500, Message: "ton-http-api endpoint is not specified"}
	}
	baseUrl, err := url.Parse(settings.V2Endpoint)
	if err != nil {
		return "", models.IndexError{Code: 500, Message: err.Error()}
	}
	baseUrl.Path = strings.TrimRight(baseUrl.Path, "/") + "/" + endpoint
	if params == nil {
		params = url.Values{}
	}
	if len(settings.V2ApiKey) > 0 {
		params.Set("api_key", settings.V2ApiKey)
	}
	baseUrl.RawQuery, baseUrl.Fragment = params.Encode(), ""
	return baseUrl.String(), nil
}

func executeV2Request(client *fasthttp.Client, method string, requestURL string, requestBody []byte, timeout time.Duration) ([]byte, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethod(method)
	req.SetRequestURI(requestURL)
	if requestBody != nil {
		req.Header.SetContentType("application/json")
		req.SetBody(requestBody)
	}

	var err error
	if timeout > 0 {
		err = client.DoTimeout(req, resp, timeout)
	} else {
		err = client.Do(req, resp)
	}
	if err != nil {
		return nil, err
	}

	return append([]byte(nil), resp.Body()...), nil
}

func GetV2AddressInformation(state_req models.V2AccountRequest, settings models.RequestSettings) (*models.V2AddressInformation, error) {
	if len(settings.V2Endpoint) == 0 {
		return nil, models.IndexError{Code: 500, Message: "ton-http-api endpoint is not specified"}
	}

	baseUrl, err := url.Parse(settings.V2Endpoint)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	baseUrl.Path += "/getAddressInformation"
	params := url.Values{}
	params.Add("address", string(state_req.AccountAddress))
	if len(settings.V2ApiKey) > 0 {
		params.Add("api_key", settings.V2ApiKey)
	}
	baseUrl.RawQuery = params.Encode()
	body, err := doV2Request(fasthttp.MethodGet, baseUrl.String(), nil, settings.Timeout)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	var jsn map[string]interface{}
	if err = json.Unmarshal(body, &jsn); err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	if jsn["ok"] != true {
		return nil, models.IndexError{Code: 500, Message: fmt.Sprintf("%v", jsn["error"])}
	}

	res := jsn["result"].(map[string]interface{})

	var acc models.V2AddressInformation
	switch v := res["balance"].(type) {
	case float64:
		acc.Balance = fmt.Sprintf("%d", int64(v))
	case string:
		acc.Balance = v
	default:
		return nil, models.IndexError{Code: 500, Message: fmt.Sprintf("failed to parse balance of type: %v", v)}
	}
	if v := res["code"].(string); len(v) > 0 {
		acc.Code = new(string)
		*acc.Code = v
	}
	if v := res["data"].(string); len(v) > 0 {
		acc.Data = new(string)
		*acc.Data = v
	}

	last_trans := res["last_transaction_id"].(map[string]interface{})
	if v := last_trans["lt"].(string); len(v) > 0 {
		acc.LastTransactionLt = new(string)
		*acc.LastTransactionLt = v
	}
	if v := last_trans["hash"].(string); len(v) > 0 {
		acc.LastTransactionHash = new(string)
		*acc.LastTransactionHash = v
	}
	if v := res["frozen_hash"].(string); len(v) > 0 {
		acc.FrozenHash = new(string)
		*acc.FrozenHash = v
	}
	acc.Status = res["state"].(string)

	return &acc, nil
}

func GetV2WalletInformation(state_req models.V2AccountRequest, settings models.RequestSettings) (*models.V2WalletInformation, error) {
	if len(settings.V2Endpoint) == 0 {
		return nil, models.IndexError{Code: 500, Message: "ton-http-api endpoint is not specified"}
	}

	baseUrl, err := url.Parse(settings.V2Endpoint)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	baseUrl.Path += "/getWalletInformation"
	params := url.Values{}
	params.Add("address", string(state_req.AccountAddress))
	if len(settings.V2ApiKey) > 0 {
		params.Add("api_key", settings.V2ApiKey)
	}
	baseUrl.RawQuery = params.Encode()
	body, err := doV2Request(fasthttp.MethodGet, baseUrl.String(), nil, settings.Timeout)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	var jsn map[string]interface{}
	if err = json.Unmarshal(body, &jsn); err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	if jsn["ok"] != true {
		return nil, models.IndexError{Code: 500, Message: fmt.Sprintf("%v", jsn["error"])}
	}

	res := jsn["result"].(map[string]interface{})

	if res["wallet"] != true && res["account_state"].(string) != "uninitialized" {
		return nil, models.IndexError{Code: 409, Message: "not a wallet"}
	}

	var acc models.V2WalletInformation
	switch v := res["balance"].(type) {
	case string:
		acc.Balance = res["balance"].(string)
	case float64:
		acc.Balance = fmt.Sprintf("%d", int64(res["balance"].(float64)))
	default:
		return nil, models.IndexError{Code: 500, Message: fmt.Sprintf("failed to parse balance of type %v", v)}
	}

	if v, ok := res["wallet_type"]; ok && len(v.(string)) > 0 {
		acc.WalletType = new(string)
		*acc.WalletType = v.(string)
	}
	if v, ok := res["wallet_id"]; ok {
		acc.WalletId = new(int64)
		*acc.WalletId = int64(v.(float64))
	}
	if v, ok := res["seqno"]; ok {
		acc.Seqno = new(int64)
		*acc.Seqno = int64(v.(float64))
	}

	last_trans := res["last_transaction_id"].(map[string]interface{})
	if v := last_trans["lt"].(string); len(v) > 0 {
		acc.LastTransactionLt = v
	}
	if v := last_trans["hash"].(string); len(v) > 0 {
		acc.LastTransactionHash = v
	}
	acc.Status = res["account_state"].(string)

	return &acc, nil
}

func PostMessage(req models.V2SendMessageRequest, settings models.RequestSettings) (*models.V2SendMessageResult, error) {
	if len(settings.V2Endpoint) == 0 {
		return nil, models.IndexError{Code: 500, Message: "ton-http-api endpoint is not specified"}
	}

	baseUrl, err := url.Parse(settings.V2Endpoint)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	baseUrl.Path += "/sendBocReturnHash"
	params := url.Values{}
	if len(settings.V2ApiKey) > 0 {
		params.Add("api_key", settings.V2ApiKey)
	}
	baseUrl.RawQuery = params.Encode()
	var req_body []byte
	if req_body, err = json.Marshal(req); err != nil {
		return nil, models.IndexError{Code: 500, Message: fmt.Sprintf("failed to send request: %s", err.Error())}
	}
	body, err := doV2Request(fasthttp.MethodPost, baseUrl.String(), req_body, settings.Timeout)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	var jsn map[string]interface{}
	if err = json.Unmarshal(body, &jsn); err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	if jsn["ok"] != true {
		code := 500
		switch val := jsn["code"].(type) {
		case int:
			code = val
		case float64:
			code = int(val)
		default:
			log.Printf("unexpected type: '%v' value: '%v'", reflect.TypeOf(jsn["code"]), jsn["code"])
		}
		return nil, models.IndexError{Code: code, Message: fmt.Sprintf("%v", jsn["error"])}
	}
	res := jsn["result"].(map[string]interface{})

	var result models.V2SendMessageResult

	result.MessageHash = new(models.HashType)
	*result.MessageHash = models.HashType(res["hash"].(string))
	if v, ok := res["hash_norm"]; ok {
		result.MessageHashNorm = new(models.HashType)
		*result.MessageHashNorm = models.HashType(v.(string))
	}
	return &result, nil
}

func PostEstimateFee(req models.V2EstimateFeeRequest, settings models.RequestSettings) (*models.V2EstimateFeeResult, error) {
	if len(settings.V2Endpoint) == 0 {
		return nil, models.IndexError{Code: 500, Message: "ton-http-api endpoint is not specified"}
	}

	baseUrl, err := url.Parse(settings.V2Endpoint)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	baseUrl.Path += "/estimateFee"
	params := url.Values{}
	if len(settings.V2ApiKey) > 0 {
		params.Add("api_key", settings.V2ApiKey)
	}
	baseUrl.RawQuery = params.Encode()
	var req_body []byte
	if req_body, err = json.Marshal(req); err != nil {
		return nil, models.IndexError{Code: 500, Message: fmt.Sprintf("failed to send request: %s", err.Error())}
	}
	body, err := doV2Request(fasthttp.MethodPost, baseUrl.String(), req_body, settings.Timeout)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	var resp_full struct {
		Ok     bool                       `json:"ok"`
		Result models.V2EstimateFeeResult `json:"result"`
		Error  string                     `json:"error"`
		Code   int                        `json:"code"`
	}
	if err = json.Unmarshal(body, &resp_full); err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	if !resp_full.Ok {
		return nil, models.IndexError{Code: resp_full.Code, Message: resp_full.Error}
	}
	return &resp_full.Result, nil
}

func PostRunGetMethod(req models.V2RunGetMethodRequest, settings models.RequestSettings) (*models.V2RunGetMethodResult, error) {
	if len(settings.V2Endpoint) == 0 {
		return nil, models.IndexError{Code: 500, Message: "ton-http-api endpoint is not specified"}
	}

	baseUrl, err := url.Parse(settings.V2Endpoint)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	baseUrl.Path += "/runGetMethod"
	params := url.Values{}
	if len(settings.V2ApiKey) > 0 {
		params.Add("api_key", settings.V2ApiKey)
	}
	baseUrl.RawQuery = params.Encode()
	var requestBody []byte
	{
		body := make(map[string]interface{})
		body["address"] = string(req.Address)
		body["method"] = req.Method

		stack := [][]interface{}{}
		for _, v := range req.Stack {
			vv := []interface{}{}
			switch v.Type {
			case "num":
				vv = append(vv, "num")
			case "cell":
				vv = append(vv, "tvm.Cell")
			case "slice":
				vv = append(vv, "tvm.Slice")
			default:
				return nil, models.IndexError{Code: 500, Message: fmt.Sprintf("unsupported stack parameter type: %s", v.Type)}
			}
			vv = append(vv, v.Value)
			stack = append(stack, vv)
		}
		body["stack"] = stack

		if requestBody, err = json.Marshal(body); err != nil {
			return nil, models.IndexError{Code: 500, Message: fmt.Sprintf("failed to send request: %s", err.Error())}
		}
	}
	body, err := doV2Request(fasthttp.MethodPost, baseUrl.String(), requestBody, settings.Timeout)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	var jsn map[string]interface{}
	if err = json.Unmarshal(body, &jsn); err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	if jsn["ok"] != true {
		return nil, models.IndexError{Code: 500, Message: fmt.Sprintf("%v", jsn["error"])}
	}
	res := jsn["result"].(map[string]interface{})

	// log.Println(res)
	var result models.V2RunGetMethodResult
	switch v := res["gas_used"].(type) {
	case float64:
		result.GasUsed = int64(v)
	case int64:
		result.GasUsed = v
	default:
		return nil, models.IndexError{Code: 501, Message: fmt.Sprintf("Unknown type of gas_used: %s", v)}
	}
	switch v := res["exit_code"].(type) {
	case float64:
		result.ExitCode = int64(v)
	case int64:
		result.ExitCode = v
	default:
		return nil, models.IndexError{Code: 501, Message: fmt.Sprintf("Unknown type of exit_code: %s", v)}
	}
	{
		stack, err := DecodeStack(res["stack"])
		if err != nil {
			return nil, models.IndexError{Code: 501, Message: fmt.Sprintf("failed to decode api/v2 stack: %s", err.Error())}
		}
		result.Stack = stack
	}
	return &result, nil
}

// tonlib flattens Lisp lists into one entry, so only genuine nested tuples add
// depth: the bound is far above any real stack and only caps runaway recursion.
const maxStackDecodeDepth = 1024

// DecodeStackEntry renders one API v2 stack entry, accepting both the legacy
// ["num", "0x..."] pairs and the standard tvm.stackEntry* objects.
func DecodeStackEntry(stack interface{}) (interface{}, error) {
	return decodeStackEntry(stack, 0)
}

func decodeStackEntry(stack interface{}, depth int) (interface{}, error) {
	if depth > maxStackDecodeDepth {
		return nil, fmt.Errorf("stack entry nested deeper than %d levels", maxStackDecodeDepth)
	}
	var stack_row models.V2StackEntity
	switch val := stack.(type) {
	case []interface{}:
		// only the first two positions carry meaning
		if len(val) < 2 {
			return nil, fmt.Errorf("legacy stack entry must be a [type, value] pair")
		}
		kind, ok := val[0].(string)
		if !ok {
			return nil, fmt.Errorf("legacy stack entry type must be a string, got %T", val[0])
		}
		switch kind {
		case "num":
			stack_row.Type = "num"
			stack_row.Value = val[1]
		case "cell", "slice":
			bytes, err := stackBytes(val[1], kind)
			if err != nil {
				return nil, err
			}
			stack_row.Type, stack_row.Value = kind, bytes
		case "tuple", "list":
			elements, err := stackElements(val[1], kind, depth)
			if err != nil {
				return nil, err
			}
			stack_row.Type, stack_row.Value = kind, elements
		default:
			return nil, fmt.Errorf("unsupported stack entry type: %s", kind)
		}
	case map[string]interface{}:
		marker, ok := val["@type"].(string)
		if !ok {
			return nil, fmt.Errorf("stack entry is missing its @type marker")
		}
		switch marker {
		case "tvm.stackEntryNumber":
			payload, err := stackPayload(val["number"], "number")
			if err != nil {
				return nil, err
			}
			text, ok := payload["number"].(string)
			if !ok {
				return nil, fmt.Errorf("unsupported type for number: %T", payload["number"])
			}
			i := new(big.Int)
			if _, ok := i.SetString(text, 10); !ok {
				return nil, fmt.Errorf("failed to parse decimal %s", text)
			}
			stack_row.Type = "num"
			stack_row.Value = fmt.Sprintf("%#x", i)
		case "tvm.stackEntryCell", "tvm.stackEntrySlice":
			kind := map[string]string{"tvm.stackEntryCell": "cell", "tvm.stackEntrySlice": "slice"}[marker]
			bytes, err := stackBytes(val[kind], kind)
			if err != nil {
				return nil, err
			}
			stack_row.Type, stack_row.Value = kind, bytes
		case "tvm.stackEntryTuple", "tvm.stackEntryList":
			kind := map[string]string{"tvm.stackEntryTuple": "tuple", "tvm.stackEntryList": "list"}[marker]
			elements, err := stackElements(val[kind], kind, depth)
			if err != nil {
				return nil, err
			}
			stack_row.Type, stack_row.Value = kind, elements
		default:
			return nil, fmt.Errorf("unsupported stack entry type: %s", marker)
		}
	default:
		return nil, fmt.Errorf("failed to parse stack entry of type: %T", stack)
	}
	return stack_row, nil
}

func stackPayload(value interface{}, kind string) (map[string]interface{}, error) {
	payload, ok := value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("%s stack entry carries no object payload", kind)
	}
	return payload, nil
}

func stackBytes(value interface{}, kind string) (interface{}, error) {
	payload, err := stackPayload(value, kind)
	if err != nil {
		return nil, err
	}
	bytes, ok := payload["bytes"]
	if !ok {
		return nil, fmt.Errorf("%s stack entry carries no bytes", kind)
	}
	return bytes, nil
}

func stackElements(value interface{}, kind string, depth int) ([]interface{}, error) {
	payload, err := stackPayload(value, kind)
	if err != nil {
		return nil, err
	}
	items, ok := payload["elements"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("%s stack entry carries no elements array", kind)
	}
	elements := []interface{}{}
	for _, item := range items {
		decoded, err := decodeStackEntry(item, depth+1)
		if err != nil {
			return nil, err
		}
		elements = append(elements, decoded)
	}
	return elements, nil
}

func DecodeStack(stack interface{}) ([]models.V2StackEntity, error) {
	rows, ok := stack.([]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to decode top level stack of type %T", stack)
	}
	result := []models.V2StackEntity{}
	for _, row := range rows {
		decoded, err := DecodeStackEntry(row)
		if err != nil {
			return nil, err
		}
		entry, ok := decoded.(models.V2StackEntity)
		if !ok {
			return nil, fmt.Errorf("stack entry decoded to %T", decoded)
		}
		result = append(result, entry)
	}
	return result, nil
}
