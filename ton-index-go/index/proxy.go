package index

import (
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/url"
	"reflect"

	"github.com/gofiber/fiber/v2"
)

func GetV2AddressInformation(state_req V2AccountRequest, settings RequestSettings) (*V2AddressInformation, error) {
	if len(settings.V2Endpoint) == 0 {
		return nil, IndexError{Code: 500, Message: "ton-http-api endpoint is not specified"}
	}

	baseUrl, err := url.Parse(settings.V2Endpoint)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	baseUrl.Path += "/getAddressInformation"
	params := url.Values{}
	params.Add("address", string(state_req.AccountAddress))
	if len(settings.V2ApiKey) > 0 {
		params.Add("api_key", settings.V2ApiKey)
	}
	baseUrl.RawQuery = params.Encode()
	agent := fiber.Get(baseUrl.String())
	agent.Timeout(settings.Timeout)
	_, body, errs := agent.Bytes()
	if len(errs) > 0 {
		return nil, IndexError{Code: 500, Message: errs[0].Error()}
	}

	var jsn map[string]interface{}
	if err = json.Unmarshal(body, &jsn); err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	if jsn["ok"] != true {
		return nil, IndexError{Code: 500, Message: fmt.Sprintf("%v", jsn["error"])}
	}

	res := jsn["result"].(map[string]interface{})

	var acc V2AddressInformation
	switch v := res["balance"].(type) {
	case float64:
		acc.Balance = fmt.Sprintf("%d", int64(v))
	case string:
		acc.Balance = v
	default:
		return nil, IndexError{Code: 500, Message: fmt.Sprintf("failed to parse balance of type: %v", v)}
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

func GetV2WalletInformation(state_req V2AccountRequest, settings RequestSettings) (*V2WalletInformation, error) {
	if len(settings.V2Endpoint) == 0 {
		return nil, IndexError{Code: 500, Message: "ton-http-api endpoint is not specified"}
	}

	baseUrl, err := url.Parse(settings.V2Endpoint)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	baseUrl.Path += "/getWalletInformation"
	params := url.Values{}
	params.Add("address", string(state_req.AccountAddress))
	if len(settings.V2ApiKey) > 0 {
		params.Add("api_key", settings.V2ApiKey)
	}
	baseUrl.RawQuery = params.Encode()
	agent := fiber.Get(baseUrl.String())
	agent.Timeout(settings.Timeout)
	_, body, errs := agent.Bytes()
	if len(errs) > 0 {
		return nil, IndexError{Code: 500, Message: errs[0].Error()}
	}

	var jsn map[string]interface{}
	if err = json.Unmarshal(body, &jsn); err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	if jsn["ok"] != true {
		return nil, IndexError{Code: 500, Message: fmt.Sprintf("%v", jsn["error"])}
	}

	res := jsn["result"].(map[string]interface{})

	if res["wallet"] != true && res["account_state"].(string) != "uninitialized" {
		return nil, IndexError{Code: 409, Message: "not a wallet"}
	}

	var acc V2WalletInformation
	switch v := res["balance"].(type) {
	case string:
		acc.Balance = res["balance"].(string)
	case float64:
		acc.Balance = fmt.Sprintf("%d", int64(res["balance"].(float64)))
	default:
		return nil, IndexError{Code: 500, Message: fmt.Sprintf("failed to parse balance of type %v", v)}
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

func PostMessage(req V2SendMessageRequest, settings RequestSettings) (*V2SendMessageResult, error) {
	if len(settings.V2Endpoint) == 0 {
		return nil, IndexError{Code: 500, Message: "ton-http-api endpoint is not specified"}
	}

	baseUrl, err := url.Parse(settings.V2Endpoint)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	baseUrl.Path += "/sendBocReturnHash"
	params := url.Values{}
	if len(settings.V2ApiKey) > 0 {
		params.Add("api_key", settings.V2ApiKey)
	}
	baseUrl.RawQuery = params.Encode()
	agent := fiber.Post(baseUrl.String())
	agent.Timeout(settings.Timeout)
	var req_body []byte
	if req_body, err = json.Marshal(req); err != nil {
		return nil, IndexError{Code: 500, Message: fmt.Sprintf("failed to send request: %s", err.Error())}
	}
	agent.Add("Content-Type", "application/json")
	agent.Body(req_body)
	_, body, errs := agent.Bytes()
	if len(errs) > 0 {
		return nil, IndexError{Code: 500, Message: errs[0].Error()}
	}
	var jsn map[string]interface{}
	if err = json.Unmarshal(body, &jsn); err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
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
		return nil, IndexError{Code: code, Message: fmt.Sprintf("%v", jsn["error"])}
	}
	res := jsn["result"].(map[string]interface{})

	var result V2SendMessageResult

	result.MessageHash = new(HashType)
	*result.MessageHash = HashType(res["hash"].(string))
	if v, ok := res["hash_norm"]; ok {
		result.MessageHashNorm = new(HashType)
		*result.MessageHashNorm = HashType(v.(string))
	}
	return &result, nil
}

func PostEstimateFee(req V2EstimateFeeRequest, settings RequestSettings) (*V2EstimateFeeResult, error) {
	if len(settings.V2Endpoint) == 0 {
		return nil, IndexError{Code: 500, Message: "ton-http-api endpoint is not specified"}
	}

	baseUrl, err := url.Parse(settings.V2Endpoint)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	baseUrl.Path += "/estimateFee"
	params := url.Values{}
	if len(settings.V2ApiKey) > 0 {
		params.Add("api_key", settings.V2ApiKey)
	}
	baseUrl.RawQuery = params.Encode()
	agent := fiber.Post(baseUrl.String())
	agent.Timeout(settings.Timeout)
	var req_body []byte
	if req_body, err = json.Marshal(req); err != nil {
		return nil, IndexError{Code: 500, Message: fmt.Sprintf("failed to send request: %s", err.Error())}
	}
	agent.Add("Content-Type", "application/json")
	agent.Body(req_body)
	_, body, errs := agent.Bytes()
	if len(errs) > 0 {
		return nil, IndexError{Code: 500, Message: errs[0].Error()}
	}
	var resp_full struct {
		Ok     bool                `json:"ok"`
		Result V2EstimateFeeResult `json:"result"`
		Error  string              `json:"error"`
		Code   int                 `json:"code"`
	}
	if err = json.Unmarshal(body, &resp_full); err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	if !resp_full.Ok {
		return nil, IndexError{Code: resp_full.Code, Message: resp_full.Error}
	}
	return &resp_full.Result, nil
}

func PostRunGetMethod(req V2RunGetMethodRequest, settings RequestSettings) (*V2RunGetMethodResult, error) {
	if len(settings.V2Endpoint) == 0 {
		return nil, IndexError{Code: 500, Message: "ton-http-api endpoint is not specified"}
	}

	baseUrl, err := url.Parse(settings.V2Endpoint)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	baseUrl.Path += "/runGetMethod"
	params := url.Values{}
	if len(settings.V2ApiKey) > 0 {
		params.Add("api_key", settings.V2ApiKey)
	}
	baseUrl.RawQuery = params.Encode()
	agent := fiber.Post(baseUrl.String())
	agent.Timeout(settings.Timeout)
	agent.Add("Content-Type", "application/json")
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
				return nil, IndexError{Code: 500, Message: fmt.Sprintf("unsupported stack parameter type: %s", v.Type)}
			}
			vv = append(vv, v.Value)
			stack = append(stack, vv)
		}
		body["stack"] = stack

		var body_json []byte
		if body_json, err = json.Marshal(body); err != nil {
			return nil, IndexError{Code: 500, Message: fmt.Sprintf("failed to send request: %s", err.Error())}
		}
		agent.Body(body_json)
	}
	_, body, errs := agent.Bytes()
	if len(errs) > 0 {
		return nil, IndexError{Code: 500, Message: errs[0].Error()}
	}
	var jsn map[string]interface{}
	if err = json.Unmarshal(body, &jsn); err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	if jsn["ok"] != true {
		return nil, IndexError{Code: 500, Message: fmt.Sprintf("%v", jsn["error"])}
	}
	res := jsn["result"].(map[string]interface{})

	// log.Println(res)
	var result V2RunGetMethodResult
	switch v := res["gas_used"].(type) {
	case float64:
		result.GasUsed = int64(v)
	case int64:
		result.GasUsed = v
	default:
		return nil, IndexError{Code: 501, Message: fmt.Sprintf("Unknown type of gas_used: %s", v)}
	}
	switch v := res["exit_code"].(type) {
	case float64:
		result.ExitCode = int64(v)
	case int64:
		result.ExitCode = v
	default:
		return nil, IndexError{Code: 501, Message: fmt.Sprintf("Unknown type of exit_code: %s", v)}
	}
	{
		stack, err := DecodeStack(res["stack"])
		if err != nil {
			return nil, IndexError{Code: 501, Message: fmt.Sprintf("failed to decode api/v2 stack: %s", err.Error())}
		}
		result.Stack = stack
	}
	return &result, nil
}

func DecodeStackEntry(stack interface{}) (interface{}, error) {
	var stack_row V2StackEntity
	switch val := stack.(type) {
	case []interface{}:
		switch val[0].(string) {
		case "num":
			stack_row.Type = "num"
			stack_row.Value = val[1]
		case "cell":
			stack_row.Type = "cell"
			stack_row.Value = val[1].(map[string]interface{})["bytes"]
		case "slice":
			stack_row.Type = "slice"
			stack_row.Value = val[1].(map[string]interface{})["bytes"]
		case "tuple", "list":
			stack_row.Type = val[0].(string)
			tuple := []interface{}{}
			for _, item := range val[1].(map[string]interface{})["elements"].([]interface{}) {
				loc, err := DecodeStackEntry(item)
				if err != nil {
					return nil, err
				}
				tuple = append(tuple, loc)
			}
			stack_row.Value = tuple
		default:
			return nil, fmt.Errorf("unsupported stack entry type: %s", val[0].(string))
		}
	case map[string]interface{}:
		switch val["@type"].(string) {
		case "tvm.stackEntryNumber":
			stack_row.Type = "num"
			num := val["number"].(map[string]interface{})["number"]
			switch nn := num.(type) {
			case string:
				i := new(big.Int)
				if _, ok := i.SetString(nn, 10); !ok {
					return nil, fmt.Errorf("failed to parse decimal %s", nn)
				}
				stack_row.Value = fmt.Sprintf("%#x", i)
			default:
				return nil, fmt.Errorf("unsupport type for number: %s", reflect.TypeOf(num).Name())
			}
		case "tvm.stackEntryCell":
			stack_row.Type = "cell"
			stack_row.Value = val["cell"].(map[string]interface{})["bytes"]
		case "tvm.stackEntrySlice":
			stack_row.Type = "slice"
			stack_row.Value = val["slice"].(map[string]interface{})["bytes"]
		case "tvm.stackEntryTuple", "tvm.stackEntryList":
			if val["@type"] == "tvm.stackEntryTuple" {
				stack_row.Type = "tuple"
			} else {
				stack_row.Type = "list"
			}
			tuple := []interface{}{}
			for _, item := range val[stack_row.Type].(map[string]interface{})["elements"].([]interface{}) {
				loc, err := DecodeStackEntry(item)
				if err != nil {
					return nil, err
				}
				tuple = append(tuple, loc)
			}
			stack_row.Value = tuple
		default:
			return nil, fmt.Errorf("unsupported stack entry type: %s", val["@type"].(string))
		}
	default:
		return nil, fmt.Errorf("failed to parse stack entry of type: %s", reflect.TypeOf(stack).Name())
	}
	return stack_row, nil
}

func DecodeStack(stack interface{}) (interface{}, error) {
	result := []interface{}{}
	switch st := stack.(type) {
	case []interface{}:
		for _, row := range st {
			loc, err := DecodeStackEntry(row)
			if err != nil {
				return nil, err
			}
			result = append(result, loc)
		}
	default:
		return nil, fmt.Errorf("failed to decode top level stack of type %s", reflect.TypeOf(st).Name())
	}

	return result, nil
}
