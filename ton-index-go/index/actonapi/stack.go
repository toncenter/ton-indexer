package actonapi

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/ton-blockchain/acton/packages/abi-go"
)

const maxStackDepth = 32
const maxStackEntries = 1024

// Decimal preserves integers exactly, including JSON numbers above 2^53. TVM
// integers are signed 257-bit values. Floats, exponents and fractions are refused.
func Decimal(value any) (string, error) {
	var text string
	switch v := value.(type) {
	case string:
		text = v
	case json.Number:
		text = string(v)
	case *big.Int:
		if v == nil {
			return "", fmt.Errorf("nil integer")
		}
		text = v.String()
	case int:
		text = fmt.Sprint(v)
	case int32:
		text = fmt.Sprint(v)
	case int64:
		text = fmt.Sprint(v)
	case uint32:
		text = fmt.Sprint(v)
	case uint64:
		text = fmt.Sprint(v)
	default:
		return "", fmt.Errorf("integer must be an exact decimal or hexadecimal string or JSON integer")
	}
	if len(text) == 0 || len(text) > 80 {
		return "", fmt.Errorf("invalid TVM integer length")
	}
	digits := text
	if digits[0] == '-' {
		digits = digits[1:]
	}
	base := 10
	if strings.HasPrefix(digits, "0x") {
		digits = digits[2:]
		base = 16
	}
	if digits == "" {
		return "", fmt.Errorf("invalid integer")
	}
	for _, c := range digits {
		if !(c >= '0' && c <= '9' || base == 16 && (c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F')) {
			return "", fmt.Errorf("invalid integer digits")
		}
	}
	n, ok := new(big.Int).SetString(digits, base)
	if !ok {
		return "", fmt.Errorf("invalid integer")
	}
	if text[0] == '-' {
		n.Neg(n)
	}
	bound := new(big.Int).Lsh(big.NewInt(1), 256)
	if n.Cmp(bound) >= 0 || n.Cmp(new(big.Int).Neg(bound)) < 0 {
		return "", fmt.Errorf("integer outside TVM signed 257-bit range")
	}
	return n.String(), nil
}

func stackChildren(value any) ([]acton.StackValue, error) {
	if children, ok := value.([]acton.StackValue); ok {
		return children, nil
	}
	children, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("tuple/list value must be an array of typed stack entries")
	}
	if len(children) > maxStackEntries {
		return nil, fmt.Errorf("stack exceeds entry limit")
	}
	result := make([]acton.StackValue, 0, len(children))
	for _, child := range children {
		entry, ok := child.(map[string]any)
		if !ok || len(entry) > 2 {
			return nil, fmt.Errorf("invalid nested stack entry")
		}
		for key := range entry {
			if key != "type" && key != "value" {
				return nil, fmt.Errorf("unknown nested stack field %q", key)
			}
		}
		kind, ok := entry["type"].(string)
		if !ok {
			return nil, fmt.Errorf("nested stack entry requires type")
		}
		result = append(result, acton.StackValue{Type: kind, Value: entry["value"]})
	}
	return result, nil
}

func ValidateStack(stack []acton.StackValue) error {
	_, err := NormalizeStack(stack)
	return err
}

// NormalizeStack is the boundary between public/wire aliases and native codecs.
// Tonlib lists are flattened Lisp lists, not TVM arrays: [] is null, and [a,b]
// is the tuple pair (a, (b, null)). "num" is accepted as an alias for "int", and
// stackEntryUnsupported is never coerced to null. See to_tonlib_api /
// from_tonlib_api in TonlibClient.cpp:
// https://github.com/ton-blockchain/ton/blob/9a42919dce98971a6653d326347efcce40bad026/tonlib/tonlib/TonlibClient.cpp#L4896-L5026
func NormalizeStack(stack []acton.StackValue) ([]acton.StackValue, error) {
	remaining := maxStackEntries
	bytesRemaining := MaxBodyBytes
	var walk func([]acton.StackValue, int) ([]acton.StackValue, error)
	walk = func(entries []acton.StackValue, depth int) ([]acton.StackValue, error) {
		if depth > maxStackDepth || len(entries) > remaining {
			return nil, fmt.Errorf("stack exceeds depth or entry limit")
		}
		remaining -= len(entries)
		result := make([]acton.StackValue, 0, len(entries))
		for _, entry := range entries {
			switch entry.Type {
			case "num", "int":
				value, err := Decimal(entry.Value)
				if err != nil {
					return nil, err
				}
				entry.Type, entry.Value = "int", value
			case "builder":
				return nil, fmt.Errorf("runGetMethodStd cannot represent builder inputs")
			case "cell", "slice":
				boc, ok := entry.Value.(string)
				if !ok || boc == "" {
					return nil, fmt.Errorf("%s requires a BOC string", entry.Type)
				}
				bytesRemaining -= len(boc)
				if bytesRemaining < 0 {
					return nil, fmt.Errorf("stack BOCs exceed size limit")
				}
				if _, err := acton.DecodeOpaqueBOC(boc); err != nil {
					return nil, fmt.Errorf("invalid %s BOC: %w", entry.Type, err)
				}
			case "tuple", "list":
				children, err := stackChildren(entry.Value)
				if err != nil {
					return nil, err
				}
				if len(children) > 255 && entry.Type == "tuple" {
					return nil, fmt.Errorf("tuple/list exceeds 255 entries")
				}
				values, err := walk(children, depth+1)
				if err != nil {
					return nil, err
				}
				if entry.Type == "list" {
					remaining -= len(values) + 1
					if remaining < 0 || depth+len(values) > maxStackDepth {
						return nil, fmt.Errorf("expanded Lisp list exceeds stack limit")
					}
					entry = lispList(values)
				} else {
					entry.Value = values
				}
			case "null":
				if entry.Value != nil {
					return nil, fmt.Errorf("null stack entry must have null value")
				}
			default:
				return nil, fmt.Errorf("unsupported stack type %q", entry.Type)
			}
			result = append(result, entry)
		}
		return result, nil
	}
	normalized, err := walk(stack, 0)
	if err != nil {
		return nil, err
	}
	// Flattened nested lists can gain depth when expanded to cons pairs. Check
	// the resulting native shape, not just the original JSON nesting depth.
	var checkDepth func([]acton.StackValue, int) error
	checkDepth = func(entries []acton.StackValue, depth int) error {
		if depth > maxStackDepth {
			return fmt.Errorf("expanded Lisp list exceeds stack depth limit")
		}
		for _, entry := range entries {
			if entry.Type == "tuple" {
				if err := checkDepth(entry.Value.([]acton.StackValue), depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := checkDepth(normalized, 0); err != nil {
		return nil, err
	}
	return normalized, nil
}

func lispList(items []acton.StackValue) acton.StackValue {
	tail := acton.StackValue{Type: "null"}
	for i := len(items) - 1; i >= 0; i-- {
		tail = acton.StackValue{Type: "tuple", Value: []acton.StackValue{items[i], tail}}
	}
	return tail
}

func EncodeStandardStack(stack []acton.StackValue) ([]any, error) {
	normalized, err := NormalizeStack(stack)
	if err != nil {
		return nil, err
	}
	var walk func([]acton.StackValue) ([]any, error)
	walk = func(entries []acton.StackValue) ([]any, error) {
		result := make([]any, 0, len(entries))
		for _, entry := range entries {
			var wire any
			switch entry.Type {
			case "int":
				wire = map[string]any{"@type": "tvm.stackEntryNumber", "number": map[string]any{"@type": "tvm.numberDecimal", "number": entry.Value}}
			case "cell", "slice":
				marker := map[string]string{"cell": "Cell", "slice": "Slice"}[entry.Type]
				wire = map[string]any{"@type": "tvm.stackEntry" + marker, entry.Type: map[string]any{"@type": "tvm." + entry.Type, "bytes": entry.Value}}
			case "tuple":
				values, err := walk(entry.Value.([]acton.StackValue))
				if err != nil {
					return nil, err
				}
				wire = map[string]any{"@type": "tvm.stackEntryTuple", "tuple": map[string]any{"@type": "tvm.tuple", "elements": values}}
			case "null":
				wire = map[string]any{"@type": "tvm.stackEntryList", "list": map[string]any{"@type": "tvm.list", "elements": []any{}}}
			}
			result = append(result, wire)
		}
		return result, nil
	}
	return walk(normalized)
}

// DecodeStandardStack refuses lossy unsupported entries. The caller retains the
// original raw stack independently, including on VM failure or wire limitations.
func DecodeStandardStack(raw json.RawMessage) ([]acton.StackValue, error) {
	var entries []any
	if err := decodeJSON(raw, &entries); err != nil {
		return nil, err
	}
	if entries == nil {
		return nil, fmt.Errorf("upstream stack must be an array")
	}
	remaining := maxStackEntries
	var walk func([]any, int) ([]acton.StackValue, error)
	walk = func(entries []any, depth int) ([]acton.StackValue, error) {
		if depth > maxStackDepth || len(entries) > remaining {
			return nil, fmt.Errorf("upstream stack exceeds depth or entry limit")
		}
		remaining -= len(entries)
		result := make([]acton.StackValue, 0, len(entries))
		for _, value := range entries {
			entry, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expected standard stack object, not legacy stack")
			}
			marker, ok := entry["@type"].(string)
			if !ok {
				return nil, fmt.Errorf("missing stack entry marker")
			}
			var kind, payloadMarker string
			switch marker {
			case "tvm.stackEntryNumber":
				kind, payloadMarker = "number", "tvm.numberDecimal"
			case "tvm.stackEntryCell":
				kind, payloadMarker = "cell", "tvm.cell"
			case "tvm.stackEntrySlice":
				kind, payloadMarker = "slice", "tvm.slice"
			case "tvm.stackEntryTuple":
				kind, payloadMarker = "tuple", "tvm.tuple"
			case "tvm.stackEntryList":
				kind, payloadMarker = "list", "tvm.list"
			default:
				return nil, fmt.Errorf("upstream standard stack contains unsupported marker %q", marker)
			}
			payload, ok := entry[kind].(map[string]any)
			if !ok || payload["@type"] != payloadMarker {
				return nil, fmt.Errorf("invalid %s payload marker", kind)
			}
			out := acton.StackValue{Type: kind}
			switch kind {
			case "number":
				decimal, err := Decimal(payload["number"])
				if err != nil {
					return nil, err
				}
				out.Type, out.Value = "int", decimal
			case "cell", "slice":
				boc, ok := payload["bytes"].(string)
				if !ok {
					return nil, fmt.Errorf("invalid %s BOC", kind)
				}
				if _, err := acton.DecodeOpaqueBOC(boc); err != nil {
					return nil, err
				}
				out.Value = boc
			case "tuple", "list":
				children, ok := payload["elements"].([]any)
				if !ok || kind == "tuple" && len(children) > 255 {
					return nil, fmt.Errorf("invalid tuple/list elements")
				}
				decoded, err := walk(children, depth+1)
				if err != nil {
					return nil, err
				}
				out.Value = decoded
			}
			result = append(result, out)
		}
		return result, nil
	}
	stack, err := walk(entries, 0)
	if err != nil {
		return nil, err
	}
	return NormalizeStack(stack)
}
