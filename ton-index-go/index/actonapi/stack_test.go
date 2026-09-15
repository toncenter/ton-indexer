package actonapi

import (
	"encoding/base64"
	"encoding/json"
	"math/big"
	"slices"
	"testing"

	"github.com/ton-blockchain/tolk-abi-to-go"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestDecimalExactAndBounded(t *testing.T) {
	bound := new(big.Int).Lsh(big.NewInt(1), 256)
	for _, input := range []any{"000123", "-0xabc", json.Number("9007199254740993"), new(big.Int).Neg(bound).String(), new(big.Int).Sub(bound, big.NewInt(1)).String()} {
		if _, err := Decimal(input); err != nil {
			t.Fatalf("%v: %v", input, err)
		}
	}
	for _, input := range []any{"", "1.5", "1e3", " 1", "+1", "0x", "--1", float64(1), bound.String(), new(big.Int).Sub(new(big.Int).Neg(bound), big.NewInt(1)).String()} {
		if _, err := Decimal(input); err == nil {
			t.Fatalf("accepted inexact/invalid integer %v", input)
		}
	}
	if value, _ := Decimal("0010"); value != "10" {
		t.Fatal("decimal was interpreted as octal")
	}
}

// Every runGetMethodStd spelling is pinned, nested entries included; an empty list is TVM null.
func TestStandardStackEncoding(t *testing.T) {
	boc := base64.StdEncoding.EncodeToString(cell.BeginCell().EndCell().ToBOC())
	stack := []tolkabi.StackValue{{Type: "num", Value: json.Number("9007199254740993")}, {Type: "cell", Value: boc}, {Type: "slice", Value: boc}, {Type: "tuple", Value: []tolkabi.StackValue{{Type: "num", Value: "-0x100"}, {Type: "list", Value: []tolkabi.StackValue{}}}}}
	wire, err := EncodeStandardStack(stack)
	raw, _ := json.Marshal(wire)
	want := `[{"@type":"tvm.stackEntryNumber","number":{"@type":"tvm.numberDecimal","number":"9007199254740993"}},{"@type":"tvm.stackEntryCell","cell":{"@type":"tvm.cell","bytes":"` + boc + `"}},{"@type":"tvm.stackEntrySlice","slice":{"@type":"tvm.slice","bytes":"` + boc + `"}},` +
		`{"@type":"tvm.stackEntryTuple","tuple":{"@type":"tvm.tuple","elements":[{"@type":"tvm.stackEntryNumber","number":{"@type":"tvm.numberDecimal","number":"-256"}},{"@type":"tvm.stackEntryList","list":{"@type":"tvm.list","elements":[]}}]}}]`
	if err != nil || string(raw) != want {
		t.Fatalf("not standard wire format: %v\n got %s\nwant %s", err, raw, want)
	}
}

func TestStandardStackLimitsAndUnsupported(t *testing.T) {
	deep := []tolkabi.StackValue{{Type: "num", Value: "1"}}
	for i := 0; i < 34; i++ {
		deep = []tolkabi.StackValue{{Type: "tuple", Value: deep}}
	}
	wide := slices.Repeat([]tolkabi.StackValue{{Type: "null"}}, maxStackEntries+1) // valid entries, so only the entry limit refuses them
	boc := base64.StdEncoding.EncodeToString(cell.BeginCell().EndCell().ToBOC())
	for i, stack := range [][]tolkabi.StackValue{deep, wide, {{Type: "null", Value: "invalid"}}, {{Type: "builder", Value: boc}}, {{Type: "cell", Value: "garbage"}},
		{{Type: "num", Value: "1.5"}}, {{Type: "tuple", Value: "not an array"}}, {{Type: "cont"}}} {
		if _, err := EncodeStandardStack(stack); err == nil {
			t.Fatalf("accepted oversized, unsupported or invalid stack #%d", i)
		}
	}
	for _, raw := range []string{`null`, `[["num","0x1"]]`, `[{"@type":"tvm.stackEntryUnsupported"}]`, `[{"@type":"tvm.stackEntryNumber","number":{"@type":"wrong","number":"1"}}]`, `[{"@type":"tvm.stackEntryTuple","tuple":{"@type":"tvm.tuple","elements":null}}]`, `[{}]`} {
		if _, err := DecodeStandardStack(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted malformed stack %s", raw)
		}
	}
}
