package actonapi

import (
	"encoding/base64"
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	"github.com/ton-blockchain/acton/packages/abi-go"
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

func TestStandardStackRoundTrip(t *testing.T) {
	boc := base64.StdEncoding.EncodeToString(cell.BeginCell().EndCell().ToBOC())
	stack := []acton.StackValue{{Type: "num", Value: json.Number("9007199254740993")}, {Type: "cell", Value: boc}, {Type: "slice", Value: boc}, {Type: "tuple", Value: []acton.StackValue{{Type: "num", Value: "-0x100"}, {Type: "list", Value: []acton.StackValue{}}}}}
	wire, err := EncodeStandardStack(stack)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(wire)
	decoded, err := DecodeStandardStack(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 4 || decoded[0].Type != "int" || decoded[0].Value != "9007199254740993" {
		t.Fatalf("lost precision: %+v", decoded)
	}
	tuple := decoded[3].Value.([]acton.StackValue)
	if tuple[0].Type != "int" || tuple[0].Value != "-256" || tuple[1].Type != "null" {
		t.Fatalf("bad recursive stack: %+v", tuple)
	}
	if !strings.Contains(string(raw), `"@type":"tvm.numberDecimal"`) {
		t.Fatal("not standard wire format")
	}
}

func TestStandardStackLimitsAndUnsupported(t *testing.T) {
	boc := base64.StdEncoding.EncodeToString(cell.BeginCell().EndCell().ToBOC())
	for _, entry := range []acton.StackValue{{Type: "null", Value: "invalid"}, {Type: "builder", Value: boc}, {Type: "nan"}, {Type: "cont", Value: boc}, {Type: "cell", Value: "garbage"}, {Type: "tuple", Value: "bad"}} {
		if _, err := EncodeStandardStack([]acton.StackValue{entry}); err == nil {
			t.Fatalf("accepted unsupported/invalid entry: %+v", entry)
		}
	}
	stack := []acton.StackValue{{Type: "num", Value: "1"}}
	for i := 0; i < 34; i++ {
		stack = []acton.StackValue{{Type: "tuple", Value: stack}}
	}
	if err := ValidateStack(stack); err == nil {
		t.Fatal("accepted excessive depth")
	}
	stack = make([]acton.StackValue, maxStackEntries+1)
	if err := ValidateStack(stack); err == nil {
		t.Fatal("accepted excessive entries")
	}
	for _, raw := range []string{`null`, `[null]`, `[["num","0x1"]]`, `[{}]`, `[{"@type":"tvm.stackEntryUnsupported"}]`, `[{"@type":"tvm.stackEntryNumber","number":{"@type":"wrong","number":"1"}}]`, `[{"@type":"tvm.stackEntryTuple","tuple":{"@type":"tvm.tuple","elements":null}}]`} {
		if _, err := DecodeStandardStack(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted malformed stack %s", raw)
		}
	}
}
