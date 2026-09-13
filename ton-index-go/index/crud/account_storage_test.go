package crud

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/ton-blockchain/acton/packages/abi-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func storageStates(count int, boc string) []models.AccountStateFull {
	states := make([]models.AccountStateFull, count)
	for i := range states {
		address := models.AccountAddress(fmt.Sprintf("0:%064X", i+1))
		data := models.BytesType(boc)
		states[i] = models.AccountStateFull{AccountAddress: &address,
			CodeHash: new(models.HashType(vestingCodeHash)), DataBoc: &data}
	}
	return states
}

func storageContract(decode func(*acton.Context, *cell.Cell) (any, error)) func(string) []*acton.Contract {
	contract := &acton.Contract{ID: "counter", Storage: &acton.Binding{
		Type: acton.TypeInfo{Name: "Storage"}, DecodeWith: decode}}
	return func(string) []*acton.Contract { return []*acton.Contract{contract} }
}

func TestAccountStorageRejectsOversizedBOCsBeforeDecoding(t *testing.T) {
	decodes := 0
	lookup := storageContract(func(*acton.Context, *cell.Cell) (any, error) { decodes++; return "value", nil })
	states := storageStates(1000, base64.StdEncoding.EncodeToString(cell.BeginCell().EndCell().ToBOC()))
	for i := range states {
		code := models.BytesType(strings.Repeat("x", 16384))
		states[i].CodeBoc = &code
	}
	if err := decodeAccountStorage(states, lookup); err == nil {
		t.Fatal("oversized raw BOC batch was accepted")
	}
	if decodes != 0 {
		t.Fatalf("oversized raw BOC batch reached native decoding: %d decodes", decodes)
	}
}

func TestAccountStorageStopsBeforeOutputAmplification(t *testing.T) {
	decodes := 0
	large := strings.Repeat("<", 700000)
	lookup := storageContract(func(*acton.Context, *cell.Cell) (any, error) {
		decodes++
		return map[string]any{"value": large}, nil
	})
	states := storageStates(1000, base64.StdEncoding.EncodeToString(cell.BeginCell().EndCell().ToBOC()))
	if err := decodeAccountStorage(states, lookup); err == nil {
		t.Fatal("amplified batch was accepted")
	}
	if decodes == 0 || decodes >= len(states) {
		t.Fatalf("did not stop retaining amplified values early: %d of %d decoded", decodes, len(states))
	}
}

// A large storage batch is allowed, but the decode work it can demand is not:
// the batch shares one budget, so an attacker naming a thousand deliberately
// expensive accounts is cut off after a bounded amount of work rather than
// multiplying it by the batch size.
func TestAccountStorageSharesOneDecodeBudget(t *testing.T) {
	decodes := 0
	lookup := storageContract(func(ctx *acton.Context, c *cell.Cell) (any, error) {
		decodes++
		expensive := acton.MapCodec(intCodec(), intCodec(), 32)
		return expensive.DecodeWith(ctx, c)
	})
	states := storageStates(1000, base64.StdEncoding.EncodeToString(bombCell(t).ToBOC()))
	if err := decodeAccountStorage(states, lookup); err == nil {
		t.Fatal("bomb batch was accepted")
	}
	if decodes == 0 || decodes >= len(states) {
		t.Fatalf("shared budget did not bound the batch: %d of %d accounts decoded", decodes, len(states))
	}
	t.Logf("budget stopped the batch after %d of %d expensive accounts", decodes, len(states))
}

func TestAccountStorageFailureDoesNotStopTheBatch(t *testing.T) {
	unsupported := &acton.Contract{ID: "counter", Storage: &acton.Binding{Unsupported: "unsupported binding"}}
	states := storageStates(2, base64.StdEncoding.EncodeToString(cell.BeginCell().EndCell().ToBOC()))
	if err := decodeAccountStorage(states, func(string) []*acton.Contract {
		return []*acton.Contract{unsupported}
	}); err != nil {
		t.Fatalf("a per-account failure aborted the batch: %v", err)
	}
	for i := range states {
		if states[i].Storage != nil || states[i].StorageError == "" {
			t.Fatalf("account %d: expected a reported failure, got %+v", i, states[i])
		}
	}
}

func intCodec() *acton.Codec {
	c := acton.IntegerCodec(32, false, false)
	return &c
}

// bombCell builds a dictionary whose cells are almost entirely shared: a few
// hundred bytes of input that decode into tens of kilobytes.
func bombCell(t *testing.T) *cell.Cell {
	t.Helper()
	dict := acton.MapCodec(intCodec(), intCodec(), 32)
	entries := make([]any, 2000)
	for i := range entries {
		entries[i] = map[string]any{"key": strconv.Itoa(i), "value": "123456789"}
	}
	c, err := dict.Encode(entries)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
