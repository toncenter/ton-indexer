package actonapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/ton-blockchain/acton/packages/abi-go"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func storageAddresses(count int) []string {
	values := make([]string, count)
	for i := range values {
		values[i] = fmt.Sprintf("0:%064X", i+1)
	}
	return values
}

// A storage batch is bounded by the shared decode budget, not by a separate
// account count, so it accepts the same MaxBatch addresses as any other batch.
func TestStorageBatchLimitBeforeQuery(t *testing.T) {
	queries := 0
	app := testApp(New(nil, "test", Dependencies{QueryAccounts: func(*fiber.Ctx, []string, bool) ([]AccountState, error) { queries++; return nil, nil }}))
	body, _ := json.Marshal(AccountsRequest{Addresses: storageAddresses(MaxBatch + 1), IncludeStorage: true})
	call(t, app, "POST", "/accounts", string(body), 422, nil)
	if queries != 0 {
		t.Fatal("oversized batch reached DB")
	}
	// Canonical deduplication happens before the query, and a full storage batch
	// is one DB call like any other.
	body, _ = json.Marshal(AccountsRequest{Addresses: []string{testAddress, strings.ToLower(testAddress)}, IncludeStorage: true})
	call(t, app, "POST", "/accounts", string(body), 200, nil)
	body, _ = json.Marshal(AccountsRequest{Addresses: storageAddresses(MaxBatch), IncludeStorage: true})
	call(t, app, "POST", "/accounts", string(body), 200, nil)
	if queries != 2 {
		t.Fatalf("expected one DB call per valid batch, got %d", queries)
	}
}

func TestStorageAggregateBudgetStopsBeforeAmplification(t *testing.T) {
	for _, mode := range []string{"boc", "decoded"} {
		t.Run(mode, func(t *testing.T) {
			contract := testContract()
			decodes, queries := 0, 0
			large := strings.Repeat("<", 700000)
			contract.Storage = &acton.Binding{Type: acton.TypeInfo{Name: "Storage"}, DecodeWith: func(*acton.Context, *cell.Cell) (any, error) { decodes++; return map[string]any{"value": large}, nil }}
			boc := base64.StdEncoding.EncodeToString(cell.BeginCell().EndCell().ToBOC())
			addresses := storageAddresses(MaxStorageAccounts)
			app := testApp(New([]*acton.Contract{contract}, "test", Dependencies{QueryAccounts: func(_ *fiber.Ctx, request []string, include bool) ([]AccountState, error) {
				queries++
				if !include || len(request) != MaxStorageAccounts {
					t.Fatal("unexpected storage query")
				}
				rows := make([]AccountState, len(request))
				for i, addr := range request {
					rows[i] = AccountState{Address: addr, CodeHash: &testHash, DataBOC: &boc}
					if mode == "boc" {
						rows[i].BOCBytes = 2 * MaxBodyBytes
					}
				}
				return rows, nil
			}}))
			body, _ := json.Marshal(AccountsRequest{Addresses: addresses, IncludeStorage: true})
			raw := call(t, app, "POST", "/accounts", string(body), 413, nil)
			if queries != 1 || len(raw) > 1024 {
				t.Fatalf("unbounded/error batch: queries=%d bytes=%d", queries, len(raw))
			}
			if mode == "boc" && decodes != 0 {
				t.Fatal("oversized raw BOC batch reached native decoding")
			}
			if mode == "decoded" && decodes != 2 {
				t.Fatalf("did not stop retaining amplified values early: decodes=%d", decodes)
			}
		})
	}
}

// A large storage batch is allowed, but the decode work it can demand is not:
// the batch shares one budget, so an attacker naming a thousand deliberately
// expensive accounts is cut off after a bounded amount of work rather than
// multiplying it by the batch size.
func TestStorageBatchSharesOneDecodeBudget(t *testing.T) {
	contract := testContract()
	decodes := 0
	// Charge the shared budget the way a real expensive cell would.
	contract.Storage = &acton.Binding{Type: acton.TypeInfo{Name: "Storage"},
		DecodeWith: func(ctx *acton.Context, c *cell.Cell) (any, error) {
			decodes++
			expensive := acton.MapCodec(intCodec(), intCodec(), 32)
			return expensive.DecodeWith(ctx, c)
		}}
	boc := base64.StdEncoding.EncodeToString(bombCell(t).ToBOC())
	app := testApp(New([]*acton.Contract{contract}, "test", Dependencies{
		QueryAccounts: func(_ *fiber.Ctx, request []string, _ bool) ([]AccountState, error) {
			rows := make([]AccountState, len(request))
			for i, addr := range request {
				rows[i] = AccountState{Address: addr, CodeHash: &testHash, DataBOC: &boc}
			}
			return rows, nil
		}}))
	body, _ := json.Marshal(AccountsRequest{Addresses: storageAddresses(MaxBatch), IncludeStorage: true})
	call(t, app, "POST", "/accounts", string(body), 413, nil)
	if decodes == 0 || decodes >= MaxBatch {
		t.Fatalf("shared budget did not bound the batch: %d of %d accounts decoded", decodes, MaxBatch)
	}
	t.Logf("budget stopped the batch after %d of %d expensive accounts", decodes, MaxBatch)
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
