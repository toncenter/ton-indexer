package actonapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
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

func TestStorageBatchLimitBeforeQuery(t *testing.T) {
	queries := 0
	app := testApp(New(nil, "test", Dependencies{QueryAccounts: func(*fiber.Ctx, []string, bool) ([]AccountState, error) { queries++; return nil, nil }}))
	body, _ := json.Marshal(AccountsRequest{Addresses: storageAddresses(MaxStorageAccounts + 1), IncludeStorage: true})
	call(t, app, "POST", "/accounts", string(body), 422, nil)
	if queries != 0 {
		t.Fatal("oversized storage batch reached DB")
	}
	// The limit applies after canonical deduplication, and ordinary hover batches
	// still support 1000 addresses without retrieving BOCs.
	body, _ = json.Marshal(AccountsRequest{Addresses: []string{testAddress, strings.ToLower(testAddress)}, IncludeStorage: true})
	call(t, app, "POST", "/accounts", string(body), 200, nil)
	body, _ = json.Marshal(AccountsRequest{Addresses: storageAddresses(MaxBatch)})
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
			contract.Storage = &acton.Binding{Type: acton.TypeInfo{Name: "Storage"}, Decode: func(*cell.Cell) (any, error) { decodes++; return map[string]any{"value": large}, nil }}
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
