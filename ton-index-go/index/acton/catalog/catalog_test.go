package catalog_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ton-blockchain/tolk-abi-to-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/acton/catalog"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// A tolk-abi-to-go upgrade or catalog re-pin must not silently drop contracts
// or disable or enable binding roots: a disabled root surfaces as a 422 or
// decode_error from /runGetMethod, /decode and include_storage.
func TestCatalogCapabilities(t *testing.T) {
	var unsupported []string
	root := func(id, reason string) {
		if reason != "" {
			unsupported = append(unsupported, id+": "+reason)
		}
	}
	for _, c := range catalog.Contracts {
		for i, b := range []*tolkabi.Binding{c.Storage, c.DeploymentStorage} {
			if b != nil {
				root(c.ID+"/"+[]string{"storage", "deployment_storage"}[i], b.Unsupported)
			}
		}
		for _, direction := range []string{"incoming_messages", "incoming_external", "outgoing_messages", "emitted_events"} {
			for _, b := range c.Messages[direction] {
				root(c.ID+"/"+direction+"/"+b.Type.Name, b.Unsupported)
			}
		}
		for _, m := range c.GetMethods {
			root(c.ID+"/"+m.Name, m.Unsupported)
		}
	}
	if len(catalog.Contracts) != 288 || len(unsupported) != 17 {
		t.Fatalf("catalog drifted from the pinned snapshot: contracts=%d unsupported=%d\n%s", len(catalog.Contracts), len(unsupported), strings.Join(unsupported, "\n"))
	}
}

func same(t *testing.T, got, want any) {
	t.Helper()
	a, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("got %s want %s", a, b)
	}
}
func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func boc(c *cell.Cell) string { return base64.StdEncoding.EncodeToString(c.ToBOC()) }
func contract(t *testing.T, id string) *tolkabi.Contract {
	t.Helper()
	c := catalog.ByID(id)
	if c == nil {
		t.Fatal("contract missing", id)
	}
	return c
}
func method(t *testing.T, c *tolkabi.Contract, name string) *tolkabi.GetMethod {
	t.Helper()
	for i := range c.GetMethods {
		if c.GetMethods[i].Name == name {
			return &c.GetMethods[i]
		}
	}
	t.Fatal("getter missing", name)
	return nil
}

// These exact storage values and layouts are from the pinned Acton
// packages/explorer-core/tests/walletV4Builder.test.ts and its golden snapshots.
func TestWalletV4StorageAndGetterGoldens(t *testing.T) {
	for _, id := range []string{"wallets/w4r1.WalletV4r1", "wallets/w4r2.WalletV4r2"} {
		t.Run(id, func(t *testing.T) {
			c := contract(t, id)
			for _, populated := range []bool{false, true} {
				dict := cell.NewDict(264)
				entries := []tolkabi.MapEntry{}
				if populated {
					key := cell.BeginCell()
					check(t, key.StoreInt(-1, 8))
					check(t, key.StoreUInt(42, 256))
					check(t, dict.Set(key.EndCell(), cell.BeginCell().EndCell()))
					entries = append(entries, tolkabi.MapEntry{Key: tolkabi.Bits{Bits: 264, Hex: "ff" + strings.Repeat("00", 31) + "2a"}, Value: nil})
				}
				b := cell.BeginCell()
				check(t, b.StoreUInt(41, 32))
				check(t, b.StoreUInt(698983191, 32))
				check(t, b.StoreUInt(5, 256))
				check(t, b.StoreDict(dict))
				got, err := tolkabi.DecodeStorage(c, boc(b.EndCell()))
				check(t, err)
				same(t, got, map[string]any{"seqno": "41", "subwalletId": "698983191", "publicKey": "5", "plugins": entries})
			}
			stack := []tolkabi.StackValue{{Type: "tuple", Value: []tolkabi.StackValue{{Type: "tuple", Value: []tolkabi.StackValue{{Type: "int", Value: "-1"}, {Type: "int", Value: "42"}}}, {Type: "null"}}}}
			got, err := method(t, c, "get_plugin_list").DecodeResult(stack)
			check(t, err)
			same(t, got, []any{map[string]any{"workchain": "-1", "address": "42"}})
		})
	}
}

// WalletTg is the real catalog entry that emits fully qualified instantiation
// names and a cell-only client override for the raw bulk-message slice.
func TestWalletTgGenericBulk(t *testing.T) {
	c := contract(t, "wallets.WalletTg")
	header := map[string]any{"subwalletId": "698983191", "validUntil": "1788865000", "seqno": "41"}
	messageCell := boc(cell.BeginCell().MustStoreUInt(0, 32).EndCell())
	items := []any{}
	for _, mode := range []string{"0", "1", "3", "128"} {
		items = append(items, map[string]any{"sendMode": mode, "messageCell": messageCell})
	}
	want := map[string]any{"signature": tolkabi.Bits{Bits: 512, Hex: strings.Repeat("00", 64)}, "request": tolkabi.UnionValue{Type: "SendBulkMessagesRequestE", Value: map[string]any{"header": header, "msgArr": items}}}
	root, err := c.Messages["incoming_external"][0].Encode(want)
	check(t, err)
	decoded, err := tolkabi.DecodeMessage(c, "incoming_external", boc(root))
	check(t, err)
	same(t, decoded.Value, want)
	// Independent layout checks catch a bug that breaks encode and decode alike.
	s, err := root.BeginParse()
	check(t, err)
	_, err = s.LoadSlice(512)
	check(t, err)
	op, err := s.LoadUInt(32)
	check(t, err)
	_, err = s.LoadSlice(96)
	check(t, err)
	n, err := s.LoadUInt(8)
	check(t, err)
	if op != 1938386549 || n != 4 {
		t.Fatalf("union prefix duplicated or wrong: op=%d count=%d", op, n)
	}
	head, err := s.LoadMaybeRef()
	check(t, err)
	tail, err := head.LoadMaybeRef()
	check(t, err)
	if tail == nil || head.RefsNum() != 1 || tail.RefsNum() != 3 {
		t.Fatal("continuation ref is not before element refs")
	}
}
