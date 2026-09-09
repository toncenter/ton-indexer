package catalog_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/format"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/ton-blockchain/acton/packages/abi-go"
	"github.com/ton-blockchain/acton/packages/abi-go/codegen"
	"github.com/toncenter/ton-indexer/ton-index-go/index/acton/catalog"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const pinnedRevision = "b442556faa253aba85e59cf7372aa90d9701fab684b4ec565480fa74a6efde38"

func TestOfflineGeneration(t *testing.T) {
	data, err := os.ReadFile("catalog.json")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != pinnedRevision {
		t.Fatalf("snapshot revision drift: %s", got)
	}
	if catalog.Revision != pinnedRevision {
		t.Fatal("generated revision drift", catalog.Revision)
	}
	out, err := codegen.Generate(data, codegen.Options{Package: "catalog", Snapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := out.Write(".", true); err != nil {
		t.Fatal(err)
	}
	if len(catalog.Contracts) != 288 || len(out.Diagnostics) != 17 {
		t.Fatalf("contracts=%d diagnostics=%+v", len(catalog.Contracts), out.Diagnostics)
	}
	for name, source := range out.Files {
		if !strings.HasSuffix(name, ".go") {
			continue
		}
		formatted, err := format.Source(source)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(source, formatted) {
			t.Fatalf("%s is not gofmt formatted", name)
		}
	}
}

func TestCatalogCapabilities(t *testing.T) {
	type count struct{ supported, unsupported int }
	counts := map[string]*count{}
	expectedUnsupported := map[string]string{
		"bidask.BidaskRange/incoming_messages/BidaskInternalSwap":           "nonterminal remainder",
		"bidask.BidaskRange/incoming_messages/BidaskInternalSwapV2":         "nonterminal remainder",
		"bidask.BidaskRange/incoming_messages/BidaskInternalContinueSwap":   "nonterminal remainder",
		"bidask.BidaskRange/incoming_messages/BidaskInternalContinueSwapV2": "nonterminal remainder",
		"bidask.BidaskRange/outgoing_messages/BidaskInternalContinueSwap":   "nonterminal remainder",
		"bidask.BidaskRange/outgoing_messages/BidaskInternalContinueSwapV2": "nonterminal remainder",
		"system.Config/storage": "custom pack/unpack hooks",
		"frt-gram-adapter.FrtGramAdapterCoordinator/get_method_75874_eadb": "unknown",
		"gaspump.GasPumpMasterV0/get_full_jetton_data":                     "unknown",
		"gaspump.GasPumpMasterV1/get_full_jetton_data":                     "unknown",
		"gaspump.GasPumpMasterV2/get_full_jetton_data":                     "unknown",
		"gaspump.GasPumpMasterV4/get_full_jetton_data":                     "unknown",
		"gaspump.GasPumpMasterV5/get_full_jetton_data":                     "unknown",
		"payment_channels.AsyncPaymentChannel/get_channel_data":            "unknown",
		"storages.StorageAggregateContract/get_providers":                  "unknown",
		"tonco.Pool/getTickInfosFrom":                                      "unknown",
		"tonkeeper_2fa.Tonkeeper2fa/get_delegation_state":                  "unknown",
	}
	add := func(group, id, reason string, encode, decode bool) {
		if counts[group] == nil {
			counts[group] = &count{}
		}
		if reason == "" {
			counts[group].supported++
			if !encode || !decode {
				t.Errorf("%s supported without callbacks", id)
			}
		} else {
			counts[group].unsupported++
			if encode || decode {
				t.Errorf("%s unsupported but has callbacks", id)
			}
			t.Logf("UNSUPPORTED %s: %s", id, reason)
			fragment, ok := expectedUnsupported[id]
			if !ok || !strings.Contains(reason, fragment) {
				t.Errorf("unexpected unsupported root/reason: %s: %s", id, reason)
			}
			delete(expectedUnsupported, id)
		}
	}
	for _, c := range catalog.Contracts {
		if catalog.ByID(c.ID) != c {
			t.Fatal("ID lookup mismatch", c.ID)
		}
		for _, h := range c.CodeHashes {
			if matches := catalog.ByCodeHash(strings.ToUpper(h)); len(matches) == 0 {
				t.Fatal("hash lookup missing", c.ID)
			}
			data, err := hex.DecodeString(h)
			if err != nil {
				t.Fatal(err)
			}
			if len(catalog.ByCodeHash(base64.RawURLEncoding.EncodeToString(data))) == 0 {
				t.Fatal("base64 hash lookup missing", c.ID)
			}
		}
		for i, b := range []*acton.Binding{c.Storage, c.DeploymentStorage} {
			if b != nil {
				group := []string{"storage", "deployment_storage"}[i]
				add(group, c.ID+"/"+group, b.Unsupported, b.Encode != nil, b.Decode != nil)
			}
		}
		for _, direction := range []string{"incoming_messages", "incoming_external", "outgoing_messages", "emitted_events"} {
			for _, b := range c.Messages[direction] {
				add(direction, c.ID+"/"+direction+"/"+b.Type.Name, b.Unsupported, b.Encode != nil, b.Decode != nil)
			}
		}
		for _, m := range c.GetMethods {
			add("getters", c.ID+"/"+m.Name, m.Unsupported, m.EncodeArgs != nil, m.DecodeResult != nil)
		}
	}
	groups := []string{}
	for group := range counts {
		groups = append(groups, group)
	}
	sort.Strings(groups)
	for _, group := range groups {
		n := counts[group]
		t.Logf("%s: %d supported, %d unsupported, %d total", group, n.supported, n.unsupported, n.supported+n.unsupported)
	}
	for group, want := range map[string]count{
		"storage": {280, 1}, "deployment_storage": {38, 0}, "incoming_messages": {1854, 4},
		"incoming_external": {62, 0}, "outgoing_messages": {1173, 2}, "emitted_events": {23, 0}, "getters": {1199, 10},
	} {
		if counts[group] == nil || *counts[group] != want {
			t.Errorf("capability drift for %s: got %+v want %+v", group, counts[group], want)
		}
	}
	if len(expectedUnsupported) != 0 {
		t.Fatalf("unsupported roots changed; update documented capabilities: %v", expectedUnsupported)
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
func contract(t *testing.T, id string) *acton.Contract {
	t.Helper()
	c := catalog.ByID(id)
	if c == nil {
		t.Fatal("contract missing", id)
	}
	return c
}
func method(t *testing.T, c *acton.Contract, name string) *acton.GetMethod {
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
				entries := []acton.MapEntry{}
				if populated {
					key := cell.BeginCell()
					check(t, key.StoreInt(-1, 8))
					check(t, key.StoreUInt(42, 256))
					check(t, dict.Set(key.EndCell(), cell.BeginCell().EndCell()))
					entries = append(entries, acton.MapEntry{Key: acton.Bits{Bits: 264, Hex: "ff" + strings.Repeat("00", 31) + "2a"}, Value: nil})
				}
				b := cell.BeginCell()
				check(t, b.StoreUInt(41, 32))
				check(t, b.StoreUInt(698983191, 32))
				check(t, b.StoreUInt(5, 256))
				check(t, b.StoreDict(dict))
				root := b.EndCell()
				want := map[string]any{"seqno": "41", "subwalletId": "698983191", "publicKey": "5", "plugins": entries}
				got, err := acton.DecodeStorage(c, boc(root))
				check(t, err)
				same(t, got, want)
				encoded, err := c.Storage.Encode(want)
				check(t, err)
				if !bytes.Equal(encoded.Hash(), root.Hash()) {
					t.Fatal("storage differs from independent golden layout")
				}
			}
			getter := method(t, c, "get_plugin_list")
			stack := []acton.StackValue{{Type: "tuple", Value: []acton.StackValue{{Type: "tuple", Value: []acton.StackValue{{Type: "int", Value: "-1"}, {Type: "int", Value: "42"}}}, {Type: "null"}}}}
			got, err := getter.DecodeResult(stack)
			check(t, err)
			same(t, got, []any{map[string]any{"workchain": "-1", "address": "42"}})
			if _, err := getter.DecodeResult(append(stack, acton.StackValue{Type: "null"})); err == nil {
				t.Fatal("getter ignored trailing stack")
			}
			args, err := method(t, c, "is_plugin_installed").EncodeArgs(map[string]any{"workchain": "-1", "addrHash": "42"})
			check(t, err)
			same(t, args, []acton.StackValue{{Type: "int", Value: "-1"}, {Type: "int", Value: "42"}})
		})
	}
}

// Query ID and expected decoded payload are the Wallet V5 UI golden. The
// independent bit builder also checks the published extn opcode and Maybe refs.
func TestWalletV5MessageGolden(t *testing.T) {
	c := contract(t, "wallets.WalletV5r1")
	b := cell.BeginCell()
	check(t, b.StoreUInt(0x6578746e, 32))
	check(t, b.StoreUInt(427, 64))
	check(t, b.StoreMaybeRef(nil))
	check(t, b.StoreMaybeRef(nil))
	root := b.EndCell()
	want := map[string]any{"queryId": "427", "outActions": nil, "extendedActions": nil}
	decoded, err := acton.DecodeMessage(c, "incoming_messages", boc(root))
	check(t, err)
	if decoded.Type.Name != "WalletExtensionActionV5r1" {
		t.Fatal(decoded.Type)
	}
	same(t, decoded.Value, want)
	var binding *acton.Binding
	for i := range c.Messages["incoming_messages"] {
		if c.Messages["incoming_messages"][i].Type.Name == decoded.Type.Name {
			binding = &c.Messages["incoming_messages"][i]
		}
	}
	if binding == nil {
		t.Fatal("message binding missing")
	}
	encoded, err := binding.Encode(want)
	check(t, err)
	if !bytes.Equal(encoded.Hash(), root.Hash()) {
		t.Fatal("message differs from golden layout")
	}
	check(t, b.StoreBoolBit(true))
	if _, err := acton.DecodeMessage(c, "incoming_messages", boc(b.EndCell())); err == nil {
		t.Fatal("message ignored trailing bit")
	}
}

// WalletTg is the real catalog entry that emits fully qualified instantiation
// names and a cell-only client override for the raw bulk-message slice.
func TestWalletTgGenericBulk(t *testing.T) {
	c := contract(t, "wallets.WalletTg")
	header := map[string]any{"subwalletId": "698983191", "validUntil": "1788865000", "seqno": "41"}
	message := cell.BeginCell()
	check(t, message.StoreUInt(0, 32))
	messageCell := message.EndCell()
	items := []any{}
	for _, mode := range []string{"0", "1", "3", "128"} {
		items = append(items, map[string]any{"sendMode": mode, "messageCell": boc(messageCell)})
	}
	want := map[string]any{"signature": acton.Bits{Bits: 512, Hex: strings.Repeat("00", 64)}, "request": acton.UnionValue{Type: "SendBulkMessagesRequestE", Value: map[string]any{"header": header, "msgArr": items}}}
	root, err := c.Messages["incoming_external"][0].Encode(want)
	check(t, err)
	decoded, err := acton.DecodeMessage(c, "incoming_external", boc(root))
	check(t, err)
	same(t, decoded.Value, want)
	s := root.BeginParse()
	_, err = s.LoadSlice(512)
	check(t, err)
	op, err := s.LoadUInt(32)
	check(t, err)
	if op != 1938386549 {
		t.Fatal("union prefix duplicated or wrong", op)
	}
	_, err = s.LoadSlice(96)
	check(t, err)
	n, err := s.LoadUInt(8)
	check(t, err)
	if n != 4 {
		t.Fatal(n)
	}
	head, err := s.LoadMaybeRef()
	check(t, err)
	tail, err := head.LoadMaybeRef()
	check(t, err)
	if tail == nil || head.RefsNum() != 1 || tail.RefsNum() != 3 {
		t.Fatal("continuation ref is not before element refs")
	}
	mode, err := head.LoadUInt(8)
	check(t, err)
	if mode != 0 {
		t.Fatal(mode)
	}
}
