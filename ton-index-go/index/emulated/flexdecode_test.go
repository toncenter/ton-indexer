package emulated

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// pythonExclusions lists top-level fields kept numeric by the Python encoder.
var pythonExclusions = map[string]bool{
	"start_lt": true, "end_lt": true, "start_utime": true, "end_utime": true,
	"opcode": true, "trace_start_lt": true, "finality": true, "trace_end_lt": true,
	"trace_end_utime": true, "mc_seqno_end": true, "trace_mc_seqno_end": true,
}

// pythonStyle reproduces the production Python numeric conversion.
func pythonStyle(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		if pythonExclusions[k] {
			out[k] = v
		} else {
			out[k] = pythonConvert(v)
		}
	}
	return out
}

func pythonConvert(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, item := range t {
			out[k] = pythonConvert(item)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, item := range t {
			out[i] = pythonConvert(item)
		}
		return out
	case bool, string, nil, []string:
		return v
	default:
		return fmt.Sprint(v)
	}
}

// pythonStyleAction applies Python's string encoding, including an integral
// float with a ".0" suffix.
func pythonStyleAction() map[string]interface{} {
	m := pythonStyle(naturalAction())
	m["change_dns_record_data"].(map[string]interface{})["flags"] = "1.0"
	return m
}

// naturalAction carries numeric fields in their native msgpack forms.
func naturalAction() map[string]interface{} {
	return map[string]interface{}{
		"action_id":                "9C7SqQ==",
		"type":                     "change_dns_record",
		"trace_id":                 "dHJhY2VfaWQ=",
		"trace_external_hash":      "ZXh0X2hhc2g=",
		"trace_external_hash_norm": "ZXh0X2hhc2hfbm9ybQ==",
		"tx_hashes":                []string{"aGFzaDE=", "aGFzaDI="},
		"success":                  true,

		// top-level numbers Python keeps native
		"start_lt":           uint64(58913247000001),
		"end_lt":             uint64(58913247000007),
		"start_utime":        uint32(1753800000),
		"end_utime":          uint32(1753800003),
		"trace_start_lt":     uint64(58913247000000),
		"opcode":             uint32(0x0f8a7ea5),
		"finality":           1,
		"trace_mc_seqno_end": uint32(48210934),

		// top-level *string fields Python stringifies
		"value":  uint64(1500000000),
		"amount": "115792089237316195423570985008687907853269984665640564039457584007913129639935",

		"source":      "0:9F9C2E1B0D4A6F8E3C5B7A1D2E4F60718293A4B5C6D7E8F90A1B2C3D4E5F6071",
		"destination": "0:1122334455667788990011223344556677889900112233445566778899001122",
		"asset":       "0:AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899",

		"change_dns_record_data": map[string]interface{}{
			"key":          "d2FsbGV0",
			"value_schema": "DNSNextResolver",
			"value":        "0:1122334455667788990011223344556677889900112233445566778899001122",
			"flags":        int64(1),
		},

		"multisig_create_order_data": map[string]interface{}{
			"query_id":             uint64(17),
			"order_seqno":          uint64(4),
			"is_created_by_signer": true,
			"creator_index":        int64(2),
			"expiration_date":      int64(1753900000),
			"order_boc":            "te6cckEB",
		},
		"multisig_approve_data": map[string]interface{}{
			"signer_index": int64(3),
			"exit_code":    int32(0),
		},
		"multisig_execute_data": map[string]interface{}{
			"query_id":        uint64(18),
			"order_seqno":     uint64(5),
			"expiration_date": int64(1753900001),
			"approvals_num":   int64(2),
		},
		"layerzero_send_data": map[string]interface{}{
			"send_request_id": uint64(99),
			"native_fee":      uint64(120000000),
			"zro_fee":         uint64(0),
			"msglib":          "0:2233445566778899001122334455667788990011223344556677889900112233",
		},
		"layerzero_packet_data": map[string]interface{}{
			"src_eid": int32(30343),
			"dst_eid": int32(30101),
			"nonce":   int64(7),
		},
		"layerzero_dvn_verify_data": map[string]interface{}{
			"nonce":  int64(8),
			"status": "succeeded",
		},
		"cocoon_worker_payout_data": map[string]interface{}{
			"payout_type":   "reward",
			"worker_state":  int64(2),
			"worker_tokens": uint64(4200000000),
		},
		"cocoon_unregister_proxy_data": map[string]interface{}{
			"query_id": uint64(21),
			"seqno":    int64(11),
		},
		"jvault_stake_data": map[string]interface{}{
			"period":               int64(2592000),
			"minted_stake_jettons": uint64(1000000000),
		},
	}
}

func mustMarshal(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := msgpack.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func decodeAction(t *testing.T, m map[string]interface{}) Action {
	t.Helper()
	var a Action
	if err := msgpack.Unmarshal(mustMarshal(t, m), &a); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return a
}

// TestOldAndNewStyleDecodeIdentically checks both numeric wire representations.
func TestOldAndNewStyleDecodeIdentically(t *testing.T) {
	natural := naturalAction()
	old := decodeAction(t, pythonStyleAction())
	new_ := decodeAction(t, natural)

	if !reflect.DeepEqual(old, new_) {
		t.Fatalf("python-style and natural-style decode differ:\nold = %+v\nnew = %+v", old, new_)
	}

	// Python-style scalar values decode without changing their values.
	if old.ActionId != "9C7SqQ==" || old.Type != "change_dns_record" || !old.Success {
		t.Errorf("scalars: %+v", old)
	}
	if got := old.StartLt; got == nil || *got != 58913247000001 {
		t.Errorf("start_lt = %v", got)
	}
	if got := old.Opcode; got == nil || *got != 0x0f8a7ea5 {
		t.Errorf("opcode = %v", got)
	}
	if old.Finality != 1 {
		t.Errorf("finality = %v", old.Finality)
	}
	if got := old.Value; got == nil || *got != "1500000000" {
		t.Errorf("value = %v", got)
	}
	if got := old.Amount; got == nil || *got != "115792089237316195423570985008687907853269984665640564039457584007913129639935" {
		t.Errorf("amount = %v", got)
	}
	if !reflect.DeepEqual(old.TxHashes, []string{"aGFzaDE=", "aGFzaDI="}) {
		t.Errorf("tx_hashes = %v", old.TxHashes)
	}

	// Composite numeric fields accept Python string encodings.
	if d := old.MultisigCreateOrderData; d == nil || *d.CreatorIndex != 2 || *d.ExpirationDate != 1753900000 || *d.OrderSeqno != "4" {
		t.Errorf("multisig_create_order_data = %+v", d)
	}
	if d := old.MultisigApproveData; d == nil || *d.SignerIndex != 3 || *d.ExitCode != 0 {
		t.Errorf("multisig_approve_data = %+v", d)
	}
	if d := old.MultisigExecuteData; d == nil || *d.ExpirationDate != 1753900001 || *d.ApprovalsNum != 2 {
		t.Errorf("multisig_execute_data = %+v", d)
	}
	if d := old.LayerzeroSendData; d == nil || *d.SendRequestId != 99 || *d.NativeFee != 120000000 || *d.ZroFee != 0 {
		t.Errorf("layerzero_send_data = %+v", d)
	}
	if d := old.LayerzeroDvnVerifyData; d == nil || *d.Nonce != 8 {
		t.Errorf("layerzero_dvn_verify_data = %+v", d)
	}
	if d := old.CocoonWorkerPayoutData; d == nil || *d.WorkerState != 2 || *d.WorkerTokens != "4200000000" {
		t.Errorf("cocoon_worker_payout_data = %+v", d)
	}
	if d := old.CocoonUnregisterProxyData; d == nil || *d.Seqno != 11 {
		t.Errorf("cocoon_unregister_proxy_data = %+v", d)
	}
	// These fields also use decimal-string encoding.
	if d := old.JvaultStakeData; d == nil || *d.Period != 2592000 {
		t.Errorf("jvault_stake_data = %+v", d)
	}
	if d := old.LayerzeroPacketData; d == nil || *d.SrcEid != 30343 || *d.DstEid != 30101 || *d.Nonce != 7 {
		t.Errorf("layerzero_packet_data = %+v", d)
	}

	// Integral floating-point spellings decode into integer fields.
	if d := old.ChangeDnsRecordData; d == nil || d.Flags == nil || *d.Flags != 1 {
		t.Fatalf("change_dns_record_data = %+v", d)
	}
}

// TestDnsFlagsForms checks every supported flags representation through RawAction.
func TestDnsFlagsForms(t *testing.T) {
	for _, tc := range []struct {
		name  string
		flags interface{}
		want  int64
	}{
		{"python str(float)", "1.0", 1},
		{"decimal string", "3", 3},
		{"native int", int64(3), 3},
		{"exponent form", "1e+16", 10000000000000000},
		{"negative", "-1", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := decodeAction(t, map[string]interface{}{
				"action_id": "a", "type": "change_dns_record",
				"change_dns_record_data": map[string]interface{}{"flags": tc.flags},
			})
			raw, err := a.ToRawAction()
			if err != nil {
				t.Fatalf("ToRawAction: %v", err)
			}
			if raw.ChangeDNSRecordFlags == nil || *raw.ChangeDNSRecordFlags != tc.want {
				t.Fatalf("flags = %v, want %d", raw.ChangeDNSRecordFlags, tc.want)
			}
		})
	}

	// absent stays nil
	a := decodeAction(t, map[string]interface{}{
		"action_id": "a", "type": "change_dns_record",
		"change_dns_record_data": map[string]interface{}{"key": "d2FsbGV0"},
	})
	raw, err := a.ToRawAction()
	if err != nil {
		t.Fatalf("ToRawAction: %v", err)
	}
	if raw.ChangeDNSRecordFlags != nil {
		t.Fatalf("absent flags = %v, want nil", *raw.ChangeDNSRecordFlags)
	}
}

// TestNativeNumberIntoStringField checks decimal rendering for native integers.
func TestNativeNumberIntoStringField(t *testing.T) {
	a := decodeAction(t, map[string]interface{}{
		"action_id": "a",
		"type":      "jetton_transfer",
		"value":     uint64(1) << 63, // Uint64 code: must not read back as negative
		"amount":    int64(-5),       // Int8 code
		"jetton_transfer_data": map[string]interface{}{
			"forward_amount": uint32(1000000),
			"query_id":       0, // positive fixnum
		},
		"nft_transfer_data": map[string]interface{}{
			"nft_item_index": uint64(18446744073709551615),
			"price":          float64(1037),
		},
	})
	checks := []struct {
		got  *string
		want string
	}{
		{a.Value, "9223372036854775808"},
		{a.Amount, "-5"},
		{a.JettonTransferData.ForwardAmount, "1000000"},
		{a.JettonTransferData.QueryId, "0"},
		{a.NftTransferData.NftItemIndex, "18446744073709551615"},
		{a.NftTransferData.Price, "1037"},
	}
	for i, c := range checks {
		if c.got == nil || *c.got != c.want {
			t.Errorf("check %d: got %v, want %q", i, c.got, c.want)
		}
	}
}

// TestFlexDecodeRejectsNonNumbers checks invalid input and nil handling.
func TestFlexDecodeRejectsNonNumbers(t *testing.T) {
	for _, bad := range []interface{}{"abc", "1.5", "", "0x10", true} {
		var a Action
		err := msgpack.Unmarshal(mustMarshal(t, map[string]interface{}{
			"multisig_approve_data": map[string]interface{}{"signer_index": bad},
		}), &a)
		if err == nil {
			t.Errorf("signer_index = %#v decoded to %v, want error", bad, *a.MultisigApproveData.SignerIndex)
		}
	}

	a := decodeAction(t, map[string]interface{}{
		"value": nil,
		"multisig_approve_data": map[string]interface{}{
			"signer_index": nil,
			"exit_code":    int32(-7),
		},
	})
	if a.Value != nil {
		t.Errorf("value = %v, want nil", *a.Value)
	}
	if a.MultisigApproveData.SignerIndex != nil {
		t.Errorf("signer_index = %v, want nil", *a.MultisigApproveData.SignerIndex)
	}
	if *a.MultisigApproveData.ExitCode != -7 {
		t.Errorf("exit_code = %v", *a.MultisigApproveData.ExitCode)
	}
}

// TestControlStockDecoderStillRejectsStrings checks exact-type registration.
func TestControlStockDecoderStillRejectsStrings(t *testing.T) {
	type stockInt64 int64
	var v struct {
		SignerIndex *stockInt64 `msgpack:"signer_index"`
	}
	blob := mustMarshal(t, map[string]interface{}{"signer_index": "3"})
	if err := msgpack.Unmarshal(blob, &v); err == nil {
		t.Fatalf("stock decoder accepted %q -> %v", "3", *v.SignerIndex)
	}
	var flex struct {
		SignerIndex *int64 `msgpack:"signer_index"`
	}
	if err := msgpack.Unmarshal(blob, &flex); err != nil {
		t.Fatalf("flex decoder rejected %q: %v", "3", err)
	}
}

// TestParseFlexBoundaries checks rounded float bounds and exact decimal maxima.
func TestParseFlexBoundaries(t *testing.T) {
	// 2^63-1024 and 2^64-2048: the largest value of each type float64 holds exactly
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"9223372036854774784.0", 9223372036854774784},
		{"-9223372036854775808.0", math.MinInt64},
		// MinInt64 is exactly representable and remains in range.
		{"-9223372036854775809.0", math.MinInt64},
	} {
		got, err := parseFlexInt(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("parseFlexInt(%q) = %d, %v; want %d, nil", tc.in, got, err, tc.want)
		}
	}
	// -2^63-2048 is the next float64 below MinInt64.
	for _, in := range []string{"9223372036854775807.0", "9223372036854775808.0", "-9223372036854777856.0"} {
		if got, err := parseFlexInt(in); err == nil {
			t.Errorf("parseFlexInt(%q) = %d, want error", in, got)
		}
	}

	if got, err := parseFlexUint("18446744073709549568.0"); err != nil || got != 18446744073709549568 {
		t.Errorf("parseFlexUint = %d, %v; want 18446744073709549568, nil", got, err)
	}
	for _, in := range []string{"18446744073709551615.0", "18446744073709551616.0", "-1.0"} {
		if got, err := parseFlexUint(in); err == nil {
			t.Errorf("parseFlexUint(%q) = %d, want error", in, got)
		}
	}

	// Plain decimal maxima use exact ParseInt and ParseUint paths.
	if got, err := parseFlexInt("9223372036854775807"); err != nil || got != math.MaxInt64 {
		t.Errorf("parseFlexInt(MaxInt64) = %d, %v", got, err)
	}
	if got, err := parseFlexUint("18446744073709551615"); err != nil || got != math.MaxUint64 {
		t.Errorf("parseFlexUint(MaxUint64) = %d, %v", got, err)
	}
}

func TestNormAddr(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"0:9f9c2e1b0d4a6f8e3c5b7a1d2e4f60718293a4b5c6d7e8f90a1b2c3d4e5f6071",
			"0:9F9C2E1B0D4A6F8E3C5B7A1D2E4F60718293A4B5C6D7E8F90A1B2C3D4E5F6071"},
		{"0:9F9C2E1B0D4A6F8E3C5B7A1D2E4F60718293A4B5C6D7E8F90A1B2C3D4E5F6071",
			"0:9F9C2E1B0D4A6F8E3C5B7A1D2E4F60718293A4B5C6D7E8F90A1B2C3D4E5F6071"},
		{"-1:3333333333333333333333333333333333333333333333333333333333333333",
			"-1:3333333333333333333333333333333333333333333333333333333333333333"},
		// not raw hex: left alone
		{"EQAvlWFDfGHYPGZbFCPnfnbeSuBnpXpvOMNTHKHNqm6ZKlU-", "EQAvlWFDfGHYPGZbFCPnfnbeSuBnpXpvOMNTHKHNqm6ZKlU-"},
		{"", ""},
		{"0:", "0:"},
		{"addr_none", "addr_none"},
	} {
		if got := normAddr(tc.in); got != tc.want {
			t.Errorf("normAddr(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	const (
		lower = "0:aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
		upper = "0:AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899"
	)
	a := decodeAction(t, map[string]interface{}{
		"action_id": "a", "type": "ton_transfer",
		"source":      lower,
		"destination": upper,
		"accounts":    []string{lower, upper},
		"vesting_add_whitelist_data": map[string]interface{}{
			"accounts_added": []string{lower},
		},
		"jvault_claim_data": map[string]interface{}{
			"claimed_jettons": []string{lower},
		},
	})
	raw, err := a.ToRawAction()
	if err != nil {
		t.Fatalf("ToRawAction: %v", err)
	}
	if raw.Source == nil || string(*raw.Source) != string(*raw.Destination) {
		t.Errorf("source %v not normalized to destination %v", raw.Source, raw.Destination)
	}
	// the four sites that bypass ptrAccountAddress
	if len(raw.Accounts) != 2 || string(raw.Accounts[0]) != upper || string(raw.Accounts[1]) != upper {
		t.Errorf("accounts = %v", raw.Accounts)
	}
	if len(raw.VestingAddWhitelistAccountsAdded) != 1 || string(raw.VestingAddWhitelistAccountsAdded[0]) != upper {
		t.Errorf("accounts_added = %v", raw.VestingAddWhitelistAccountsAdded)
	}
	if len(raw.JvaultClaimClaimedJettons) != 1 || string(raw.JvaultClaimClaimedJettons[0]) != upper {
		t.Errorf("claimed_jettons = %v", raw.JvaultClaimClaimedJettons)
	}
	// ComputePh must be populated: ToTransaction:1397 type-asserts Data unconditionally.
	node := TraceNode{Transaction: transaction{
		Account:     lower,
		Description: transactionDescr{ComputePh: computePhaseVar{Data: trComputePhaseSkipped{}}},
	}}
	tx, err := node.ToTransaction()
	if err != nil {
		t.Fatalf("ToTransaction: %v", err)
	}
	if string(tx.Account) != upper {
		t.Errorf("transaction account = %q", tx.Account)
	}
	if raw.Asset != nil {
		t.Errorf("absent asset = %v, want nil", *raw.Asset)
	}
}

// TestConvertHSetBothEncodings checks numeric encodings and compression states.
func TestConvertHSetBothEncodings(t *testing.T) {
	rootHash := make([]byte, 32)
	for i := range rootHash {
		rootHash[i] = byte(i)
	}
	rootKey := base64.StdEncoding.EncodeToString(rootHash)

	node := map[string]interface{}{
		"transaction": map[string]interface{}{
			"hash":       rootHash,
			"account":    "0:9F9C2E1B0D4A6F8E3C5B7A1D2E4F60718293A4B5C6D7E8F90A1B2C3D4E5F6071",
			"lt":         uint64(58913247000007),
			"now":        uint32(1753800003),
			"total_fees": uint64(3040000),
			"out_msgs":   []interface{}{},
		},
		"emulated":       false,
		"mc_block_seqno": uint32(48210934),
		"finality":       1,
		"block_id": map[string]interface{}{
			"workchain": int32(0),
			"shard":     uint64(0x8000000000000000),
			"seqno":     uint32(51230012),
		},
	}

	gzipBytes := func(b []byte) []byte {
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		if _, err := w.Write(b); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}

	build := func(actionMap map[string]interface{}, nodeMap map[string]interface{}, zip bool) map[string]string {
		actions := mustMarshal(t, []interface{}{actionMap})
		if zip {
			actions = gzipBytes(actions)
		}
		return map[string]string{
			"root_node": rootKey,
			rootKey:     string(mustMarshal(t, nodeMap)),
			"actions":   string(actions),
		}
	}

	natural := naturalAction()
	oldTrace, err := ConvertHSet(build(pythonStyleAction(), node, false), rootKey)
	if err != nil {
		t.Fatalf("python-style ConvertHSet: %v", err)
	}
	newTrace, err := ConvertHSet(build(natural, node, true), rootKey)
	if err != nil {
		t.Fatalf("natural-style gzipped ConvertHSet: %v", err)
	}
	if !reflect.DeepEqual(oldTrace, newTrace) {
		t.Fatalf("traces differ:\nold = %+v\nnew = %+v", oldTrace, newTrace)
	}

	if !oldTrace.Classified || len(oldTrace.Actions) != 1 || len(oldTrace.Nodes) != 1 {
		t.Fatalf("trace = %+v", oldTrace)
	}
	n := oldTrace.Nodes[0].Transaction
	if n.Lt != 58913247000007 || n.Now != 1753800003 || n.TotalFees != 3040000 {
		t.Errorf("node numerics: lt=%d now=%d fees=%d", n.Lt, n.Now, n.TotalFees)
	}
	if oldTrace.Nodes[0].BlockId.Shard != 0x8000000000000000 || oldTrace.Nodes[0].McBlockSeqno != 48210934 {
		t.Errorf("block_id = %+v", oldTrace.Nodes[0].BlockId)
	}
	if oldTrace.TraceId == nil || *oldTrace.TraceId != rootKey {
		t.Errorf("trace_id = %v", oldTrace.TraceId)
	}
	if got := oldTrace.Actions[0].TraceEndLt; got == nil || *got != 58913247000007 {
		t.Errorf("trace_end_lt = %v", got)
	}

	// the whole action still converts
	if _, err := oldTrace.Actions[0].ToRawAction(); err != nil {
		t.Fatalf("ToRawAction: %v", err)
	}
}
