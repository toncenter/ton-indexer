package emulated

import (
	"os"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestActionsWireFixture checks the action writer and decoder wire contract.
//
// Generate testdata/actions_wire.msgpack with:
//
//	ton-mch-dev-engine --actions-msgpack-out ton-index-go/index/emulated/testdata/actions_wire.msgpack
//
// The fixture covers every writer wire type. Regenerate it when
// wire_fixture_action changes.
func TestActionsWireFixture(t *testing.T) {
	blob, err := os.ReadFile("testdata/actions_wire.msgpack")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	var actions []Action
	if err := msgpack.Unmarshal(blob, &actions); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("got %d actions, want 1", len(actions))
	}
	a := actions[0]

	str := func(name string, got *string, want string) {
		t.Helper()
		if got == nil {
			t.Errorf("%s is nil, want %q", name, want)
		} else if *got != want {
			t.Errorf("%s = %q, want %q", name, *got, want)
		}
	}

	// --- fixed keys ---------------------------------------------------------
	if a.Type != "jetton_transfer" {
		t.Errorf("type = %q, want jetton_transfer", a.Type)
	}
	if !a.Success {
		t.Error("success = false, want true")
	}
	if a.StartLt == nil || *a.StartLt != 58291746000001 {
		t.Errorf("start_lt = %v, want 58291746000001", a.StartLt)
	}
	if a.EndUtime == nil || *a.EndUtime != 1753900002 {
		t.Errorf("end_utime = %v, want 1753900002", a.EndUtime)
	}
	// min over the view's node finalities: {confirmed, finalized} -> confirmed.
	if a.Finality != 1 {
		t.Errorf("finality = %d, want 1 (confirmed)", a.Finality)
	}
	if len(a.TxHashes) != 2 || a.TxHashes[0] != "txA" {
		t.Errorf("tx_hashes = %v", a.TxHashes)
	}
	if len(a.Accounts) != 2 {
		t.Errorf("accounts = %v", a.Accounts)
	}
	// Child-action recursion: the row is a leg some non-v1_ops action absorbed.
	str("parent_action_id", a.ParentActionId, "QIoX0flkEEHBMhk2Z6QlnxrXlWXACZBCee6T8YKYSKE=")
	if len(a.AncestorType) != 1 || a.AncestorType[0] != "jvault_stake" {
		t.Errorf("ancestor_type = %v", a.AncestorType)
	}

	// --- the natural-types rule --------------------------------------------
	// opcode is *uint32: a natural msgpack integer.
	if a.Opcode == nil || *a.Opcode != 0x0f8a7ea5 {
		t.Errorf("opcode = %v, want 0x0f8a7ea5", a.Opcode)
	}
	// amount is *string and 256-bit: only a decimal string survives.
	str("amount", a.Amount, "340282366920938463463374607431768211456")

	if a.JettonTransferData == nil {
		t.Fatal("jetton_transfer_data is nil")
	}
	// query_id sits above 2^63, so it cannot be an int64 in either direction.
	str("jetton_transfer_data.query_id", a.JettonTransferData.QueryId, "18446744073709551615")
	str("jetton_transfer_data.forward_amount", a.JettonTransferData.ForwardAmount, "400000000")

	// Bounded ints into *int32 / *int64, decoded from natural integers.
	if a.LayerzeroPacketData == nil {
		t.Fatal("layerzero_packet_data is nil")
	}
	if a.LayerzeroPacketData.SrcEid == nil || *a.LayerzeroPacketData.SrcEid != 30101 {
		t.Errorf("src_eid = %v, want 30101", a.LayerzeroPacketData.SrcEid)
	}
	if a.LayerzeroPacketData.Nonce == nil || *a.LayerzeroPacketData.Nonce != 1234567890123 {
		t.Errorf("nonce = %v, want 1234567890123", a.LayerzeroPacketData.Nonce)
	}

	// uint64 keys use decimal strings: send_request_id sits above 2^63 and
	// the fees are uint128-domain, so the writer cannot emit them as integers.
	if a.LayerzeroSendData == nil {
		t.Fatal("layerzero_send_data is nil")
	}
	if a.LayerzeroSendData.SendRequestId == nil ||
		*a.LayerzeroSendData.SendRequestId != 18446744073709551614 {
		t.Errorf("send_request_id = %v, want 18446744073709551614", a.LayerzeroSendData.SendRequestId)
	}
	if a.LayerzeroSendData.NativeFee == nil || *a.LayerzeroSendData.NativeFee != 14835102020 {
		t.Errorf("native_fee = %v, want 14835102020", a.LayerzeroSendData.NativeFee)
	}
	if a.LayerzeroSendData.ZroFee == nil || *a.LayerzeroSendData.ZroFee != 0 {
		t.Errorf("zro_fee = %v, want 0", a.LayerzeroSendData.ZroFee)
	}
	str("layerzero_send_data.msglib", a.LayerzeroSendData.Msglib, "0x63bdfbf347f883dd")

	// LayerZero nonce values use decimal strings because key rules apply at every depth.

	// A bounded *int64 beside the status STRING the parser maps from the code.
	if a.LayerzeroDvnVerifyData == nil {
		t.Fatal("layerzero_dvn_verify_data is nil")
	}
	if a.LayerzeroDvnVerifyData.Nonce == nil || *a.LayerzeroDvnVerifyData.Nonce != 125 {
		t.Errorf("dvn nonce = %v, want 125", a.LayerzeroDvnVerifyData.Nonce)
	}
	str("layerzero_dvn_verify_data.status", a.LayerzeroDvnVerifyData.Status, "succeeded")

	// All ten tonco_deploy_pool fields are *string, but only the Q64.96 price
	// exceeds int64 and therefore has to travel as a decimal string; the u16
	// fees and the int24 tick spacing stay natural integers and reach the same
	// *string field through the flexible decoder.
	if a.ToncoDeployPoolData == nil {
		t.Fatal("tonco_deploy_pool_data is nil")
	}
	str("tonco_deploy_pool_data.initial_price_x96",
		a.ToncoDeployPoolData.InitialPriceX96, "2159938633973433351177391787024")
	str("tonco_deploy_pool_data.protocol_fee", a.ToncoDeployPoolData.ProtocolFee, "33268")
	str("tonco_deploy_pool_data.tick_spacing", a.ToncoDeployPoolData.TickSpacing, "60")
	if a.ToncoDeployPoolData.PoolActive == nil || !*a.ToncoDeployPoolData.PoolActive {
		t.Errorf("pool_active = %v, want true", a.ToncoDeployPoolData.PoolActive)
	}

	// These Cocoon composites cover every family wire shape. The nonce exceeds int64.
	if a.CocoonWorkerPayoutData == nil {
		t.Fatal("cocoon_worker_payout_data is nil")
	}
	str("cocoon_worker_payout_data.payout_type", a.CocoonWorkerPayoutData.PayoutType, "last")
	str("cocoon_worker_payout_data.new_tokens",
		a.CocoonWorkerPayoutData.NewTokens, "9229614747703451079")
	str("cocoon_worker_payout_data.worker_tokens",
		a.CocoonWorkerPayoutData.WorkerTokens, "73063940556")
	// *int64 beside those *string amounts: the u2 worker state stays an integer.
	if a.CocoonWorkerPayoutData.WorkerState == nil || *a.CocoonWorkerPayoutData.WorkerState != 2 {
		t.Errorf("worker_state = %v, want 2", a.CocoonWorkerPayoutData.WorkerState)
	}
	if a.CocoonClientRegisterData == nil {
		t.Fatal("cocoon_client_register_data is nil")
	}
	str("cocoon_client_register_data.nonce",
		a.CocoonClientRegisterData.Nonce, "11924145372215500834")
	// The family's only *bool, and its only u32 reaching a *int64.
	if a.CocoonClientRequestRefundData == nil {
		t.Fatal("cocoon_client_request_refund_data is nil")
	}
	if a.CocoonClientRequestRefundData.ViaWallet == nil ||
		!*a.CocoonClientRequestRefundData.ViaWallet {
		t.Errorf("via_wallet = %v, want true", a.CocoonClientRequestRefundData.ViaWallet)
	}
	if a.CocoonUnregisterProxyData == nil {
		t.Fatal("cocoon_unregister_proxy_data is nil")
	}
	if a.CocoonUnregisterProxyData.Seqno == nil || *a.CocoonUnregisterProxyData.Seqno != 1 {
		t.Errorf("seqno = %v, want 1", a.CocoonUnregisterProxyData.Seqno)
	}

	if a.ChangeDnsRecordData == nil {
		t.Fatal("change_dns_record_data is nil")
	}
	if a.ChangeDnsRecordData.Flags == nil || *a.ChangeDnsRecordData.Flags != 7 {
		t.Errorf("flags = %v, want 7", a.ChangeDnsRecordData.Flags)
	}

	// --- []string, whose elements bypass the flexible decoders --------------
	if a.JvaultClaimData == nil {
		t.Fatal("jvault_claim_data is nil")
	}
	want := []string{"1000", "340282366920938463463374607431768211456"}
	if len(a.JvaultClaimData.ClaimedAmounts) != len(want) {
		t.Fatalf("claimed_amounts = %v, want %v", a.JvaultClaimData.ClaimedAmounts, want)
	}
	for i, w := range want {
		if a.JvaultClaimData.ClaimedAmounts[i] != w {
			t.Errorf("claimed_amounts[%d] = %q, want %q", i, a.JvaultClaimData.ClaimedAmounts[i], w)
		}
	}
	if len(a.JvaultClaimData.ClaimedJettons) != 2 {
		t.Errorf("claimed_jettons = %v", a.JvaultClaimData.ClaimedJettons)
	}
	// The whitelist's accounts_added is the second []string on the wire, and the
	// only one carrying ADDRESSES rather than decimal amounts.
	if a.VestingAddWhitelistData == nil {
		t.Fatal("vesting_add_whitelist_data is nil")
	}
	wantAccounts := []string{"0:" + strings.Repeat("A", 64), "0:" + strings.Repeat("B", 64)}
	if len(a.VestingAddWhitelistData.AccountsAdded) != len(wantAccounts) {
		t.Fatalf("accounts_added = %v, want %v",
			a.VestingAddWhitelistData.AccountsAdded, wantAccounts)
	}
	for i, w := range wantAccounts {
		if a.VestingAddWhitelistData.AccountsAdded[i] != w {
			t.Errorf("accounts_added[%d] = %q, want %q",
				i, a.VestingAddWhitelistData.AccountsAdded[i], w)
		}
	}

	if a.MultisigCreateOrderData == nil || a.MultisigApproveData == nil ||
		a.MultisigExecuteData == nil {
		t.Fatal("multisig wire composites are nil")
	}
	str("multisig_create_order_data.query_id",
		a.MultisigCreateOrderData.QueryId, "18446744073709551615")
	str("multisig_create_order_data.order_seqno", a.MultisigCreateOrderData.OrderSeqno,
		"115792089237316195423570985008687907853269984665640564039457584007913129639935")
	if a.MultisigCreateOrderData.IsCreatedBySigner == nil ||
		!*a.MultisigCreateOrderData.IsCreatedBySigner {
		t.Errorf("is_created_by_signer = %v, want true", a.MultisigCreateOrderData.IsCreatedBySigner)
	}
	if a.MultisigApproveData.SignerIndex == nil || *a.MultisigApproveData.SignerIndex != -1 {
		t.Errorf("signer_index = %v, want -1", a.MultisigApproveData.SignerIndex)
	}
	str("multisig_execute_data.order_seqno", a.MultisigExecuteData.OrderSeqno, "899")
	str("multisig_execute_data.signers_hash", a.MultisigExecuteData.SignersHash,
		"C32M4/TYDdcDTvMVw09kw5jnsfQJaZGrLCsRJuShiZQ=")

	// --- ints reaching *string fields, via decodeFlexString ------------------
	if a.NftTransferData == nil {
		t.Fatal("nft_transfer_data is nil")
	}
	// A float-backed index: msgpack-c normalizes the integral double to an
	// integer, and the flexible string decoder renders it.
	str("nft_transfer_data.nft_item_index", a.NftTransferData.NftItemIndex, "1037")
	str("nft_transfer_data.price", a.NftTransferData.Price, "2500000000")
	if a.DexDepositLiquidityData == nil {
		t.Fatal("dex_deposit_liquidity_data is nil")
	}
	// Negative int24 into a *string field.
	str("dex_deposit_liquidity_data.tick_lower", a.DexDepositLiquidityData.TickLower, "-887220")
	str("dex_deposit_liquidity_data.nft_index", a.DexDepositLiquidityData.NFTIndex, "4242")

	// --- dex_wallet_1 / dex_wallet_2 ----------------------------------------
	// Pool-side fields in the dex_withdraw_liquidity_details composite.
	if a.DexWithdrawLiquidityData == nil {
		t.Fatal("dex_withdraw_liquidity_data is nil")
	}
	str("dex_withdraw_liquidity_data.dex_wallet_1", a.DexWithdrawLiquidityData.DexWallet1,
		"0:"+strings.Repeat("A", 64))
	str("dex_withdraw_liquidity_data.dex_wallet_2", a.DexWithdrawLiquidityData.DexWallet2,
		"0:"+strings.Repeat("B", 64))
	str("dex_withdraw_liquidity_data.lp_tokens_burnt", a.DexWithdrawLiquidityData.LpTokensBurnt,
		"123456789")

	// These fields must survive conversion into the API details struct.
	raw, err := a.ToRawAction()
	if err != nil {
		t.Fatalf("ToRawAction: %v", err)
	}
	if raw.DexWithdrawLiquidityDataDexWallet1 == nil {
		t.Error("ToRawAction dropped dex_wallet_1")
	}
	if raw.DexWithdrawLiquidityDataDexWallet2 == nil {
		t.Error("ToRawAction dropped dex_wallet_2")
	}

	// --- unset composites stay nil, not zero values -------------------------
	if a.StakingData != nil {
		t.Error("staking_data decoded non-nil, but the writer omitted it")
	}
	if a.EvaaWithdrawData != nil {
		t.Error("evaa_withdraw_data decoded non-nil, but the writer omitted it")
	}
}
