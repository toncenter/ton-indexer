// ton-abi bridge: AbiValue to mch::Value adapter and production
// ABI parser rows. Registry keys are the stable, bare spec-facing declaration
// names.
#include "AbiBridge.h"

#include "common/refint.h"
#include "vm/cells/CellBuilder.h"

#include "cocoon_gen.h"
#include "coffee_gen.h"
#include "coffee_staking_withdraw3_gen.h"
#include "dedust_gen.h"
#include "dedust_v2_gen.h"
#include "evaa_gen.h"
#include "evaa_supply_forward_gen.h"
#include "jetton_gen.h"
#include "jvault_gen.h"
#include "jvault_payload_gen.h"
#include "multisig_gen.h"
#include "nft_sale_gen.h"
#include "pton_gen.h"
#include "stonfi_gen.h"
#include "subscriptions_gen.h"
#include "teleitem_gen.h"
#include "tonco_gen.h"
#include "tonstakers_gen.h"

#include <string>

namespace mch {

using ton_abi::AbiValue;
using ton_abi::AbiValueKind;
using ton_abi::AbiAddressKind;

namespace {

// Copy a bit slice into a fresh, root-hash-comparable cell. Use the non-throwing
// append operation to preserve adapter totality.
Value cell_from_slice(const td::Ref<vm::CellSlice> &slice) {
  vm::CellBuilder cb;
  if (slice.not_null() && !cb.append_cellslice_bool(*slice)) {
    return Value::make_cell(vm::CellBuilder().finalize());
  }
  return Value::make_cell(cb.finalize());
}

Value adapt_address(const ton_abi::AbiAddress &a) {
  switch (a.kind) {
    case AbiAddressKind::Std: {
      // "wc:hex" -> canonical "wc:HEX" (uppercase raw form, AccountId.as_str parity).
      std::string raw = std::to_string(a.workchain) + ":" + a.hash.to_hex();
      auto canon = normalize_raw_address(raw);
      return Value::make_account_raw(canon ? *canon : raw);
    }
    case AbiAddressKind::None:
      // Present-but-empty address (addr_none from any_address). This is not null;
      // faithful to AccountId(None) vs a null Value.
      return Value::make_account_none();
    case AbiAddressKind::Extern: {
      // There is no native external-address VType. Mirror
      // AbiValue's {"extern":{bits,value}} as an Obj without the pytoniq string.
      Value::Fields f;
      f.emplace_back("bits", Value::make_int(td::make_refint(a.ext_bits)));
      f.emplace_back("value", cell_from_slice(a.ext_value));
      return Value::make_obj(std::move(f));
    }
  }
  return Value::null();  // unreachable
}

}  // namespace

Value abi_value_to_mch(const AbiValue &v) {
  switch (v.kind) {
    case AbiValueKind::Int:
      return Value::make_int(v.int_v);
    case AbiValueKind::Bool:
      return Value::make_bool(v.bool_v);
    case AbiValueKind::Address:
      return adapt_address(v.address_v);
    case AbiValueKind::Cell:
      return Value::make_cell(v.cell_v);
    case AbiValueKind::CellOf: {
      // Mirror AbiValue dump {"ref": inner}. `inner` is always set by the
      // emitter for this kind; the guard keeps the adapter total (no UB) if a
      // future producer ever hands over a moved-from value.
      Value::Fields f;
      f.emplace_back("ref", v.inner ? abi_value_to_mch(*v.inner) : Value::null());
      return Value::make_obj(std::move(f));
    }
    case AbiValueKind::Bits:
      return cell_from_slice(v.bits_v);
    case AbiValueKind::String:
      // Raw bytes into Str (byte-preserving). The lossy-UTF8 decode is a
      // dump-side concern (AbiValue::to_json), not re-imposed here.
      return Value::make_str(v.string_v);
    case AbiValueKind::List: {
      std::vector<Value> items;
      items.reserve(v.list_v.size());
      for (const auto &e : v.list_v) {
        items.push_back(abi_value_to_mch(e));
      }
      return Value::make_list(std::move(items));
    }
    case AbiValueKind::Struct: {
      // Obj (attribute access), "$" = struct name FIRST, then fields in decl
      // order. This mirrors the AbiValue dump so a consumer can switch on ".$".
      Value::Fields f;
      f.emplace_back("$", Value::make_str(v.struct_name));
      for (const auto &kv : v.struct_fields) {
        f.emplace_back(kv.first, abi_value_to_mch(kv.second));
      }
      return Value::make_obj(std::move(f));
    }
    case AbiValueKind::Union: {
      // has_value_field variants only reach here: {"$":label,"value":inner}.
      // Struct-labeled variants are Struct-kind (carry their own "$"). Same
      // totality guard on `inner` as the CellOf branch.
      Value::Fields f;
      f.emplace_back("$", Value::make_str(v.union_label));
      f.emplace_back("value", v.inner ? abi_value_to_mch(*v.inner) : Value::null());
      return Value::make_obj(std::move(f));
    }
    case AbiValueKind::Void: {
      Value::Fields f;
      f.emplace_back("$", Value::make_str("void"));
      return Value::make_obj(std::move(f));
    }
    case AbiValueKind::Null:
      return Value::null();
    case AbiValueKind::Map: {
      // Mirror dump [[k,v],...] in wire order; List-of-pairs preserves non-string
      // key types (mch Dict keys are std::string only).
      std::vector<Value> pairs;
      pairs.reserve(v.map_entries.size());
      for (const auto &kv : v.map_entries) {
        std::vector<Value> pair;
        pair.push_back(abi_value_to_mch(kv.first));
        pair.push_back(abi_value_to_mch(kv.second));
        pairs.push_back(Value::make_list(std::move(pair)));
      }
      return Value::make_list(std::move(pairs));
    }
  }
  return Value::null();  // unreachable
}

namespace {

// One public logical parser, two honest wire prefixes. The generated API is
// per-struct, so retry the Ethena arm with a fresh slice when the standard
// arm's from_slice rejects its prefix.
td::Result<Value> abi_parse_jetton_internal_transfer(const td::Ref<vm::Cell> &body) {
  auto standard = abi_parse_body<ton_abi::gen::jetton::JettonInternalTransfer>(body);
  if (standard.is_ok()) {
    return standard;
  }
  auto error = standard.move_as_error();
  if (error.message().str().find("JettonInternalTransfer: prefix:") == std::string::npos) {
    return std::move(error);
  }
  return abi_parse_body<ton_abi::gen::jetton::JettonInternalTransferEthena>(body);
}

}  // namespace

const std::vector<std::pair<std::string, MsgParserFn>> &abi_message_parsers() {
  namespace cocoon = ton_abi::gen::cocoon;
  namespace coffee = ton_abi::gen::coffee;
  namespace coffee_w3 = ton_abi::gen::coffee_staking_withdraw3;
  namespace dedust = ton_abi::gen::dedust;
  namespace dedust_v2 = ton_abi::gen::dedust_v2;
  namespace evaa = ton_abi::gen::evaa;
  namespace evaa_forward = ton_abi::gen::evaa_supply_forward;
  namespace jetton = ton_abi::gen::jetton;
  namespace jvault = ton_abi::gen::jvault;
  namespace jvault_payload = ton_abi::gen::jvault_payload;
  namespace multisig = ton_abi::gen::multisig;
  namespace nft_sale = ton_abi::gen::nft_sale;
  namespace pton = ton_abi::gen::pton;
  namespace stonfi = ton_abi::gen::stonfi;
  namespace subscriptions = ton_abi::gen::subscriptions;
  namespace teleitem = ton_abi::gen::teleitem;
  namespace tonco = ton_abi::gen::tonco;
  namespace tonstakers = ton_abi::gen::tonstakers;

  static const std::vector<std::pair<std::string, MsgParserFn>> rows = {
      {"ClaimRewardMessage", &abi_parse_body<dedust_v2::ClaimRewardMessage>},
      {"PayoutRewardMessage", &abi_parse_body<dedust_v2::PayoutRewardMessage>},
      {"PayoutMessage", &abi_parse_body<dedust_v2::PayoutMessage>},
      {"JettonInternalTransfer", &abi_parse_jetton_internal_transfer},
      {"JettonBurn", &abi_parse_body<jetton::JettonBurn>},
      {"JettonNotify", &abi_parse_body<jetton::JettonNotify>},
      {"JettonMint", &abi_parse_body<jetton::JettonMint>},
      {"MultisigNewOrder", &abi_parse_body<multisig::MultisigNewOrder>},
      {"MultisigInitOrder", &abi_parse_body<multisig::MultisigInitOrder>},
      {"MultisigApprove", &abi_parse_body<multisig::MultisigApprove>},
      {"MultisigApproveRejected", &abi_parse_body<multisig::MultisigApproveRejected>},
      {"MultisigExecute", &abi_parse_body<multisig::MultisigExecute>},
      {"PTonTransfer", &abi_parse_body<pton::PTonTransfer>},
      {"StonfiV2ProvideLiquidity", &abi_parse_body<stonfi::StonfiV2ProvideLiquidity>},
      {"StonfiPaymentRequest", &abi_parse_body<stonfi::StonfiPaymentRequest>},
      {"StonfiSwapMessage", &abi_parse_body<stonfi::StonfiSwapMessage>},
      {"StonfiV2PayTo", &abi_parse_body<stonfi::StonfiV2PayTo>},
      {"DedustPayoutFromPool", &abi_parse_body<dedust::DedustPayoutFromPool>},
      {"DedustSwapNotification", &abi_parse_body<dedust::DedustSwapNotification>},
      {"DedustDepositLiquidityToPool", &abi_parse_body<dedust::DedustDepositLiquidityToPool>},
      {"DedustV2PayNative", &abi_parse_body<dedust_v2::DedustV2PayNative>},
      {"DedustV2SwapEvent", &abi_parse_body<dedust_v2::DedustV2SwapEvent>},
      {"DedustV2PayoutPositionFees", &abi_parse_body<dedust_v2::DedustV2PayoutPositionFees>},
      {"DedustV2Withdraw", &abi_parse_body<dedust_v2::DedustV2Withdraw>},
      {"DedustV2WithdrawalEvent", &abi_parse_body<dedust_v2::DedustV2WithdrawalEvent>},
      {"DedustV2CreditAsset", &abi_parse_body<dedust_v2::DedustV2CreditAsset>},
      {"DedustV2JoinLiquidity", &abi_parse_body<dedust_v2::DedustV2JoinLiquidity>},
      {"DedustV2DepositEvent", &abi_parse_body<dedust_v2::DedustV2DepositEvent>},
      {"CoffeeCreateVault", &abi_parse_body<coffee::CoffeeCreateVault>},
      {"CoffeeSwapEvent", &abi_parse_body<coffee::CoffeeSwapEvent>},
      {"CoffeeCreateLiquidityDepositoryRequest",
       &abi_parse_body<coffee::CoffeeCreateLiquidityDepositoryRequest>},
      {"CoffeeDepositLiquiditySuccessfulEvent",
       &abi_parse_body<coffee::CoffeeDepositLiquiditySuccessfulEvent>},
      {"CoffeeLiquidityWithdrawalEvent", &abi_parse_body<coffee::CoffeeLiquidityWithdrawalEvent>},
      {"CoffeeCreatePoolCreatorRequest", &abi_parse_body<coffee::CoffeeCreatePoolCreatorRequest>},
      {"CoffeeCreatePoolRequest", &abi_parse_body<coffee::CoffeeCreatePoolRequest>},
      {"CoffeeStakingClaimRewards", &abi_parse_body<coffee::CoffeeStakingClaimRewards>},
      {"CoffeeStakingPositionWithdraw2",
       &abi_parse_body<coffee::CoffeeStakingPositionWithdraw2>},
      {"CoffeeStakingPositionWithdraw3",
       &abi_parse_body<coffee_w3::CoffeeStakingPositionWithdraw3>},
      {"EvaaSupplyMaster", &abi_parse_body<evaa::EvaaSupplyMaster>},
      {"EvaaSupplySuccess", &abi_parse_body<evaa::EvaaSupplySuccess>},
      {"EvaaWithdrawMaster", &abi_parse_body<evaa::EvaaWithdrawMaster>},
      {"EvaaWithdrawCollateralized", &abi_parse_body<evaa::EvaaWithdrawCollateralized>},
      {"EvaaSupplyJettonForward", &abi_parse_body<evaa_forward::EvaaSupplyJettonForward>},
      {"JVaultUnstakeJettons", &abi_parse_body<jvault::JVaultUnstakeJettons>},
      {"JVaultUnstakeRequest", &abi_parse_body<jvault::JVaultUnstakeRequest>},
      {"JVaultClaim", &abi_parse_body<jvault::JVaultClaim>},
      {"JVaultStakePeriodPayload",
       &abi_parse_body<jvault_payload::JVaultStakePeriodPayload>},
      {"SubscriptionPaymentRequest",
       &abi_parse_body<subscriptions::SubscriptionPaymentRequest>},
      {"ToncoRouterV3SwapSourceWallet",
       &abi_parse_body<tonco::ToncoRouterV3SwapSourceWallet>},
      {"ToncoPoolV3StartBurn", &abi_parse_body<tonco::ToncoPoolV3StartBurn>},
      {"ToncoPositionNftV3PositionBurn",
       &abi_parse_body<tonco::ToncoPositionNftV3PositionBurn>},
      {"ToncoPoolV3Burn", &abi_parse_body<tonco::ToncoPoolV3Burn>},
      {"ToncoRouterV3CreatePool", &abi_parse_body<tonco::ToncoRouterV3CreatePool>},
      {"ToncoPoolV3Init", &abi_parse_body<tonco::ToncoPoolV3Init>},
      {"TeleitemStartAuction", &abi_parse_body<teleitem::TeleitemStartAuction>},
      {"NftOwnershipAssignedPrevOwner",
       &abi_parse_body<nft_sale::NftOwnershipAssignedPrevOwner>},
      {"SaleUpdateMessage", &abi_parse_body<nft_sale::SaleUpdateMessage>},
      {"NftReportStaticData", &abi_parse_body<nft_sale::NftReportStaticData>},
      {"TONStakersNftBurnNotification",
       &abi_parse_body<tonstakers::TONStakersNftBurnNotification>},
      {"CocoonPayoutPayload", &abi_parse_body<cocoon::CocoonPayoutPayload>},
      {"CocoonLastPayoutPayload", &abi_parse_body<cocoon::CocoonLastPayoutPayload>},
      {"CocoonWorkerProxyRequest", &abi_parse_body<cocoon::CocoonWorkerProxyRequest>},
      {"CocoonExtProxyPayoutRequest", &abi_parse_body<cocoon::CocoonExtProxyPayoutRequest>},
      {"CocoonChargePayload", &abi_parse_body<cocoon::CocoonChargePayload>},
      {"CocoonGrantRefundPayload", &abi_parse_body<cocoon::CocoonGrantRefundPayload>},
      {"CocoonExtClientTopUp", &abi_parse_body<cocoon::CocoonExtClientTopUp>},
      {"CocoonRegisterProxy", &abi_parse_body<cocoon::CocoonRegisterProxy>},
      {"CocoonUnregisterProxy", &abi_parse_body<cocoon::CocoonUnregisterProxy>},
      {"CocoonOwnerClientRegister", &abi_parse_body<cocoon::CocoonOwnerClientRegister>},
      {"CocoonOwnerClientChangeSecretHash",
       &abi_parse_body<cocoon::CocoonOwnerClientChangeSecretHash>},
      {"CocoonOwnerClientRequestRefund",
       &abi_parse_body<cocoon::CocoonOwnerClientRequestRefund>},
      {"CocoonOwnerClientIncreaseStake",
       &abi_parse_body<cocoon::CocoonOwnerClientIncreaseStake>},
      {"CocoonOwnerClientWithdraw", &abi_parse_body<cocoon::CocoonOwnerClientWithdraw>},
  };
  return rows;
}

}  // namespace mch
