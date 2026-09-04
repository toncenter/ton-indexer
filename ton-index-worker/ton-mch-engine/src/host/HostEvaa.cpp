#include "host/HostImpls.h"

#include "host/HostAdapter.h"
#include "host/HostCommon.h"

#include "BlockTree.h"
#include "BuildRuntime.h"
#include "MsgParse.h"
#include "TraceLoader.h"
#include "parse/PSlice.h"
#include "btypes_gen.h"

#include "common/refint.h"
#include "vm/cellslice.h"

#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mch {

namespace {

// Skip the EVAA user header (version coins + maybe upgrade-info ref + 2 exec
// bits), leaving the cursor on the 32-bit opcode.
bool skip_user_header(vm::CellSlice &cs) {
  if (load_coins_py(cs).is_error()) {  // user_version
    return false;
  }
  if (!cs.have(1)) {  // load_maybe_ref: the Maybe bit
    return false;
  }
  if (cs.fetch_ulong(1)) {
    if (cs.size_refs() == 0) {
      return false;
    }
    cs.fetch_ref();  // upgrade_info
  }
  if (!cs.have(2)) {  // upgrade_exec
    return false;
  }
  cs.advance(2);
  return true;
}

// Open a user-contract body positioned on its 32-bit opcode.
bool open_user_body(const Block *b, vm::CellSlice &cs) {
  if (b->btype != mch::btype::kCallContract) {
    return false;
  }
  auto r_body = block_body(b);
  if (r_body.is_error()) {
    return false;
  }
  bool special = false;
  try {
    cs = vm::load_cell_slice_special(r_body.ok(), special);
  } catch (...) {
    return false;
  }
  return skip_user_header(cs);
}

// Skip the header, then compare the 32-bit opcode. Any malformed body
// never matches.
bool opcode_after_user_header(const Block *b, td::uint32 opcode) {
  vm::CellSlice cs;
  if (!open_user_body(b, cs)) {
    return false;
  }
  if (!cs.have(32)) {
    return false;
  }
  return static_cast<td::uint32>(cs.fetch_ulong(32)) == opcode;
}

constexpr td::uint32 kSupplyMaster = 0x1;
constexpr td::uint32 kSupplyUser = 0x11;
constexpr td::uint32 kSupplySuccess = 0x11a;
constexpr td::uint32 kSupplyFail = 0x11f;
constexpr td::uint32 kLiquidateMaster = 0x3;
constexpr td::uint32 kLiquidateUser = 0x31;
constexpr td::uint32 kLiquidateSatisfied = 0x311;
constexpr td::uint32 kLiquidateUnsatisfied = 0x31f;
constexpr td::uint32 kLiquidateSuccess = 0x311a;
constexpr td::uint32 kLiquidateFail = 0x311f;
const td::RefInt256 kTonAssetId = td::hex_string_to_int256(
    std::string("1A4219FE5E60D63AF2A3CC7DCE6FEC69B45C6B5718497A6148E7C232AC87BD8A"));

Value as_account_id(Value v) {
  return v.is_null() ? Value::make_account_none() : std::move(v);
}

struct LiquidateMasterData {
  Value borrower;
  td::RefInt256 collateral_asset_id;
  td::RefInt256 incoming_amount;
};

td::Result<LiquidateMasterData> parse_liquidate_master(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(ctx, open_body(body));
  auto &cs = ctx.cs;
  if (!cs.have(32 + 64) || !cs.advance(32 + 64)) {
    return td::Status::Error("liquidate_master header");
  }
  TRY_RESULT(borrower, load_address_py(cs));
  TRY_RESULT(liquidator, load_address_py(cs));
  (void)liquidator;
  if (!cs.have(256 + 64 + 2 + 64)) {
    return td::Status::Error("liquidate_master fields");
  }
  auto collateral = cs.fetch_int256(256, false);
  cs.advance(64 + 2);
  auto amount = refint_u64(cs.fetch_ulong(64));
  if (collateral.is_null() || amount.is_null()) {
    return td::Status::Error("liquidate_master integers");
  }
  return LiquidateMasterData{as_account_id(std::move(borrower)), std::move(collateral),
                             std::move(amount)};
}

struct LiquidateSatisfiedData {
  Value owner;
  td::RefInt256 transferred_asset_id;
  td::RefInt256 collateral_asset_id;
  td::RefInt256 collateral_reward;
  td::RefInt256 liquidatable_amount;
};

td::Result<LiquidateSatisfiedData> parse_liquidate_satisfied(
    const td::Ref<vm::Cell> &body) {
  TRY_RESULT(ctx, open_body(body));
  auto &cs = ctx.cs;
  if (!cs.have(32 + 64) || !cs.advance(32 + 64)) {
    return td::Status::Error("liquidate_satisfied header");
  }
  TRY_RESULT(owner, load_address_py(cs));
  TRY_RESULT(liquidator, load_address_py(cs));
  (void)liquidator;
  if (!cs.have(256) || cs.size_refs() == 0) {
    return td::Status::Error("liquidate_satisfied root");
  }
  auto transferred = cs.fetch_int256(256, false);
  TRY_RESULT(ref, open_ref_cell(cs.fetch_ref()));
  if (!ref.have(64 + 64 + 64 + 64 + 256 + 64 + 64)) {
    return td::Status::Error("liquidate_satisfied ref");
  }
  ref.advance(64);  // delta_loan_principal
  auto amount = refint_u64(ref.fetch_ulong(64));
  ref.advance(64 + 64);  // protocol_gift + new_user_loan_principal
  auto collateral = ref.fetch_int256(256, false);
  ref.advance(64);  // delta_collateral_principal
  auto reward = refint_u64(ref.fetch_ulong(64));
  if (transferred.is_null() || amount.is_null() || collateral.is_null() || reward.is_null()) {
    return td::Status::Error("liquidate_satisfied integers");
  }
  return LiquidateSatisfiedData{as_account_id(std::move(owner)), std::move(transferred),
                                std::move(collateral), std::move(reward), std::move(amount)};
}

td::Result<std::string> parse_liquidation_reason(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(ctx, open_body(body));
  auto &cs = ctx.cs;
  if (!cs.have(32 + 64) || !cs.advance(32 + 64)) {
    return td::Status::Error("liquidate_unsatisfied header");
  }
  TRY_RESULT(owner, load_address_py(cs));
  TRY_RESULT(liquidator, load_address_py(cs));
  (void)owner;
  (void)liquidator;
  if (!cs.have(256) || cs.size_refs() == 0) {
    return td::Status::Error("liquidate_unsatisfied root");
  }
  cs.advance(256);  // transferred_asset_id
  TRY_RESULT(ref, open_ref_cell(cs.fetch_ref()));
  if (!ref.have(64 + 256 + 64 + 64) || !ref.advance(64 + 256 + 64 + 64) ||
      ref.size_refs() == 0) {
    return td::Status::Error("liquidate_unsatisfied ref");
  }
  ref.fetch_ref();  // custom_response_payload
  if (!ref.have(32)) {
    return td::Status::Error("liquidation error opcode");
  }
  switch (static_cast<td::uint32>(ref.fetch_ulong(32))) {
    case 0xE001: return std::string("master_liquidating_too_much");
    case 0xE002: return std::string("user_withdraw_in_progress");
    case 0xE003: return std::string("not_liquidatable");
    case 0xE004: return std::string("execution_crashed");
    case 0xE005: return std::string("min_collateral_not_satisfied");
    case 0xE006: return std::string("user_not_enough_collateral");
    case 0xE007: return std::string("user_liquidating_too_much");
    case 0xE008: return std::string("master_not_enough_liquidity");
    case 0xE009: return std::string("liquidation_prices_missing");
    default: return std::string("unknown");
  }
}

}  // namespace

bool evaa_user_withdraw_user(const Block *b) {
  return opcode_after_user_header(b, 0x21);
}

bool evaa_user_withdraw_success(const Block *b) {
  return opcode_after_user_header(b, 0x211a);
}

bool evaa_user_withdraw_fail(const Block *b) {
  return opcode_after_user_header(b, 0x211f);
}

bool evaa_user_supply(const Block *b) {
  return opcode_after_user_header(b, kSupplyUser);
}

bool evaa_user_liquidate(const Block *b) {
  return opcode_after_user_header(b, kLiquidateUser);
}

bool evaa_liquidate_success_header(const Block *b) {
  return opcode_after_user_header(b, kLiquidateSuccess);
}

bool evaa_bounced_call(const Block *b) {
  const Message *m = block_msg(b);
  return b->btype == mch::btype::kCallContract && m != nullptr && m->bounced;
}

namespace {

// The master call itself (TON side), or a jetton transfer whose forward
// payload starts with that opcode (jetton side). Match on btype.
bool evaa_master_anchor(const Block *b, td::uint32 op) {
  if (is_call_op(b, op)) {
    return true;
  }
  if (b->btype != mch::btype::kJettonTransfer) {
    return false;
  }
  Value fp = data_field(b, "forward_payload");
  if (fp.t != VType::Str) {
    return false;
  }
  auto r_cell = cell_from_pystr(fp.str);
  if (r_cell.is_error()) {
    return false;
  }
  auto r_ctx = open_body(r_cell.ok());
  if (r_ctx.is_error()) {
    return false;
  }
  auto ctx = r_ctx.move_as_ok();
  return ctx.cs.have(32) &&
         static_cast<td::uint32>(ctx.cs.fetch_ulong(32)) == op;
}

}  // namespace

bool evaa_supply_anchor(const Block *b) { return evaa_master_anchor(b, kSupplyMaster); }

bool evaa_liquidate_anchor(const Block *b) {
  return evaa_master_anchor(b, kLiquidateMaster);
}

// Spec supplies the parsed jetton recipient; the TON-vs-jetton discriminator
// and role derivation remain here.
EvalResult evaa_supply_data(BuildEnv &env, const std::vector<Value> &args) {
  (void)env;
  auto decoded = decode_consumed_or_none(args);
  if (!decoded) {
    if (args[0].t == VType::List && !args[0].items->empty()) {
      return host_reject("empty consumed");
    }
    return host_reject("bad arguments");
  }
  std::vector<const Block *> consumed = std::move(decoded->blocks);
  const Block *block = consumed.front();

  // Label roles, derived structurally from the consumed set (the anchor is
  // excluded, on the jetton arm it is itself a jetton_transfer).
  const Block *user = nullptr, *success = nullptr, *fail = nullptr, *refund = nullptr;
  for (std::size_t i = 1; i < consumed.size(); i++) {
    const Block *b = consumed[i];
    if (user == nullptr && opcode_after_user_header(b, kSupplyUser)) user = b;
    if (success == nullptr && is_call_op(b, kSupplySuccess)) success = b;
    if (fail == nullptr && is_call_op(b, kSupplyFail)) fail = b;
    if (refund == nullptr && b->btype == mch::btype::kJettonTransfer) refund = b;
  }
  if (user == nullptr) {
    return host_reject("no supply_user block");
  }

  bool is_ton = block->btype == mch::btype::kCallContract;
  Value sender, recipient, master, asset;
  Value sender_jetton_wallet = Value::null();
  Value recipient_jetton_wallet = Value::null();
  Value master_jetton_wallet = Value::null();
  td::RefInt256 amount;

  const Message *bm = block_msg(block);
  if (is_ton) {
    auto rb = block_body(block);
    if (rb.is_error()) {
      return host_reject("supply_master body");
    }
    auto rm = parse_message_body("EvaaSupplyMaster", rb.ok());
    if (rm.is_error()) {
      return host_reject("supply_master parse");
    }
    Value m = rm.move_as_ok();
    const Value *sa = m.field("supply_amount");
    const Value *ra = m.field("recipient_address");
    if (sa == nullptr || ra == nullptr) {
      return host_reject("supply_master fields");
    }
    sender = account_from_opt(bm != nullptr ? bm->source : std::nullopt);
    amount = sa->num;
    recipient = *ra;
    master = account_from_opt(bm != nullptr ? bm->destination : std::nullopt);
    asset = Value::make_asset_ton();
  } else {
    sender = data_field(block, "sender");
    sender_jetton_wallet = data_field(block, "sender_wallet");
    master_jetton_wallet = data_field(block, "receiver_wallet");
    Value amt = data_field(block, "amount");
    amount = amt.num;
    master = data_field(block, "receiver");
    asset = data_field(block, "asset");
    recipient = args[1];
    if (same_account(sender, recipient)) {
      recipient_jetton_wallet = sender_jetton_wallet;
    } else {
      // Supplying on someone else's behalf: name their wallet from the sibling
      // transfer the master sent out.
      for (std::size_t i = 1; i < consumed.size(); i++) {
        const Block *b = consumed[i];
        if (b->btype != mch::btype::kJettonTransfer) continue;
        if (same_account(data_field(b, "sender_wallet"), master_jetton_wallet)) {
          recipient_jetton_wallet = data_field(b, "receiver_wallet");
          break;
        }
      }
    }
  }

  const Message *um = block_msg(user);
  Value recipient_contract =
      account_from_opt(um != nullptr ? um->destination : std::nullopt);

  // The user's asset_id sits behind the variable header, so no ABI row exists.
  td::RefInt256 asset_id;
  {
    vm::CellSlice cs;
    if (!open_user_body(user, cs)) {
      return host_reject("supply_user body");
    }
    if (!cs.have(32 + 64 + 256)) {
      return host_reject("supply_user underflow");
    }
    cs.advance(32 + 64);  // opcode + query_id
    asset_id = cs.fetch_int256(256, false);
    if (asset_id.is_null()) {
      return host_reject("supply_user asset_id");
    }
  }

  if (success != nullptr) {
    auto rb = block_body(success);
    if (rb.is_error()) {
      return host_reject("supply_success body");
    }
    auto rs = parse_message_body("EvaaSupplySuccess", rb.ok());
    if (rs.is_error()) {
      return host_reject("supply_success parse");
    }
    const Value *as = rs.ok().field("amount_supplied");
    if (as == nullptr) {
      return host_reject("supply_success amount");
    }
    amount = as->num;  // the authoritative amount
  } else if (fail == nullptr && refund == nullptr) {
    return host_reject("supply not completed");
  }

  Value::Fields d;
  d.emplace_back("sender", std::move(sender));
  d.emplace_back("recipient", std::move(recipient));
  d.emplace_back("recipient_contract", std::move(recipient_contract));
  d.emplace_back("amount", Value::make_int(std::move(amount)));
  d.emplace_back("is_success", Value::make_bool(success != nullptr));
  d.emplace_back("is_ton", Value::make_bool(is_ton));
  d.emplace_back("asset_id", Value::make_int(std::move(asset_id)));
  d.emplace_back("sender_jetton_wallet", std::move(sender_jetton_wallet));
  d.emplace_back("recipient_jetton_wallet", std::move(recipient_jetton_wallet));
  d.emplace_back("master_jetton_wallet", std::move(master_jetton_wallet));
  d.emplace_back("master", std::move(master));
  d.emplace_back("asset", std::move(asset));
  return rt_ok(Value::make_obj(std::move(d)));
}

EvalResult evaa_liquidate_data(BuildEnv &env, const std::vector<Value> &args) {
  (void)env;
  auto decoded = decode_consumed_or_none(args);
  if (!decoded) {
    if (args[0].t == VType::List && !args[0].items->empty()) {
      return host_reject("empty consumed");
    }
    return host_reject("bad arguments");
  }
  std::vector<const Block *> consumed = std::move(decoded->blocks);
  const Block *block = consumed.front();
  const bool is_ton = block->btype == mch::btype::kCallContract;
  Value liquidator;
  td::Ref<vm::Cell> master_cell;
  if (is_ton) {
    const Message *m = block_msg(block);
    liquidator = account_from_opt(m != nullptr ? m->source : std::nullopt);
    auto r_body = block_body(block);
    if (r_body.is_error()) return host_reject("liquidate_master body");
    master_cell = r_body.move_as_ok();
  } else {
    liquidator = as_account_id(data_field(block, "sender"));
    Value fp = data_field(block, "forward_payload");
    if (fp.t != VType::Str) return host_reject("liquidate_master body");
    auto r_cell = cell_from_pystr(fp.str);
    if (r_cell.is_error()) return host_reject("liquidate_master body");
    master_cell = r_cell.move_as_ok();
  }
  auto r_master = parse_liquidate_master(master_cell);
  if (r_master.is_error()) {
    return host_reject("liquidate_master parse");
  }
  LiquidateMasterData master = r_master.move_as_ok();
  Value anchor_debt_asset_id = is_ton
      ? Value::make_int(kTonAssetId)
      : data_field(block, "receiver_wallet");

  const Block *user = nullptr, *satisfied = nullptr, *unsatisfied = nullptr;
  for (std::size_t i = 1; i < consumed.size(); i++) {
    const Block *b = consumed[i];
    if (user == nullptr && evaa_user_liquidate(b)) user = b;
    if (satisfied == nullptr && is_call_op(b, kLiquidateSatisfied)) satisfied = b;
    if (unsatisfied == nullptr && is_call_op(b, kLiquidateUnsatisfied)) unsatisfied = b;
  }

  auto output = [&](Value borrower, Value borrower_contract, td::RefInt256 collateral,
                    td::RefInt256 collateral_amount, bool success, Value fail_reason,
                    td::RefInt256 debt_amount, Value debt_asset_id) {
    Value::Fields d;
    d.emplace_back("liquidator", liquidator);
    d.emplace_back("borrower", std::move(borrower));
    d.emplace_back("borrower_contract", std::move(borrower_contract));
    d.emplace_back("collateral_asset_id", Value::make_int(std::move(collateral)));
    d.emplace_back("collateral_amount", Value::make_int(std::move(collateral_amount)));
    d.emplace_back("is_success", Value::make_bool(success));
    d.emplace_back("fail_reason", std::move(fail_reason));
    d.emplace_back("debt_amount", Value::make_int(std::move(debt_amount)));
    d.emplace_back("debt_asset_id", std::move(debt_asset_id));
    return rt_ok(Value::make_obj(std::move(d)));
  };

  if (user == nullptr) {
    bool immediate = false;
    for (std::size_t i = 1; i < consumed.size(); i++) {
      immediate = immediate || evaa_bounced_call(consumed[i]) ||
                  consumed[i]->btype == mch::btype::kJettonTransfer;
    }
    if (!immediate) return host_reject("no immediate refund");
    return output(master.borrower, Value::null(), master.collateral_asset_id,
                  td::make_refint(0), false, Value::make_str("immediate_rejection"),
                  master.incoming_amount, anchor_debt_asset_id);
  }

  const Message *um = block_msg(user);
  Value borrower_contract =
      account_from_opt(um != nullptr ? um->destination : std::nullopt);
  bool satisfied_success = false, satisfied_fail = false;
  if (satisfied != nullptr) {
    for (const Block *child : satisfied->next_blocks) {
      satisfied_success = satisfied_success || evaa_liquidate_success_header(child);
      satisfied_fail = satisfied_fail || is_call_op(child, kLiquidateFail);
    }
  }
  if (satisfied_success) {
    auto r_body = block_body(satisfied);
    if (r_body.is_error()) return host_reject("liquidate_satisfied body");
    auto r = parse_liquidate_satisfied(r_body.ok());
    if (r.is_error()) return host_reject("liquidate_satisfied parse");
    LiquidateSatisfiedData s = r.move_as_ok();
    return output(std::move(s.owner), std::move(borrower_contract),
                  std::move(s.collateral_asset_id), std::move(s.collateral_reward), true,
                  Value::null(), std::move(s.liquidatable_amount),
                  Value::make_int(std::move(s.transferred_asset_id)));
  }
  if (unsatisfied != nullptr) {
    std::string reason = "liquidation_error";
    auto r_body = block_body(unsatisfied);
    if (r_body.is_ok()) {
      auto r_reason = parse_liquidation_reason(r_body.ok());
      if (r_reason.is_ok()) reason = r_reason.move_as_ok();
    }
    return output(master.borrower, std::move(borrower_contract), master.collateral_asset_id,
                  td::make_refint(0), false, Value::make_str(std::move(reason)),
                  master.incoming_amount, anchor_debt_asset_id);
  }
  if (satisfied_fail) {
    return output(master.borrower, std::move(borrower_contract), master.collateral_asset_id,
                  td::make_refint(0), false,
                  Value::make_str("master_not_enough_liquidity"), master.incoming_amount,
                  anchor_debt_asset_id);
  }
  return host_reject("liquidation not completed");
}

}  // namespace mch
