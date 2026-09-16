// Both fns reconstruct label roles via previous_block / children_blocks
// navigation and return Null to reject. Any parse/nav failure is a Null
// reject (intentional).
#include "host/HostImpls.h"

#include "host/BlockViews.h"
#include "host/DexPton.h"
#include "host/DexRecords.h"
#include "host/HostAdapter.h"
#include "host/HostCommon.h"

#include "BlockTree.h"
#include "BuildRuntime.h"
#include "ExprRuntime.h"
#include "MsgParse.h"
#include "btypes_gen.h"
#include "parse/PSlice.h"

#include "vm/cellslice.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace mch {

namespace {

constexpr std::uint32_t kJettonTransfer = 0x0f8a7ea5;
constexpr std::uint32_t kJettonInternalTransfer = 0x178d4519;
constexpr std::uint32_t kJettonNotify = 0x7362d09c;
constexpr std::uint32_t kDepositTONToVault = 0xd55e4686;
constexpr std::uint32_t kTopUp = 0x54240fe5;
constexpr std::uint32_t kDepositLiquidityToPool = 0xb56b9598;
constexpr std::uint32_t kReturnExcessFromVault = 0x6b0b787f;
constexpr std::uint32_t kDedustPayout = 0x474f86cf;
constexpr std::uint32_t kRejection = 0xe1a36cd4;
constexpr std::uint32_t kJettonForwardPayload = 0x40e108d6;

// 4-bit kind (0 -> TON, else wc:uint8 + account_id:32 bytes). Advances cs.
td::Result<Value> load_asset_4(vm::CellSlice &cs) {
  if (!cs.have(4)) {
    return td::Status::Error("asset: kind underflow");
  }
  auto kind = cs.fetch_ulong(4);
  if (kind == 0) {
    return Value::make_asset_ton();
  }
  if (!cs.have(8 + 256)) {
    return td::Status::Error("asset: jetton underflow");
  }
  auto wc = cs.fetch_ulong(8);
  unsigned char id[32];
  if (!cs.fetch_bytes(id, 32)) {
    return td::Status::Error("asset: id fetch");
  }
  return Value::make_asset_jetton(std::to_string(wc) + ":" + hex_upper(id, 32));
}

struct Leg {
  const Block *deposit{nullptr};       // vault-facing call the cores parse
  std::vector<const Block *> jt_calls; // raw user JettonTransfer calls
  bool jt_none{false};                 // jt_calls must come from consumed
};

Leg resolve_leg(const Block *leg) {
  Leg out;
  if (is_call_op(leg, kDepositTONToVault)) {
    out.deposit = leg;
    return out;  // jt_calls = []
  }
  if (leg != nullptr && leg->btype == mch::btype::kJettonTransfer) {
    out.deposit = first_call(leg->children_blocks, kJettonNotify);
    for (const Block *b : leg->children_blocks) {
      if (is_call_op(b, kJettonTransfer)) {
        out.jt_calls.push_back(b);
      }
    }
    return out;
  }
  out.deposit = leg;
  out.jt_none = true;
  return out;
}

// DedustDepositLiquidityToPool(body): owner_addr, asset0, asset0_amount,
// asset1, asset1_amount. proof + field4 are refs (in order).
struct PoolDeposit {
  Value owner;
  Value asset0, asset1;
  td::RefInt256 asset0_amount, asset1_amount;
};
td::Result<PoolDeposit> parse_pool_deposit(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(v, parse_message_body("DedustDepositLiquidityToPool", body));
  const Value *field4_cell = v.field("field4");
  const Value *field4 = field4_cell != nullptr ? field4_cell->field("ref") : nullptr;
  if (field4 == nullptr) {
    return td::Status::Error("pool deposit: field4.ref missing");
  }
  const Value *asset0 = field4->field("asset0");
  const Value *asset1 = field4->field("asset1");
  if (asset0 == nullptr || asset1 == nullptr) {
    return td::Status::Error("pool deposit: asset missing");
  }
  EvalResult converted0 = rt_builtin_asset_of(*asset0);
  EvalResult converted1 = rt_builtin_asset_of(*asset1);
  if (converted0.faulted || converted1.faulted) {
    return td::Status::Error(converted0.faulted ? converted0.message : converted1.message);
  }
  PoolDeposit pd;
  pd.owner = *v.field("owner");
  pd.asset0 = std::move(converted0.value);
  pd.asset0_amount = field4->field("asset0_amount")->num;
  pd.asset1 = std::move(converted1.value);
  pd.asset1_amount = field4->field("asset1_amount")->num;
  return pd;
}

}  // namespace

EvalResult dedust_deposit_final_data(BuildEnv &env, const std::vector<Value> &args) {
  auto decoded = decode_consumed_or_none(args);
  if (!decoded) {
    return rt_ok(Value::null());
  }
  std::vector<const Block *> consumed = std::move(decoded->blocks);
  const Block *block = consumed.front();

  // anchor <- topup <- ask <- deposit leg (null on any missing link -> Null reject).
  const Block *p1 = block->previous_block;
  const Block *p2 = p1 != nullptr ? p1->previous_block : nullptr;
  const Block *leg = p2 != nullptr ? p2->previous_block : nullptr;
  if (leg == nullptr) {
    return rt_ok(Value::null());
  }
  Leg lg = resolve_leg(leg);
  std::vector<const Block *> jt_calls = lg.jt_calls;
  if (lg.jt_none) {
    jt_calls.clear();
    for (const Block *b : consumed) {
      if (is_call_op(b, kJettonTransfer)) {
        jt_calls.push_back(b);
      }
    }
  }

  // lp_transfer / rejection: consumed[1:] whose previous_block is the anchor.
  const Block *lp_transfer = nullptr;
  const Block *rejection = nullptr;
  for (std::size_t i = 1; i < consumed.size(); i++) {
    const Block *b = consumed[i];
    if (lp_transfer == nullptr && is_call_op(b, kJettonInternalTransfer) &&
        b->previous_block == block) {
      lp_transfer = b;
    }
    if (rejection == nullptr && is_call_op(b, kRejection) && b->previous_block == block) {
      rejection = b;
    }
  }
  // Both TON and jetton excess legs hang off a DedustReturnExcessFromVault:
  // TON excess is a DedustPayout child, jetton excess is a jetton_transfer child.
  std::vector<const Block *> returns;
  for (const Block *b : consumed) {
    if (is_call_op(b, kReturnExcessFromVault)) {
      returns.push_back(b);
    }
  }
  auto in_returns = [&](const Block *p) {
    return std::find(returns.begin(), returns.end(), p) != returns.end();
  };
  std::vector<const Block *> ton_excesses;
  std::vector<const Block *> jetton_excesses;
  for (const Block *b : consumed) {
    if (is_call_op(b, kDedustPayout) && in_returns(b->previous_block)) {
      ton_excesses.push_back(b);
    }
    if (b->btype == mch::btype::kJettonTransfer && in_returns(b->previous_block)) {
      jetton_excesses.push_back(b);
    }
  }

  auto r_anchor_body = block_body(block);
  if (r_anchor_body.is_error()) {
    return rt_ok(Value::null());
  }
  auto r_pd = parse_pool_deposit(r_anchor_body.move_as_ok());
  if (r_pd.is_error()) {
    return rt_ok(Value::null());
  }
  PoolDeposit pd = r_pd.move_as_ok();
  Value sender = pd.owner;
  const Message *anchor_msg = block_msg(block);
  Value deposit_contract = account_from_opt(anchor_msg != nullptr ? anchor_msg->source
                                                                  : std::nullopt);

  Value lpool;
  Value lp_tokens;
  if (lp_transfer != nullptr) {
    auto r_body = block_body(lp_transfer);
    if (r_body.is_error()) {
      return rt_ok(Value::null());
    }
    auto r = parse_message_body("JettonInternalTransfer", r_body.ok());
    if (r.is_error()) {
      return rt_ok(Value::null());
    }
    const Value *amt = r.ok().field("amount");
    lpool = account_from_opt(anchor_msg != nullptr ? anchor_msg->destination : std::nullopt);
    lp_tokens = amt != nullptr ? to_amount(*amt) : Value::make_amount_none();
  } else if (rejection != nullptr) {
    const Message *rm = block_msg(rejection);
    lpool = account_from_opt(rm != nullptr ? rm->source : std::nullopt);
    lp_tokens = Value::null();
  } else {
    return rt_ok(Value::null());  // unrecognized outcome -> Null reject
  }

  // actual asset/amount from the deposit leg head.
  Value actual_asset = Value::make_asset_ton();
  Value actual_amount = Value::make_amount_none();
  if (is_call_op(lg.deposit, kDepositTONToVault)) {
    auto r_body = block_body(lg.deposit);
    if (r_body.is_error()) {
      return rt_ok(Value::null());
    }
    auto r_ctx = open_body(r_body.ok());
    if (r_ctx.is_error()) {
      return rt_ok(Value::null());
    }
    auto ctx = r_ctx.move_as_ok();
    auto &cs = ctx.cs;
    if (!cs.have(32 + 64) || !cs.advance(32 + 64)) {
      return rt_ok(Value::null());
    }
    auto r_amt = load_coins_py(cs);
    if (r_amt.is_error()) {
      return rt_ok(Value::null());
    }
    actual_amount = Value::make_amount(r_amt.move_as_ok());
  } else {
    if (lg.deposit == nullptr) {
      return rt_ok(Value::null());  // missing deposit -> Null reject
    }
    auto r_body = block_body(lg.deposit);
    if (r_body.is_error()) {
      return rt_ok(Value::null());
    }
    auto r = parse_message_body("JettonNotify", r_body.ok());
    if (r.is_error()) {
      return rt_ok(Value::null());
    }
    const Value *ja = r.ok().field("jetton_amount");
    actual_amount = ja != nullptr ? to_amount(*ja) : Value::make_amount_none();
    const Message *dm = block_msg(lg.deposit);
    if (dm == nullptr || !dm->source) {
      return rt_ok(Value::null());
    }
    Value jw = env.lookups->get("jetton_wallet", std::vector<Value>{Value::make_str(*dm->source)});
    auto master = wallet_jetton_master_str(jw);
    if (!master) {
      return rt_ok(Value::null());  // wallet with no jetton master -> Null reject
    }
    actual_asset = Value::make_asset_jetton(*master);
  }

  // user_jetton_wallet_0: the jetton deposit whose wallet's jetton == actual_asset.
  Value user_jw_0 = Value::make_account_none();
  Value user_jw_1 = Value::make_account_none();  // never resolvable
  std::string actual_master = actual_asset.t == VType::Asset && !actual_asset.is_ton
                                  ? actual_asset.str
                                  : std::string{};
  for (const Block *jdep : jt_calls) {
    const Message *jm = block_msg(jdep);
    if (jm == nullptr || !jm->source) {
      continue;
    }
    // Keep only transfers whose source equals sender.
    Value jsrc = account_from_opt(jm->source);
    if (sender.t != VType::Account || sender.addr_none || jsrc.t != VType::Account ||
        jsrc.addr_none || !same_account(jsrc, sender)) {
      continue;
    }
    if (!jm->destination) {
      continue;
    }
    Value jw = env.lookups->get("jetton_wallet",
                                std::vector<Value>{Value::make_str(*jm->destination)});
    auto jw_master = wallet_jetton_master_str(jw);
    if (!jw_master) {
      continue;
    }
    if (!actual_master.empty() && *jw_master == actual_master) {
      user_jw_0 = account_from_opt(*jm->destination);
    }
  }

  // vault_excesses: TON excesses first, then jetton excesses whose receiver
  // equals sender, each as (asset, amount) from the leg's data.
  std::vector<Value> excesses;
  for (const Block *te : ton_excesses) {
    const Message *tm = block_msg(te);
    if (tm == nullptr) {
      continue;
    }
    Value dst = account_from_opt(tm->destination);
    if (dst.t == VType::Account && !dst.addr_none && sender.t == VType::Account &&
        !sender.addr_none && same_account(dst, sender)) {
      Value amt = msg_value_amount(tm);
      excesses.push_back(excess_pair(Value::make_asset_ton(), std::move(amt)));
    }
  }
  for (const Block *je : jetton_excesses) {
    Value recv = data_field(je, "receiver");
    if (recv.t == VType::Account && !recv.addr_none && sender.t == VType::Account &&
        !sender.addr_none && same_account(recv, sender)) {
      excesses.push_back(excess_pair(data_field(je, "asset"), data_field(je, "amount")));
    }
  }

  bool success = !lp_tokens.is_null();
  Value::Fields d;
  d.emplace_back("sender", sender);
  d.emplace_back("pool_address", std::move(lpool));
  d.emplace_back("deposit_contract", std::move(deposit_contract));
  d.emplace_back("lp_tokens_minted", lp_tokens);
  d.emplace_back("asset_1", actual_asset);
  d.emplace_back("asset_2", Value::null());
  d.emplace_back("amount_1", std::move(actual_amount));
  d.emplace_back("amount_2", Value::null());
  d.emplace_back("target_asset_1", pd.asset0);
  d.emplace_back("target_amount_1", amount_or_zero(pd.asset0_amount));
  d.emplace_back("target_asset_2", pd.asset1);
  d.emplace_back("target_amount_2", amount_or_zero(pd.asset1_amount));
  d.emplace_back("user_jetton_wallet_1", std::move(user_jw_0));
  d.emplace_back("user_jetton_wallet_2", std::move(user_jw_1));
  d.emplace_back("vault_excesses", Value::make_list(std::move(excesses)));
  d.emplace_back("success", Value::make_bool(success));
  return rt_ok(Value::make_obj(std::move(d)));
}

EvalResult dedust_deposit_partial_data(BuildEnv &env, const std::vector<Value> &args) {
  auto decoded = decode_consumed_or_none(args);
  if (!decoded) {
    return rt_ok(Value::null());
  }
  std::vector<const Block *> consumed = std::move(decoded->blocks);
  const Block *block = consumed.front();  // topup anchor

  const Block *p1 = block->previous_block;
  const Block *leg = p1 != nullptr ? p1->previous_block : nullptr;
  if (leg == nullptr) {
    return rt_ok(Value::null());
  }
  Leg lg = resolve_leg(leg);
  const Block *vault_call = lg.deposit;

  // Guard: final-deposit opcodes after the topup reject.
  const Block *topup = first_call(consumed, kTopUp);
  if (topup == nullptr) {
    return rt_ok(Value::null());
  }
  for (const Block *n : topup->next_blocks) {
    if (is_call_op(n, kDepositLiquidityToPool) || is_call_op(n, kRejection)) {
      return rt_ok(Value::null());  // Unexpected call contract after deposit top up
    }
  }

  const Message *bm = block_msg(block);
  Value deposit_contract = account_from_opt(bm != nullptr ? bm->destination : std::nullopt);

  Value sender;
  Value actual_asset = Value::make_asset_ton();
  Value actual_amount = Value::make_amount_none();
  Value asset0, asset1;
  td::RefInt256 asset0_amount, asset1_amount;
  Value user_jw_0 = Value::make_account_none();
  Value user_jw_1 = Value::make_account_none();

  if (vault_call == nullptr) {
    return rt_ok(Value::null());
  }
  auto r_body = block_body(vault_call);
  if (r_body.is_error()) {
    return rt_ok(Value::null());
  }

  // Try TON-to-vault first; on any failure fall to the jetton branch.
  // Distinguish by opcode (a TON-to-vault call parses cleanly, else JettonNotify).
  bool ton_branch = is_call_op(vault_call, kDepositTONToVault);
  if (ton_branch) {
    const Message *vm_msg = block_msg(vault_call);
    sender = account_from_opt(vm_msg != nullptr ? vm_msg->source : std::nullopt);
    // amount, pool_type bit, asset0, asset1, ref{min_lp, a0_target, a1_target}.
    auto r_ctx2 = open_body(r_body.ok());
    if (r_ctx2.is_error()) {
      return rt_ok(Value::null());
    }
    auto ctx2 = r_ctx2.move_as_ok();
    auto &c2 = ctx2.cs;
    if (!c2.have(32 + 64) || !c2.advance(32 + 64)) {
      return rt_ok(Value::null());
    }
    auto r_amt = load_coins_py(c2);
    if (r_amt.is_error() || !c2.have(1)) {
      return rt_ok(Value::null());
    }
    actual_amount = Value::make_amount(r_amt.move_as_ok());
    c2.advance(1);  // pool_type bit
    auto r_a0 = load_asset_4(c2);
    auto r_a1 = load_asset_4(c2);
    if (r_a0.is_error() || r_a1.is_error() || c2.size_refs() < 1) {
      return rt_ok(Value::null());
    }
    asset0 = r_a0.move_as_ok();
    asset1 = r_a1.move_as_ok();
    bool special = false;
    vm::CellSlice pr;
    try {
      pr = vm::load_cell_slice_special(c2.fetch_ref(), special);
    } catch (...) {
      return rt_ok(Value::null());
    }
    auto r_min = load_coins_py(pr);
    auto r_t0 = load_coins_py(pr);
    auto r_t1 = load_coins_py(pr);
    if (r_min.is_error() || r_t0.is_error() || r_t1.is_error()) {
      return rt_ok(Value::null());
    }
    asset0_amount = r_t0.move_as_ok();
    asset1_amount = r_t1.move_as_ok();
  } else {
    // jetton deposit branch: JettonNotify -> forward payload.
    auto r = parse_message_body("JettonNotify", r_body.ok());
    if (r.is_error()) {
      return rt_ok(Value::null());
    }
    Value notif = r.move_as_ok();
    const Value *ja = notif.field("jetton_amount");
    const Value *fu = notif.field("from_user");
    const Value *fp = notif.field("forward_payload_cell");
    sender = fu != nullptr ? *fu : Value::make_account_none();
    actual_amount = ja != nullptr ? to_amount(*ja) : Value::make_amount_none();
    if (fp == nullptr) {
      return rt_ok(Value::null());  // missing forward payload -> Null reject
    }
    EvalResult unwrapped = rt_builtin_tail_unwrap(*fp);
    if (unwrapped.faulted || unwrapped.value.t != VType::Cell) {
      return rt_ok(Value::null());
    }
    bool special = false;
    vm::CellSlice cs;
    try {
      cs = vm::load_cell_slice_special(unwrapped.value.cell, special);
    } catch (...) {
      return rt_ok(Value::null());
    }
    if (!cs.have(32) || cs.fetch_ulong(32) != kJettonForwardPayload || !cs.have(1)) {
      return rt_ok(Value::null());
    }
    cs.advance(1);  // pool_type bit
    auto r_a0 = load_asset_4(cs);
    auto r_a1 = load_asset_4(cs);
    if (r_a0.is_error() || r_a1.is_error()) {
      return rt_ok(Value::null());
    }
    asset0 = r_a0.move_as_ok();
    asset1 = r_a1.move_as_ok();
    auto r_min = load_coins_py(cs);
    auto r_t0 = load_coins_py(cs);
    auto r_t1 = load_coins_py(cs);
    if (r_min.is_error() || r_t0.is_error() || r_t1.is_error()) {
      return rt_ok(Value::null());
    }
    asset0_amount = r_t0.move_as_ok();
    asset1_amount = r_t1.move_as_ok();

    // user wallet is the vault call's previous-block source
    const Block *vprev = vault_call->previous_block;
    if (vprev == nullptr) {
      return rt_ok(Value::null());
    }
    const Message *vpm = block_msg(vprev);
    user_jw_0 = account_from_opt(vpm != nullptr ? vpm->source : std::nullopt);

    // actual_asset from the dex jetton wallet (vault_call source).
    const Message *vm_msg = block_msg(vault_call);
    if (vm_msg == nullptr || !vm_msg->source) {
      return rt_ok(Value::null());
    }
    Value jw = env.lookups->get("jetton_wallet",
                                std::vector<Value>{Value::make_str(*vm_msg->source)});
    auto master = wallet_jetton_master_str(jw);
    if (!master) {
      return rt_ok(Value::null());  // dex wallet with no jetton master -> Null reject
    }
    actual_asset = Value::make_asset_jetton(*master);
  }

  Value::Fields d;
  d.emplace_back("sender", (sender.t == VType::Account) ? sender : Value::make_account_none());
  d.emplace_back("deposit_contract", std::move(deposit_contract));
  d.emplace_back("asset_1", std::move(actual_asset));
  d.emplace_back("amount_1", std::move(actual_amount));
  d.emplace_back("asset_2", Value::null());
  d.emplace_back("amount_2", Value::null());
  d.emplace_back("user_jetton_wallet_1", std::move(user_jw_0));
  d.emplace_back("user_jetton_wallet_2", std::move(user_jw_1));
  d.emplace_back("target_asset_1", std::move(asset0));
  d.emplace_back("target_amount_1", amount_or_zero(asset0_amount));
  d.emplace_back("target_asset_2", std::move(asset1));
  d.emplace_back("target_amount_2", amount_or_zero(asset1_amount));
  return rt_ok(Value::make_obj(std::move(d)));
}

}  // namespace mch
