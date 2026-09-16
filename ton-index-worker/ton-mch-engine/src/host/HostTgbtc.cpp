// tgbtc_mint_data derives head, success log, and jetton_mint from the consumed
// set. Event-log parsers return Null for unreadable bodies. BurnEvent
// tolerates truncated addresses as addr_none (an ABI address leaf would fail
// the whole parse). Other events need hex renderings the language cannot express.
#include "host/HostImpls.h"

#include "host/BlockViews.h"
#include "host/HostAdapter.h"
#include "host/HostCommon.h"

#include "BlockTree.h"
#include "BuildRuntime.h"
#include "HostRegistry.h"
#include "MsgParse.h"
#include "btypes_gen.h"
#include "parse/PSlice.h"

#include "common/bigint.hpp"
#include "common/refint.h"
#include "vm/cellslice.h"

#include <string>
#include <vector>

namespace mch {

namespace {

constexpr std::uint32_t kTgbtcMintHead = 0x3F781D24;
constexpr std::uint32_t kTgbtcMintSuccessLog = 0x77A80EF3;

// 32-byte little-endian, lowercase hex.
std::string txid_le_hex(const td::RefInt256 &v) {
  unsigned char le[32] = {0};
  v->export_bytes_lsb(le, 32, /*sgnd=*/false);
  static const char *hexd = "0123456789abcdef";
  std::string out(64, '0');
  for (int i = 0; i < 32; i++) {
    out[2 * i] = hexd[le[i] >> 4];
    out[2 * i + 1] = hexd[le[i] & 0xF];
  }
  return out;
}

// Minimal-length lowercase hex, no leading zeros, "0" for zero. Used for
// both tgBTC pubkeys.
Value hex_min(const td::RefInt256 &v) {
  return Value::make_str(td::hex_string(v, /*upcase=*/false));
}

// Account passes through; addr_none stays none; an external-address Str
// rejects (ok is cleared).
Value account_id_of(const Value &v, bool &ok) {
  if (v.t == VType::Account) {
    return v;
  }
  if (v.is_null()) {
    return Value::make_account_none();
  }
  ok = false;
  return Value::null();
}

// Guarded form: an extern address is addr_none here, never a reject.
Value account_id_or_none(const Value &v) {
  return v.t == VType::Account ? v : Value::make_account_none();
}

// Log Block whose body is opened and positioned past the 32 opcode bits
// (skipped unchecked). Absent/unreadable -> no context.
struct LogCursor {
  BodyCtx ctx;
  bool ok{false};
};

LogCursor open_log(const std::vector<Value> &args) {
  LogCursor lc;
  const Block *b = as_block(args[0]);
  if (b == nullptr) {
    return lc;
  }
  auto r_body = block_body(b);
  if (r_body.is_error()) {
    return lc;
  }
  auto r_ctx = open_body(r_body.ok());
  if (r_ctx.is_error()) {
    return lc;
  }
  lc.ctx = r_ctx.move_as_ok();
  if (!lc.ctx.cs.have(32)) {
    return lc;
  }
  lc.ctx.cs.advance(32);
  lc.ok = true;
  return lc;
}

// TgBTCMintEvent tail after the 32-bit opcode: coins, recipient, uint256 txid.
// Any step failure leaves ok false; callers return Null.
struct MintLog {
  td::RefInt256 amount;
  Value recipient;
  td::RefInt256 txid;
  bool ok{false};
};

MintLog parse_mint_log(vm::CellSlice &cs) {
  MintLog out;
  auto r_amt = load_coins_py(cs);
  if (r_amt.is_error()) {
    return out;
  }
  auto r_rcpt = load_address_py(cs);
  if (r_rcpt.is_error()) {
    return out;
  }
  bool rcpt_ok = true;
  out.recipient = account_id_of(r_rcpt.ok(), rcpt_ok);
  if (!rcpt_ok || !cs.have(256)) {
    return out;
  }
  out.txid = cs.fetch_int256(256, false);
  if (out.txid.is_null()) {
    return out;
  }
  out.amount = r_amt.move_as_ok();
  out.ok = true;
  return out;
}

}  // namespace

// 9-field mint record, or Null if head/log/mint is missing or the log is
// unparseable.
EvalResult tgbtc_mint_data(BuildEnv &, const std::vector<Value> &args) {
  if (args[0].t != VType::List) {
    return rt_fault("tgbtc_mint_data: bad arguments");
  }
  auto decoded = decode_consumed_or_none(args);
  if (!decoded) {
    return rt_ok(Value::null());
  }
  std::vector<const Block *> consumed = std::move(decoded->blocks);
  const Block *head = first_call(consumed, kTgbtcMintHead);
  const Block *success_log = first_call(consumed, kTgbtcMintSuccessLog);
  const Block *jetton_mint = nullptr;
  for (const Block *b : consumed) {
    if (b->btype == mch::btype::kJettonMint) {
      jetton_mint = b;
      break;
    }
  }
  if (head == nullptr || success_log == nullptr || jetton_mint == nullptr) {
    return rt_ok(Value::null());  // core: missing piece -> reject
  }

  const Message *hmsg = block_msg(head);
  Value sender = account_from_opt(hmsg != nullptr ? hmsg->source : std::nullopt);

  // Success flag is set after the log parse; treat a parse failure as reject
  // rather than keeping a half-built record.
  auto r_body = block_body(success_log);
  if (r_body.is_error()) {
    return rt_ok(Value::null());  // unreadable log -> Null reject
  }
  auto r_ctx = open_body(r_body.ok());
  if (r_ctx.is_error()) {
    return rt_ok(Value::null());
  }
  auto ctx = r_ctx.move_as_ok();
  auto &cs = ctx.cs;
  if (!cs.have(32)) {
    return rt_ok(Value::null());
  }
  cs.advance(32);
  auto parsed = parse_mint_log(cs);
  if (!parsed.ok) {
    return rt_ok(Value::null());
  }
  td::RefInt256 amount = std::move(parsed.amount);
  Value recipient = std::move(parsed.recipient);
  td::RefInt256 txid = std::move(parsed.txid);

  const Message *lmsg = block_msg(success_log);
  Value teleport_contract = account_from_opt(lmsg != nullptr ? lmsg->source : std::nullopt);
  std::string bitcoin_txid = txid_le_hex(txid);

  Value asset = data_field(jetton_mint, "asset");
  Value to_wallet = data_field(jetton_mint, "to_jetton_wallet");
  Value recipient_wallet =
      to_wallet.t == VType::Str ? account_from_opt(std::optional<std::string>(to_wallet.str))
      : to_wallet.t == VType::Account ? to_wallet
                                      : Value::make_account_none();

  Value::Fields ns;
  ns.emplace_back("sender", std::move(sender));
  ns.emplace_back("recipient", std::move(recipient));
  ns.emplace_back("amount", Value::make_int(std::move(amount)));
  ns.emplace_back("asset", std::move(asset));
  ns.emplace_back("bitcoin_txid", Value::make_str(std::move(bitcoin_txid)));
  ns.emplace_back("success", Value::make_bool(true));
  ns.emplace_back("recipient_wallet", std::move(recipient_wallet));
  ns.emplace_back("teleport_contract", std::move(teleport_contract));
  ns.emplace_back("crippled", Value::make_bool(false));
  return rt_ok(Value::make_obj(std::move(ns)));
}


// TgBTCMintEvent 0x77A80EF3: coins amount, MsgAddress recipient, uint256 txid.
EvalResult tgbtc_mint_log(BuildEnv &, const std::vector<Value> &args) {
  LogCursor lc = open_log(args);
  if (!lc.ok) {
    return rt_ok(Value::null());
  }
  auto parsed = parse_mint_log(lc.ctx.cs);
  if (!parsed.ok) {
    return rt_ok(Value::null());
  }
  Value::Fields ns;
  ns.emplace_back("amount", Value::make_int(std::move(parsed.amount)));
  ns.emplace_back("recipient", std::move(parsed.recipient));
  ns.emplace_back("bitcoin_txid", Value::make_str(txid_le_hex(parsed.txid)));
  return rt_ok(Value::make_obj(std::move(ns)));
}

// Two addresses parsed tolerantly. On the first address-load error, the
// already-parsed address stays set and the rest remain addr_none (partial
// results retained deliberately; no atomic rollback).
EvalResult tgbtc_burn_log(BuildEnv &, const std::vector<Value> &args) {
  LogCursor lc = open_log(args);
  if (!lc.ok) {
    return rt_ok(Value::null());
  }
  auto &cs = lc.ctx.cs;
  auto r_amt = load_coins_py(cs);
  if (r_amt.is_error()) {
    return rt_ok(Value::null());  // amount underflow rejects; only address loads are tolerant
  }
  Value sender = Value::make_account_none();
  Value pegout = Value::make_account_none();
  auto r_sender = load_address_py(cs);
  if (r_sender.is_ok()) {
    sender = account_id_or_none(r_sender.ok());
    auto r_pegout = load_address_py(cs);
    if (r_pegout.is_ok()) {
      pegout = account_id_or_none(r_pegout.ok());
    }
  }
  Value::Fields ns;
  ns.emplace_back("amount", Value::make_int(r_amt.move_as_ok()));
  ns.emplace_back("sender", std::move(sender));
  ns.emplace_back("pegout", std::move(pegout));
  return rt_ok(Value::make_obj(std::move(ns)));
}

// TgBTCNewKeyEvent 0x27756729: coins amount, uint256 pubkey, MsgAddress pegout.
EvalResult tgbtc_new_key_log(BuildEnv &, const std::vector<Value> &args) {
  LogCursor lc = open_log(args);
  if (!lc.ok) {
    return rt_ok(Value::null());
  }
  auto &cs = lc.ctx.cs;
  auto r_amt = load_coins_py(cs);
  if (r_amt.is_error()) {
    return rt_ok(Value::null());
  }
  auto pubkey = cs.have(256) ? cs.fetch_int256(256, false) : td::RefInt256{};
  if (pubkey.is_null()) {
    return rt_ok(Value::null());
  }
  auto r_pegout = load_address_py(cs);
  if (r_pegout.is_error()) {
    return rt_ok(Value::null());
  }
  bool pegout_ok = true;
  Value pegout = account_id_of(r_pegout.ok(), pegout_ok);
  if (!pegout_ok) {
    return rt_ok(Value::null());
  }
  Value::Fields ns;
  ns.emplace_back("amount", Value::make_int(r_amt.move_as_ok()));
  ns.emplace_back("pubkey", hex_min(pubkey));
  ns.emplace_back("pegout", std::move(pegout));
  return rt_ok(Value::make_obj(std::move(ns)));
}

// TgBTCDkgCompletedEvent 0x453443A6: u64 timestamp, uint256 pubkey.
EvalResult tgbtc_dkg_completed_log(BuildEnv &, const std::vector<Value> &args) {
  LogCursor lc = open_log(args);
  if (!lc.ok) {
    return rt_ok(Value::null());
  }
  auto &cs = lc.ctx.cs;
  if (!cs.have(64)) {
    return rt_ok(Value::null());
  }
  td::RefInt256 timestamp = refint_u64(cs.fetch_ulong(64));
  auto pubkey = cs.have(256) ? cs.fetch_int256(256, false) : td::RefInt256{};
  if (pubkey.is_null()) {
    return rt_ok(Value::null());
  }
  Value::Fields ns;
  ns.emplace_back("timestamp", Value::make_int(std::move(timestamp)));
  ns.emplace_back("pubkey", hex_min(pubkey));
  return rt_ok(Value::make_obj(std::move(ns)));
}

// Merge every ContractDeploy in the consumed set's children into the
// produced block (after wrapper-merge).
void tgbtc_deploy_absorb(Block *produced, const ShaperMatch &m) {
  std::vector<Block *> deploys;
  for (Block *b : m.consumed) {
    for (Block *c : b->children_blocks) {
      if (c->btype == mch::btype::kContractDeploy) {
        deploys.push_back(c);
      }
    }
  }
  if (!deploys.empty()) {
    produced->merge_blocks(deploys);
  }
}

}  // namespace mch
