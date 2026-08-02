// tgBTC host functions and shaper. See
// host/HostImpls.h for the internal registry surface.
//
// tgbtc_mint_data derives its head, success log, and jetton_mint from the consumed
// set. Four shared event-log parsers return field objects or Null for unreadable
// bodies; each serves a full matcher and its log-only fallback.
// TgBTCBurnEvent tolerates truncated addresses by yielding addr_none, while an
// ABI address leaf would fail the whole parse. The other events need hexadecimal
// renderings unavailable in the expression language.
#include "host/HostImpls.h"

#include "host/HostCommon.h"

#include "BlockTree.h"
#include "BuildRuntime.h"
#include "HostRegistry.h"
#include "MsgParse.h"
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

const Block *first_call_op(const std::vector<const Block *> &consumed, std::uint32_t op) {
  for (const Block *b : consumed) {
    if (is_call_op(b, op)) {
      return b;
    }
  }
  return nullptr;
}

// int(txid).to_bytes(32, "little").hex(): 32-byte little-endian, lowercase hex.
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

// hex(x)[2:]: Python's minimal-length lowercase hex, no leading zeros, "0" for
// zero (to_hex_string(false, 0), bigint.hpp:2333). Used for both tgBTC pubkeys.
Value hex_min(const td::RefInt256 &v) {
  return Value::make_str(td::hex_string(v, /*upcase=*/false));
}

// `AccountId(x)` where x came straight out of pytoniq load_address(): an
// Account passes through, addr_none (a NULL Value, MsgParse.h contract) becomes
// AccountId(None), and an ExternalAddress (a Str) RAISES in Python
// (ton_utils.py AccountId.__init__), so `ok` clears and the caller rejects.
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

// TgBTCBurnEvent's GUARDED form, `AccountId(x) if isinstance(x, Address) else
// AccountId(None)`: an extern address is addr_none here, never a raise.
Value account_id_or_none(const Value &v) {
  return v.t == VType::Account ? v : Value::make_account_none();
}

// The single argument every parse fn below takes: a log Block whose body is
// opened and positioned past the 32 opcode bits (skipped unchecked, like every
// Python parser's leading load_uint(32)). Absent/unreadable -> no context.
struct LogCursor {
  BodyCtx ctx;
  bool ok{false};
};

LogCursor open_log(const std::vector<Value> &args) {
  LogCursor lc;
  if (args.size() != 1) {
    return lc;
  }
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

}  // namespace

// builders/tgbtc.py tgbtc_mint_data(consumed): the 9-field namespace, or Null
// to reject (missing head/log/mint or unparseable log).
EvalResult tgbtc_mint_data(BuildEnv &, const std::vector<Value> &args) {
  if (args.size() != 1 || args[0].t != VType::List) {
    return rt_fault("tgbtc_mint_data: bad arguments");
  }
  std::vector<const Block *> consumed;
  for (const Value &v : *args[0].items) {
    const Block *b = as_block(v);
    if (b != nullptr) {
      consumed.push_back(b);
    }
  }
  const Block *head = first_call_op(consumed, kTgbtcMintHead);
  const Block *success_log = first_call_op(consumed, kTgbtcMintSuccessLog);
  const Block *jetton_mint = nullptr;
  for (const Block *b : consumed) {
    if (b->btype == "jetton_mint") {
      jetton_mint = b;
      break;
    }
  }
  if (head == nullptr || success_log == nullptr || jetton_mint == nullptr) {
    return rt_ok(Value::null());  // core: missing piece -> reject
  }

  const Message *hmsg = block_msg(head);
  Value sender = account_from_opt(hmsg != nullptr ? hmsg->source : std::nullopt);

  // TgBTCMintEvent(success_log body): the reference `success` flag is set right
  // after the log parse; a later failure (jetton_mint field access) keeps the
  // already-built fields. Mirror by treating a parse failure as reject.
  auto r_body = block_body(success_log);
  if (r_body.is_error()) {
    return rt_ok(Value::null());  // success stays False -> None
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
  auto r_amt = load_coins_py(cs);
  td::RefInt256 amount = r_amt.is_ok() ? r_amt.move_as_ok() : td::make_refint(0);
  auto r_rcpt = load_address_py(cs);
  if (r_rcpt.is_error()) {
    return rt_ok(Value::null());
  }
  bool rcpt_ok = true;
  Value recipient = account_id_of(r_rcpt.ok(), rcpt_ok);
  if (!rcpt_ok || !cs.have(256)) {
    return rt_ok(Value::null());
  }
  auto txid = cs.fetch_int256(256, false);
  if (txid.is_null()) {
    return rt_ok(Value::null());
  }

  const Message *lmsg = block_msg(success_log);
  Value teleport_contract = account_from_opt(lmsg != nullptr ? lmsg->source : std::nullopt);
  std::string bitcoin_txid = txid_le_hex(txid);

  // jetton_mint.data['asset'] + AccountId(jetton_mint.data['to_jetton_wallet']).
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

// Event-log parsers

// TgBTCMintEvent 0x77A80EF3: coins amount, MsgAddress recipient, uint256 txid.
EvalResult tgbtc_mint_log(BuildEnv &, const std::vector<Value> &args) {
  LogCursor lc = open_log(args);
  if (!lc.ok) {
    return rt_ok(Value::null());
  }
  auto &cs = lc.ctx.cs;
  auto r_amt = load_coins_py(cs);
  if (r_amt.is_error()) {
    return rt_ok(Value::null());
  }
  auto r_rcpt = load_address_py(cs);
  if (r_rcpt.is_error()) {
    return rt_ok(Value::null());
  }
  bool rcpt_ok = true;
  Value recipient = account_id_of(r_rcpt.ok(), rcpt_ok);
  auto txid = cs.have(256) ? cs.fetch_int256(256, false) : td::RefInt256{};
  if (!rcpt_ok || txid.is_null()) {
    return rt_ok(Value::null());
  }
  Value::Fields ns;
  ns.emplace_back("amount", Value::make_int(r_amt.move_as_ok()));
  ns.emplace_back("recipient", std::move(recipient));
  ns.emplace_back("bitcoin_txid", Value::make_str(txid_le_hex(txid)));
  return rt_ok(Value::make_obj(std::move(ns)));
}

// TgBTCBurnEvent 0xCA444CE6: coins amount, then TWO MsgAddresses parsed
// TOLERANTLY. The Python parser assigns both locals to None up front and wraps
// the pair in one try/except, so a body that runs out mid-address leaves the
// already-read one set and the rest addr_none, reproduced exactly: on the
// first load_address error, everything from there on stays addr_none.
EvalResult tgbtc_burn_log(BuildEnv &, const std::vector<Value> &args) {
  LogCursor lc = open_log(args);
  if (!lc.ok) {
    return rt_ok(Value::null());
  }
  auto &cs = lc.ctx.cs;
  auto r_amt = load_coins_py(cs);
  if (r_amt.is_error()) {
    return rt_ok(Value::null());  // the one failure the Python try does NOT cover
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

// builders/tgbtc.py tgbtc_deploy_absorb shaper: merge every ContractDeploy in
// the consumed set's children_blocks into the produced block (post wrapper-merge).
void tgbtc_deploy_absorb(Block *produced, const ShaperMatch &m) {
  std::vector<Block *> deploys;
  for (Block *b : m.consumed) {
    for (Block *c : b->children_blocks) {
      if (c->btype == "contract_deploy") {
        deploys.push_back(c);
      }
    }
  }
  if (!deploys.empty()) {
    produced->merge_blocks(deploys);
  }
}

}  // namespace mch
