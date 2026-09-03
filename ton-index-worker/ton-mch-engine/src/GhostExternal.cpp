// Ghost-external fallback, see GhostExternal.h for WHY this is engine code and
// not an MCH spec.
//
// The wallet-external decoder below is a port of
// indexer/events/blocks/messages/externals.py, kept pytoniq-faithful down to
// the failure modes because the fallback chain DEPENDS on them: the
// opcode-checked tg-wallet reader must run before the permissive v3/v4 readers;
// a v5 body is then tried as v3 first and must FAIL (its single ref is an
// OutList node, not a message), and a v4 body is accepted BY the v3 reader (v4
// is v3 + an op byte, and the message refs sit at the same place). The decoder
// is therefore four whole-body attempts in a deliberate order, not a tag sniff.
#include "GhostExternal.h"

#include "BuildRuntime.h"  // LookupSource
#include "host/HostCommon.h"
#include "parse/PSlice.h"
#include "parse/Parsers.h"

#include "common/refint.h"
#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cellslice.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mch {

namespace {

constexpr std::uint32_t kJettonTransferOpcode = 0x0F8A7EA5;
constexpr std::uint32_t kTgSendOneMessageExternal = 0x63896E75;
constexpr std::uint32_t kTgSendBulkMessagesExternal = 0x73896E75;
constexpr std::uint32_t kTgChangePublicKeyExternal = 0xFBBA99C8;

// externals.py PayloadMessage: ONE unsent outgoing message from a wallet body.
struct WalletPayload {
  // PayloadMessage.info is not None. False == the cell is not an int_msg_info
  // (tag bit 1); Python leaves `info` None and init_from_external SKIPS the
  // payload, it is not a parse error and does not fail the wallet attempt.
  bool has_info{false};
  Value dest;  // Account (addr_none when the message had no destination)
  td::RefInt256 value;
  bool bounce{false};
  bool bounced{false};
  td::Ref<vm::Cell> body;
  std::optional<std::uint32_t> opcode;
  std::string cell_hash_b64;  // PayloadMessage.hash
};

// InternalMsgInfo.deserialize (externals.py:9-61). `with_extra` picks the two
// variants Python tries in order: the TL-B-correct one (value is a
// CurrencyCollection: grams + Maybe ^ExtraCurrencyCollection) and the
// grams-only one it retries with when the first raises.
//
// Deviation, same as ParseVesting: a present extra-currency dict ref is skipped,
// not deep-validated (pytoniq's HashMap.parse would validate it). Bit-equivalent
// for every well-formed dict.
td::Status load_int_msg_info(vm::CellSlice &cs, bool with_extra, WalletPayload &p) {
  if (!cs.have(4)) return td::Status::Error("payload: info underflow");
  if (cs.fetch_ulong(1) != 0) return td::Status::Error("payload: not int_msg_info");
  cs.advance(1);  // ihr_disabled
  bool bounce = cs.fetch_ulong(1) != 0;
  bool bounced = cs.fetch_ulong(1) != 0;
  TRY_RESULT(src, load_address_py(cs));
  (void)src;
  TRY_RESULT(dest, load_address_py(cs));
  TRY_RESULT(grams, load_coins_py(cs));
  if (with_extra) {
    if (!cs.have(1)) return td::Status::Error("payload: extra dict underflow");
    if (cs.fetch_ulong(1)) {
      if (cs.size_refs() == 0) return td::Status::Error("payload: extra dict ref missing");
      cs.fetch_ref();
    }
  }
  TRY_RESULT(ihr_fee, load_coins_py(cs));
  (void)ihr_fee;
  TRY_RESULT(fwd_fee, load_coins_py(cs));
  (void)fwd_fee;
  // created_lt:uint64 created_at:uint32, parsed by Python but then OVERWRITTEN
  // with the external's own tx lt/now, so only the cursor matters.
  if (!cs.have(64 + 32)) return td::Status::Error("payload: lt/at underflow");
  cs.advance(64 + 32);
  p.bounce = bounce;
  p.bounced = bounced;
  p.dest = std::move(dest);
  p.value = std::move(grams);
  return td::Status::OK();
}

// PayloadMessage.__init__ (externals.py:63-101).
td::Result<WalletPayload> parse_payload(const td::Ref<vm::Cell> &cell) {
  WalletPayload p;
  p.cell_hash_b64 = td::base64_encode(cell->get_hash().as_slice());
  TRY_RESULT(fresh, open_ref_cell(cell));  // cell.to_slice()
  vm::CellSlice cs = fresh;
  bool ok = cs.have(1) && cs.prefetch_ulong(1) == 0 &&
            load_int_msg_info(cs, /*with_extra=*/true, p).is_ok();
  if (!ok) {
    cs = fresh;  // Python's `cp` retry copy
    if (!cs.have(1)) return td::Status::Error("payload: empty");
    if (cs.prefetch_ulong(1) != 0) {
      return p;  // tag 1 -> info stays None -> the payload is skipped, not fatal
    }
    TRY_STATUS(load_int_msg_info(cs, /*with_extra=*/false, p));
  }
  TRY_RESULT(body, message_any_body(cs));
  p.body = std::move(body);
  // PayloadMessage.opcode: body.to_slice().load_uint(32), None on any failure.
  auto r_body_cs = open_ref_cell(p.body);
  if (r_body_cs.is_ok()) {
    vm::CellSlice bcs = r_body_cs.move_as_ok();
    if (bcs.have(32)) {
      p.opcode = static_cast<std::uint32_t>(bcs.prefetch_ulong(32));
    }
  }
  p.has_info = true;
  return p;
}

// The shared tail of WalletV3/V4ExternalMessage: every remaining ref is a
// message. One bad payload aborts the whole attempt (Python: the exception
// escapes the wallet class and extract_payload tries the next one).
td::Result<std::vector<WalletPayload>> payloads_from_refs(vm::CellSlice &cs) {
  std::vector<WalletPayload> out;
  while (cs.size_refs() > 0) {
    TRY_RESULT(p, parse_payload(cs.fetch_ref()));
    out.push_back(std::move(p));
  }
  return out;
}

// WalletTgExternalMessage: signature:bits512 opcode:uint32, then the common
// subwallet_id/valid_until/seqno header. SendOneMessageRequestE carries one
// inline mode + message ref. SendBulkMessagesRequestE carries a Tolk array:
// uint8 length + Maybe ^first_chunk; each chunk starts with Maybe ^next_chunk
// and stores every remaining ref as one mode:uint8 + ^message item.
//
// The declared array length and send modes are intentionally consumed but not
// validated. This mirrors externals.py; the wallet contract validates them.
td::Result<std::vector<WalletPayload>> parse_wallet_tg(vm::CellSlice cs) {
  constexpr unsigned kSignedRequestHeaderBits = 512 + 32 + 96;
  if (!cs.have(kSignedRequestHeaderBits)) {
    return td::Status::Error("tg-wallet: header underflow");
  }

  cs.advance(512);  // signature
  const auto opcode = static_cast<std::uint32_t>(cs.fetch_ulong(32));
  if (opcode != kTgSendOneMessageExternal && opcode != kTgSendBulkMessagesExternal &&
      opcode != kTgChangePublicKeyExternal) {
    return td::Status::Error("tg-wallet: unknown request");
  }
  cs.advance(96);  // subwallet_id, valid_until, seqno

  std::vector<WalletPayload> out;
  if (opcode == kTgChangePublicKeyExternal) {
    return out;
  }

  if (!cs.have(8)) return td::Status::Error("tg-wallet: payload header underflow");
  cs.advance(8);  // send mode for one message, declared array length for bulk

  if (opcode == kTgSendOneMessageExternal) {
    if (cs.size_refs() == 0) return td::Status::Error("tg-wallet: message ref missing");
    TRY_RESULT(p, parse_payload(cs.fetch_ref()));
    out.push_back(std::move(p));
    return out;
  }

  if (!cs.have(1)) return td::Status::Error("tg-wallet: array maybe-ref underflow");
  td::Ref<vm::Cell> chunk;
  if (cs.fetch_ulong(1)) {
    if (cs.size_refs() == 0) return td::Status::Error("tg-wallet: first chunk ref missing");
    chunk = cs.fetch_ref();
  }

  while (chunk.not_null()) {
    TRY_RESULT(s, open_ref_cell(chunk));
    if (!s.have(1)) return td::Status::Error("tg-wallet: next chunk maybe-ref underflow");

    td::Ref<vm::Cell> next;
    if (s.fetch_ulong(1)) {
      if (s.size_refs() == 0) return td::Status::Error("tg-wallet: next chunk ref missing");
      next = s.fetch_ref();
    }

    const unsigned items = s.size_refs();
    for (unsigned i = 0; i < items; ++i) {
      if (!s.have(8)) return td::Status::Error("tg-wallet: send mode underflow");
      s.advance(8);
      TRY_RESULT(p, parse_payload(s.fetch_ref()));
      out.push_back(std::move(p));
    }
    chunk = std::move(next);
  }
  return out;
}

// WalletV3ExternalMessage: signature:bits512 subwallet_id valid_until seqno.
td::Result<std::vector<WalletPayload>> parse_wallet_v3(vm::CellSlice cs) {
  if (!cs.have(512 + 96)) return td::Status::Error("v3: header underflow");
  cs.advance(512 + 96);
  return payloads_from_refs(cs);
}

// WalletV4ExternalMessage: v3 + an 8-bit op.
td::Result<std::vector<WalletPayload>> parse_wallet_v4(vm::CellSlice cs) {
  if (!cs.have(512 + 96 + 8)) return td::Status::Error("v4: header underflow");
  cs.advance(512 + 96 + 8);
  return payloads_from_refs(cs);
}

// WalletV5R1ExternalMessage: opcode wallet_id valid_until seqno, then the
// out_actions OutList walked ref-first (prev at ref 0, the message at ref 1).
// Python reads NEITHER the 0x7369676e opcode nor the action tags, the walk
// alone is the discriminator, which is what makes v3/v4 bodies fail it.
td::Result<std::vector<WalletPayload>> parse_wallet_v5r1(vm::CellSlice cs) {
  if (!cs.have(128)) return td::Status::Error("v5r1: header underflow");
  cs.advance(128);
  std::vector<WalletPayload> out;
  if (!cs.have(1)) return td::Status::Error("v5r1: maybe-ref underflow");
  td::Ref<vm::Cell> cur;
  if (cs.fetch_ulong(1)) {
    if (cs.size_refs() == 0) return td::Status::Error("v5r1: out_actions ref missing");
    cur = cs.fetch_ref();
  }
  while (cur.not_null()) {
    TRY_RESULT(node, open_ref_cell(cur));
    if (node.size() == 0) break;  // out_list_empty
    if (node.size_refs() < 2) return td::Status::Error("v5r1: action node refs missing");
    cur = node.fetch_ref();
    TRY_RESULT(p, parse_payload(node.fetch_ref()));
    out.push_back(std::move(p));
  }
  return out;
}

// extract_payload_from_wallet_message: tg-wallet, v3, v4, v5r1 in that order;
// first whole-body success wins, none -> no payloads. Tg-wallet has to precede
// v3/v4 because their readers accept almost any sufficiently large body.
std::vector<WalletPayload> extract_payload_from_wallet_message(const td::Ref<vm::Cell> &body) {
  auto r_cs = open_ref_cell(body);
  if (r_cs.is_error()) return {};
  const vm::CellSlice cs = r_cs.move_as_ok();
  using Attempt = td::Result<std::vector<WalletPayload>> (*)(vm::CellSlice);
  static const Attempt kAttempts[] = {parse_wallet_tg, parse_wallet_v3, parse_wallet_v4,
                                      parse_wallet_v5r1};
  for (Attempt attempt : kAttempts) {
    auto r = attempt(cs);
    if (r.is_ok()) return r.move_as_ok();
  }
  return {};
}

}  // namespace

std::size_t synthesize_ghost_children(EventTree &tree, EventNode *root) {
  root->forced_failed = true;  // node.failed = True, before anything can fail
  const Message *ext = root->msg;
  const Transaction *tx = root->tx;
  if (ext == nullptr || !ext->content || tx == nullptr) return 0;
  auto r_raw = td::base64_decode(td::Slice(ext->content->body));
  if (r_raw.is_error()) return 0;
  auto r_cell = vm::std_boc_deserialize(r_raw.move_as_ok());
  if (r_cell.is_error()) return 0;

  std::size_t added = 0;
  std::vector<WalletPayload> payloads = extract_payload_from_wallet_message(r_cell.move_as_ok());
  for (std::size_t idx = 0; idx < payloads.size(); idx++) {
    const WalletPayload &p = payloads[idx];
    if (!p.has_info) continue;  // enumerate() counted it, so idx keeps its slot
    // dest is addr_std (Account) or addr_none (Null). Anything else is an
    // addr_extern in an int_msg_info, where Python's `.dest.to_str()` raises and
    // takes the WHOLE fallback down to the `unknown` row, so bail, don't skip.
    // Unwind the children already hung on `root`: reporting 0 has to mean the
    // node is untouched, or a later walk would see half a synthesis.
    if (!p.dest.is_null() && p.dest.t != VType::Account) {
      root->children.resize(root->children.size() - added);
      return 0;
    }
    auto r_boc = td_boc_serialize(p.body);
    if (r_boc.is_error()) continue;
    std::string body_hash = td::base64_encode(p.body->get_hash().as_slice());

    auto m = std::make_unique<Message>();
    // The ONE identifier in the row set that no chain object supplies: these
    // messages were never sent, so they have no hash. Python mints
    // b64(b64(payload_cell_hash) + str(idx)) and it reaches the action_id
    // through sha256, the `idx` suffix is load-bearing, it is what keeps two
    // IDENTICAL payloads in one external from collapsing into one row.
    m->msg_hash = td::base64_encode(td::Slice(p.cell_hash_b64 + std::to_string(idx)));
    m->tx_hash = tx->hash;
    m->tx_lt = tx->lt;
    m->direction = "in";
    m->source = tx->account;
    m->destination = (p.dest.t == VType::Account && !p.dest.addr_none)
                         ? std::optional<std::string>(p.dest.str)
                         : std::nullopt;
    m->value = p.value.is_null() ? std::nullopt : std::optional<std::int64_t>(p.value->to_long());
    m->created_lt = tx->lt;
    m->created_at = tx->now;
    m->opcode = p.opcode ? std::optional<std::int64_t>(*p.opcode) : std::nullopt;
    m->bounce = p.bounce;
    m->bounced = p.bounced;
    // Provably the tx the ghost hangs off (to_tree took it from this same
    // non-const Trace); const only because EventNode holds tx read-only.
    m->tx = const_cast<Transaction *>(tx);
    // MsgContent::body is a base64 BOC by contract (block_body / the leaf comment
    // parse both base64_decode it), td_boc_serialize hands back RAW bytes.
    m->content = MsgContent{body_hash, td::base64_encode(td::Slice(r_boc.ok()))};

    auto n = std::make_unique<EventNode>();
    n->msg = m.get();
    n->tx = tx;
    n->ghost = true;
    n->forced_failed = true;
    n->parent = root;
    root->children.push_back(n.get());
    tree.synthetic_msgs.push_back(std::move(m));
    tree.nodes.push_back(std::move(n));
    added++;
  }
  return added;
}

Block *fallback_jetton_transfer(Block *block, BlockArena &arena, const LookupSource &src) {
  if (block->btype != "call_contract" || !block->opcode ||
      *block->opcode != kJettonTransferOpcode) {
    return nullptr;  // test_self
  }
  const Message *msg = block_msg(block);
  if (msg == nullptr) return nullptr;
  auto r_body = block_body(block);
  if (r_body.is_error()) return nullptr;
  auto r_msg = parse_jetton_transfer(r_body.move_as_ok());
  if (r_msg.is_error()) return nullptr;  // JettonTransfer() raised -> try_build None
  const Value jt = r_msg.move_as_ok();
  auto fld = [&jt](const char *name) {
    const Value *v = jt.field(name);
    return v != nullptr ? *v : Value::null();
  };

  Value sender_wallet = account_from_opt(msg->destination);
  // No internal-transfer leg exists on this path, so the asset comes off the
  // SENDER's wallet interface. get_jetton_wallet None -> asset None -> the fill
  // leaves action.asset null.
  Value asset = Value::null();
  Value wallet = src.fetch("jetton_wallet", {sender_wallet});
  if (!wallet.is_null()) {
    const Value *jetton = wallet.field("jetton");
    if (jetton != nullptr && jetton->t == VType::Str) {
      auto norm = normalize_raw_address(jetton->str);
      asset = Value::make_asset_jetton(norm ? *norm : jetton->str);
    }
  }

  Value::Fields f;
  f.emplace_back("has_internal_transfer", Value::make_bool(false));
  f.emplace_back("sender", account_from_opt(msg->source));
  f.emplace_back("sender_wallet", std::move(sender_wallet));
  f.emplace_back("receiver", fld("destination"));
  f.emplace_back("receiver_wallet", Value::null());
  f.emplace_back("response_address", fld("response"));
  f.emplace_back("forward_amount", to_amount(fld("forward_amount")));
  f.emplace_back("query_id", fld("query_id"));
  f.emplace_back("asset", std::move(asset));
  f.emplace_back("amount", to_amount(fld("amount")));
  // Python holds forward/custom payload as base64 STR here and as raw bytes on
  // the spec path; the engine carries Bytes everywhere and both renderers
  // (cellhash / base64 text) erase the difference.
  f.emplace_back("forward_payload", fld("forward_payload"));
  f.emplace_back("custom_payload", fld("custom_payload"));
  f.emplace_back("comment", fld("comment"));
  f.emplace_back("encrypted_comment", fld("encrypted_comment"));
  f.emplace_back("payload_opcode", fld("payload_sum_type"));
  f.emplace_back("stonfi_swap_body", fld("stonfi_swap_body"));

  Block *produced = arena.make("jetton_transfer");
  produced->data = Value::make_dict(std::move(f));
  if (!produced->merge_blocks({block})) return nullptr;
  produced->failed = block->failed;
  return produced;
}

}  // namespace mch
