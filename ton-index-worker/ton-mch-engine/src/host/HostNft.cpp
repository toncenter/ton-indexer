#include "host/HostImpls.h"

#include "host/HostCommon.h"

#include "BlockTree.h"
#include "BuildRuntime.h"
#include "HostRegistry.h"
#include "MsgParse.h"
#include "parse/PSlice.h"
#include "btypes_gen.h"

#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cellslice.h"

#include <algorithm>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace mch {

namespace {

constexpr std::uint32_t kTeleitemBidInfoOpcode = 0x38127DE1;
constexpr std::uint32_t kTelemintOpcode = 0x299a3e15;

// Raw-address Str -> normalized Account; anything else (null, non-address)
// becomes addr_none.
Value account_from_value(const Value &v) {
  if (v.t == VType::Str) {
    return account_from_opt(std::optional<std::string>(v.str));
  }
  if (v.t == VType::Account) {
    return v;
  }
  return Value::make_account_none();
}

// Int/Amount becomes Amount; null becomes Amount-none.
Value amount_or_none(const Value &v) {
  if (v.is_null()) {
    return Value::make_amount_none();
  }
  return to_amount(v);
}

const EventNode *first_node(const Block *b) {
  return (b == nullptr || b->event_nodes.empty()) ? nullptr : b->event_nodes.front();
}


struct NftTransferMsg {
  td::RefInt256 query_id;
  Value new_owner{Value::null()};              // Account (addr_none if empty)
  Value response_destination{Value::null()};   // Account (empty -> addr_none)
  Value custom_payload{Value::null()};         // Bytes or Null
  td::RefInt256 forward_amount;
  Value forward_payload{Value::null()};        // Bytes or Null
};

td::Result<NftTransferMsg> parse_nft_transfer(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(ctx, open_body(body));
  auto &cs = ctx.cs;
  NftTransferMsg out;
  if (!cs.have(32 + 64)) {
    return td::Status::Error("nft_transfer: header underflow");
  }
  cs.advance(32);  // opcode
  out.query_id = refint_u64(cs.fetch_ulong(64));
  TRY_RESULT_ASSIGN(out.new_owner, load_address_py(cs));
  TRY_RESULT_ASSIGN(out.response_destination, load_address_py(cs));
  // custom_payload maybe-ref, encoded as BOC with crc32.
  td::Ref<vm::Cell> cp;
  if (!cs.fetch_maybe_ref(cp)) {
    return td::Status::Error("nft_transfer: bad custom_payload");
  }
  if (cp.not_null()) {
    TRY_RESULT(cp_boc, td_boc_serialize_crc(cp));
    out.custom_payload = Value::make_bytes(std::move(cp_boc));
  }
  TRY_RESULT_ASSIGN(out.forward_amount, load_coins_py(cs));
  // forward_payload: either a ref or the remaining slice, then BOC with crc32.
  if (cs.size() > 0) {
    bool is_right = cs.fetch_ulong(1) != 0;
    td::Ref<vm::Cell> fp_cell;
    if (is_right) {
      if (cs.size_refs() == 0) {
        return td::Status::Error("nft_transfer: forward_payload ref missing");
      }
      fp_cell = cs.fetch_ref();
    } else {
      PSlice ps;
      ps.cs = cs;
      ps.refs = ctx.all_refs;
      ps.off = 0;
      TRY_RESULT_ASSIGN(fp_cell, pslice_to_cell(ps));
    }
    TRY_RESULT(fp_boc, td_boc_serialize_crc(fp_cell));
    out.forward_payload = Value::make_bytes(std::move(fp_boc));
  }
  return out;
}

// Only prev_owner is read by the nft_transfer core. The full
// ownership+payload parse for the telegram core stays hand-written because
// it is a soft partial parse.
td::Result<Value> parse_ownership_prev_owner(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(v, parse_message_body("NftOwnershipAssignedPrevOwner", body));
  return *v.field("prev_owner");
}

// Full NftOwnershipAssigned + NftPayload parse for the telegram core.
struct OwnershipFull {
  td::RefInt256 query_id;
  Value prev_owner{Value::null()};   // Account (empty -> addr_none)
  bool has_payload{false};
  std::string payload_raw_boc;       // payload cell as BOC with crc32
  bool is_bid{false};
  td::RefInt256 bid;
};

td::Result<OwnershipFull> parse_ownership_full(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(ctx, open_body(body));
  auto &cs = ctx.cs;
  OwnershipFull out;
  if (!cs.have(32 + 64)) {
    return td::Status::Error("ownership: header underflow");
  }
  cs.advance(32);
  out.query_id = refint_u64(cs.fetch_ulong(64));
  TRY_RESULT_ASSIGN(out.prev_owner, load_address_py(cs));
  // Payload parse is soft: any failure leaves nft_payload unset.
  if (!cs.have(1)) {
    return out;  // no bit -> nft_payload unset
  }
  bool in_ref = cs.fetch_ulong(1) != 0;
  PSlice ps;
  if (in_ref) {
    if (cs.size_refs() == 0) {
      return out;  // missing ref -> nft_payload unset
    }
    ps = pslice_from_cell(cs.fetch_ref());
  } else {
    ps.cs = cs;
    ps.refs = ctx.all_refs;
    ps.off = 0;
  }
  // payload_raw_boc is the payload cell as BOC with crc32.
  auto r_cell = pslice_to_cell(ps);
  if (r_cell.is_error()) {
    return out;
  }
  auto r_boc = td_boc_serialize_crc(r_cell.move_as_ok());
  if (r_boc.is_error()) {
    return out;
  }
  out.payload_raw_boc = r_boc.move_as_ok();
  out.has_payload = true;
  // op peek on a copy; TeleitemBidInfo when op == 0x38127DE1.
  if (ps.cs.size() >= 32) {
    vm::CellSlice tmp = ps.cs;
    auto op = static_cast<std::uint32_t>(tmp.fetch_ulong(32));
    if (op == kTeleitemBidInfoOpcode) {
      auto r_bid = load_coins_py(tmp);
      if (r_bid.is_ok()) {
        out.bid = r_bid.move_as_ok();
        out.is_bid = true;
      }
    }
  }
  return out;
}


// Returns the `nft` dict Value, or faults on a malformed content.
EvalResult get_nft_data(BuildEnv &env, const Value &nft_address) {
  const std::string &addr_str = nft_address.str;
  Value nft = env.lookups->get("nft_item", std::vector<Value>{Value::make_str(addr_str)});

  Value::Fields d;
  d.emplace_back("address", nft_address);
  Value index = Value::null();
  Value collection = Value::null();
  bool exists = false;
  Value name = Value::null();
  Value type = Value::null();
  Value meta = Value::null();
  bool has_name_type = false;
  bool has_meta = false;

  if (!nft.is_null()) {
    exists = true;
    const Value *idx = nft.field("index");
    index = idx != nullptr ? *idx : Value::null();
    const Value *content = nft.field("content");
    if (content == nullptr || content->t != VType::Dict) {
      // Non-dict content faults (the uri membership test would be undefined).
      return rt_fault("nft content is not a dict");
    }
    const Value *uri = content->field("uri");
    bool fragment = fragment_uri(uri);
    if (fragment) {
      // Last path segment minus 5 chars is name; previous segment is type.
      std::vector<std::string> toks;
      std::string cur;
      for (char c : uri->str) {
        if (c == '/') {
          toks.push_back(cur);
          cur.clear();
        } else {
          cur.push_back(c);
        }
      }
      toks.push_back(cur);
      std::string last = toks.back();
      std::string nm = last.size() >= 5 ? last.substr(0, last.size() - 5) : std::string{};
      std::string ty = toks.size() >= 2 ? toks[toks.size() - 2] : std::string{};
      name = Value::make_str(nm);
      type = Value::make_str(ty);
      has_name_type = true;
    } else {
      meta = *content;
      has_meta = true;
    }
    const Value *coll = nft.field("collection_address");
    if (coll != nullptr && !coll->is_null()) {
      Value::Fields cf;
      cf.emplace_back("address", account_from_value(*coll));
      collection = Value::make_dict(std::move(cf));
    }
  }

  d.emplace_back("index", std::move(index));
  d.emplace_back("collection", std::move(collection));
  d.emplace_back("exists", Value::make_bool(exists));
  if (has_name_type) {
    d.emplace_back("name", std::move(name));
    d.emplace_back("type", std::move(type));
  }
  if (has_meta) {
    d.emplace_back("meta", std::move(meta));
  }
  return rt_ok(Value::make_dict(std::move(d)));
}

}  // namespace

// Fixed 13-key record, or Null for an unknown NFT item.
EvalResult nft_transfer_data(BuildEnv &env, const std::vector<Value> &args) {
  const Block *transfer = as_block(args[0]);
  const Block *assigned = as_block(args[1]);
  const Block *funding_parent = as_block(args[2]);
  if (transfer == nullptr) {
    return rt_fault("nft_transfer_data: null transfer");
  }
  const EventNode *tnode = first_node(transfer);
  if (tnode == nullptr) {
    return rt_fault("nft_transfer_data: transfer has no node");
  }

  auto r_body = block_body(transfer);
  if (r_body.is_error()) {
    return rt_fault("nft_transfer_data: transfer body");
  }
  auto r_nt = parse_nft_transfer(r_body.move_as_ok());
  if (r_nt.is_error()) {
    return rt_fault("nft_transfer_data: transfer parse");
  }
  NftTransferMsg nt = r_nt.move_as_ok();

  // prev_owner: ownership notification's prev_owner if present, else the
  // anchor message source.
  Value prev_owner;
  if (assigned != nullptr) {
    auto r_ab = block_body(assigned);
    if (r_ab.is_error()) {
      return rt_fault("nft_transfer_data: assigned body");
    }
    auto r_po = parse_ownership_prev_owner(r_ab.move_as_ok());
    if (r_po.is_error()) {
      return rt_fault("nft_transfer_data: ownership parse");
    }
    prev_owner = account_from_value(r_po.move_as_ok());
  } else {
    prev_owner = account_from_opt(tnode->msg != nullptr ? tnode->msg->source : std::nullopt);
  }

  Value new_owner = nt.new_owner;  // Account (addr_none if empty)
  Value response_destination =
      (nt.response_destination.t == VType::Account && !nt.response_destination.addr_none)
          ? nt.response_destination
          : Value::null();
  Value custom_payload = nt.custom_payload.is_null()
                             ? Value::null()
                             : Value::make_str(td::base64_encode(td::Slice(nt.custom_payload.str)));
  Value forward_payload = nt.forward_payload.is_null()
                              ? Value::null()
                              : Value::make_str(td::base64_encode(td::Slice(nt.forward_payload.str)));

  // nft dict: keyed by the anchor transaction account (the NFT item).
  Value nft_address = account_from_opt(
      tnode->msg != nullptr && tnode->msg->tx != nullptr
          ? std::optional<std::string>(tnode->msg->tx->account)
          : std::nullopt);
  EvalResult r_nftd = get_nft_data(env, nft_address);
  if (r_nftd.faulted) {
    return r_nftd;
  }
  Value nft = std::move(r_nftd.value);
  const Value *exists_f = nft.field("exists");
  if (exists_f == nullptr || exists_f->t != VType::Bool || !exists_f->boolean) {
    return rt_ok(Value::null());  // reject when d == null (unknown NFT item)
  }
  const Value *nft_addr_f = nft.field("address");
  Value nft_addr = nft_addr_f != nullptr ? *nft_addr_f : Value::make_account_none();

  bool is_purchase = false;
  Value marketplace = Value::null();
  Value marketplace_address = Value::null();
  Value price = Value::null();
  Value real_prev_owner = Value::null();

  const Block *prev = funding_parent;
  if (prev != nullptr) {
    const EventNode *pnode = first_node(prev);
    const Message *pmsg = pnode != nullptr ? pnode->msg : nullptr;
    std::string tx_account =
        (pmsg != nullptr && pmsg->tx != nullptr) ? pmsg->tx->account
        : (pnode != nullptr && pnode->tx != nullptr) ? pnode->tx->account
                                                     : std::string{};
    bool owner_match =
        prev->btype == mch::btype::kTonTransfer && pmsg != nullptr && pmsg->source &&
        same_account(account_from_opt(pmsg->source), new_owner);

    Value pd_marketplace = Value::null();
    Value pd_nft_address = Value::null();
    Value pd_price = Value::null();
    Value pd_real_prev = Value::null();
    bool have_pd = false;

    if (owner_match && !tx_account.empty()) {
      Value sale = env.lookups->get("nft_sale", std::vector<Value>{Value::make_str(tx_account)});
      if (!sale.is_null()) {
        const Value *ma = sale.field("marketplace_address");
        const Value *na = sale.field("nft_address");
        const Value *fp = sale.field("full_price");
        const Value *no = sale.field("nft_owner_address");
        pd_marketplace = ma != nullptr ? *ma : Value::null();
        pd_nft_address = na != nullptr ? *na : Value::null();
        pd_price = fp != nullptr ? *fp : Value::null();
        pd_real_prev = no != nullptr ? *no : Value::null();
        have_pd = true;
      }
    }
    if (!have_pd && !tx_account.empty()) {
      Value auc = env.lookups->get("nft_auction", std::vector<Value>{Value::make_str(tx_account)});
      if (!auc.is_null()) {
        const Value *ma = auc.field("mp_addr");
        const Value *na = auc.field("nft_addr");
        const Value *lb = auc.field("last_bid");
        const Value *no = auc.field("nft_owner");
        pd_marketplace = ma != nullptr ? *ma : Value::null();
        pd_nft_address = na != nullptr ? *na : Value::null();
        pd_price = lb != nullptr ? *lb : Value::null();
        pd_real_prev = no != nullptr ? *no : Value::null();
        have_pd = true;
      }
    }

    if (have_pd && same_account(account_from_value(pd_nft_address), nft_addr)) {
      Value real_owner = account_from_value(pd_real_prev);
      if (!same_account(real_owner, new_owner)) {
        is_purchase = true;
        marketplace = Value::make_str("getgems");
        marketplace_address = account_from_value(pd_marketplace);
        price = amount_or_none(pd_price);
        real_prev_owner = account_from_value(pd_real_prev);
      }
    }
  }

  Value::Fields ns;
  ns.emplace_back("is_purchase", Value::make_bool(is_purchase));
  ns.emplace_back("prev_owner", std::move(prev_owner));
  ns.emplace_back("new_owner", std::move(new_owner));
  ns.emplace_back("query_id", Value::make_int(std::move(nt.query_id)));
  ns.emplace_back("forward_amount", to_amount(Value::make_int(std::move(nt.forward_amount))));
  ns.emplace_back("response_destination", std::move(response_destination));
  ns.emplace_back("custom_payload", std::move(custom_payload));
  ns.emplace_back("forward_payload", std::move(forward_payload));
  ns.emplace_back("nft", std::move(nft));
  ns.emplace_back("marketplace", std::move(marketplace));
  ns.emplace_back("marketplace_address", std::move(marketplace_address));
  ns.emplace_back("price", std::move(price));
  ns.emplace_back("real_prev_owner", std::move(real_prev_owner));
  return rt_ok(Value::make_obj(std::move(ns)));
}

// Fixed 16-key record, or Null for an unknown NFT item. Royalty pair present
// only for 2-payout traces.
EvalResult telegram_nft_purchase_data(BuildEnv &env, const std::vector<Value> &args) {
  const Block *assigned = as_block(args[0]);
  const Block *payment = as_block(args[1]);
  const Block *payout_1 = as_block(args[2]);
  const Block *payout_2 = as_block(args[3]);
  if (assigned == nullptr) {
    return rt_fault("telegram: null assigned");
  }
  const EventNode *anode = first_node(assigned);
  const Message *amsg = anode != nullptr ? anode->msg : nullptr;
  auto r_body = block_body(assigned);
  if (r_body.is_error()) {
    return rt_fault("telegram: assigned body");
  }
  auto r_own = parse_ownership_full(r_body.move_as_ok());
  if (r_own.is_error()) {
    return rt_fault("telegram: ownership parse");
  }
  OwnershipFull own = r_own.move_as_ok();

  Value new_owner = account_from_opt(amsg != nullptr ? amsg->destination : std::nullopt);
  Value prev_owner = own.prev_owner.addr_none ? Value::null() : own.prev_owner;

  Value nft_address = account_from_opt(amsg != nullptr ? amsg->source : std::nullopt);
  EvalResult r_nftd = get_nft_data(env, nft_address);
  if (r_nftd.faulted) {
    return r_nftd;
  }
  Value nft = std::move(r_nftd.value);
  const Value *exists_f = nft.field("exists");
  if (exists_f == nullptr || exists_f->t != VType::Bool || !exists_f->boolean) {
    return rt_ok(Value::null());  // reject: unknown NFT item
  }

  bool is_purchase = false;
  Value forward_payload = Value::null();
  Value price = Value::null();
  Value marketplace = Value::null();
  Value real_prev_owner = Value::null();
  Value royalty_amount = Value::null();
  Value royalty_address = Value::null();
  Value payout_amount = Value::null();
  Value payout_address = Value::null();

  if (own.has_payload) {
    forward_payload = Value::make_str(td::base64_encode(td::Slice(own.payload_raw_boc)));
  }
  if (own.has_payload && own.is_bid) {
    is_purchase = true;
    price = to_amount(Value::make_int(own.bid));
    marketplace = Value::make_str("fragment");
    // real_prev_owner stays null.
    // is_mint: telemint call or nft_mint parent -> not a purchase.
    // Live on telegram_nft_assigned (telemint mint variant); inert on
    // telegram_nft_purchase, whose pattern forces a ton_transfer/external parent.
    bool is_mint = payment != nullptr &&
                   ((payment->btype == mch::btype::kCallContract && payment->opcode &&
                     *payment->opcode == kTelemintOpcode) ||
                    payment->btype == mch::btype::kNftMint);
    if (is_mint) {
      is_purchase = false;
    }
    bool payment_is_source_none =
        payment != nullptr && payment->btype == mch::btype::kCallContract &&
        [&] {
          const Value *s = payment->data.field("source");
          return s == nullptr || s->is_null();
        }();
    if (payment != nullptr &&
        (payment->btype == mch::btype::kTonTransfer || payment_is_source_none)) {
      std::vector<const Block *> payouts;
      if (payout_1 != nullptr) payouts.push_back(payout_1);
      if (payout_2 != nullptr) payouts.push_back(payout_2);
      std::stable_sort(payouts.begin(), payouts.end(),
                       [](const Block *a, const Block *b) {
                         return *first_node(a)->msg->created_lt < *first_node(b)->msg->created_lt;
                       });
      auto val = [](const Block *b) {
        const Value *v = b->data.field("value");
        return v != nullptr ? *v : Value::make_amount_none();
      };
      auto dest = [](const Block *b) {
        const Value *d = b->data.field("destination");
        return d != nullptr ? *d : Value::make_account_none();
      };
      if (payouts.size() > 1) {
        royalty_amount = val(payouts[0]);
        payout_amount = val(payouts[1]);
        royalty_address = dest(payouts[0]);
        payout_address = dest(payouts[1]);
      } else if (payouts.size() == 1) {
        payout_address = dest(payouts[0]);
        payout_amount = val(payouts[0]);
      }
    }
  }

  Value::Fields ns;
  ns.emplace_back("is_purchase", Value::make_bool(is_purchase));
  ns.emplace_back("new_owner", std::move(new_owner));
  ns.emplace_back("prev_owner", std::move(prev_owner));
  ns.emplace_back("query_id", Value::make_int(std::move(own.query_id)));
  ns.emplace_back("forward_amount", Value::null());
  ns.emplace_back("response_destination", Value::null());
  ns.emplace_back("custom_payload", Value::null());
  ns.emplace_back("forward_payload", std::move(forward_payload));
  ns.emplace_back("nft", std::move(nft));
  ns.emplace_back("real_prev_owner", std::move(real_prev_owner));
  ns.emplace_back("price", std::move(price));
  ns.emplace_back("marketplace", std::move(marketplace));
  ns.emplace_back("royalty_amount", std::move(royalty_amount));
  ns.emplace_back("royalty_address", std::move(royalty_address));
  ns.emplace_back("payout_amount", std::move(payout_amount));
  ns.emplace_back("payout_address", std::move(payout_address));
  return rt_ok(Value::make_obj(std::move(ns)));
}


// Looked-up nft_item in, Bool out. Null item or missing/absent content is
// false; otherwise the metadata uri must contain the Fragment NFT host.
// Never faults: missing content is false.
EvalResult is_teleitem(BuildEnv &, const std::vector<Value> &args) {
  const Value *content = args[0].is_null() ? nullptr : args[0].field("content");
  if (content == nullptr || content->t != VType::Dict) {
    return rt_ok(Value::make_bool(false));
  }
  const Value *uri = content->field("uri");
  bool fragment = fragment_uri(uri);
  return rt_ok(Value::make_bool(fragment));
}

// Purchase branch only: absorb the funding parent (produced.previous_block)
// unless it is a finish/stop sale-contract ton_transfer.
void nft_transfer_parent_absorb(Block *produced, const ShaperMatch &) {
  if (produced->data.is_null()) {
    return;
  }
  const Value *ip = produced->data.field("is_purchase");
  if (ip == nullptr || ip->t != VType::Bool || !ip->boolean) {
    return;
  }
  Block *prev = produced->previous_block;
  if (prev == nullptr) {
    return;
  }
  Block *parent = nullptr;
  if (prev->btype == mch::btype::kTonTransfer) {
    const Value *c = prev->data.field("comment");
    std::string cs = (c != nullptr && c->t == VType::Str) ? c->str : std::string{};
    bool has = c != nullptr && c->t == VType::Str;
    if (!(has && (cs == "finish" || cs == "stop"))) {
      parent = prev;
    }
  } else if (prev->btype == mch::btype::kCallContract) {
    const Value *src = prev->data.field("source");
    if (src == nullptr || src->is_null()) {
      parent = prev;
    }
  }
  if (parent != nullptr) {
    produced->merge_blocks(std::vector<Block *>{parent});
    produced->compact_connections();
  }
}

// True when the block's event nodes deploy exactly one distinct account.
// Repeated deployment nodes for the same normalized account still count once.
// Frozen-to-active transactions qualify here even though they do not create the
// separate contract_deploy side-effect block.
bool single_contract_deploy(const Block *b) {
  if (b == nullptr) {
    return false;
  }
  std::set<std::string> deployed;
  for (const EventNode *n : b->event_nodes) {
    const Transaction *tx = n != nullptr ? n->tx : nullptr;
    if (tx == nullptr || tx->orig_status == "active" || tx->end_status != "active") {
      continue;
    }
    deployed.insert(tx->account);
  }
  return deployed.size() == 1;
}

}  // namespace mch
