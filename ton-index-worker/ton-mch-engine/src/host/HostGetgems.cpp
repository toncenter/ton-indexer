// Getgems host shaper (builders/getgems.py getgems_proxy_insert). See
// host/HostImpls.h for the internal registry surface and HostRegistry.h for the
// public one.
//
// The GetGems purchase/seller-payout predicates and proxy-insert shaper are
// registered in HostRegistry and are available to classify. The finish/stop
// paths read the parent ton_transfer comment populated by leaf_comment_info in
// BlockTree, so both predicate selection and shaping use production comment data.
#include "host/HostImpls.h"

#include "host/HostCommon.h"

#include "BlockTree.h"
#include "BuildRuntime.h"
#include "HostRegistry.h"

#include <vector>

namespace mch {

namespace {

// builders/getgems.py _from_finish_stop_sale: the anchor's parent is a
// finish/stop ton_transfer (the sale contract finalizing the deal).
bool from_finish_stop_sale(const Block *nft_block) {
  const Block *prev = nft_block->previous_block;
  if (prev == nullptr || prev->btype != "ton_transfer") {
    return false;
  }
  const Value *c = prev->data.field("comment");
  return c != nullptr && c->t == VType::Str && (c->str == "finish" || c->str == "stop");
}

// builders/getgems.py _find_seller_payout: the seller ton_transfer among the
// finish/stop parent's children (sale-contract branch) else the nft_transfer's
// own children, whose destination is real_prev_owner.
Block *find_seller_payout(const Block *nft) {
  const Value *rpo = nft->data.field("real_prev_owner");
  if (rpo == nullptr || rpo->is_null()) {
    return nullptr;
  }
  const std::vector<Block *> *candidates = &nft->next_blocks;
  if (from_finish_stop_sale(nft) && nft->previous_block != nullptr) {
    candidates = &nft->previous_block->next_blocks;
  }
  for (Block *n : *candidates) {
    if (n == nft || n->btype != "ton_transfer") {
      continue;
    }
    const Value *dest = n->data.field("destination");
    if (dest != nullptr && same_account(*dest, *rpo)) {
      return n;
    }
  }
  return nullptr;
}

Value copy_field(const Block *b, const char *name) {
  const Value *f = b->data.field(name);
  return f != nullptr ? *f : Value::null();
}

}  // namespace

// builders/getgems.py getgems_purchase predicate.
bool getgems_purchase(const Block *block) {
  if (block->btype != "nft_transfer" || block->data.is_null()) {
    return false;
  }
  const Value *ip = block->data.field("is_purchase");
  const Value *mp = block->data.field("marketplace");
  return ip != nullptr && ip->t == VType::Bool && ip->boolean && mp != nullptr &&
         mp->t == VType::Str && mp->str == "getgems";
}

// builders/getgems.py getgems_seller_payout predicate (normal child branch only).
bool getgems_seller_payout(const Block *block) {
  if (block->btype != "ton_transfer") {
    return false;
  }
  const Block *anchor = block->previous_block;
  if (anchor == nullptr || anchor->data.is_null() || from_finish_stop_sale(anchor)) {
    return false;
  }
  const Value *rpo = anchor->data.field("real_prev_owner");
  const Value *dest = block->data.field("destination");
  return rpo != nullptr && dest != nullptr && same_account(*dest, *rpo);
}

// builders/getgems.py getgems_nft_purchase_data(nft): seller-payout scan + the
// NftPurchaseData core, returned as the 18-field namespace, or Null to reject
// (missing real_prev_owner / seller payout not found).
EvalResult getgems_nft_purchase_data(BuildEnv &, const std::vector<Value> &args) {
  if (args.size() != 1) {
    return rt_fault("getgems_nft_purchase_data: bad arguments");
  }
  const Block *nft = as_block(args[0]);
  if (nft == nullptr || nft->data.is_null()) {
    return rt_ok(Value::null());
  }
  const Value *rpo = nft->data.field("real_prev_owner");
  if (rpo == nullptr || rpo->is_null()) {
    return rt_ok(Value::null());  // core: real_prev_owner None -> reject
  }
  Block *payout = find_seller_payout(nft);
  if (payout == nullptr) {
    return rt_ok(Value::null());  // core: ton_transfer to seller not found -> reject
  }

  const Value *nft_d = nft->data.field("nft");
  if (nft_d == nullptr) {
    return rt_fault("getgems: nft dict missing");
  }
  const Value *addr = nft_d->field("address");
  const Value *coll = nft_d->field("collection");
  const Value *index = nft_d->field("index");
  Value collection_address = Value::null();
  if (coll != nullptr && !coll->is_null()) {
    const Value *ca = coll->field("address");
    collection_address = ca != nullptr ? *ca : Value::null();
  }

  Value::Fields ns;
  ns.emplace_back("nft_address", addr != nullptr ? *addr : Value::null());
  ns.emplace_back("collection_address", std::move(collection_address));
  ns.emplace_back("nft_index", index != nullptr ? *index : Value::null());
  ns.emplace_back("prev_owner", copy_field(nft, "prev_owner"));
  ns.emplace_back("new_owner", copy_field(nft, "new_owner"));
  ns.emplace_back("query_id", copy_field(nft, "query_id"));
  ns.emplace_back("forward_amount", copy_field(nft, "forward_amount"));
  ns.emplace_back("response_destination", copy_field(nft, "response_destination"));
  ns.emplace_back("custom_payload", copy_field(nft, "custom_payload"));
  ns.emplace_back("forward_payload", copy_field(nft, "forward_payload"));
  ns.emplace_back("payout_amount", copy_field(payout, "value"));
  ns.emplace_back("payout_comment_encrypted", copy_field(payout, "encrypted"));
  ns.emplace_back("payout_comment_encoded", copy_field(payout, "comment_encoded"));
  ns.emplace_back("payout_comment", copy_field(payout, "comment"));
  ns.emplace_back("price", copy_field(nft, "price"));
  ns.emplace_back("real_prev_owner", copy_field(nft, "real_prev_owner"));
  ns.emplace_back("marketplace", copy_field(nft, "marketplace"));
  ns.emplace_back("marketplace_address", copy_field(nft, "marketplace_address"));
  return rt_ok(Value::make_obj(std::move(ns)));
}

// builders/getgems.py getgems_proxy_insert shaper: finish/stop sale-contract
// branch only. Verbatim legacy surgery — insert an EmptyBlock proxy between the
// unconsumed finish/stop parent and [produced, payout], then absorb the sibling
// payout into the produced block (it was never pattern-consumed).
void getgems_proxy_insert(Block *produced, const ShaperMatch &m) {
  Block *nft = m.capture("nft");
  if (nft == nullptr || produced == nullptr) {
    return;
  }
  Block *parent = produced->previous_block;  // finish/stop parent (post-merge)
  if (parent == nullptr || parent->btype != "ton_transfer") {
    return;  // normal branch: no proxy, no surgery
  }
  const Value *cmt = parent->data.field("comment");
  if (cmt == nullptr || cmt->t != VType::Str || (cmt->str != "finish" && cmt->str != "stop")) {
    return;
  }
  const Value *real_prev_owner = nft->data.field("real_prev_owner");
  if (real_prev_owner == nullptr || real_prev_owner->is_null()) {
    return;
  }
  Block *payout = nullptr;
  for (Block *n : parent->next_blocks) {
    if (n == produced || n->btype != "ton_transfer") {
      continue;
    }
    const Value *dest = n->data.field("destination");
    if (dest != nullptr && same_account(*dest, *real_prev_owner)) {
      payout = n;
      break;
    }
  }
  if (payout == nullptr || m.arena == nullptr) {
    return;
  }
  Block *proxy = m.arena->make("empty");
  parent->insert_between({produced, payout}, proxy);
  produced->merge_blocks({payout, proxy});
  produced->compact_connections();
}

}  // namespace mch
