// StateInit readers take listing parameters from the deployment message and
// select the layout by code hash. Parse failures return Null. The rest are
// predicates and a chained auction-to-NFT lookup the language cannot express.
#include "host/HostImpls.h"

#include "host/BlockViews.h"
#include "host/HostCommon.h"

#include "BlockTree.h"
#include "BuildRuntime.h"
#include "HostRegistry.h"
#include "parse/PSlice.h"
#include "btypes_gen.h"

#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cellslice.h"

#include <map>
#include <optional>
#include <string>
#include <vector>

namespace mch {

namespace {

constexpr char kDnsCodeHash[] = "i1/8nr/TkGTY1fVuRlnIJrt1k5I/XKSHKL5NYK9vUfk=";
constexpr char kDnsCollection[] =
    "0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF";

// Unlisted or missing code hash falls back to "latest", which each reader
// resolves to its newest layout.
const std::map<std::string, std::string> &sale_versions() {
  static const std::map<std::string, std::string> m = {
      {"2pufziLofEllctIDZSWVebzO+RpyA1fMvowFLvyb4I8=", "v1"},
      {"gnj0xSM95vvtyWmvUZNEp6m//FRIVtuphqlcC8+Fcck=", "v2"},
      {"MgUN+sRPZIZrzIbyzZ4TBf6dyts5WcACI3z7CQLUQyM=", "v3"},
      {"3rU7bFdlwebNI4v0e8XoO6WWvcwEsLhM1Qqx5HSgjzE=", "v3r2"},
      {"JCIfpXHlQuBVx3vt/b9SfHr0YM/cfzRMRQeHtM+h600=", "v3r3"},
      {"a5WmQYucnSNZBF0edVm41UmuDlBvJMqrWPowyPsf64Y=", "v4r1"},
  };
  return m;
}

const std::map<std::string, std::string> &auction_versions() {
  static const std::map<std::string, std::string> m = {
      {"zlp4U06qps7tja/UhtB262CpsNbb+1Nnb2YmScBomVY=", "v4r1"},
      {"ZmiHL6eXBUQ//UdSPo6eqfdquZ+aC1nSfej4GhwnudQ=", "v2"},
      {"G9nFo5v/t6DzQViLXdkrgTqEK/Ze8UEJOCIAzq+Pct8=", "v3r2"},
      {"u29ireD+stefqzuK6/CTCvmFU99gCTsgJ/Covxab/Ow=", "v3r3"},
      {"/ACindAgW83MDT/7nKOMw8jBWexg2KpUMkCpLxBZLUA=", "v1"},
  };
  return m;
}

std::string pick_version(const std::map<std::string, std::string> &table, const Value &code_hash,
                         const char *latest) {
  if (code_hash.t == VType::Str) {
    auto it = table.find(code_hash.str);
    if (it != table.end()) {
      return it->second;
    }
  }
  return latest;
}

// StateInit data cell of a deploying message.
td::Result<vm::CellSlice> state_init_data(const Block *b) {
  const Message *msg = block_msg(b);
  if (msg == nullptr || !msg->init_state) {
    return td::Status::Error("no init_state");
  }
  TRY_RESULT(raw, td::base64_decode(td::Slice(msg->init_state->body)));
  TRY_RESULT(root, vm::std_boc_deserialize(raw));
  TRY_RESULT(cs, open_ref_cell(root));
  TRY_RESULT(data, state_init_data_cell(cs));
  return open_ref_cell(data);
}

// Fixed-width unsigned read; underflow is a parse reject.
td::Result<td::RefInt256> load_uint_py(vm::CellSlice &cs, int bits) {
  if (!cs.have(bits)) return td::Status::Error("uint underflow");
  return cs.fetch_int256(bits, false);
}

td::Status skip_bits(vm::CellSlice &cs, int bits) {
  if (!cs.have(bits)) return td::Status::Error("bits underflow");
  cs.advance(bits);
  return td::Status::OK();
}

td::Status skip_address(vm::CellSlice &cs) {
  TRY_RESULT(v, load_address_py(cs));
  (void)v;
  return td::Status::OK();
}

td::Status skip_coins(vm::CellSlice &cs) {
  TRY_RESULT(v, load_coins_py(cs));
  (void)v;
  return td::Status::OK();
}

Value opt_amount(const td::RefInt256 &v) {
  return v.is_null() ? Value::null() : Value::make_int(v);
}

td::Result<vm::CellSlice> load_ref_slice(vm::CellSlice &cs) {
  if (cs.size_refs() == 0) return td::Status::Error("ref missing");
  return open_ref_cell(cs.fetch_ref());
}


td::Result<Value> parse_sale_data(vm::CellSlice cs, const std::string &version) {
  Value marketplace_address{Value::null()};
  Value marketplace_fee_address{Value::null()};
  Value royalty_address{Value::null()};
  td::RefInt256 full_price;
  td::RefInt256 marketplace_fee;
  td::RefInt256 royalty_amount;

  if (version == "v4r1") {
    TRY_STATUS(skip_bits(cs, 1));  // is_complete
    TRY_RESULT_ASSIGN(marketplace_address, load_address_py(cs));
    TRY_STATUS(skip_address(cs));  // nft_owner_address
    TRY_RESULT_ASSIGN(full_price, load_coins_py(cs));
    TRY_STATUS(skip_bits(cs, 32));  // sold_at
    TRY_STATUS(skip_bits(cs, 64));  // query_id
    TRY_RESULT(stat, load_ref_slice(cs));
    TRY_RESULT_ASSIGN(marketplace_fee_address, load_address_py(stat));
    TRY_RESULT_ASSIGN(royalty_address, load_address_py(stat));
    // fee_percent / royalty_percent never reach the block; the reader returns
    // early, so marketplace_fee / royalty_amount stay null on this version.
    TRY_STATUS(skip_bits(stat, 17));
    TRY_STATUS(skip_bits(stat, 17));
  } else {
    if (version == "v2" || version == "v3" || version == "v3r2" || version == "v3r3") {
      TRY_STATUS(skip_bits(cs, 1));   // is_complete
      TRY_STATUS(skip_bits(cs, 32));  // created_at
    }
    TRY_RESULT_ASSIGN(marketplace_address, load_address_py(cs));
    TRY_STATUS(skip_address(cs));  // nft_address
    TRY_STATUS(skip_address(cs));  // nft_owner_address
    TRY_RESULT_ASSIGN(full_price, load_coins_py(cs));
    TRY_RESULT(fees, load_ref_slice(cs));
    if (version == "v1") {
      TRY_RESULT_ASSIGN(marketplace_fee, load_coins_py(fees));
      TRY_RESULT_ASSIGN(marketplace_fee_address, load_address_py(fees));
      TRY_RESULT_ASSIGN(royalty_address, load_address_py(fees));
      TRY_RESULT_ASSIGN(royalty_amount, load_coins_py(fees));
    } else {  // v2, v3, v3r2, v3r3 use the opposite address/amount order.
      TRY_RESULT_ASSIGN(marketplace_fee_address, load_address_py(fees));
      TRY_RESULT_ASSIGN(marketplace_fee, load_coins_py(fees));
      TRY_RESULT_ASSIGN(royalty_address, load_address_py(fees));
      TRY_RESULT_ASSIGN(royalty_amount, load_coins_py(fees));
    }
  }

  Value::Fields f;
  f.emplace_back("full_price", opt_amount(full_price));
  f.emplace_back("marketplace_address", marketplace_address);
  f.emplace_back("marketplace_fee_address", marketplace_fee_address);
  f.emplace_back("marketplace_fee", opt_amount(marketplace_fee));
  f.emplace_back("royalty_address", royalty_address);
  f.emplace_back("royalty_amount", opt_amount(royalty_amount));
  return Value::make_obj(std::move(f));
}


struct AuctionInit {
  Value mp_fee_addr{Value::null()};
  Value royalty_fee_addr{Value::null()};
  td::RefInt256 mp_fee_factor, mp_fee_base, royalty_fee_base, max_bid, min_bid;
};

td::Status parse_auction_v1(vm::CellSlice &cs, AuctionInit &d) {
  TRY_RESULT(fees, load_ref_slice(cs));
  TRY_RESULT(bids, load_ref_slice(cs));
  TRY_RESULT_ASSIGN(d.mp_fee_addr, load_address_py(fees));
  TRY_RESULT_ASSIGN(d.mp_fee_factor, load_uint_py(fees, 32));
  TRY_RESULT_ASSIGN(d.mp_fee_base, load_uint_py(fees, 32));
  TRY_RESULT_ASSIGN(d.royalty_fee_addr, load_address_py(fees));
  TRY_STATUS(skip_bits(fees, 32));  // royalty_fee_factor
  TRY_RESULT_ASSIGN(d.royalty_fee_base, load_uint_py(fees, 32));
  TRY_RESULT_ASSIGN(d.min_bid, load_coins_py(bids));
  TRY_RESULT_ASSIGN(d.max_bid, load_coins_py(bids));
  return td::Status::OK();
}

td::Status parse_auction_v3r2(vm::CellSlice &cs, AuctionInit &d) {
  TRY_STATUS(skip_bits(cs, 3));  // end?, activated?, is_canceled?
  TRY_STATUS(skip_address(cs));  // last_member
  TRY_STATUS(skip_coins(cs));    // last_bid
  TRY_STATUS(skip_bits(cs, 32));  // last_bid_at
  TRY_STATUS(skip_bits(cs, 32));  // end_time
  TRY_RESULT(fees, load_ref_slice(cs));
  TRY_RESULT(constant, load_ref_slice(cs));
  TRY_STATUS(skip_bits(constant, 32));  // sub_gas_price_from_bid
  TRY_STATUS(skip_address(constant));   // mp_addr
  TRY_RESULT_ASSIGN(d.min_bid, load_coins_py(constant));
  TRY_RESULT_ASSIGN(d.max_bid, load_coins_py(constant));
  TRY_RESULT_ASSIGN(d.mp_fee_addr, load_address_py(fees));
  TRY_RESULT_ASSIGN(d.mp_fee_factor, load_uint_py(fees, 32));
  TRY_RESULT_ASSIGN(d.mp_fee_base, load_uint_py(fees, 32));
  TRY_RESULT_ASSIGN(d.royalty_fee_addr, load_address_py(fees));
  TRY_STATUS(skip_bits(fees, 32));  // royalty_fee_factor
  TRY_RESULT_ASSIGN(d.royalty_fee_base, load_uint_py(fees, 32));
  return td::Status::OK();
}

td::Status parse_auction_v3r3(vm::CellSlice &cs, AuctionInit &d) {
  TRY_STATUS(skip_bits(cs, 2));  // end?, is_canceled?
  TRY_STATUS(skip_address(cs));  // last_member
  TRY_STATUS(skip_coins(cs));    // last_bid
  TRY_STATUS(skip_bits(cs, 32));  // last_bid_at
  TRY_STATUS(skip_bits(cs, 32));  // end_time
  TRY_STATUS(skip_address(cs));   // nft_owner
  TRY_STATUS(skip_bits(cs, 64));  // last_query_id
  TRY_RESULT_ASSIGN(d.mp_fee_factor, load_uint_py(cs, 32));
  TRY_RESULT_ASSIGN(d.mp_fee_base, load_uint_py(cs, 32));
  TRY_STATUS(skip_bits(cs, 32));  // royalty_fee_factor
  TRY_RESULT_ASSIGN(d.royalty_fee_base, load_uint_py(cs, 32));
  TRY_RESULT(fees, load_ref_slice(cs));
  TRY_RESULT(constant, load_ref_slice(cs));
  TRY_RESULT_ASSIGN(d.mp_fee_addr, load_address_py(fees));
  TRY_RESULT_ASSIGN(d.royalty_fee_addr, load_address_py(fees));
  TRY_STATUS(skip_address(constant));  // mp_addr
  TRY_RESULT_ASSIGN(d.min_bid, load_coins_py(constant));
  TRY_RESULT_ASSIGN(d.max_bid, load_coins_py(constant));
  return td::Status::OK();
}

td::Status parse_auction_v4r1(vm::CellSlice &cs, AuctionInit &d) {
  TRY_STATUS(skip_bits(cs, 2));  // end?, is_canceled?
  TRY_STATUS(skip_address(cs));  // last_member
  TRY_STATUS(skip_coins(cs));    // last_bid
  TRY_STATUS(skip_bits(cs, 32));  // last_bid_at
  TRY_STATUS(skip_bits(cs, 32));  // end_time
  TRY_STATUS(skip_address(cs));   // nft_owner
  TRY_STATUS(skip_bits(cs, 64));  // last_query_id
  TRY_RESULT(fees, load_ref_slice(cs));
  TRY_RESULT(constant, load_ref_slice(cs));
  TRY_RESULT_ASSIGN(d.mp_fee_addr, load_address_py(fees));
  TRY_RESULT_ASSIGN(d.royalty_fee_addr, load_address_py(fees));
  TRY_RESULT_ASSIGN(d.mp_fee_factor, load_uint_py(fees, 32));
  TRY_RESULT_ASSIGN(d.mp_fee_base, load_uint_py(fees, 32));
  TRY_STATUS(skip_bits(fees, 32));  // royalty_fee_factor
  TRY_RESULT_ASSIGN(d.royalty_fee_base, load_uint_py(fees, 32));
  TRY_STATUS(skip_address(constant));  // mp_addr
  TRY_RESULT_ASSIGN(d.min_bid, load_coins_py(constant));
  TRY_RESULT_ASSIGN(d.max_bid, load_coins_py(constant));
  return td::Status::OK();
}


std::string block_comment(const Block *b) {
  const Value *c = b != nullptr ? b->data.field("comment") : nullptr;
  return (c != nullptr && c->t == VType::Str) ? c->str : std::string{};
}

bool is_cancel_comment(const Block *b) {
  std::string c = block_comment(b);
  return c == "cancel" || c == "finish" || c == "stop";
}

bool has_deployment(const Block *b) {
  if (b == nullptr) {
    return false;
  }
  for (const EventNode *n : b->event_nodes) {
    const Transaction *tx = n != nullptr ? n->tx : nullptr;
    if (tx != nullptr && tx->orig_status != "active" && tx->end_status == "active") {
      return true;
    }
  }
  return false;
}

// The item's code hash or its collection identifies it as a TON DNS domain.
bool is_dns_item(const Value &nft) {
  const Value *ch = nft.field("code_hash");
  if (ch != nullptr && ch->t == VType::Str && ch->str == kDnsCodeHash) {
    return true;
  }
  const Value *col = nft.field("collection_address");
  return col != nullptr && col->t == VType::Str && col->str == kDnsCollection;
}

bool is_fragment_item(const Value &nft) {
  const Value *content = nft.field("content");
  if (content == nullptr || (content->t != VType::Dict && content->t != VType::Obj)) {
    return false;
  }
  return fragment_uri(content->field("uri"));
}

const std::string *msg_dest(const Block *b) {
  const Message *m = block_msg(b);
  return (m != nullptr && m->destination) ? &*m->destination : nullptr;
}

// Interface-field address: a raw-address Str, or Null for a missing optional.
Value account_of(const Value &v) {
  if (v.t != VType::Str) {
    return Value::null();
  }
  return account_from_opt(std::optional<std::string>(v.str));
}

}  // namespace


// A plain ton_transfer/call_contract leaf that deployed at least one
// contract. `> 0`, not `== 1`: deliberately weaker than nft_mint's
// single_contract_deploy.
bool sale_contract_deploy(const Block *b) {
  if (b == nullptr || (b->btype != mch::btype::kTonTransfer &&
                       b->btype != mch::btype::kCallContract)) {
    return false;
  }
  return has_deployment(b);
}

bool nft_trade_cancel_comment(const Block *b) {
  return b != nullptr && b->btype == mch::btype::kTonTransfer && is_cancel_comment(b);
}

bool nft_trade_finish_comment(const Block *b) {
  if (b == nullptr || b->btype != mch::btype::kTonTransfer) {
    return false;
  }
  std::string c = block_comment(b);
  return c == "finish" || c == "stop";
}

// Cheap guards before the auction-bid path touches the interface repository.
bool auction_bid_candidate(const Block *b) {
  return b != nullptr && b->btype == mch::btype::kTonTransfer && !has_deployment(b) &&
         !is_cancel_comment(b);
}

// The peeked auction_bid must have exactly one qualifying outbid child.
bool auction_outbid_leg(const Block *bid) {
  if (bid == nullptr || bid->btype != mch::btype::kAuctionBid) {
    return false;
  }
  const Value *auction = bid->data.field("auction");
  const Value *kind = bid->data.field("auction_type");
  if (auction == nullptr || kind == nullptr || kind->t != VType::Str) {
    return false;
  }
  int n = 0;
  for (const Block *leg : bid->next_blocks) {
    if (leg == nullptr || leg->previous_block != bid ||
        !same_account(data_field(leg, "source"), *auction)) {
      continue;
    }
    bool qualifies = false;
    if (leg->btype == mch::btype::kTonTransfer) {
      qualifies = kind->str == "getgems" &&
                  block_comment(leg).find("Your bid has been outbid by another user") !=
                      std::string::npos;
    } else {
      qualifies = kind->str == "fragment" && is_call_op(leg, 0x557CEA20);
    }
    if (qualifies && ++n > 1) {
      return false;
    }
  }
  return n == 1;
}


EvalResult getgems_sale_init(BuildEnv &env, const std::vector<Value> &args) {
  (void)env;
  const Block *b = as_block(args[0]);
  if (b == nullptr) {
    return rt_ok(Value::null());
  }
  auto r_data = state_init_data(b);
  if (r_data.is_error()) {
    return rt_ok(Value::null());  // StateInit parse failure is Null
  }
  auto r = parse_sale_data(r_data.move_as_ok(), pick_version(sale_versions(), args[1], "v4r1"));
  return rt_ok(r.is_ok() ? r.move_as_ok() : Value::null());
}

EvalResult getgems_auction_init(BuildEnv &env, const std::vector<Value> &args) {
  (void)env;
  const Block *b = as_block(args[0]);
  if (b == nullptr) {
    return rt_ok(Value::null());
  }
  auto r_data = state_init_data(b);
  if (r_data.is_error()) {
    return rt_ok(Value::null());
  }
  vm::CellSlice cs = r_data.move_as_ok();
  // "v2" is in the version table but has no auction-data branch, so reject it.
  const std::string version = pick_version(auction_versions(), args[1], "v3r3");
  AuctionInit d;
  td::Status st = td::Status::Error("unsupported auction version");
  if (version == "v1") {
    st = parse_auction_v1(cs, d);
  } else if (version == "v3r2") {
    st = parse_auction_v3r2(cs, d);
  } else if (version == "v3r3") {
    st = parse_auction_v3r3(cs, d);
  } else if (version == "v4r1") {
    st = parse_auction_v4r1(cs, d);
  }
  if (st.is_error()) {
    return rt_ok(Value::null());
  }
  Value::Fields f;
  f.emplace_back("mp_fee_addr", d.mp_fee_addr);
  f.emplace_back("mp_fee_factor", opt_amount(d.mp_fee_factor));
  f.emplace_back("mp_fee_base", opt_amount(d.mp_fee_base));
  f.emplace_back("royalty_fee_addr", d.royalty_fee_addr);
  f.emplace_back("royalty_fee_base", opt_amount(d.royalty_fee_base));
  f.emplace_back("max_bid", opt_amount(d.max_bid));
  f.emplace_back("min_bid", opt_amount(d.min_bid));
  return rt_ok(Value::make_obj(std::move(f)));
}

// Getgems branch reads the auction interface and looks the item up by the
// address that interface carries. A lookup keyed on another lookup's result,
// which the build language cannot express. Any rejected branch returns Null.
EvalResult auction_bid_data(BuildEnv &env, const std::vector<Value> &args) {
  const Block *bid = as_block(args[0]);
  const Message *msg = block_msg(bid);
  const std::string *dest = msg_dest(bid);
  if (bid == nullptr || msg == nullptr || dest == nullptr) {
    return rt_ok(Value::null());
  }
  Value amount = msg_value_amount(msg);
  Value bidder = account_from_opt(msg->source);
  Value auction_addr = account_from_opt(std::optional<std::string>(*dest));

  Value::Fields f;
  Value auction = env.lookups->get("nft_auction", std::vector<Value>{Value::make_str(*dest)});
  if (!auction.is_null()) {
    // getgems: the auction contract IS the destination; the item is elsewhere.
    const Value *nft_addr = auction.field("nft_addr");
    Value index{Value::null()};
    Value collection{Value::null()};
    if (nft_addr != nullptr && nft_addr->t == VType::Str) {
      Value item = env.lookups->get("nft_item", std::vector<Value>{*nft_addr});
      if (!item.is_null()) {
        const Value *ix = item.field("index");
        const Value *col = item.field("collection_address");
        if (ix != nullptr) index = *ix;
        if (col != nullptr) collection = account_of(*col);
      }
    }
    f.emplace_back("amount", amount);
    f.emplace_back("bidder", bidder);
    f.emplace_back("auction", auction_addr);
    f.emplace_back("nft_address",
                   nft_addr != nullptr ? account_of(*nft_addr) : Value::null());
    f.emplace_back("nft_item_index", index);
    f.emplace_back("nft_collection", collection);
    f.emplace_back("auction_type", Value::make_str("getgems"));
    return rt_ok(Value::make_obj(std::move(f)));
  }

  // fragment / DNS: the destination is the ITEM, which runs its own auction.
  Value item = env.lookups->get("nft_item", std::vector<Value>{Value::make_str(*dest)});
  if (item.is_null()) {
    return rt_ok(Value::null());
  }
  const bool dns = is_dns_item(item);
  if (!dns && !is_fragment_item(item)) {
    return rt_ok(Value::null());
  }
  if (dns) {
    // A DNS bid requires the teleitem outbid notification.
    if (first_call(bid->next_blocks, 0x557CEA20) == nullptr) {
      return rt_ok(Value::null());
    }
  }
  const Value *col = item.field("collection_address");
  const Value *ix = item.field("index");
  f.emplace_back("amount", amount);
  f.emplace_back("bidder", bidder);
  f.emplace_back("auction", auction_addr);
  f.emplace_back("nft_address", auction_addr);
  f.emplace_back("nft_item_index", ix != nullptr ? *ix : Value::null());
  f.emplace_back("nft_collection",
                 col != nullptr && !col->is_null() ? account_of(*col) : Value::null());
  f.emplace_back("auction_type", Value::make_str("fragment"));
  return rt_ok(Value::make_obj(std::move(f)));
}

// Finish/stop purchases place the seller payout beside the produced block.
// Insert a proxy under the sale parent, then absorb the peeked payout and proxy.
void getgems_proxy_insert(Block *produced, const ShaperMatch &m) {
  Block *payout = m.capture("sib_payout");
  if (produced == nullptr || payout == nullptr || m.arena == nullptr) {
    return;
  }
  Block *parent = produced->previous_block;
  if (parent == nullptr || parent->btype != mch::btype::kTonTransfer) {
    return;
  }
  const Value *comment = parent->data.field("comment");
  if (comment == nullptr || comment->t != VType::Str ||
      (comment->str != "finish" && comment->str != "stop")) {
    return;
  }
  Block *proxy = m.arena->make(mch::btype::kEmpty);
  parent->insert_between({produced, payout}, proxy);
  produced->merge_blocks({payout, proxy});
  produced->compact_connections();
}

}  // namespace mch
