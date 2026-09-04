#include "FixtureLookupSource.h"

#include "parse/PSlice.h"  // load_address_py

#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cellslice.h"

#include <map>

namespace mch {

namespace {

std::string upper_ascii(std::string s) {
  for (char &c : s) {
    if (c >= 'a' && c <= 'z') {
      c = static_cast<char>(c - 'a' + 'A');
    }
  }
  return s;
}

// Project an interface fields Dict into an Obj; a field absent from the
// fixture map becomes Null.
Value iface_obj(const Value &fields_dict, std::initializer_list<const char *> names) {
  Value::Fields fs;
  for (const char *n : names) {
    const Value *f = fields_dict.field(n);
    fs.emplace_back(n, f != nullptr ? *f : Value::null());
  }
  return Value::make_obj(std::move(fs));
}

}  // namespace

Value FixtureLookupSource::fetch(const std::string &kind, const std::vector<Value> &args) const {
  // An Account key (a `.data` address field, e.g. the dedust_v2 failed-swap
  // pool) carries the same raw-uppercase text as the Str form.
  const bool addr_arg = args.size() == 1 &&
                        (args[0].t == VType::Str ||
                         (args[0].t == VType::Account && !args[0].addr_none));
  if (interfaces_ == nullptr || !addr_arg) {
    return Value::null();  // non-address keys miss
  }
  enum class Handler {
    JettonWallet,
    NftItem,
    NftSale,
    MultisigOrder,
    NftAuction,
    NominatorPool,
    DedustPool,
    JvaultAssets,
  };
  static const std::map<std::string, Handler> handlers = {
      {"jetton_wallet", Handler::JettonWallet},
      {"nft_item", Handler::NftItem},
      {"nft_sale", Handler::NftSale},
      {"multisig_order", Handler::MultisigOrder},
      {"nft_auction", Handler::NftAuction},
      {"nominator_pool", Handler::NominatorPool},
      {"dedust_pool", Handler::DedustPool},
      {"jvault_assets", Handler::JvaultAssets},
  };
  auto handler = handlers.find(kind);
  if (handler == handlers.end()) {
    return Value::null();
  }
  const std::string &addr = args[0].str;
  switch (handler->second) {
    case Handler::JettonWallet: {
    auto it = interfaces_->find(upper_ascii(addr));
    if (it == interfaces_->end()) {
      return Value::null();
    }
    const Value *jw = it->second.field("JettonWallet");
    if (jw == nullptr) {
      return Value::null();
    }
    return iface_obj(*jw, {"balance", "address", "owner", "jetton"});
  }
    case Handler::NftItem: {
    auto it = interfaces_->find(addr);  // raw key, no upper(); intentional
    if (it == interfaces_->end()) {
      return Value::null();
    }
    const Value *nft = it->second.field("NftItem");
    if (nft == nullptr) {
      return Value::null();
    }
    return iface_obj(*nft, {"address", "init", "index", "collection_address",
                            "owner_address", "content", "code_hash"});
  }
    case Handler::NftSale: {
    // NftSale: raw key, no upper().
    auto it = interfaces_->find(addr);
    if (it == interfaces_->end()) {
      return Value::null();
    }
    const Value *sale = it->second.field("NftSale");
    if (sale == nullptr) {
      return Value::null();
    }
    return iface_obj(*sale, {"marketplace_address", "nft_address", "full_price",
                             "nft_owner_address", "code_hash"});
  }
    case Handler::MultisigOrder: {
    // MultisigOrder: raw key, no upper().
    auto it = interfaces_->find(addr);
    if (it == interfaces_->end()) {
      return Value::null();
    }
    const Value *o = it->second.field("MultisigOrder");
    if (o == nullptr) {
      return Value::null();
    }
    return iface_obj(*o, {"signers"});
  }
    case Handler::NftAuction: {
    // NftAuction: raw key, no upper().
    auto it = interfaces_->find(addr);
    if (it == interfaces_->end()) {
      return Value::null();
    }
    const Value *auc = it->second.field("NftAuction");
    if (auc == nullptr) {
      return Value::null();
    }
    return iface_obj(*auc, {"mp_addr", "nft_addr", "last_bid", "nft_owner", "code_hash"});
  }
    case Handler::NominatorPool: {
    auto it = interfaces_->find(addr);
    if (it == interfaces_->end() || it->second.field("NominatorPool") == nullptr) {
      return Value::null();
    }
    return it->second;  // the account's whole interfaces Dict
  }
    case Handler::DedustPool: {
    // DedustPool: lookup by the address as given. Absent is null; the host
    // then rejects. Intentional.
    auto it = interfaces_->find(addr);
    if (it == interfaces_->end()) {
      return Value::null();
    }
    const Value *pool = it->second.field("DedustPool");
    if (pool == nullptr) {
      return Value::null();
    }
    return iface_obj(*pool, {"assets"});
  }
    case Handler::JvaultAssets: {
    // jvault_assets: parse stake extra-data (pool + minter), then pool extra-data
    // (lock wallet) then that wallet's jetton. Any failure is null; a missing
    // pool extra-data with a resolved staking_pool is the partial record.
    auto it = interfaces_->find(upper_ascii(addr));
    if (it == interfaces_->end()) {
      return Value::null();
    }
    const Value *extra = it->second.field("data_boc");
    if (extra == nullptr) {
      return Value::null();  // missing stake extra-data
    }
    const Value *boc_str = extra->field("data_boc");
    if (boc_str == nullptr || boc_str->t != VType::Str) {
      return Value::null();
    }
    auto r_raw = td::base64_decode(td::Slice(boc_str->str));
    if (r_raw.is_error()) {
      return Value::null();
    }
    auto r_cell = vm::std_boc_deserialize(r_raw.move_as_ok());
    if (r_cell.is_error() || r_cell.ok().is_null()) {
      return Value::null();
    }
    vm::CellSlice cs;
    bool special = false;
    try {
      cs = vm::load_cell_slice_special(r_cell.move_as_ok(), special);
    } catch (...) {
      return Value::null();
    }
    auto r_pool = load_address_py(cs);
    if (r_pool.is_error()) {
      return Value::null();
    }
    Value pool = r_pool.move_as_ok();
    auto r_minter = load_address_py(cs);
    if (r_minter.is_error()) {
      return Value::null();
    }
    Value minter = r_minter.move_as_ok();
    // addr_none pool fails the whole lookup (no string form). Intentional.
    if (pool.t != VType::Account || pool.addr_none) {
      return Value::null();
    }
    Value jvault_asset = Value::null();
    if (minter.t == VType::Account && !minter.addr_none) {
      jvault_asset = Value::make_asset_jetton(minter.str);
    }
    auto make_record = [&](Value asset) {
      Value::Fields f;
      f.emplace_back("staking_pool", pool);
      f.emplace_back("asset", std::move(asset));
      f.emplace_back("jvault_asset", jvault_asset);
      return Value::make_obj(std::move(f));
    };
    auto it2 = interfaces_->find(upper_ascii(pool.str));
    const Value *pool_extra =
        it2 == interfaces_->end() ? nullptr : it2->second.field("data_boc");
    if (pool_extra == nullptr) {
      return make_record(Value::null());  // stake_pool_extra is None: partial
    }
    const Value *lock = pool_extra->field("lock_wallet_address");
    if (lock == nullptr || lock->t != VType::Str) {
      return Value::null();  // missing lock_wallet_address fails the whole lookup
    }
    auto it3 = interfaces_->find(upper_ascii(lock->str));
    const Value *jw =
        it3 == interfaces_->end() ? nullptr : it3->second.field("JettonWallet");
    if (jw == nullptr) {
      return Value::null();  // missing lock-wallet jetton fails the whole lookup
    }
    const Value *jetton = jw->field("jetton");
    if (jetton == nullptr || jetton->t != VType::Str) {
      return Value::null();
    }
    return make_record(Value::make_asset_jetton(jetton->str));
  }
  }
  return Value::null();
}

}  // namespace mch
