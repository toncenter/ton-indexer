#include "FixtureLookupSource.h"

#include "parse/PSlice.h"  // load_address_py

#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cellslice.h"

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

// Project an interface fields Dict into an Obj with the deserializer's field
// set (TestInterfaceRepository -> ORM attribute access; a field absent from
// the fixture map becomes Null).
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
    return Value::null();  // see the non-Str deviation note in the header
  }
  const std::string &addr = args[0].str;
  if (kind == "jetton_wallet") {
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
  if (kind == "nft_item") {
    auto it = interfaces_->find(addr);  // RAW key (dns.py variant, no .upper())
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
  if (kind == "nft_sale") {
    // TestInterfaceRepository.get_nft_sale: interfaces[addr]["NftSale"] (RAW key)
    // -> NftSale ORM. The nft host fns read marketplace_address / nft_address /
    // full_price / nft_owner_address / code_hash.
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
  if (kind == "multisig_order") {
    // TestInterfaceRepository.get_multisig_order: interfaces[addr]["MultisigOrder"]
    // (RAW key) -> MultisigOrder ORM. Only `signers` is read.
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
  if (kind == "nft_auction") {
    // TestInterfaceRepository.get_nft_auction: interfaces[addr]["NftAuction"]
    // (RAW key) -> NftAuction ORM. The nft host fns read mp_addr / nft_addr /
    // last_bid / nft_owner / code_hash.
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
  if (kind == "nominator_pool") {
    auto it = interfaces_->find(addr);
    if (it == interfaces_->end() || it->second.field("NominatorPool") == nullptr) {
      return Value::null();
    }
    return it->second;  // the account's whole interfaces Dict
  }
  if (kind == "dedust_pool") {
    // TestInterfaceRepository.get_dedust_pool: interfaces[addr]["DedustPool"]
    // (RAW key == AccountId.as_str() upper) -> DedustPool(assets=data["assets"]).
    // Absent -> null; the host fn then rejects (Python None.assets raises).
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
  if (kind == "jvault_assets") {
    // extract_jvault_assets (blocks/jvault.py) as a record lookup: read the
    // stake wallet's extra-data cell (b64 BOC string), parse two addresses
    // (staking_pool + minter), then chain a second extra-data read (pool ->
    // lock_wallet_address) and a jetton_wallet read (lock wallet -> jetton).
    // Python wraps the whole thing in try/except returning (None,None,None) and
    // the registered lookup maps that to Null; a MISSING pool extra-data (but
    // resolved staking_pool) is the partial case {staking_pool, null, jvault}.
    auto it = interfaces_->find(upper_ascii(addr));
    if (it == interfaces_->end()) {
      return Value::null();
    }
    const Value *extra = it->second.field("data_boc");
    if (extra == nullptr) {
      return Value::null();  // get_extra_data(stake_wallet) is None
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
    // Python then does staking_pool.to_str(...): addr_none (load_address None)
    // AttributeErrors into the outer except -> whole result None.
    if (pool.t != VType::Account || pool.addr_none) {
      return Value::null();
    }
    Value jvault_asset = Value::null();
    if (minter.t == VType::Account && !minter.addr_none) {
      auto mn = normalize_raw_address(minter.str);
      jvault_asset = Value::make_asset_jetton(mn ? *mn : minter.str);
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
      return Value::null();  // KeyError on ['lock_wallet_address'] -> except -> None
    }
    auto it3 = interfaces_->find(upper_ascii(lock->str));
    const Value *jw =
        it3 == interfaces_->end() ? nullptr : it3->second.field("JettonWallet");
    if (jw == nullptr) {
      return Value::null();  // get_jetton_wallet None -> .jetton AttributeError -> None
    }
    const Value *jetton = jw->field("jetton");
    if (jetton == nullptr || jetton->t != VType::Str) {
      return Value::null();
    }
    auto jn = normalize_raw_address(jetton->str);
    return make_record(Value::make_asset_jetton(jn ? *jn : jetton->str));
  }
  return Value::null();
}

}  // namespace mch
