#include "host/HostNftOffer.h"

#include "host/HostCommon.h"
#include "host/HostImpls.h"

#include "parse/PSlice.h"

#include "vm/cells/CellBuilder.h"
#include "vm/cells/CellSlice.h"

#include <utility>

namespace mch {

namespace {

constexpr std::pair<const char *, int> kNftOfferCodes[] = {
    {"jUjSF20IDx2xXmHCFnnQ9wEHUgOA1TMa55WqvIm7ztw=", 5},
};

// Only phase changes after deploy; a crafted initial phase can bypass the buyer's funding.
bool is_nft_offer(const td::Bits256 &address, const vm::CellSlice &storage) {
  td::Bits256 public_key;
  vm::CellBuilder cb;
  if (!storage.prefetch_bits_to(public_key) || !cb.store_ref_bool(storage.prefetch_ref()) ||
      !cb.store_bits_bool(public_key) || !cb.store_long_bool(0, 8)) {
    return false;
  }
  auto initial = cb.finalize();
  for (const auto &[hash_b64, depth] : kNftOfferCodes) {
    if (is_state_init_address(address, td::Slice(hash_b64), depth, initial)) {
      return true;
    }
  }
  return false;
}

td::Result<Value> parse(const td::Bits256 &address, const td::Ref<vm::Cell> &data) {
  TRY_RESULT(storage, ordinary_cell_slice(data));
  if (storage.size() != 256 + 8 || storage.size_refs() != 1) {
    return td::Status::Error("nft_offer: not an NftOffer data cell");
  }
  if (!is_nft_offer(address, storage)) {
    return td::Status::Error("nft_offer: address does not match the offer code");
  }
  TRY_RESULT(config, ordinary_cell_slice(storage.prefetch_ref()));
  TRY_RESULT(nft, load_std_account(config));
  TRY_RESULT(price, load_coins_py(config));
  unsigned long long expires_at = 0;
  if (!config.advance(64) || !config.fetch_uint_to(32, expires_at) || !config.have_refs(2)) {
    return td::Status::Error("nft_offer: config underflow");
  }
  TRY_RESULT(parties, ordinary_cell_slice(config.prefetch_ref(0)));
  TRY_RESULT(buyer, load_std_account(parties));
  TRY_RESULT(recipient, load_std_account(parties));
  TRY_RESULT(seller, load_std_account(parties));
  TRY_RESULT(fee, ordinary_cell_slice(config.prefetch_ref(1)));
  TRY_RESULT(fee_address, load_std_account(fee));
  unsigned long long fee_bps = 0;
  if (!fee.fetch_uint_to(16, fee_bps)) {
    return td::Status::Error("nft_offer: fee underflow");
  }
  Value::Fields f;
  f.emplace_back("nft", std::move(nft));
  f.emplace_back("price", Value::make_int(price));
  f.emplace_back("expires_at", Value::make_int64(static_cast<std::int64_t>(expires_at)));
  f.emplace_back("buyer", std::move(buyer));
  f.emplace_back("recipient", std::move(recipient));
  f.emplace_back("seller", std::move(seller));
  f.emplace_back("fee_address", std::move(fee_address));
  auto fee_amount = td::muldiv(price, td::make_refint(fee_bps), td::make_refint(10000));
  f.emplace_back("fee_amount", Value::make_int(fee_amount));
  return Value::make_obj(std::move(f));
}

}  // namespace

td::Result<Value> parse_nft_offer(const td::Bits256 &address, const td::Ref<vm::Cell> &data) {
  // Loading a pruned child cell throws.
  try {
    return parse(address, data);
  } catch (...) {
    return td::Status::Error("nft_offer: data cell not loadable");
  }
}

EvalResult nft_offer_init(BuildEnv &env, const std::vector<Value> &args) {
  (void)env;
  td::Bits256 address;
  auto r_data = deploy_data(block_msg(as_block(args[0])), address);
  if (r_data.is_error()) {
    return rt_ok(Value::null());
  }
  auto r = parse_nft_offer(address, r_data.move_as_ok());
  return rt_ok(r.is_ok() ? r.move_as_ok() : Value::null());
}

}  // namespace mch
