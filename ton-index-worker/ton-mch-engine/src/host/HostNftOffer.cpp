#include "host/HostNftOffer.h"

#include "host/HostCommon.h"
#include "host/HostImpls.h"

#include "BlockTree.h"
#include "parse/PSlice.h"

#include "block/block.h"

#include "td/utils/base64.h"
#include "td/utils/crypto.h"
#include "vm/cells/CellBuilder.h"
#include "vm/cells/CellSlice.h"

#include <string>
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
    std::string repr = {2, 1, 0b00110100, static_cast<char>(depth >> 8), static_cast<char>(depth & 0xff),
                        static_cast<char>(initial->get_depth() >> 8), static_cast<char>(initial->get_depth() & 0xff)};
    repr += td::base64_decode(td::Slice(hash_b64)).move_as_ok();
    repr += initial->get_hash().as_slice().str();
    td::Bits256 state_init_hash;
    td::sha256(repr, state_init_hash.as_slice());
    if (state_init_hash == address) {
      return true;
    }
  }
  return false;
}

td::Result<vm::CellSlice> cell_slice(const td::Ref<vm::Cell> &cell) {
  if (cell.is_null()) {
    return td::Status::Error("nft_offer: no cell");
  }
  bool special = false;
  vm::CellSlice cs = vm::load_cell_slice_special(cell, special);
  if (special) {
    return td::Status::Error("nft_offer: exotic cell");
  }
  return cs;
}

td::Result<Value> load_account(vm::CellSlice &cs) {
  TRY_RESULT(v, load_address_py(cs));
  if (v.t != VType::Account || v.addr_none) {
    return td::Status::Error("nft_offer: not a std address");
  }
  return v;
}

td::Result<Value> parse(const td::Bits256 &address, const td::Ref<vm::Cell> &data) {
  TRY_RESULT(storage, cell_slice(data));
  if (storage.size() != 256 + 8 || storage.size_refs() != 1) {
    return td::Status::Error("nft_offer: not an NftOffer data cell");
  }
  if (!is_nft_offer(address, storage)) {
    return td::Status::Error("nft_offer: address does not match the offer code");
  }
  TRY_RESULT(config, cell_slice(storage.prefetch_ref()));
  TRY_RESULT(nft, load_account(config));
  TRY_RESULT(price, load_coins_py(config));
  unsigned long long expires_at = 0;
  if (!config.advance(64) || !config.fetch_uint_to(32, expires_at) || !config.have_refs(2)) {
    return td::Status::Error("nft_offer: config underflow");
  }
  TRY_RESULT(parties, cell_slice(config.prefetch_ref(0)));
  TRY_RESULT(buyer, load_account(parties));
  TRY_RESULT(recipient, load_account(parties));
  TRY_RESULT(seller, load_account(parties));
  TRY_RESULT(royalty, cell_slice(config.prefetch_ref(1)));
  TRY_RESULT(royalty_address, load_account(royalty));
  unsigned long long royalty_bps = 0;
  if (!royalty.fetch_uint_to(16, royalty_bps)) {
    return td::Status::Error("nft_offer: royalty underflow");
  }
  Value::Fields f;
  f.emplace_back("nft", std::move(nft));
  f.emplace_back("price", Value::make_int(price));
  f.emplace_back("expires_at", Value::make_int64(static_cast<std::int64_t>(expires_at)));
  f.emplace_back("buyer", std::move(buyer));
  f.emplace_back("recipient", std::move(recipient));
  f.emplace_back("seller", std::move(seller));
  f.emplace_back("royalty_address", std::move(royalty_address));
  auto royalty_amount = td::muldiv(price, td::make_refint(royalty_bps), td::make_refint(10000));
  f.emplace_back("royalty_amount", Value::make_int(royalty_amount));
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
  const Message *msg = block_msg(as_block(args[0]));
  if (msg == nullptr || !msg->init_state || !msg->destination) {
    return rt_ok(Value::null());
  }
  auto r_addr = block::StdAddress::parse(td::Slice(*msg->destination));
  auto r_root = cell_from_pystr(msg->init_state->body);
  if (r_addr.is_error() || r_root.is_error() || td::Bits256(r_root.ok()->get_hash().bits()) != r_addr.ok().addr) {
    return rt_ok(Value::null());
  }
  auto r_cs = open_ref_cell(r_root.move_as_ok());
  if (r_cs.is_error()) {
    return rt_ok(Value::null());
  }
  auto r_data = state_init_data_cell(r_cs.ok_ref());
  if (r_data.is_error()) {
    return rt_ok(Value::null());
  }
  auto r = parse_nft_offer(r_addr.ok().addr, r_data.move_as_ok());
  return rt_ok(r.is_ok() ? r.move_as_ok() : Value::null());
}

}  // namespace mch
