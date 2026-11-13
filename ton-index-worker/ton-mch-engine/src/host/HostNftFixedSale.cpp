#include "host/HostNftFixedSale.h"

#include "host/HostCommon.h"
#include "host/HostImpls.h"

#include "BlockTree.h"
#include "TraceLoader.h"
#include "parse/PSlice.h"
#include "btypes_gen.h"

#include "block/block-parse.h"

#include "vm/cells/CellBuilder.h"
#include "vm/cells/CellSlice.h"

namespace mch {

namespace {

constexpr const char *kFixedSaleCodeHash = "8k0Y8Bj2bsonmo84WeXCgQ2xUkCsKnQpJf/2jWVCtJA=";
constexpr int kFixedSaleCodeDepth = 4;

td::Result<Value> parse(const td::Bits256 &address, const td::Ref<vm::Cell> &data) {
  TRY_RESULT(storage, ordinary_cell_slice(data));
  if (storage.size_refs() != 1) {
    return td::Status::Error("nft_fixed_sale: not a FixedSale data cell");
  }
  auto config_cell = storage.prefetch_ref();
  // price:Coins phase:uint8 follow the config ref.
  if (load_coins_py(storage).is_error() || !storage.advance(8) || storage.size() != 0) {
    return td::Status::Error("nft_fixed_sale: not a FixedSale data cell");
  }
  TRY_RESULT(config, ordinary_cell_slice(config_cell));
  TRY_RESULT(seller, load_std_account(config));
  TRY_RESULT(nft, load_std_account(config));
  TRY_RESULT(initial_price, load_coins_py(config));
  if (!config.advance(64) || !config.have_refs(1)) {
    return td::Status::Error("nft_fixed_sale: config underflow");
  }
  TRY_RESULT(fee, ordinary_cell_slice(config.prefetch_ref()));
  TRY_RESULT(fee_address, load_std_account(fee));
  unsigned long long fee_bps = 0;
  if (!fee.fetch_uint_to(16, fee_bps)) {
    return td::Status::Error("nft_fixed_sale: fee underflow");
  }
  // Only price and phase change after deploy; a crafted initial phase would skip the deposit.
  vm::CellBuilder cb;
  if (!cb.store_ref_bool(config_cell) || !block::tlb::t_Grams.store_integer_ref(cb, initial_price) ||
      !cb.store_long_bool(0, 8) ||
      !is_state_init_address(address, td::Slice(kFixedSaleCodeHash), kFixedSaleCodeDepth, cb.finalize())) {
    return td::Status::Error("nft_fixed_sale: address does not match the sale code");
  }
  Value::Fields f;
  f.emplace_back("seller", std::move(seller));
  f.emplace_back("nft", std::move(nft));
  f.emplace_back("initial_price", Value::make_int(initial_price));
  f.emplace_back("fee_address", std::move(fee_address));
  f.emplace_back("fee_bps", Value::make_int64(static_cast<std::int64_t>(fee_bps)));
  return Value::make_obj(std::move(f));
}

}  // namespace

td::Result<Value> parse_nft_fixed_sale(const td::Bits256 &address, const td::Ref<vm::Cell> &data) {
  // Loading a pruned child cell throws.
  try {
    return parse(address, data);
  } catch (...) {
    return td::Status::Error("nft_fixed_sale: data cell not loadable");
  }
}

EvalResult nft_fixed_sale_init(BuildEnv &env, const std::vector<Value> &args) {
  (void)env;
  const Block *b = as_block(args[0]);
  td::Bits256 address;
  auto r_data = b != nullptr ? deploy_data(block_msg(b), address)
                             : td::Result<td::Ref<vm::Cell>>(td::Status::Error("no deploy"));
  if (r_data.is_error()) {
    return rt_ok(Value::null());
  }
  auto r = parse_nft_fixed_sale(address, r_data.move_as_ok());
  return rt_ok(r.is_ok() ? r.move_as_ok() : Value::null());
}

EvalResult nft_fixed_sale_fee(BuildEnv &env, const std::vector<Value> &args) {
  (void)env;
  const Value *bps = args[0].field("fee_bps");
  const Value &price = args[1];
  if (bps == nullptr || bps->t != VType::Int || (price.t != VType::Int && price.t != VType::Amount) ||
      price.num.is_null()) {
    return rt_ok(Value::null());
  }
  return rt_ok(Value::make_int(td::muldiv(price.num, bps->num, td::make_refint(10000))));
}

// ChangePrice#474d0002 price:Coins
EvalResult nft_fixed_sale_new_price(BuildEnv &env, const std::vector<Value> &args) {
  (void)env;
  const Block *b = as_block(args[0]);
  auto r_body = b != nullptr ? block_body(b) : td::Result<td::Ref<vm::Cell>>(td::Status::Error("no block"));
  if (r_body.is_error()) {
    return rt_ok(Value::null());
  }
  auto r_cs = ordinary_cell_slice(r_body.move_as_ok());
  if (r_cs.is_error() || !r_cs.ok_ref().advance(32)) {
    return rt_ok(Value::null());
  }
  auto r_price = load_coins_py(r_cs.ok_ref());
  return rt_ok(r_price.is_ok() ? Value::make_int(r_price.move_as_ok()) : Value::null());
}

// A top-up carrying the same StateInit can reach a sale deployed earlier.
bool state_init_deploy(const Block *b) {
  const Message *m = b != nullptr && b->btype == mch::btype::kTonTransfer ? block_msg(b) : nullptr;
  return m != nullptr && m->init_state && m->tx != nullptr &&
         (m->tx->orig_status == "nonexist" || m->tx->orig_status == "uninit");
}

}  // namespace mch
