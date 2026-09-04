#include "EmuJvaultChain.h"

#include "parse/PSlice.h"  // load_address_py

#include "vm/cells/CellSlice.h"

namespace mch {

namespace {

// Treat short, pruned, and exotic data cells as parse failures.
td::Result<vm::CellSlice> data_slice(const td::Ref<vm::Cell> &data) {
  if (data.is_null()) {
    return td::Status::Error("jvault: no account data");
  }
  bool special = false;
  try {
    return vm::load_cell_slice_special(data, special);
  } catch (...) {
    return td::Status::Error("jvault: data cell not loadable");
  }
}

std::optional<std::string> account_canonical(const Value &v) {
  if (v.t != VType::Account || v.addr_none) {
    return std::nullopt;
  }
  return canonicalize_or_passthrough(v.str);
}

}  // namespace

td::Result<JvaultStakeWallet> parse_jvault_stake_wallet(const td::Ref<vm::Cell> &data) {
  TRY_RESULT(cs, data_slice(data));
  TRY_RESULT(pool, load_address_py(cs));
  auto pool_raw = account_canonical(pool);
  if (!pool_raw) {
    // A missing staking-pool address invalidates the complete record.
    return td::Status::Error("jvault: staking pool is not an address");
  }
  TRY_RESULT(minter, load_address_py(cs));
  return JvaultStakeWallet{
      .staking_pool = pool,
      .staking_pool_raw = *pool_raw,
      .minter = account_canonical(minter),
  };
}

td::Result<std::string> parse_jvault_pool_lock_wallet(const td::Ref<vm::Cell> &data) {
  TRY_RESULT(cs, data_slice(data));
  if (!cs.have(1 + 32) || !cs.advance(1 + 32)) {
    return td::Status::Error("jvault: pool data underflow");
  }
  TRY_RESULT(admin, load_address_py(cs));
  TRY_RESULT(creator, load_address_py(cs));
  (void)admin;
  (void)creator;
  TRY_RESULT(lock, load_address_py(cs));
  auto lock_raw = account_canonical(lock);
  if (!lock_raw) {
    return td::Status::Error("jvault: lock wallet is not an address");
  }
  return *lock_raw;
}

Value jvault_record(const Value &staking_pool, Value asset, Value jvault_asset) {
  Value::Fields f;
  f.emplace_back("staking_pool", staking_pool);
  f.emplace_back("asset", std::move(asset));
  f.emplace_back("jvault_asset", std::move(jvault_asset));
  return Value::make_obj(std::move(f));
}

Value jvault_asset_of(const std::optional<std::string> &canonical_master) {
  return canonical_master ? Value::make_asset_jetton(*canonical_master) : Value::null();
}

}  // namespace mch
