// Raw data-cell parsers for the JVault asset lookup. Account reads and the jetton
// wallet step remain with the caller, which owns the memo and budget.
#pragma once

#include "Value.h"

#include "td/utils/Status.h"
#include "vm/cells/Cell.h"

namespace mch {

// The stake-wallet data contains a required staking-pool address followed by an
// optional minter address. A missing pool invalidates the record.
struct JvaultStakeWallet {
  Value staking_pool;                  // VType::Account, never addr_none
  std::string staking_pool_raw;        // canonical "wc:HEX" for the next lookup
  std::optional<std::string> minter;   // canonical "wc:HEX"
};
td::Result<JvaultStakeWallet> parse_jvault_stake_wallet(const td::Ref<vm::Cell> &data);

// The pool data stores the lock wallet after a 33-bit header, admin, and creator.
// A missing lock wallet invalidates the record.
td::Result<std::string> parse_jvault_pool_lock_wallet(const td::Ref<vm::Cell> &data);

// The record the lookup yields: Obj{staking_pool, asset, jvault_asset}.
// `asset` null is the legitimate PARTIAL case (pool resolved, its state absent).
Value jvault_record(const Value &staking_pool, Value asset, Value jvault_asset);

// Asset(jetton=addr) from a canonical raw address, or Null for nullopt.
Value jvault_asset_of(const std::optional<std::string> &canonical_master);

}  // namespace mch
