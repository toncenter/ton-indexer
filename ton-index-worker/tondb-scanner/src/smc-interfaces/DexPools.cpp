#include "smc-interfaces/DexPools.h"

#include <atomic>

#include "convert-utils.h"

namespace {

std::atomic<bool> g_dex_snapshots{false};

schema::DexPoolSnapshot pool_identity(const DedustPoolDetector::Result& result, const td::Bits256& code_hash) {
  schema::DexPoolSnapshot snapshot;
  snapshot.address = result.address;
  snapshot.code_hash = code_hash;
  snapshot.dex = "dedust";
  snapshot.dex_version = result.version;
  snapshot.pool_type = result.is_stable ? "stable" : "volatile";
  // DeDust numbers assets from 1, snapshots from 0; '' is native TON.
  snapshot.asset0 = result.asset_1 ? convert::to_raw_address(*result.asset_1) : std::string();
  snapshot.asset1 = result.asset_2 ? convert::to_raw_address(*result.asset_2) : std::string();
  return snapshot;
}

schema::DexPoolSnapshot pool_identity(const StonfiPoolDetector::Result& result, const td::Bits256& code_hash) {
  schema::DexPoolSnapshot snapshot;
  snapshot.address = result.address;
  snapshot.code_hash = code_hash;
  snapshot.dex = "stonfi";
  snapshot.dex_version = result.version;
  snapshot.pool_type = result.pool_type;
  snapshot.router = result.router;
  snapshot.asset0 = convert::to_raw_address(result.asset_0);
  snapshot.asset1 = convert::to_raw_address(result.asset_1);
  return snapshot;
}

}

void enable_dex_snapshots() {
  g_dex_snapshots = true;
}

bool dex_snapshots_enabled() {
  return g_dex_snapshots;
}

schema::DexPoolSnapshot make_dex_pool_snapshot(const DedustPoolDetector::Result& result,
                                                uint64_t last_transaction_lt, const td::Bits256& code_hash) {
  auto snapshot = pool_identity(result, code_hash);
  snapshot.last_transaction_lt = last_transaction_lt;
  snapshot.reserve0 = result.reserve_1;
  snapshot.reserve1 = result.reserve_2;
  snapshot.lp_fee = result.fee_numerator;
  snapshot.fee_denominator = result.fee_denominator;
  snapshot.is_locked = result.is_locked;
  // V1 stable: asset1's rate relative to asset0, × 1e18 (the meaning of `rate` for STON.fi weighted stableswap).
  if (result.is_stable && result.rate_1.not_null() && result.rate_2.not_null() && td::sgn(result.rate_1) > 0) {
    snapshot.rate = td::muldiv(result.rate_2, td::make_refint(1000000000000000000LL), result.rate_1);
  }
  return snapshot;
}

schema::DexPoolSnapshot make_dex_pool_snapshot(const StonfiPoolDetector::Result& result,
                                                uint64_t last_transaction_lt, const td::Bits256& code_hash) {
  auto snapshot = pool_identity(result, code_hash);
  snapshot.last_transaction_lt = last_transaction_lt;
  snapshot.reserve0 = result.reserve_0;
  snapshot.reserve1 = result.reserve_1;
  snapshot.lp_fee = result.lp_fee;
  snapshot.protocol_fee = result.protocol_fee;
  snapshot.ref_fee = result.ref_fee;
  snapshot.fee_denominator = td::make_refint(kDexFeeDivider);
  snapshot.amp = result.amp;
  snapshot.rate = result.rate;
  snapshot.w0 = result.w0;
  snapshot.is_locked = result.is_locked;
  snapshot.lp_supply = result.total_supply_lp;
  return snapshot;
}

std::optional<DexPoolCache::Entry> DexPoolCache::get(const block::StdAddress& address) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = entries_.find(address);
  return it == entries_.end() ? std::nullopt : std::optional<Entry>(it->second);
}

void DexPoolCache::put(const DedustPoolDetector::Result& result, const td::Bits256& code_hash) {
  std::lock_guard<std::mutex> lock(mutex_);
  entries_[result.address] = Entry{.identity = pool_identity(result, code_hash)};
}

void DexPoolCache::put(const StonfiPoolDetector::Result& result, const td::Bits256& code_hash) {
  std::lock_guard<std::mutex> lock(mutex_);
  entries_[result.address] = Entry{.identity = pool_identity(result, code_hash),
                                 .stonfi_asset_0 = result.asset_0, .stonfi_asset_1 = result.asset_1};
}

void DexPoolCache::erase(const block::StdAddress& address) {
  std::lock_guard<std::mutex> lock(mutex_);
  entries_.erase(address);
}

void DexPoolCache::restore_identity(schema::DexPoolSnapshot identity) {
  identity.code_hash = td::Bits256::zero();
  const auto address = identity.address;
  std::lock_guard<std::mutex> lock(mutex_);
  entries_.try_emplace(address, Entry{.identity = std::move(identity)});
}

DexPoolCache& dex_pool_cache() {
  static DexPoolCache cache;
  return cache;
}

std::optional<schema::DexPoolSnapshot> make_dex_pool_removal(const block::StdAddress& address,
                                                       const td::Bits256& code_hash, uint64_t last_transaction_lt) {
  // Blocks finish out of order; another removal may still need this identity.
  auto entry = dex_pool_cache().get(address);
  if (!entry) {
    return std::nullopt;
  }
  auto snapshot = std::move(entry->identity);
  snapshot.removed = true;
  snapshot.code_hash = code_hash;
  snapshot.last_transaction_lt = last_transaction_lt;
  return snapshot;
}
