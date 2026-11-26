#pragma once

#include <mutex>
#include <optional>
#include <unordered_map>

#include "DexConstants.h"
#include "IndexData.h"
#include "smc-interfaces/Stonfi.h"
#include "smc-interfaces/Tokens.h"

// Off by default: STON.fi pools are not detected and no snapshots are built. A DEX snapshots writer turns it on.
void enable_dex_snapshots();
bool dex_snapshots_enabled();

schema::DexPoolSnapshot make_dex_pool_snapshot(const DedustPoolDetector::Result& result,
                                                uint64_t last_transaction_lt, const td::Bits256& code_hash);
schema::DexPoolSnapshot make_dex_pool_snapshot(const StonfiPoolDetector::Result& result,
                                                uint64_t last_transaction_lt, const td::Bits256& code_hash);

class DexPoolCache {
 public:
  struct Entry {
    schema::DexPoolSnapshot identity;
    block::StdAddress stonfi_asset_0;
    block::StdAddress stonfi_asset_1;
  };

  std::optional<Entry> get(const block::StdAddress& address) const;
  void put(const DedustPoolDetector::Result& result, const td::Bits256& code_hash);
  void put(const StonfiPoolDetector::Result& result, const td::Bits256& code_hash);
  // Persisted identity is enough for removals; detection must verify it again.
  void restore_identity(schema::DexPoolSnapshot identity);
  void erase(const block::StdAddress& address);

 private:
  mutable std::mutex mutex_;
  std::unordered_map<block::StdAddress, Entry> entries_;
};

DexPoolCache& dex_pool_cache();

std::optional<schema::DexPoolSnapshot> make_dex_pool_removal(const block::StdAddress& address,
                                                       const td::Bits256& code_hash, uint64_t last_transaction_lt);
