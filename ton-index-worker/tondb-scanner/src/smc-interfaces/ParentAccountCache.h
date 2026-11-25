#pragma once

#include "IndexData.h"
#include "MemoCache.h"

class ParentAccountCache;

struct ParentAccountView {
  const ParentAccountCache* owner = nullptr;
  uint64_t epoch = 0;
  uint32_t mc_seqno = 0;
};

// One latest parent state per address. Updates are published before launching
// parallel detectors, after the existing ordered trace-assembly stage.
class ParentAccountCache {
 public:
  static constexpr size_t DEFAULT_BYTES = 512ULL * 1024 * 1024;
  static constexpr size_t MIN_BYTES = 256 * 1024; // fixed journal and cache objects
  struct Update {
    block::StdAddress address;
    const schema::AccountState* state = nullptr; // null = invalidate
  };
  explicit ParentAccountCache(size_t max_bytes = DEFAULT_BYTES);
  ~ParentAccountCache();

  ParentAccountView observe(uint32_t mc_seqno, const ParsedBlock& block);
  ParentAccountView advance(uint32_t mc_seqno, td::Bits256 root_hash, td::Bits256 prev_root_hash,
                            const std::vector<Update>& changes);
  td::Result<schema::AccountState> lookup(
      const ParentAccountView& view, const block::StdAddress& address,
      const std::shared_ptr<vm::CellDbReader>& reader,
      const std::function<td::Result<schema::AccountState>()>& load);
  size_t retained_bytes() const;
  memo::Stats state_stats() const;
  memo::Stats cell_stats() const;
  std::string report();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
