#pragma once

#include "MemoCache.h"
#include "vm/db/DynamicBagOfCellsDb.h"

// Detached, immutable cell images: no recursive cell ownership or CellDB reader
// in the inter-block cache. Each VM receives lazy references backed by its own
// reader; only the cells it actually loads are admitted and charged.
class ParentCellCache {
 public:
  struct Root {
    bool present = false;
    unsigned mask = 0;
    std::array<vm::CellHash, vm::Cell::max_level + 1> hashes{};
    std::array<td::uint16, vm::Cell::max_level + 1> depths{};
    bool operator==(const Root&) const = default;
  };
  explicit ParentCellCache(size_t max_bytes);
  static Root describe(const td::Ref<vm::Cell>& cell);
  td::Ref<vm::Cell> wrap(const Root& root, std::shared_ptr<vm::CellDbReader> reader) const;
  memo::Stats stats() const;
  std::string report();

 private:
  class LazyCell;
  struct Impl;
  std::shared_ptr<Impl> impl_;
};
