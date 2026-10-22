#pragma once
#include <cstddef>
#include <cstdint>
#include "td/utils/Status.h"
#include "crypto/vm/cells/Cell.h"
#include "crypto/vm/db/DynamicBagOfCellsDb.h"

// Cells of account code, data and libraries, kept loaded across blocks.
// Between two versions of an account only a few cells change, and accounts
// share code, so most cells a block needs were already loaded for an earlier
// one; without the cache each of them is a cell db read again.
namespace firehose {

struct CellCacheStats {
  std::uint64_t hits;
  std::uint64_t misses;
  std::uint64_t bytes;
};

// Sets the size of the process-wide cache in bytes; 0 disables it.
void set_cell_cache_size(std::size_t bytes);

// Returns the tree of the cell with this hash with every cell loaded: subtrees
// found in the cache are taken from it, the other cells are loaded through
// reader and cached. With the cache disabled, returns the cell as reader loads it.
td::Result<td::Ref<vm::Cell>> load_cell_tree(const vm::CellHash& hash, vm::CellDbReader& reader);

// The same, unless the BOC of the tree is certainly over max_boc_size, which
// is found out before all its cells are loaded: null then.
td::Result<td::Ref<vm::Cell>> load_cell_tree_up_to(const vm::CellHash& hash, vm::CellDbReader& reader,
                                                   std::size_t max_boc_size);

CellCacheStats cell_cache_stats();

}  // namespace firehose
