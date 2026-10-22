#include "CellCache.h"

#include <array>
#include <memory>
#include <unordered_set>
#include "crypto/vm/cells/DataCell.h"
#include "LruCache.h"

namespace firehose {
namespace {

using td::Ref;
using vm::Cell;
using vm::CellHash;
using vm::DataCell;

// Cell hash to a loaded cell whose descendants are loaded too. Each entry is
// charged the size of its own cell: a cell evicted while a cached parent
// still holds it stays in memory uncharged, which is bounded in practice by
// the data of the accounts whose trees are cached.
using CellCache = LruCache<CellHash, Ref<Cell>>;

std::unique_ptr<CellCache> g_cache;

// A lower bound of the BOC size of the cells loaded so far: each distinct cell
// takes at least its two descriptor bytes, its data and a byte per reference.
// A cached subtree counts only its root, so the bound stays a lower bound.
struct SizeBound {
  std::size_t limit;
  std::size_t bytes{0};
  std::unordered_set<CellHash> counted;

  bool add(const Cell& cell, std::size_t data_bytes, unsigned refs_cnt) {
    if (counted.insert(cell.get_hash()).second) {
      bytes += 2 + data_bytes + refs_cnt;
    }
    return bytes <= limit;
  }
};

// Null when bound is exceeded.
td::Result<Ref<Cell>> load_tree(CellCache& cache, const Ref<Cell>& cell, SizeBound* bound) {
  if (cell->is_virtualized()) {
    return cell;
  }
  auto hash = cell->get_hash();
  if (auto cached = cache.get(hash)) {
    if (bound != nullptr) {
      auto& root = static_cast<const DataCell&>(**cached);
      if (!bound->add(root, (root.get_bits() + 7) / 8, root.get_refs_cnt())) {
        return Ref<Cell>{};
      }
    }
    return std::move(*cached);
  }
  TRY_RESULT(loaded, cell->load_cell());
  const auto& data_cell = loaded.data_cell;
  unsigned refs_cnt = data_cell->get_refs_cnt();
  if (bound != nullptr && !bound->add(*data_cell, (data_cell->get_bits() + 7) / 8, refs_cnt)) {
    return Ref<Cell>{};
  }
  std::array<Ref<Cell>, Cell::max_refs> refs;
  bool children_unchanged = true;
  for (unsigned i = 0; i < refs_cnt; i++) {
    auto child = data_cell->get_ref(i);
    TRY_RESULT_ASSIGN(refs[i], load_tree(cache, child, bound));
    if (refs[i].is_null()) {
      return Ref<Cell>{};
    }
    children_unchanged &= refs[i].get() == child.get();
  }
  Ref<Cell> tree = data_cell;
  if (!children_unchanged) {
    // The same cell over loaded children: its hashes are those of data_cell.
    auto hash_hint = [&data_cell](unsigned level, const Cell::LevelMask&, CellHash& out) {
      out = data_cell->get_hash(level);
      return true;
    };
    TRY_RESULT(copy, DataCell::create(td::Slice(data_cell->get_data(), (data_cell->get_bits() + 7) / 8),
                                      data_cell->get_bits(), td::Span<Ref<Cell>>(refs.data(), refs_cnt),
                                      data_cell->is_special(), hash_hint));
    tree = std::move(copy);
  }
  cache.put(hash, tree, data_cell->get_storage_size() + sizeof(Ref<Cell>) * refs_cnt + 96);
  return tree;
}

}  // namespace

void set_cell_cache_size(std::size_t bytes) {
  g_cache = bytes == 0 ? nullptr : std::make_unique<CellCache>(bytes);
}

td::Result<Ref<Cell>> load_cell_tree(const CellHash& hash, vm::CellDbReader& reader) {
  if (g_cache) {
    if (auto cached = g_cache->get(hash)) {
      return std::move(*cached);
    }
  }
  TRY_RESULT(root, reader.load_cell(hash.as_slice()));
  if (!g_cache) {
    return Ref<Cell>(std::move(root));
  }
  return load_tree(*g_cache, root, nullptr);
}

td::Result<Ref<Cell>> load_cell_tree_up_to(const CellHash& hash, vm::CellDbReader& reader,
                                           std::size_t max_boc_size) {
  if (g_cache) {
    if (auto cached = g_cache->get(hash)) {
      return std::move(*cached);
    }
  }
  TRY_RESULT(root, reader.load_cell(hash.as_slice()));
  if (!g_cache) {
    return Ref<Cell>(std::move(root));
  }
  SizeBound bound{max_boc_size};
  return load_tree(*g_cache, root, &bound);
}

CellCacheStats cell_cache_stats() {
  return g_cache ? CellCacheStats{g_cache->hits(), g_cache->misses(), g_cache->bytes()} : CellCacheStats{0, 0, 0};
}

}  // namespace firehose
