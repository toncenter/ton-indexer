#include "ParentCellCache.h"

#include "common/AtomicRef.h"
#include "vm/cells/DataCell.h"

namespace {
struct CellImage {
  ParentCellCache::Root root;
  std::array<ParentCellCache::Root, vm::Cell::max_refs> refs;
  std::array<unsigned char, 128> data{};
  unsigned bits = 0, ref_count = 0;
  bool special = false;
};
}  // namespace

struct ParentCellCache::Impl {
  memo::Cache<vm::CellHash, CellImage, vm::CellHashF> cells;
  static constexpr size_t overhead() { return sizeof(Impl) + 64; }
  explicit Impl(size_t bytes)
      : cells(bytes > overhead() ? bytes - overhead() : 0,
              std::numeric_limits<size_t>::max(), memo::Policy::segmented) {}
};

class ParentCellCache::LazyCell final : public vm::Cell {
 public:
  LazyCell(Root root, std::shared_ptr<Impl> cache, std::shared_ptr<vm::CellDbReader> reader)
      : root_(std::move(root)), cache_(std::move(cache)), reader_(std::move(reader)) {}

  td::Result<LoadedCell> load_cell() const override {
    auto data = loaded_.load();
    if (data.is_null()) {
      TRY_RESULT(image, cache_->cells.get(root_.hashes[vm::Cell::max_level], [&]() -> td::Result<CellImage> {
        TRY_RESULT(source, reader_->load_cell(root_.hashes[vm::Cell::max_level].as_slice()));
        CellImage result;
        result.root = describe(source);
        if (!(result.root == root_)) return td::Status::Error("Parent cell identity mismatch");
        result.bits = source->get_bits();
        result.ref_count = source->get_refs_cnt();
        result.special = source->is_special();
        std::copy_n(source->get_data(), (result.bits + 7) / 8, result.data.begin());
        for (unsigned i = 0; i < result.ref_count; ++i) result.refs[i] = describe(source->get_ref(i));
        return result;
      }, [](const CellImage&) { return size_t(0); }));
      if (!(image->root == root_)) return td::Status::Error("Cached parent cell identity mismatch");
      std::array<td::Ref<vm::Cell>, vm::Cell::max_refs> refs;
      for (unsigned i = 0; i < image->ref_count; ++i) {
        refs[i] = td::make_ref<LazyCell>(image->refs[i], cache_, reader_);
      }
      TRY_RESULT(materialized, vm::DataCell::create(
          td::Slice(image->data.data(), (image->bits + 7) / 8), image->bits,
          td::Span<td::Ref<vm::Cell>>(refs.data(), image->ref_count), image->special,
          [&root = image->root](unsigned level, const LevelMask& mask, Hash& hash) {
            if (mask.get_mask() != root.mask) return false;
            hash = root.hashes[level];
            return true;
          }));
      loaded_.store_if_empty(materialized);
      data = loaded_.load();
    }
    const auto level = data->get_level();
    return LoadedCell{std::move(data), level, {}};
  }

  td::Status set_data_cell(td::Ref<vm::DataCell>&& data) const override {
    TRY_STATUS(check_equals_unloaded(data));
    loaded_.store_if_empty(data);
    return td::Status::OK();
  }
  bool is_virtualized() const override { return false; }
  vm::CellUsageTree::NodePtr get_tree_node() const override { return {}; }
  bool is_loaded() const override { return loaded_.get_unsafe() != nullptr; }
  LevelMask get_level_mask() const override { return LevelMask(root_.mask); }

 private:
  td::uint16 do_get_depth(td::uint32 level) const override { return root_.depths[std::min<unsigned>(level, max_level)]; }
  const Hash do_get_hash(td::uint32 level) const override { return root_.hashes[std::min<unsigned>(level, max_level)]; }
  Root root_;
  std::shared_ptr<Impl> cache_;
  std::shared_ptr<vm::CellDbReader> reader_;
  mutable td::AtomicRef<vm::DataCell> loaded_;
};

ParentCellCache::ParentCellCache(size_t max_bytes) : impl_(std::make_shared<Impl>(max_bytes)) {}

ParentCellCache::Root ParentCellCache::describe(const td::Ref<vm::Cell>& cell) {
  Root root;
  if (cell.is_null()) return root;
  if (cell->is_virtualized() || !cell->get_tree_node().empty()) {
    throw std::runtime_error("Virtualized parent cell cannot be detached");
  }
  root.present = true;
  root.mask = cell->get_level_mask().get_mask();
  for (unsigned i = 0; i <= vm::Cell::max_level; ++i) {
    root.hashes[i] = cell->get_hash(i);
    root.depths[i] = cell->get_depth(i);
  }
  return root;
}

td::Ref<vm::Cell> ParentCellCache::wrap(const Root& root, std::shared_ptr<vm::CellDbReader> reader) const {
  if (!root.present) return {};
  return td::make_ref<LazyCell>(root, impl_, std::move(reader));
}

memo::Stats ParentCellCache::stats() const {
  auto stats = impl_->cells.stats();
  stats.bytes += Impl::overhead();
  return stats;
}
std::string ParentCellCache::report() { return impl_->cells.report("indexer.cache.parent_cells"); }
