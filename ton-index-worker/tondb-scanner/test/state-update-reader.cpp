#include "StateUpdateReader.h"
#include "vm/boc.h"
#include "vm/cells/ExtCell.h"
#include "vm/cells/MerkleUpdate.h"
#include "vm/cells/UsageCell.h"
#include "vm/db/CellStorage.h"
#include "td/utils/logging.h"
#include <iostream>

namespace {

struct Loader {
  static td::Result<td::Ref<vm::DataCell>> load_data_cell(
      const vm::Cell& cell, const std::shared_ptr<vm::CellDbReader>& reader) {
    return reader->load_cell(cell.get_hash().as_slice());
  }
};

class Reader final : public vm::CellDbReader, public std::enable_shared_from_this<Reader> {
 public:
  std::unordered_map<vm::CellHash, td::Ref<vm::DataCell>, vm::CellHashF> cells;
  unsigned reads = 0;
  bool fail = false;
  void add(const td::Ref<vm::Cell>& root) {
    if (cells.count(root->get_hash())) return;
    auto cell = root->load_cell().move_as_ok().data_cell;
    cells.emplace(root->get_hash(), cell);
    for (unsigned i = 0; i < cell->get_refs_cnt(); ++i) add(cell->get_ref(i));
  }
  td::Result<td::Ref<vm::DataCell>> load_cell(td::Slice hash) override {
    ++reads;
    if (fail) return td::Status::Error("Injected cell read error");
    auto it = cells.find(vm::CellHash::from_slice(hash));
    if (it == cells.end()) return td::Status::Error("Missing cell");
    return it->second;
  }
  td::Result<std::vector<td::Ref<vm::DataCell>>> load_bulk(td::Span<td::Slice> hashes) override {
    std::vector<td::Ref<vm::DataCell>> result;
    for (auto hash : hashes) { TRY_RESULT(cell, load_cell(hash)); result.push_back(std::move(cell)); }
    return result;
  }
  td::Ref<vm::Cell> create_unloaded_cell(const td::Ref<vm::Cell>& cell, int depth) override {
    auto mask = cell->get_level_mask().apply(depth);
    td::uint8 hashes[128], depths[8];
    size_t n = 0;
    for (unsigned i = 0; i <= mask.get_level(); ++i) {
      if (!mask.is_significant(i)) continue;
      td::MutableSlice{hashes + 32*n, 32}.copy_from(cell->get_hash(i).as_slice());
      vm::DataCell::store_depth(depths + 2*n, cell->get_depth(i));
      ++n;
    }
    return vm::ExtCell<std::shared_ptr<vm::CellDbReader>, Loader>::create(
        vm::PrunnedCellInfo{mask, td::Slice{hashes,32*n}, td::Slice{depths,2*n}}, shared_from_this()).move_as_ok();
  }
};

struct Fixture {
  td::Ref<vm::Cell> old_root, new_root, update, old_child, new_child;
};

Fixture fixture() {
  auto old_child = vm::CellBuilder().store_long(42,32).store_ref(vm::CellBuilder().store_long(99,32).finalize()).finalize();
  auto old_root = vm::CellBuilder().store_long(1,32).store_ref(old_child).finalize();
  auto usage = std::make_shared<vm::CellUsageTree>();
  auto tracked = vm::UsageCell::create(old_root, usage->root_ptr());
  auto reused = vm::CellSlice(vm::NoVm(), tracked).prefetch_ref();
  auto new_child = vm::CellBuilder().store_long(7,32).finalize();
  auto next = vm::CellBuilder().store_long(2,32).store_ref(reused).store_ref(new_child).finalize();
  auto update = vm::MerkleUpdate::generate(tracked, next, usage.get()).move_as_ok();
  return {old_root, next, update, old_child, new_child};
}

void check_lazy_and_lifetime() {
  auto f = fixture(); auto base = std::make_shared<Reader>(); base->add(f.old_root); base->add(f.new_root);
  auto reader = StateUpdateReader::create(f.update, f.new_root, base).move_as_ok();
  CHECK(base->reads == 0 && reader->size() == 2);
  auto root = reader->load_cell(f.new_root->get_hash().as_slice()).move_as_ok();
  auto child = reader->load_cell(f.new_child->get_hash().as_slice()).move_as_ok();
  CHECK(base->reads == 0);
  CHECK(vm::CellStorer::serialize_value(1, root, false) ==
        vm::CellStorer::serialize_value(1, f.new_root->load_cell().move_as_ok().data_cell, false));
  auto lazy = vm::load_cell_slice(root).prefetch_ref();
  CHECK(!lazy->is_loaded());
  reader.reset(); // extracted account graphs survive release of the temporary index
  base->fail = true;
  CHECK(lazy->load_cell().is_error());
  base->fail = false;
  CHECK(vm::load_cell_slice(lazy).prefetch_ulong(32) == 42 && base->reads == 2);
  CHECK(vm::std_boc_serialize(root).move_as_ok().as_slice() ==
        vm::std_boc_serialize(f.new_root).move_as_ok().as_slice());
  CHECK(vm::load_cell_slice(child).prefetch_ulong(32) == 7);
}

void check_fallback_and_identity() {
  auto f = fixture(); auto base = std::make_shared<Reader>(); base->add(f.old_root); base->add(f.new_root);
  auto reader = StateUpdateReader::create(f.update, f.new_root, base).move_as_ok();
  auto old = reader->load_cell(f.old_child->get_hash().as_slice()).move_as_ok();
  CHECK(old->get_hash() == f.old_child->get_hash() && base->reads == 1);
  auto a = f.new_root->get_hash(), b = f.old_root->get_hash();
  std::vector<td::Slice> hashes{a.as_slice(), b.as_slice()};
  auto bulk = reader->load_bulk(hashes).move_as_ok();
  CHECK(bulk.size() == 2 && bulk[0]->get_hash() == a && bulk[1]->get_hash() == b);
  base->fail = true;
  CHECK(reader->load_cell(b.as_slice()).is_error());
  CHECK(reader->load_cell(a.as_slice()).is_ok());
  CHECK(reader->load_bulk(hashes).is_error());
  CHECK(StateUpdateReader::create(f.update, f.old_root, base).is_error());
  CHECK(StateUpdateReader::create(f.new_root, f.new_root, base).is_error());
  CHECK(StateUpdateReader::create({}, f.new_root, base).is_error());
  for (size_t limit : {size_t(0), size_t(1)}) {
    auto r = StateUpdateReader::create(f.update, f.new_root, base, limit);
    CHECK(r.is_error() && r.error().code() == StateUpdateReader::LIMIT_EXCEEDED);
  }
  base->fail = false;
  auto unchanged = vm::CellBuilder::create_merkle_update(
      vm::CellBuilder::create_pruned_branch(f.old_root,1),
      vm::CellBuilder::create_pruned_branch(f.old_root,1));
  auto same = StateUpdateReader::create(unchanged,f.old_root,base).move_as_ok();
  CHECK(same->size() == 0 && same->load_cell(b.as_slice()).is_ok());
}

void check_exotic_and_shared_graphs() {
  auto base = std::make_shared<Reader>();
  auto leaf = vm::CellBuilder().store_long(123,32).finalize();
  auto pruned = vm::CellBuilder::create_pruned_branch(leaf,1);
  auto proof = vm::CellBuilder::create_merkle_proof(pruned);
  auto library = vm::CellBuilder().store_long(2,8).store_bits(leaf->get_hash().bits(),256).finalize(true);
  auto old = vm::CellBuilder().finalize();
  auto next = vm::CellBuilder().store_ref(proof).store_ref(library).store_ref(proof).finalize();
  // A full update also covers nested Merkle depth and shared descendants.
  auto update = vm::CellBuilder::create_merkle_update(old,next);
  base->add(next);
  auto r = StateUpdateReader::create(update,next,base).move_as_ok();
  auto loaded = r->load_cell(next->get_hash().as_slice()).move_as_ok();
  CHECK(vm::std_boc_serialize(loaded).move_as_ok().as_slice() == vm::std_boc_serialize(next).move_as_ok().as_slice());
  CHECK(base->reads == 0);
  // Different state roots, including a two-parent-shaped root, cannot alias.
  auto split = vm::CellBuilder().store_long(0x5f327da5,32).store_ref(next).store_ref(old).finalize();
  auto split_update = vm::CellBuilder::create_merkle_update(next,split);
  auto s = StateUpdateReader::create(split_update,split,base).move_as_ok();
  CHECK(s->load_cell(split->get_hash().as_slice()).move_as_ok()->get_hash() == split->get_hash());
}

}  // namespace

int main() {
  SET_VERBOSITY_LEVEL(verbosity_WARNING);
  check_lazy_and_lifetime();
  check_fallback_and_identity();
  check_exotic_and_shared_graphs();
  std::cout << "State update reader: lazy cells, errors, bounds, identity, lifetime and exotic parity passed\n";
}
