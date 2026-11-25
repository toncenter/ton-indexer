#include "StateUpdateReader.h"
#include "Statistics.h"
#include "td/utils/HashMap.h"
#include "vm/cells/MerkleUpdate.h"
#include "vm/excno.hpp"
#include <algorithm>

namespace {

// Equivalent to the pinned node's BuildNextState, with bounded temporary work.
// Cache keys include Merkle depth: the same proof cell can occur at different
// virtualization levels. Pruned branches belonging to the update become lazy
// references; genuine exotic cells inside account code/data keep their semantics.
class Builder {
 public:
  Builder(vm::CellDbReader& reader, size_t limit) : reader_(reader), limit_(limit) {}

  td::Result<td::Ref<vm::Cell>> rebuild(const td::Ref<vm::Cell>& cell, unsigned merkle_depth,
                                      unsigned recursion_depth = 0) {
    if (++visits_ > limit_ * 4 + 1 || recursion_depth > 1024) {
      return td::Status::Error(StateUpdateReader::LIMIT_EXCEEDED, "State update traversal limit");
    }
    vm::CellSlice cs(vm::NoVm(), cell);
    if (cs.special_type() == vm::Cell::SpecialType::PrunnedBranch) {
      if (cell->get_level() == merkle_depth + 1) {
        auto result = reader_.create_unloaded_cell(cell, merkle_depth);
        if (result.is_null() || result->get_hash() != cell->get_hash(merkle_depth)) {
          return td::Status::Error("State update pruned reference mismatch");
        }
        return result;
      }
      return cell;
    }
    auto key = std::make_pair(cell->get_hash(), merkle_depth);
    if (auto it = ready.find(key); it != ready.end()) return it->second;
    if (++new_cells_ > limit_) {
      return td::Status::Error(StateUpdateReader::LIMIT_EXCEEDED, "State update cell limit");
    }
    const auto child_depth = cs.child_merkle_depth(merkle_depth);
    vm::CellBuilder cb;
    cb.store_bits(cs.fetch_bits(cs.size()));
    for (unsigned i = 0; i < cs.size_refs(); ++i) {
      TRY_RESULT(ref, rebuild(cs.prefetch_ref(i), child_depth, recursion_depth + 1));
      cb.store_ref(std::move(ref));
    }
    auto hash_hint = [&](unsigned level, const vm::Cell::LevelMask&, vm::CellHash& hash) {
      hash = cell->get_hash(std::min(level, merkle_depth));
      return true;
    };
    auto result = cb.finalize_novm(cs.is_special(), std::move(hash_hint));
    ready.emplace(key, result);
    return td::Ref<vm::Cell>(std::move(result));
  }

  td::HashMap<std::pair<vm::CellHash, unsigned>, td::Ref<vm::Cell>> ready;

 private:
  vm::CellDbReader& reader_;
  size_t limit_, visits_ = 0, new_cells_ = 0;
};

}  // namespace

td::Result<std::unique_ptr<StateUpdateReader>> StateUpdateReader::create(
    const td::Ref<vm::Cell>& update, const td::Ref<vm::Cell>& expected_state,
    std::shared_ptr<vm::CellDbReader> reader, size_t max_cells) {
  if (update.is_null() || expected_state.is_null() || !reader) {
    return td::Status::Error("Missing state update input");
  }
  // Clamp caller-supplied bounds too; this is a transient optimization, not a
  // reason to allocate an arbitrary state graph or alter parser correctness.
  max_cells = std::min(max_cells, MAX_NEW_CELLS);
  if (!max_cells) return td::Status::Error(LIMIT_EXCEEDED, "State update cell limit");
  try {
    TRY_STATUS(vm::MerkleUpdate::validate(update));
    vm::CellSlice cs(vm::NoVm(), update);
    if (cs.special_type() != vm::Cell::SpecialType::MerkleUpdate) {
      return td::Status::Error("Invalid state update type");
    }
    auto proof = cs.prefetch_ref(1);
    if (proof->get_hash(0) != expected_state->get_hash() ||
        proof->get_depth(0) != expected_state->get_depth()) {
      return td::Status::Error("State update does not match persisted post-state");
    }
    auto result = std::unique_ptr<StateUpdateReader>(new StateUpdateReader(std::move(reader)));
    Builder builder(*result->reader_, max_cells);
    TRY_RESULT(root, builder.rebuild(proof, 0));
    if (root->get_hash() != expected_state->get_hash() || root->get_depth() != expected_state->get_depth()) {
      return td::Status::Error("Reconstructed post-state mismatch");
    }
    result->cells_.reserve(builder.ready.size());
    for (const auto& [key, cell] : builder.ready) {
      TRY_RESULT(loaded, cell->load_cell());
      result->cells_.emplace(cell->get_hash(), std::move(loaded.data_cell));
    }
    return result;
  } catch (const vm::VmError& error) {
    return td::Status::Error(PSLICE() << "State update: " << error.get_msg());
  } catch (const vm::VmVirtError& error) {
    return td::Status::Error(PSLICE() << "State update: " << error.get_msg());
  }
}

td::Result<td::Ref<vm::DataCell>> StateUpdateReader::load_cell(td::Slice hash) {
  auto it = cells_.find(vm::CellHash::from_slice(hash));
  if (it != cells_.end()) {
    g_statistics.record_count(STATE_UPDATE_HIT);
    return it->second;
  }
  g_statistics.record_count(STATE_UPDATE_MISS);
  return reader_->load_cell(hash);
}

td::Result<std::vector<td::Ref<vm::DataCell>>> StateUpdateReader::load_bulk(td::Span<td::Slice> hashes) {
  std::vector<td::Ref<vm::DataCell>> result(hashes.size());
  std::vector<td::Slice> missing;
  std::vector<size_t> positions;
  for (size_t i = 0; i < hashes.size(); ++i) {
    auto it = cells_.find(vm::CellHash::from_slice(hashes[i]));
    if (it != cells_.end()) {
      result[i] = it->second;
      g_statistics.record_count(STATE_UPDATE_HIT);
    } else {
      positions.push_back(i); missing.push_back(hashes[i]);
      g_statistics.record_count(STATE_UPDATE_MISS);
    }
  }
  if (!missing.empty()) {
    TRY_RESULT(loaded, reader_->load_bulk(missing));
    for (size_t i = 0; i < loaded.size(); ++i) result[positions[i]] = std::move(loaded[i]);
  }
  return result;
}

td::Ref<vm::Cell> StateUpdateReader::create_unloaded_cell(const td::Ref<vm::Cell>& cell, int merkle_depth) {
  return reader_->create_unloaded_cell(cell, merkle_depth);
}
