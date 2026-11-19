#pragma once

#include "vm/db/DynamicBagOfCellsDb.h"
#include <atomic>
#include <unordered_map>

class TestCellDbReader final : public vm::CellDbReader {
 public:
  std::unordered_map<vm::CellHash, td::Ref<vm::DataCell>, vm::CellHashF> cells;
  std::atomic<unsigned> reads{0};
  std::atomic<bool> fail{false};

  void add(const td::Ref<vm::Cell>& root) {
    if (root.is_null() || cells.count(root->get_hash())) return;
    auto cell = root->load_cell().move_as_ok().data_cell;
    cells.emplace(root->get_hash(), cell);
    for (unsigned i = 0; i < cell->get_refs_cnt(); ++i) add(cell->get_ref(i));
  }
  td::Result<td::Ref<vm::DataCell>> load_cell(td::Slice hash) override {
    ++reads;
    if (fail) return td::Status::Error("Injected parent cell read failure");
    auto it = cells.find(vm::CellHash::from_slice(hash));
    if (it == cells.end()) return td::Status::Error("Missing test parent cell");
    return it->second;
  }
  td::Result<std::vector<td::Ref<vm::DataCell>>> load_bulk(td::Span<td::Slice> hashes) override {
    std::vector<td::Ref<vm::DataCell>> result;
    for (auto hash : hashes) {
      TRY_RESULT(cell, load_cell(hash));
      result.push_back(cell);
    }
    return result;
  }
  td::Ref<vm::Cell> create_unloaded_cell(const td::Ref<vm::Cell>& cell, int) override { return cell; }
};
