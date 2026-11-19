#pragma once

#include "vm/db/DynamicBagOfCellsDb.h"
#include <memory>
#include <unordered_map>

// A short-lived, content-addressed view of the new side of one block's update.
// Old subtrees remain lazy references into the supplied historical CellDB reader.
// The parser releases this index after extracting the block's account states;
// only code/data subgraphs actually retained by those accounts survive it.
class StateUpdateReader final : public vm::CellDbReader {
 public:
  static constexpr size_t MAX_NEW_CELLS = 65536;
  static constexpr int LIMIT_EXCEEDED = 413;

  static td::Result<std::unique_ptr<StateUpdateReader>> create(
      const td::Ref<vm::Cell>& update, const td::Ref<vm::Cell>& expected_state,
      std::shared_ptr<vm::CellDbReader> reader, size_t max_cells = MAX_NEW_CELLS);

  td::Result<td::Ref<vm::DataCell>> load_cell(td::Slice hash) override;
  td::Result<std::vector<td::Ref<vm::DataCell>>> load_bulk(td::Span<td::Slice> hashes) override;
  td::Ref<vm::Cell> create_unloaded_cell(const td::Ref<vm::Cell>& cell, int merkle_depth) override;
  size_t size() const { return cells_.size(); }

 private:
  explicit StateUpdateReader(std::shared_ptr<vm::CellDbReader> reader) : reader_(std::move(reader)) {}
  std::shared_ptr<vm::CellDbReader> reader_;
  std::unordered_map<vm::CellHash, td::Ref<vm::DataCell>, vm::CellHashF> cells_;
};
