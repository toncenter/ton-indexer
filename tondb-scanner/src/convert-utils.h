#pragma once


namespace convert {
  td::Result<std::string> to_raw_address(td::Ref<vm::CellSlice> cs);

  std::string to_raw_address(block::StdAddress address);

  td::Result<block::StdAddress> to_std_address(td::Ref<vm::CellSlice> cs);

  td::Result<td::int64> to_balance(vm::CellSlice& balance_slice);
  td::Result<td::int64> to_balance(td::Ref<vm::CellSlice> balance_ref);
  // td::Result<td::RefInt256> to_balance256(vm::CellSlice& balance_slice);
  // td::Result<td::RefInt256> to_balance256(td::Ref<vm::CellSlice> balance_ref);

  td::Result<td::optional<std::string>> to_bytes(td::Ref<vm::Cell> cell);
}
