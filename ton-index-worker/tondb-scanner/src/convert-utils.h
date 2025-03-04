#pragma once


namespace convert {
  td::Result<std::string> to_raw_address(td::Ref<vm::CellSlice> cs);

  std::string to_raw_address(block::StdAddress address);

  td::Result<block::StdAddress> to_std_address(td::Ref<vm::CellSlice> cs);

  td::Result<std::optional<std::string>> to_bytes(td::Ref<vm::Cell> cell);
}
