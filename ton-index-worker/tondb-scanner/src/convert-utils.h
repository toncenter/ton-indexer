#pragma once

#include "td/utils/Status.h"
#include "vm/cells/CellSlice.h"
#include "vm/cells/Cell.h"
#include <optional>

#include "IndexData.h"
#include "crypto/block/block-parse.h"

namespace convert {
  td::Result<schema::AccountAddress> to_account_address(td::Ref<vm::CellSlice> cs);

  inline schema::AccountAddress to_account_address(const std::optional<block::StdAddress>& addr) {
    if (addr.has_value()) {
      return addr.value();
    }
    return schema::AddressNone{};
  }

  inline td::Result<std::vector<schema::AccountAddress>> to_account_address_vector(const std::vector<schema::AddressStd>& vec) {
    std::vector<schema::AccountAddress> result;
    for (const auto& item : vec) {
      result.push_back(item);
    }
    return result;
  }

  td::Result<std::string> to_raw_address(td::Ref<vm::CellSlice> cs);

  std::string to_raw_address(block::StdAddress address);

  td::Result<block::StdAddress> to_std_address(td::Ref<vm::CellSlice> cs);

  td::Result<std::optional<std::string>> to_bytes(td::Ref<vm::Cell> cell);
}
