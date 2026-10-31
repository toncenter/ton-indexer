#pragma once
#include <vector>
#include "td/utils/Status.h"
#include "vm/cells/Cell.h"
#include "common/refcnt.hpp"

namespace block {
class ConfigInfo;
}

// Optional MC libraries resolve code represented by a library-reference cell.
// An error means the method set is unknown, not that it is empty.
td::Result<std::vector<unsigned long long>> parse_contract_methods(
    td::Ref<vm::Cell> code_cell, const block::ConfigInfo* config = nullptr);
