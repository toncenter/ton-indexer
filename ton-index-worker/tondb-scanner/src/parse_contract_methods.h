#pragma once
#include <vector>
#include "td/utils/Status.h"
#include "vm/cells/Cell.h"
#include "common/refcnt.hpp"


td::Result<std::vector<unsigned long long>> parse_contract_methods(td::Ref<vm::Cell> code_cell);
