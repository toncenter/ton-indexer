#pragma once
#include <vector>
#include "td/utils/Status.h"
#include "vm/cells/Cell.h"
#include "common/refcnt.hpp"
#include "MemoCache.h"

namespace block {
class ConfigInfo;
}

// Code metadata, not proof of executability in the current MC. Successful
// parses are remembered by the ORIGINAL code hash, including library references.
// Only a miss resolves libraries in config; errors are never remembered.
td::Result<std::vector<unsigned long long>> parse_contract_methods(
    td::Ref<vm::Cell> code_cell, const block::ConfigInfo* config = nullptr);

// Reference path for differential tests and diagnostics (no cache or counters).
td::Result<std::vector<unsigned long long>> parse_contract_methods_uncached(
    td::Ref<vm::Cell> code_cell, const block::ConfigInfo* config = nullptr);
memo::Stats contract_methods_cache_stats();
std::string contract_methods_cache_report();
