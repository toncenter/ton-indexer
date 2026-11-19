#pragma once
#include <algorithm>
#include <vector>
#include "td/utils/Status.h"
#include "vm/cells/Cell.h"
#include "common/refcnt.hpp"
#include "MemoCache.h"

namespace block {
class ConfigInfo;
}

struct ContractMethodInfo {
  std::vector<unsigned long long> ids;
  // True only for a recognized dispatcher whose missing-key path always throws
  // 11. A successfully parsed dictionary alone does not prove completeness.
  bool exhaustive = false;

  bool has(unsigned long long id) const { return std::binary_search(ids.begin(), ids.end(), id); }
};

td::Result<std::shared_ptr<const ContractMethodInfo>> get_contract_method_info(
    td::Ref<vm::Cell> code_cell, const block::ConfigInfo* config = nullptr);

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
