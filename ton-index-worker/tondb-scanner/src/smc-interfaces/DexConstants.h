#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "block/block.h"

constexpr int kDexFeeDivider = 10000;

namespace dex_constants {

extern const std::unordered_set<std::string> stonfi_v1_pool_code_hashes;
extern const std::unordered_set<std::string> stonfi_v2_pool_code_hashes;
extern const std::unordered_map<std::string, int> stonfi_routers;
extern const std::unordered_set<std::string> dedust_v1_pool_code_hashes;
extern const block::StdAddress dedust_v1_factory;
extern const block::StdAddress dedust_v2_admin;
extern const std::vector<std::string> dedust_v2_pool_library_hashes;
extern const std::vector<td::Ref<vm::Cell>> dedust_v2_pool_library_cells;
extern const std::unordered_set<std::string> dedust_v2_pool_code_hashes;

bool is_pool_code_hash(const std::string& code_hash);

}
