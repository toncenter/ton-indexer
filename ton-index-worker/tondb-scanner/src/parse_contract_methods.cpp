#include "InsertManager.h"
#include "tokens-tlb.h"
#include "common/checksum.h"
#include "safe-parse.h"
#include "parse_contract_methods.h"
#include "crypto/block/mc-config.h"

namespace {

constexpr std::size_t kMaxContractMethods = 4096;

}  // namespace


td::Result<std::vector<unsigned long long>> parse_contract_methods(
    td::Ref<vm::Cell> code_cell, const block::ConfigInfo* config) {
  // check if first op is SETCP0
  // check if second op is DICTPUSHCONST
  // and load method ids from this dict of methods
  return safe_parse::guard("Failed to parse contract's method ids", [&]() -> td::Result<std::vector<unsigned long long>> {
    for (unsigned depth = 0; ; ++depth) {
      if (code_cell.is_null()) {
        return td::Status::Error("Contract code is missing");
      }
      vm::CellSlice code(vm::NoVm{}, code_cell);
      if (code.special_type() != vm::Cell::SpecialType::Library) {
        break;
      }
      if (!config || depth >= 16) {
        return td::Status::Error("Cannot resolve contract code library reference");
      }
      td::Bits256 hash;
      if (!(code.advance(8) && code.fetch_bits_to(hash))) {
        return td::Status::Error("Invalid contract code library reference");
      }
      code_cell = config->lookup_library(hash);
      if (code_cell.is_null()) {
        return td::Status::Error("Contract code library is unavailable");
      }
    }
    vm::CellSlice cs = vm::load_cell_slice(code_cell);

    auto first_op = cs.fetch_ulong(8);
    auto expectedCodePage = cs.fetch_ulong(8);
    if (first_op != 0xFF || expectedCodePage != 0) {
      return td::Status::Error("Failed to parse 1. SETCP or codepage is not 0");
    }

    auto second_op = cs.fetch_ulong(13);
    auto is_push_op_flag = cs.fetch_ulong(1);
    if (second_op != 0b1111010010100 || is_push_op_flag != 1) {
      return td::Status::Error("Failed to parse 2. DICTPUSHCONST");
    }

    auto methods_dict_key_len = static_cast<int>(cs.fetch_ulong(10));
    if (methods_dict_key_len > 32) {
      return td::Status::Error("Method id is wider than 32 bits");
    }
    auto methods_dict_cell = cs.fetch_ref();
    std::vector<unsigned long long> method_ids;
    TRY_STATUS(safe_parse::for_each_dict_entry(methods_dict_cell, methods_dict_key_len, kMaxContractMethods,
        [&](td::ConstBitPtr key, int key_len, td::Ref<vm::CellSlice>) -> td::Status {
          method_ids.push_back(key.get_uint(key_len));
          return td::Status::OK();
        }));
    return method_ids;
  });
}
