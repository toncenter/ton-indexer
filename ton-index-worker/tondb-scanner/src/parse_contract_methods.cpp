#include "InsertManager.h"
#include "tokens-tlb.h"
#include "common/checksum.h"
#include "safe-parse.h"

namespace {

constexpr std::size_t kMaxContractMethods = 4096;

}  // namespace


td::Result<std::vector<unsigned long long>> parse_contract_methods(td::Ref<vm::Cell> code_cell) {
  // check if first op is SETCP0
  // check if second op is DICTPUSHCONST
  // and load method ids from this dict of methods
  return safe_parse::guard("Failed to parse contract's method ids", [&]() -> td::Result<std::vector<unsigned long long>> {
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
    if (methods_dict_key_len > 64) {
      return td::Status::Error("Method id is wider than 64 bits");
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
