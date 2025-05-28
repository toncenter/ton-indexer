#include "InsertManager.h"
#include "tokens-tlb.h"
#include "common/checksum.h"


td::Result<std::vector<unsigned long long>> parse_contract_methods(td::Ref<vm::Cell> code_cell) {
  // check if first op is SETCP0
  // check if second op is DICTPUSHCONST
  // and load method ids from this dict of methods
  try {
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
    auto methods_dict_cell = cs.fetch_ref();
    vm::Dictionary methods_dict{methods_dict_cell, methods_dict_key_len};
    std::vector<unsigned long long> method_ids;
    auto iterator = methods_dict.begin();

    while (!iterator.eof()) {
      // load 32 bits from the start of the key and cut `methods_dict_key_len` bits as only needed
      auto key = td::BitArray<32>(iterator.cur_pos()).to_ulong() >> (32 - methods_dict_key_len);
      method_ids.push_back(key);
      ++iterator;
    }
    return method_ids;
  }
  catch (vm::VmError& err) {
    return td::Status::Error(PSLICE() << "Failed to parse contract's method ids " << err.get_msg());
  }
}
