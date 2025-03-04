#include "smc-envelope/SmartContract.h"

td::Result<std::vector<vm::StackEntry>> execute_smc_method(const block::StdAddress& address, 
                                        td::Ref<vm::Cell> code, td::Ref<vm::Cell> data, 
                                        std::shared_ptr<block::ConfigInfo> config, const std::string& method_id, 
                                        std::vector<vm::StackEntry> input);

template<size_t N>
td::Result<std::vector<vm::StackEntry>> execute_smc_method(const block::StdAddress& address, td::Ref<vm::Cell> code, td::Ref<vm::Cell> data, 
                                        std::shared_ptr<block::ConfigInfo> config, const std::string& method_id, 
                                        std::vector<vm::StackEntry> input, const std::array<vm::StackEntry::Type, N>& expected_types) {
  TRY_RESULT(stack, execute_smc_method(address, code, data, config, method_id, std::move(input)));
  
  if (stack.size() != expected_types.size()) {
    return td::Status::Error(method_id + " unexpected result stack depth");
  }
  for (size_t i = 0; i < expected_types.size(); i++) {
    if (stack[i].type() != expected_types[i]) {
      return td::Status::Error(method_id + " unexpected stack entry type");
    }
  }

  return stack;
}
