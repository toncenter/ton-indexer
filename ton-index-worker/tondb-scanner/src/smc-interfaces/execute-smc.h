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
  
  // some contracts return additional stack entries, so don't check exact size match
  if (stack.size() < expected_types.size()) {
    return td::Status::Error(method_id + " less than expected result stack depth");
  }
  for (size_t i = 0; i < expected_types.size(); i++) {
    if (stack[i].type() != expected_types[i]) {
      return td::Status::Error(method_id + " unexpected stack entry type");
    }
  }

  return stack;
}

struct ExpectedType {
  vm::StackEntry::Type type;
  bool nullable = false;

  constexpr ExpectedType(vm::StackEntry::Type t) : type(t), nullable(false) {}
  constexpr ExpectedType(vm::StackEntry::Type t, bool n) : type(t), nullable(n) {}
};

constexpr ExpectedType nullable(vm::StackEntry::Type t) {
  return {t, true};
}

template<size_t N>
td::Result<std::vector<vm::StackEntry>> execute_smc_method_nullable(const block::StdAddress& address, td::Ref<vm::Cell> code, td::Ref<vm::Cell> data,
                                        std::shared_ptr<block::ConfigInfo> config, const std::string& method_id,
                                        std::vector<vm::StackEntry> input, const std::array<ExpectedType, N>& expected_types) {
  TRY_RESULT(stack, execute_smc_method(address, code, data, config, method_id, std::move(input)));

  // some contracts return additional stack entries, so don't check exact size match
  if (stack.size() < expected_types.size()) {
    return td::Status::Error(method_id + " less than expected result stack depth");
  }
  for (size_t i = 0; i < expected_types.size(); i++) {
    const auto& expected = expected_types[i];
    if (expected.nullable && stack[i].type() == vm::StackEntry::Type::t_null) {
      continue;
    }
    if (stack[i].type() != expected.type) {
      return td::Status::Error(method_id + " unexpected stack entry type");
    }
  }

  return stack;
}
