#include "smc-envelope/SmartContract.h"


template<size_t N>
td::Result<std::vector<vm::StackEntry>> execute_smc_method(const block::StdAddress& address, td::Ref<vm::Cell> code, td::Ref<vm::Cell> data, 
                                        std::shared_ptr<block::ConfigInfo> config, const std::string& method_id, 
                                        std::vector<vm::StackEntry> input, const std::array<vm::StackEntry::Type, N>& expected_types) {
  ton::SmartContract smc({code, data});
  ton::SmartContract::Args args;
  args.set_libraries(vm::Dictionary(config->get_libraries_root(), 256));
  args.set_config(config);
  args.set_now(td::Time::now());
  args.set_address(std::move(address));
  args.set_stack(std::move(input));

  args.set_method_id(method_id);
  auto res = smc.run_get_method(args);

  if (!res.success) {
    return td::Status::Error(method_id + " failed");
  }
  if (res.stack->depth() != expected_types.size()) {
    return td::Status::Error(method_id + " unexpected result stack depth");
  }
  auto stack = res.stack->extract_contents();
  
  for (size_t i = 0; i < expected_types.size(); i++) {
    if (stack[i].type() != expected_types[i]) {
      return td::Status::Error(method_id + " unexpected stack entry type");
    }
  }

  return stack;
}
