#include "execute-smc.h"


td::Result<std::vector<vm::StackEntry>> execute_smc_method(const block::StdAddress& address, td::Ref<vm::Cell> code, td::Ref<vm::Cell> data, 
                                        std::shared_ptr<block::ConfigInfo> config, const std::string& method_id, 
                                        std::vector<vm::StackEntry> input) {
  if (!config) {
    return td::Status::Error("Get method requires a block configuration");
  }
  ton::SmartContract smc({code, data});
  ton::SmartContract::Args args;
  args.set_libraries(vm::Dictionary(config->get_libraries_root(), 256));
  args.set_config(config);
  // ConfigInfo::utime is the time of the MC state used for this detection.
  // Wall time makes historical replay (and NOW-dependent getters) non-deterministic.
  args.set_now(config->utime);
  args.set_address(std::move(address));
  args.set_stack(std::move(input));

  args.set_method_id(method_id);
  auto res = smc.run_get_method(args);

  if (!res.success) {
    return td::Status::Error(method_id + " failed");
  }
  
  auto stack = res.stack->extract_contents();

  return stack;
}
