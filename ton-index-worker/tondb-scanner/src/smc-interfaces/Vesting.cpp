#include "Vesting.h"
#include "convert-utils.h"
#include "execute-smc.h"


VestingContract::VestingContract(block::StdAddress address,
                                 td::Ref<vm::Cell> code_cell,
                                 td::Ref<vm::Cell> data_cell,
                                 AllShardStates shard_states,
                                 std::shared_ptr<block::ConfigInfo> config,
                                 td::Promise<Result> promise) :
  address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)),
  shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise))
{
}

void VestingContract::start_up()
{
  if (code_cell_.is_null() || data_cell_.is_null()) {
    promise_.set_error(td::Status::Error("Code or data null"));
    stop();
    return;
  }
  auto stack_r = execute_smc_method<8>(address_, code_cell_, data_cell_, config_, "get_vesting_data", {},
            {vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int,
            vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_cell});
  if (stack_r.is_error()) {
    promise_.set_error(stack_r.move_as_error());
    stop();
    return;
  }
  auto stack = stack_r.move_as_ok();

  Result data;
  data.address = address_;

  // Parse the stack fields
  data.vesting_start_time = stack[0].as_int()->to_long();
  data.vesting_total_duration = stack[1].as_int()->to_long();
  data.unlock_period = stack[2].as_int()->to_long();
  data.cliff_duration = stack[3].as_int()->to_long();
  data.vesting_total_amount = stack[4].as_int();

  auto vesting_sender_address = convert::to_std_address(stack[5].as_slice());
  if (vesting_sender_address.is_error()) {
    promise_.set_error(vesting_sender_address.move_as_error_prefix("vesting sender address parsing failed: "));
    stop();
    return;
  }
  data.vesting_sender_address = vesting_sender_address.move_as_ok();

  auto owner_address = convert::to_std_address(stack[6].as_slice());
  if (owner_address.is_error()) {
    promise_.set_error(owner_address.move_as_error_prefix("owner address parsing failed: "));
    stop();
    return;
  }
  data.owner_address = owner_address.move_as_ok();

  // Parse whitelist from the cell
  auto whitelist = stack[7].as_cell();
  vm::Dictionary whitelist_dict {whitelist, 264};
  auto it = whitelist_dict.begin();
  while (!it.eof()) {
    // Dictionary key is 264 bits: 8 bits for workchain + 256 bits for hash part
    auto key_data = it.cur_pos();

    ton::WorkchainId workchain_id = (ton::WorkchainId)key_data.get_int(8);

    td::ConstBitPtr addr_ptr = key_data + 8;
    ton::StdSmcAddress addr_bytes{addr_ptr};

    auto addr = block::StdAddress(workchain_id, addr_bytes);

    data.whitelist.push_back(addr);


    ++it;
  }

  promise_.set_value(std::move(data));
  stop();
}
