#include "Multisig.h"
#include "convert-utils.h"
#include "execute-smc.h"


MultisigContract::MultisigContract(block::StdAddress address,
                       td::Ref<vm::Cell> code_cell,
                       td::Ref<vm::Cell> data_cell,
                       AllShardStates shard_states,
                       std::shared_ptr<block::ConfigInfo> config,
                       td::Promise<Result> promise) :
  address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)),
  shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)) {}

void MultisigContract::start_up() {
  if (code_cell_.is_null() || data_cell_.is_null()) {
    promise_.set_error(td::Status::Error("Code or data null"));
    stop();
    return;
  }

  auto stack_r =   execute_smc_method(address_, code_cell_, data_cell_, config_,
    "get_multisig_data", {});

  if (stack_r.is_error()) {
    promise_.set_error(stack_r.move_as_error());
    stop();
    return;
  }

  auto stack = stack_r.move_as_ok();
  if (!stack[0].is_int()
    || !stack[1].is_int()
    || !(stack[2].is_cell() || stack[2].is_null())
    || !(stack[3].is_cell() || stack[3].is_null()))
  {
    promise_.set_error(td::Status::Error("Invalid get method call result types"));
    stop();
    return;
  }

  Result data;
  data.address = address_;
  data.next_order_seqno = stack[0].as_int()->to_long();
  data.threshold = stack[1].as_int()->to_long();

  // signers
  if (stack[2].is_cell())
  {
    auto signers_cell = stack[2].as_cell();
    vm::Dictionary signers_dict {signers_cell, 8};
    auto it = signers_dict.begin();
    while (!it.eof()) {
      auto val = it.cur_value();
      block::StdAddress address;
      block::tlb::MsgAddressInt address_int{};
      auto ok = address_int.extract_std_address(val, address);
      if (!ok)
      {
        LOG(INFO) << "FAILED";
        promise_.set_error(td::Status::Error("Unable to extract address"));
        stop();
        return;
      }
      data.signers.push_back(address);
      ++it;
    }
  }

  // proposers
  if (stack[3].is_cell())
  {
    auto proposers_cell = stack[3].as_cell();
    vm::Dictionary proposers_dict {proposers_cell, 8};
    auto it = proposers_dict.begin();
    while (!it.eof()) {
      auto val = it.cur_value();
      block::StdAddress address;
      block::tlb::MsgAddressInt address_int{};
      auto ok = address_int.extract_std_address(val, address);
      if (!ok)
      {
        LOG(INFO) << "FAILED";
        promise_.set_error(td::Status::Error("Unable to extract address"));
        stop();
        return;
      }
      data.proposers.push_back(address);
      ++it;
    }
  }
  promise_.set_value(std::move(data));
  stop();
}

MultisigOrder::MultisigOrder(block::StdAddress address,
                       td::Ref<vm::Cell> code_cell,
                       td::Ref<vm::Cell> data_cell,
                       AllShardStates shard_states,
                       std::shared_ptr<block::ConfigInfo> config,
                       td::Promise<Result> promise) :
  address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)),
  shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)) {}

void MultisigOrder::start_up() {
  if (code_cell_.is_null() || data_cell_.is_null()) {
    promise_.set_error(td::Status::Error("Code or data null"));
    stop();
    return;
  }

  if (code_cell_.is_null() || data_cell_.is_null()) {
    promise_.set_error(td::Status::Error("Code or data null"));
    stop();
    return;
  }

  auto stack_r = execute_smc_method<9>(address_, code_cell_, data_cell_, config_, "get_order_data", {},
            {vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int,
            vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_cell, vm::StackEntry::Type::t_int,
            vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_cell});

  if (stack_r.is_error()) {
    promise_.set_error(stack_r.move_as_error());
    stop();
    return;
  }
  auto stack = stack_r.move_as_ok();

  Result data;
  data.address = address_;
  auto multisig_addr = convert::to_std_address(stack[0].as_slice());
  if (multisig_addr.is_error()) {
    promise_.set_error(multisig_addr.move_as_error_prefix("multisig address parsing failed: "));
    stop();
    return;
  }
  data.order_seqno = stack[1].as_int()->to_long();
  data.threshold = stack[2].as_int()->to_long();
  data.sent_for_execution = stack[3].as_int()->to_long();
  data.approvals_mask = stack[5].as_int()->to_long();
  data.approvals_num = stack[6].as_int()->to_long();
  data.expiration_date = stack[7].as_int()->to_long();
  data.order = stack[8].as_cell();

  // signers
  auto signers_cell = stack[4].as_cell();
  vm::Dictionary signers_dict {signers_cell, 8};
  auto it = signers_dict.begin();
  while (!it.eof()) {
    auto val = it.cur_value();
    block::StdAddress address;
    block::tlb::MsgAddressInt address_int{};
    auto ok = address_int.extract_std_address(val, address);
    if (!ok)
    {
      promise_.set_error(td::Status::Error("Unable to extract address"));
      stop();
      return;
    }
    data.signers.push_back(address);
    ++it;
  }

  promise_.set_value(std::move(data));
  stop();

}