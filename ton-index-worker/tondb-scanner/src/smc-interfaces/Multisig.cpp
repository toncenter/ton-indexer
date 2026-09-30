#include "Multisig.h"
#include "convert-utils.h"
#include "execute-smc.h"
#include "FetchAccountFromShard.h"

namespace {

td::Result<std::vector<block::StdAddress>> parse_address_dict(td::Ref<vm::Cell> cell) {
  std::vector<block::StdAddress> result;
  try {
    vm::Dictionary dict{std::move(cell), 8};
    for (auto it = dict.begin(); !it.eof(); ++it) {
      block::StdAddress address;
      block::tlb::MsgAddressInt address_int{};
      if (!address_int.extract_std_address(it.cur_value(), address)) {
        return td::Status::Error("Unable to extract address");
      }
      result.push_back(address);
    }
  } catch (vm::VmError& e) {
    return td::Status::Error(PSLICE() << "Failed to parse address dict: " << e.get_msg());
  }
  return result;
}

}  // namespace

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
  if (stack.size() < 4
    || !stack[0].is_int()
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
  data.next_order_seqno = stack[0].as_int();
  data.threshold = stack[1].as_int()->to_long();

  if (stack[2].is_cell()) {
    auto signers = parse_address_dict(stack[2].as_cell());
    if (signers.is_error()) {
      promise_.set_error(signers.move_as_error());
      stop();
      return;
    }
    data.signers = signers.move_as_ok();
  }

  if (stack[3].is_cell()) {
    auto proposers = parse_address_dict(stack[3].as_cell());
    if (proposers.is_error()) {
      promise_.set_error(proposers.move_as_error());
      stop();
      return;
    }
    data.proposers = proposers.move_as_ok();
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

td::Result<MultisigOrder::Result> MultisigOrder::detect(
    const block::StdAddress& address, const td::Ref<vm::Cell>& code_cell,
    const td::Ref<vm::Cell>& data_cell, const AllShardStates& shard_states,
    const std::shared_ptr<block::ConfigInfo>& config) {
  if (code_cell.is_null() || data_cell.is_null()) {
    return td::Status::Error("Code or data null");
  }

  TRY_RESULT(stack, execute_smc_method<9>(address, code_cell, data_cell, config, "get_order_data", {},
            {vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int,
            vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_cell, vm::StackEntry::Type::t_int,
            vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_cell}));

  Result data;
  data.address = address;
  auto multisig_addr = convert::to_std_address(stack[0].as_slice());
  if (multisig_addr.is_error()) {
    return multisig_addr.move_as_error_prefix("multisig address parsing failed: ");
  }
  data.multisig_address = multisig_addr.move_as_ok();
  data.order_seqno = stack[1].as_int();
  data.threshold = stack[2].as_int()->to_long();
  data.sent_for_execution = stack[3].as_int()->to_long();
  data.approvals_mask = stack[5].as_int();
  data.approvals_num = stack[6].as_int()->to_long();
  data.expiration_date = stack[7].as_int();
  data.order = stack[8].as_cell();

  TRY_RESULT_ASSIGN(data.signers, parse_address_dict(stack[4].as_cell()));

  TRY_RESULT(multisig, lookup_account(shard_states, data.multisig_address));
  TRY_STATUS(verify_multisig_order(address, data.multisig_address, multisig.code, multisig.data,
                                   data.order_seqno, config));
  return data;
}

void MultisigOrder::start_up() {
  promise_.set_result(detect(address_, code_cell_, data_cell_, shard_states_, config_));
  stop();
}

td::Status MultisigOrder::verify_multisig_order(const block::StdAddress& order_address,
  block::StdAddress multisig_address, td::Ref<vm::Cell> multisig_code,
  td::Ref<vm::Cell> multisig_data, td::RefInt256 order_seqno,
  const std::shared_ptr<block::ConfigInfo>& config)
{
  auto stack_r = execute_smc_method<1>(multisig_address, multisig_code,
    multisig_data, config, "get_order_address", {vm::StackEntry(order_seqno)},
    {vm::StackEntry::t_slice});
  if (stack_r.is_error()) {
    return stack_r.move_as_error();
  }

  auto stack = stack_r.move_as_ok();
  auto order_addr_r = convert::to_std_address(stack[0].as_slice());
  if (order_addr_r.is_error()) {
    return order_addr_r.move_as_error();
  }

  auto order_addr = order_addr_r.move_as_ok();
  if (order_addr != order_address)
  {
    return td::Status::Error("Order address mismatch");
  }
  return td::Status::OK();
}
