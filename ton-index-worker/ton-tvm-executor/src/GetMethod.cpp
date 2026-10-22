// get_method: a get-method of an account in the VM context of block N, run as
// liteserver runs it (LiteQuery::finish_runSmcMethod), but deterministically.
#include <variant>
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"
#include "crypto/block/transaction.h"
#include "crypto/vm/vm.h"
#include "td/utils/crypto.h"
#include "Calls.h"

namespace tvm_executor {
namespace {

namespace pbtvm = ton::tvm::v1;
using Cell = td::Ref<vm::Cell>;

// What a get-method needs of an active account.
struct Active {
  td::Ref<vm::CellSlice> addr;
  block::StdAddress address;
  block::CurrencyCollection balance;
  td::RefInt256 due_payment;
  Cell code;
  Cell data;
  Cell library;
};

// An account that is not active or does not parse is a CallError.
td::Result<std::variant<Active, pbtvm::CallResult>> active_account(const Cell& account, bool from_request) {
  auto bad = [from_request](td::Slice what) -> td::Result<std::variant<Active, pbtvm::CallResult>> {
    if (from_request) {
      return call_error(pbtvm::ERROR_CODE_INVALID_REQUEST, PSTRING() << "shard_account_boc: cannot unpack " << what);
    }
    return malformed(PSLICE() << "cannot unpack " << what << " of an account");
  };
  auto not_active = [](td::Slice state) {
    return call_error(pbtvm::ERROR_CODE_ACCOUNT_NOT_ACTIVE, PSTRING() << "the account is " << state);
  };
  auto cs = vm::load_cell_slice_ref(account);
  if (block::gen::t_Account.get_tag(*cs) == block::gen::Account::account_none) {
    return not_active("none");
  }
  block::gen::Account::Record_account record;
  block::gen::StorageInfo::Record storage_info;
  block::gen::AccountStorage::Record storage;
  Active active;
  if (!tlb::csr_unpack(cs, record) || !tlb::csr_unpack(record.storage_stat, storage_info) ||
      !tlb::csr_unpack(record.storage, storage) || !active.balance.validate_unpack(storage.balance)) {
    return bad("Account");
  }
  if (!block::tlb::t_MsgAddressInt.extract_std_address(record.addr, active.address)) {
    return bad("a standard address");
  }
  active.addr = record.addr;
  using block::gen::AccountState;
  switch (block::gen::t_AccountState.get_tag(*storage.state)) {
    case AccountState::account_uninit:
      return not_active("uninit");
    case AccountState::account_frozen:
      return not_active("frozen");
    case AccountState::account_active:
      break;
    default:
      return bad("AccountState");
  }
  AccountState::Record_account_active state;
  block::gen::StateInit::Record init;
  if (!tlb::csr_unpack(storage.state, state) || !tlb::csr_unpack(state.x, init)) {
    return bad("StateInit");
  }
  active.code = init.code->prefetch_ref();
  active.data = init.data->prefetch_ref();
  active.library = init.library->prefetch_ref();
  if (active.code.is_null()) {
    return not_active("without code");
  }
  auto due = storage_info.due_payment;
  active.due_payment = due.write().fetch_long(1) ? block::tlb::t_Grams.as_integer(due) : td::zero_refint();
  if (active.due_payment.is_null()) {
    return bad("due_payment");
  }
  return active;
}

// SmartContractInfo, the first item of c7, as prepare_vm_c7 of liteserver
// makes it, with the rand seed given.
td::Ref<vm::Tuple> smart_contract_info(const CallContext& context, const Active& account,
                                       const block::Config& config, const td::Ref<vm::Tuple>& prev_blocks_info) {
  td::RefInt256 seed{true};
  seed.unique_write().import_bits(context.vm_seed(account.address, config.get_global_version()).cbits(), 256, false);
  std::vector<vm::StackEntry> tuple = {
      td::make_refint(0x076ef1ea),              // [ magic:0x076ef1ea
      td::make_refint(0),                       //   actions:Integer
      td::make_refint(0),                       //   msgs_sent:Integer
      td::make_refint(context.chain.now),       //   unixtime:Integer
      td::make_refint(context.chain.lt),        //   block_lt:Integer
      td::make_refint(context.chain.lt),        //   trans_lt:Integer
      std::move(seed),                         //   rand_seed:Integer
      account.balance.as_vm_tuple(),           //   balance_remaining:[Integer (Maybe Cell)]
      account.addr,                            //   myself:MsgAddressInt
      vm::StackEntry::maybe(config.get_root_cell())  //   global_config:(Maybe Cell) ] = SmartContractInfo;
  };
  if (config.get_global_version() >= 4) {
    tuple.push_back(vm::StackEntry::maybe(account.code));               // code:Cell
    tuple.push_back(block::CurrencyCollection::zero().as_vm_tuple());  // in_msg_value:[Integer (Maybe Cell)]
    tuple.push_back(td::zero_refint());                                // storage_fees:Integer
    tuple.push_back(prev_blocks_info.not_null() ? vm::StackEntry(prev_blocks_info) : vm::StackEntry());
  }
  if (config.get_global_version() >= 6) {
    // unpacked_config_tuple:[...]
    tuple.push_back(vm::StackEntry::maybe(config.get_unpacked_config_tuple(context.chain.now)));
    tuple.push_back(account.due_payment);                                         // due_payment:Integer
    auto precompiled = config.get_precompiled_contracts_config().get_contract(account.code->get_hash().bits());
    tuple.push_back(precompiled ? td::make_refint(precompiled.value().gas_usage) : vm::StackEntry());
  }
  if (config.get_global_version() >= 11) {
    tuple.push_back(block::transaction::Transaction::prepare_in_msg_params_tuple(nullptr, {}, {}));
  }
  return td::make_cnt_ref<std::vector<vm::StackEntry>>(std::move(tuple));
}

pbtvm::CallResult invalid(std::string message) {
  return call_error(pbtvm::ERROR_CODE_INVALID_REQUEST, std::move(message));
}

}  // namespace

td::Result<pbtvm::CallResult> run_get_method(const pbtvm::GetMethod& call, const Cell& account, bool from_request,
                                             const VmContext* context) {
  // The request first, then the account: the same call gets the same error
  // whatever the account is.
  int method_id = call.has_id() ? call.id() : static_cast<int>((td::crc16(call.name()) & 0xffff) | 0x10000);
  td::uint64 gas_limit = call.has_gas_limit() ? call.gas_limit() : kDefaultGetMethodGas;
  if (gas_limit > kMaxGetMethodGas) {
    return invalid(PSTRING() << "gas_limit " << gas_limit << " is over " << kMaxGetMethodGas);
  }
  auto stack = stack_from_proto(call.stack());
  if (stack.is_error()) {
    return invalid(stack.move_as_error().message().str());
  }
  auto overrides = parse_overrides(call.context());
  if (overrides.is_error()) {
    return invalid(overrides.move_as_error().message().str());
  }

  TRY_RESULT(parsed, active_account(account, from_request));
  if (auto* error = std::get_if<pbtvm::CallResult>(&parsed)) {
    return std::move(*error);
  }
  auto& active = std::get<Active>(parsed);
  if (context == nullptr) {
    return malformed("no VM context for an account with an address");
  }

  auto prepared = resolve_context(*context, overrides.move_as_ok());
  if (prepared.is_error()) {
    return invalid(prepared.move_as_error().message().str());
  }
  auto resolved = prepared.move_as_ok();
  const auto& config = *resolved.config;
  TRY_RESULT(prev_blocks, prev_blocks_info(*context->config));
  auto info = smart_contract_info(resolved, active, config, prev_blocks);

  auto args = stack.move_as_ok();
  args.push_back(vm::StackEntry{td::make_refint(method_id)});
  auto gas = static_cast<long long>(gas_limit);
  vm::VmState vm{active.code,
                 config.get_global_version(),
                 td::Ref<vm::Stack>{true, std::move(args)},
                 vm::GasLimits{gas, gas},
                 1,
                 active.data,
                 vm::VmLog::Null(),
                 resolved.libraries(active.library)};
  vm.set_c7(vm::make_tuple_ref(std::move(info)));
  int exit_code = ~vm.run();

  pbtvm::CallResult result;
  auto* out = result.mutable_get_method();
  out->set_exit_code(exit_code);
  out->set_gas_used(static_cast<td::uint64>(vm.get_gas_limits().gas_consumed()));
  if (auto missing = vm.get_missing_library()) {
    out->set_missing_library(missing.value().as_slice().str());
  }
  std::size_t budget = kMaxCallResultSize;
  TRY_RESULT(within, stack_to_proto(vm.get_stack(), out->mutable_stack(), budget));
  if (!within || result.ByteSizeLong() > kMaxCallResultSize) {
    return call_error(pbtvm::ERROR_CODE_TOO_LARGE, PSTRING() << "the result is over " << kMaxCallResultSize << " bytes");
  }
  return result;
}

}  // namespace tvm_executor
