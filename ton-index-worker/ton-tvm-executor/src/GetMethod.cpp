// get_method: a get-method of an account in the VM context of block N, run as
// liteserver runs it (LiteQuery::finish_runSmcMethod), but deterministically.
#include <algorithm>
#include <variant>
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"
#include "crypto/block/transaction.h"
#include "crypto/vm/boc.h"
#include "crypto/vm/cells/CellBuilder.h"
#include "crypto/vm/dict.h"
#include "crypto/vm/vm.h"
#include "td/utils/crypto.h"
#include "Calls.h"

namespace tvm_executor {
namespace {

namespace pbtvm = ton::tvm::v1;
using Cell = td::Ref<vm::Cell>;

// The end of the VM log, printable ASCII only: a string of protobuf must be
// UTF-8, and the log is cut at any byte.
class TailLog : public td::LogInterface {
 public:
  void append(td::CSlice slice) override {
    text_.append(slice.data(), slice.size());
    if (text_.size() > 2 * kMaxVmLogSize) {
      text_.erase(0, text_.size() - kMaxVmLogSize);
    }
  }

  std::string tail() const {
    std::string out = text_;
    if (out.size() > kMaxVmLogSize) {
      // From the first whole line within the limit.
      out.erase(0, out.size() - kMaxVmLogSize);
      auto line = out.find('\n');
      out.erase(0, line == std::string::npos ? out.size() : line + 1);
    }
    for (auto& c : out) {
      if ((c < 0x20 || c > 0x7e) && c != '\n' && c != '\t') {
        c = '?';
      }
    }
    return out;
  }

 private:
  std::string text_;
};

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
td::Ref<vm::Tuple> smart_contract_info(td::uint32 now, td::uint64 lt, const td::Bits256& rand_seed, const Active& account,
                                       const block::Config& config, const td::Ref<vm::Tuple>& prev_blocks_info) {
  td::RefInt256 seed{true};
  seed.unique_write().import_bits(rand_seed.cbits(), 256, false);
  std::vector<vm::StackEntry> tuple = {
      td::make_refint(0x076ef1ea),         // [ magic:0x076ef1ea
      td::make_refint(0),                  //   actions:Integer
      td::make_refint(0),                  //   msgs_sent:Integer
      td::make_refint(now),                //   unixtime:Integer
      td::make_refint(lt),                 //   block_lt:Integer
      td::make_refint(lt),                 //   trans_lt:Integer
      std::move(seed),                     //   rand_seed:Integer
      account.balance.as_vm_tuple(),       //   balance_remaining:[Integer (Maybe Cell)]
      account.addr,                        //   myself:MsgAddressInt
      vm::StackEntry::maybe(config.get_root_cell())  //   global_config:(Maybe Cell) ] = SmartContractInfo;
  };
  if (config.get_global_version() >= 4) {
    tuple.push_back(vm::StackEntry::maybe(account.code));               // code:Cell
    tuple.push_back(block::CurrencyCollection::zero().as_vm_tuple());  // in_msg_value:[Integer (Maybe Cell)]
    tuple.push_back(td::zero_refint());                                // storage_fees:Integer
    tuple.push_back(prev_blocks_info.not_null() ? vm::StackEntry(prev_blocks_info) : vm::StackEntry());
  }
  if (config.get_global_version() >= 6) {
    tuple.push_back(vm::StackEntry::maybe(config.get_unpacked_config_tuple(now)));  // unpacked_config_tuple:[...]
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

// Validate the params interpreted while preparing the VM, before mixing
// request cells with onchain cells. Database read failures must stay retryable.
td::Status validate_config_override(int param, const Cell& cell) {
  switch (param) {
    case 8:
    case 18:
    case 19:
    case 20:
    case 21:
    case 24:
    case 25:
    case 43:
    case 45:
      break;
    default:
      // Other params remain opaque to the executor, including custom ones.
      return td::Status::OK();
  }
  try {
    // The same TL-B validation and cell budget as TON's check_one_config_param.
    // The budget also bounds traversal of dictionaries with shared subtrees.
    if (block::gen::ConfigParam{static_cast<unsigned>(param)}.validate_ref(cell)) {
      return td::Status::OK();
    }
  } catch (vm::VmVirtError&) {
    return td::Status::Error(PSLICE() << "config param " << param << " is pruned");
  } catch (vm::VmError&) {
    return td::Status::Error(PSLICE() << "config param " << param << " cannot be parsed");
  }
  return td::Status::Error(PSLICE() << "config param " << param << " is invalid");
}

}  // namespace

td::Status unreadable(td::Status error) {
  return td::Status::Error(static_cast<int>(Code::Unavailable), error.to_string());
}

td::Status malformed(td::Slice message) {
  return td::Status::Error(static_cast<int>(Code::Internal), message);
}

pbtvm::CallResult call_error(pbtvm::ErrorCode code, std::string message) {
  pbtvm::CallResult result;
  result.mutable_error()->set_code(code);
  result.mutable_error()->set_message(std::move(message));
  return result;
}

td::Result<Cell> request_boc(const std::string& boc, td::Slice what) {
  if (boc.size() > kMaxInputBocSize) {
    return td::Status::Error(PSLICE() << what << " of " << boc.size() << " bytes is over " << kMaxInputBocSize);
  }
  auto roots = vm::std_boc_deserialize_multi(td::Slice(boc));
  if (roots.is_error() || roots.ok().size() != 1) {
    return td::Status::Error(PSLICE() << what << " is not a BOC with one root");
  }
  return std::move(roots.move_as_ok()[0]);
}

td::Result<Cell> request_account(const std::string& shard_account_boc) {
  TRY_RESULT(root, request_boc(shard_account_boc, "shard_account_boc"));
  block::gen::ShardAccount::Record shard_account;
  if (!tlb::unpack_cell(root, shard_account)) {
    return td::Status::Error("shard_account_boc is not a ShardAccount");
  }
  // Unpacking ShardAccount only extracts its reference. Validate Account
  // before address lookup: malformed request cells must not become retryable
  // database errors in RequestActor. Code and data remain opaque cells.
  try {
    if (!block::gen::t_Account.validate_ref(shard_account.account)) {
      return td::Status::Error("shard_account_boc: invalid Account");
    }
  } catch (vm::VmVirtError&) {
    return td::Status::Error("shard_account_boc: pruned Account");
  } catch (vm::VmError& e) {
    return td::Status::Error(PSLICE() << "shard_account_boc: invalid Account: " << e.get_msg());
  }
  return shard_account.account;
}

std::optional<block::StdAddress> account_address(const Cell& account) {
  auto cs = vm::load_cell_slice_ref(account);
  block::gen::Account::Record_account record;
  block::StdAddress address;
  if (block::gen::t_Account.get_tag(*cs) != block::gen::Account::account || !tlb::csr_unpack(cs, record) ||
      !block::tlb::t_MsgAddressInt.extract_std_address(record.addr, address)) {
    return std::nullopt;
  }
  return address;
}

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
  const auto& overrides = call.context();
  if ((overrides.has_now() && overrides.now() == 0) || (overrides.has_lt() && overrides.lt() == 0)) {
    return invalid("now and lt are not 0");
  }
  if (overrides.has_rand_seed() &&
      (overrides.rand_seed().size() != 32 || overrides.rand_seed() == std::string(32, '\0'))) {
    return invalid("rand_seed is 32 bytes, not all zero");
  }
  // Each param replaced by number, the lowest invalid one reported.
  std::vector<int> params;
  for (const auto& [param, boc] : overrides.config_params()) {
    params.push_back(param);
  }
  std::sort(params.begin(), params.end());
  std::vector<std::pair<int, Cell>> param_cells;
  for (int param : params) {
    auto cell = request_boc(overrides.config_params().at(param), PSLICE() << "config param " << param);
    if (cell.is_error()) {
      return invalid(cell.move_as_error().message().str());
    }
    auto status = validate_config_override(param, cell.ok());
    if (status.is_error()) {
      return invalid(status.message().str());
    }
    param_cells.emplace_back(param, cell.move_as_ok());
  }
  // Extra libraries, by the hash of their root cells.
  vm::Dictionary extra_libraries{256};
  for (int i = 0; i < overrides.libraries_size(); i++) {
    auto library = request_boc(overrides.libraries(i), PSLICE() << "library " << i);
    if (library.is_error()) {
      return invalid(library.move_as_error().message().str());
    }
    vm::CellBuilder cb;
    cb.store_ref(library.ok());
    extra_libraries.set_builder(library.ok()->get_hash().bits(), 256, cb);
  }

  TRY_RESULT(parsed, active_account(account, from_request));
  if (auto* error = std::get_if<pbtvm::CallResult>(&parsed)) {
    return std::move(*error);
  }
  auto& active = std::get<Active>(parsed);
  if (context == nullptr) {
    return malformed("no VM context for an account with an address");
  }

  const block::Config* config = context->config;
  std::unique_ptr<block::Config> overridden;
  if (!param_cells.empty()) {
    vm::Dictionary dict{context->config->get_root_cell(), 32};
    for (const auto& [param, cell] : param_cells) {
      dict.set_ref(td::BitArray<32>(param), cell);
    }
    overridden = std::make_unique<block::Config>(dict.get_root_cell(), td::Bits256::zero(), block::Config::needCapabilities);
    auto status = overridden->unpack_wrapped();
    if (status.is_error()) {
      return invalid(PSTRING() << "config with the params given: " << status.message());
    }
    config = overridden.get();
  }
  td::uint32 now = overrides.has_now() ? overrides.now() : context->now;
  td::uint64 lt = overrides.has_lt() ? overrides.lt() : context->lt;
  td::Bits256 rand_seed;
  if (overrides.has_rand_seed()) {
    rand_seed.as_slice().copy_from(overrides.rand_seed());
  } else {
    // As the collator: SHA256(block rand seed . account id).
    td::BitArray<512> data;
    data.bits().copy_from(context->block_rand_seed.cbits(), 256);
    (data.bits() + 256).copy_from(active.address.addr.cbits(), 256);
    td::sha256(data.as_slice(), rand_seed.as_slice());
  }
  auto prev_blocks_info = context->config->get_prev_blocks_info();
  auto info = smart_contract_info(now, lt, rand_seed, active, *config,
                                  prev_blocks_info.is_ok() ? prev_blocks_info.move_as_ok() : td::Ref<vm::Tuple>());

  // The public libraries of N, the account's own, then those of the call.
  std::vector<Cell> libraries;
  if (context->config->get_libraries_root().not_null()) {
    libraries.push_back(context->config->get_libraries_root());
  }
  if (active.library.not_null()) {
    libraries.push_back(active.library);
  }
  if (!extra_libraries.is_empty()) {
    libraries.push_back(extra_libraries.get_root_cell());
  }

  auto args = stack.move_as_ok();
  args.push_back(vm::StackEntry{td::make_refint(method_id)});
  TailLog log_text;
  vm::VmLog log = vm::VmLog::Null();
  if (call.vm_log()) {
    log = vm::VmLog{&log_text, td::LogOptions(VERBOSITY_NAME(DEBUG), true, false)};
    log.log_mask |= vm::VmLog::ExecLocation | vm::VmLog::GasRemaining;
  }
  auto gas = static_cast<long long>(gas_limit);
  vm::VmState vm{active.code,
                 config->get_global_version(),
                 td::Ref<vm::Stack>{true, std::move(args)},
                 vm::GasLimits{gas, gas},
                 1,
                 active.data,
                 log,
                 std::move(libraries)};
  vm.set_c7(vm::make_tuple_ref(std::move(info)));
  int exit_code = ~vm.run();

  pbtvm::CallResult result;
  auto* out = result.mutable_get_method();
  out->set_exit_code(exit_code);
  out->set_gas_used(static_cast<td::uint64>(vm.get_gas_limits().gas_consumed()));
  if (auto missing = vm.get_missing_library()) {
    out->set_missing_library(missing.value().as_slice().str());
  }
  if (call.vm_log()) {
    out->set_vm_log(log_text.tail());
  }
  std::size_t budget = kMaxCallResultSize;
  TRY_RESULT(within, stack_to_proto(vm.get_stack(), out->mutable_stack(), budget));
  if (!within || result.ByteSizeLong() > kMaxCallResultSize) {
    return call_error(pbtvm::ERROR_CODE_TOO_LARGE, PSTRING() << "the result is over " << kMaxCallResultSize << " bytes");
  }
  return result;
}

}  // namespace tvm_executor
