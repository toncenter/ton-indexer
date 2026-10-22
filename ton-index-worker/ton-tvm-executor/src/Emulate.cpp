// One ordinary transaction. Each call owns its native emulator and account;
// outgoing messages are returned, never executed.
#include <limits>
#include "Calls.h"
#include "FirehoseBlock.h"
#include "emulator/transaction-emulator.h"
#include "crypto/vm/boc.h"

namespace tvm_executor {
namespace {

namespace pb = ton::tvm::v1;

pb::CallResult invalid(std::string message) {
  return call_error(pb::ERROR_CODE_INVALID_REQUEST, std::move(message));
}

pb::CallResult too_large() {
  return call_error(pb::ERROR_CODE_TOO_LARGE, PSTRING() << "the result is over " << kMaxCallResultSize << " bytes");
}

}  // namespace

td::Result<EmulationMessage> request_message(const std::string& boc) {
  TRY_RESULT(root, request_boc(boc, "message_boc"));
  try {
    block::gen::Message::Record message;
    if (!block::gen::t_Message_Any.validate_ref(root) ||
        !tlb::type_unpack_cell(root, block::gen::t_Message_Any, message)) {
      return td::Status::Error("message_boc is not a valid Message");
    }
    td::Ref<vm::CellSlice> dest;
    td::uint64 created_lt = 0;
    switch (block::gen::t_CommonMsgInfo.get_tag(*message.info)) {
      case block::gen::CommonMsgInfo::int_msg_info: {
        block::gen::CommonMsgInfo::Record_int_msg_info info;
        if (!tlb::csr_unpack(message.info, info)) return td::Status::Error("cannot unpack internal message");
        dest = info.dest;
        created_lt = info.created_lt;
        break;
      }
      case block::gen::CommonMsgInfo::ext_in_msg_info: {
        block::gen::CommonMsgInfo::Record_ext_in_msg_info info;
        if (!tlb::csr_unpack(message.info, info)) return td::Status::Error("cannot unpack external message");
        dest = info.dest;
        break;
      }
      default:
        return td::Status::Error("message_boc must be internal or external-in");
    }
    block::StdAddress address;
    if (!block::tlb::t_MsgAddressInt.extract_std_address(dest, address)) {
      return td::Status::Error("message destination must be a standard address");
    }
    return EmulationMessage{root, address, created_lt};
  } catch (vm::VmVirtError&) {
    return td::Status::Error("message_boc contains a pruned message");
  } catch (vm::VmError& e) {
    return td::Status::Error(PSLICE() << "message_boc: " << e.get_msg());
  }
}

td::Result<pb::CallResult> run_emulate(const pb::Emulate& call, const EmulationMessage& message,
                                       const AccountState& state, bool from_request, const VmContext& context) {
  auto overrides = parse_overrides(call.context(), true);
  if (overrides.is_error()) {
    return invalid(overrides.error().message().str());
  }
  auto prepared = resolve_context(context, overrides.move_as_ok(), kTransactionConfig);
  if (prepared.is_error()) {
    return invalid(prepared.error().message().str());
  }
  auto resolved = prepared.move_as_ok();
  const auto& chain = resolved.chain;
  if (!chain.now || !chain.lt || chain.block_rand_seed.is_zero()) {
    return malformed("zero execution time, LT or block_rand_seed in block context");
  }
  auto address = account_address(state.account);
  const auto& dest = message.destination;
  if (address && (address->workchain != dest.workchain || address->addr != dest.addr)) {
    return invalid("account address differs from message destination");
  }
  TRY_RESULT(prev_blocks, prev_blocks_info(*context.config));
  // Checked before the emulator, which gives the same error as any other: a
  // config that cannot prepare a transaction is the call's with its params,
  // the network's without them. Its cells are read already, so it is no
  // database error either way.
  auto config_status =
      check_transaction_config(*resolved.config, prev_blocks, dest.workchain, chain.now, chain.block_rand_seed);
  if (config_status.is_error()) {
    if (resolved.config_overridden) {
      return invalid(PSTRING() << "config with the params given: " << config_status.message());
    }
    return malformed(PSLICE() << "config: " << config_status.message());
  }
  auto account = transaction_account(state, dest, *resolved.config, chain.now);
  if (account.is_error()) {
    if (from_request) {
      return invalid(PSTRING() << "shard_account_boc: " << account.error().message());
    }
    return malformed(account.error().message());
  }
  // Native LT arithmetic adds the transaction and outgoing messages. Leave
  // room for every possible 15-bit out-message index, including a bounce.
  constexpr auto max_start_lt = std::numeric_limits<td::uint64>::max() - (1ULL << 15) - 2;
  if (chain.lt > max_start_lt || account.ok().last_trans_end_lt_ > max_start_lt || message.created_lt > max_start_lt) {
    return invalid("LT leaves no room for the transaction and outgoing messages");
  }
  TRY_RESULT(libraries, resolved.merged_libraries());

  emulator::TransactionEmulator emulator{resolved.config, call.vm_log() ? 3 : 0};
  emulator.set_libs(std::move(libraries));
  emulator.set_ignore_chksig(call.ignore_chksig());
  auto seed = chain.block_rand_seed;
  emulator.set_rand_seed(seed);
  if (prev_blocks.not_null()) {
    emulator.set_prev_blocks_info(prev_blocks);
  }
  // The config is checked: an error is of this message on this account.
  auto result = emulator.emulate_transaction(account.move_as_ok(), message.root, chain.now, chain.lt,
                                             block::transaction::Transaction::tr_ord);
  if (result.is_error()) {
    return call_error(pb::ERROR_CODE_EMULATION_FAILED, result.error().message().str());
  }
  auto native = result.move_as_ok();
  pb::CallResult out;
  if (auto* rejected = dynamic_cast<emulator::TransactionEmulator::EmulationExternalNotAccepted*>(native.get())) {
    auto* value = out.mutable_emulate()->mutable_external_not_accepted();
    value->set_vm_exit_code(rejected->vm_exit_code);
    if (call.vm_log()) {
      value->set_vm_log(vm_log_tail(std::move(rejected->vm_log)));
    }
    return out;
  }
  auto* executed = dynamic_cast<emulator::TransactionEmulator::EmulationSuccess*>(native.get());
  if (executed == nullptr) {
    return malformed("unexpected native emulation result");
  }
  TRY_RESULT(within, boc_within(executed->transaction, kMaxCallResultSize));
  if (!within) {
    return too_large();
  }
  auto serialized = vm::std_boc_serialize(executed->transaction);
  if (serialized.is_error()) {
    return malformed(serialized.error().message());
  }
  if (serialized.ok().size() > kMaxCallResultSize) {
    return too_large();
  }
  auto* value = out.mutable_emulate()->mutable_executed();
  value->set_transaction_boc(serialized.ok().as_slice().str());
  auto transaction = transaction_to_proto(executed->transaction);
  if (transaction.is_error()) {
    return malformed(transaction.error().message());
  }
  *value->mutable_transaction() = transaction.move_as_ok();
  if (call.vm_log()) {
    value->set_vm_log(vm_log_tail(std::move(executed->vm_log)));
  }
  if (call.return_state()) {
    // As get_account gives it: TON stores no metadata of an account that
    // does not exist.
    auto after = account_state(executed->account);
    auto* account_after = value->mutable_account_after();
    auto parsed = account_to_proto(after.account);
    if (parsed.is_error()) {
      return malformed(parsed.error().message());
    }
    *account_after->mutable_account() = parsed.move_as_ok();
    account_after->set_last_trans_hash(after.last_trans_hash.as_slice().str());
    account_after->set_last_trans_lt(after.last_trans_lt);
    TRY_RESULT(boc, shard_account_boc(after));
    if (boc) {
      account_after->set_shard_account_boc(std::move(*boc));
    } else {
      account_after->set_boc_omitted(true);
    }
  }
  if (out.ByteSizeLong() > kMaxCallResultSize) {
    return too_large();
  }
  return out;
}

}  // namespace tvm_executor
