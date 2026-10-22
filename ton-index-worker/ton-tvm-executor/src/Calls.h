#pragma once
// The calls of ton_v1 apart from the actors that load their data: synchronous
// code on a TON worker, given the cells it needs.
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include "crypto/block/mc-config.h"
#include "crypto/block/transaction.h"
#include "crypto/vm/dict.h"
#include "crypto/vm/stack.hpp"
#include "ExecutorServer.h"

namespace tvm_executor {

// The constants of ton_v1 that decide answers, as in fireton/protocol/limits.go.
inline constexpr std::size_t kMaxInputBocSize = 1 << 20;
inline constexpr std::size_t kMaxCallResultSize = 8 << 20;
inline constexpr int kMaxStackDepth = 32;
inline constexpr std::size_t kMaxTupleSize = 255;
inline constexpr td::uint64 kDefaultGetMethodGas = 1'000'000;
inline constexpr td::uint64 kMaxGetMethodGas = 10'000'000;

// What a transaction needs of a config besides capabilities: workchains and
// special accounts.
inline constexpr int kTransactionConfig =
    block::Config::needCapabilities | block::Config::needWorkchainInfo | block::Config::needSpecialSmc;

// How a call fails its request tells fireton whether to wait. A cell db that
// could not be read may be read on another attempt, or by another backend:
// Unavailable, as are VmError exceptions, which cell loads throw. Cells that
// were read and cannot be parsed would fail the same way again: Internal.
td::Status unreadable(td::Status error);
td::Status malformed(td::Slice message);

ton::tvm::v1::CallResult call_error(ton::tvm::v1::ErrorCode code, std::string message);

// The root of a BOC of a request, which must have exactly one; an error is
// the message of INVALID_REQUEST.
td::Result<td::Ref<vm::Cell>> request_boc(const std::string& boc, td::Slice what);

// The arguments of a get-method, bottom to top; an error is the message of
// INVALID_REQUEST.
td::Result<std::vector<vm::StackEntry>> stack_from_proto(
    const google::protobuf::RepeatedPtrField<ton::tvm::v1::StackEntry>& entries);

// The values of a result stack. budget is the bytes they may still take, and
// false is returned once they would take more. An error is a cell that could
// not be read.
td::Result<bool> stack_to_proto(const vm::Stack& stack,
                                google::protobuf::RepeatedPtrField<ton::tvm::v1::StackEntry>* out,
                                std::size_t& budget);

// Whether the BOC of a tree can be within limit bytes: false once a lower
// bound of its size is over it, before every cell of a large tree is read.
td::Result<bool> boc_within(const td::Ref<vm::Cell>& root, std::size_t limit);

// Reads every cell of a tree, so that parsing it later meets no database
// error; an error is unreadable.
td::Status load_tree(const td::Ref<vm::Cell>& root);

// The chain context of block N, before the overrides of a call. Public calls
// use the config and libraries of N, now = gen_utime and lt = gen_lt of the
// shard state at N, whatever the point of their account. Historical replay
// uses the context of the original block instead, never public overrides.
struct VmContext {
  // Of the masterchain view of the request actor, with shard hashes,
  // libraries, prev blocks and what transactions need of it.
  std::shared_ptr<block::ConfigInfo> config;
  td::uint32 now;
  // GetMethod uses this for both BLOCKLT and LTIME. TransactionEmulator
  // rounds it down for BLOCKLT and uses it as a lower bound for LTIME.
  td::uint64 lt;
  // Input to TON's transaction seed derivation, not the seed in c7.
  td::Bits256 block_rand_seed;
};

// prev_blocks_info of c7, null before global version 4, when TON has none.
td::Result<td::Ref<vm::Tuple>> prev_blocks_info(const block::ConfigInfo& config);

// A full TL-B ShardAccount. AccountStorage.last_trans_lt inside account is
// the end of the last transaction's LT interval; last_trans_lt here is its
// beginning. Transaction emulation needs both, and the previous hash.
struct AccountState {
  td::Ref<vm::Cell> account;
  td::Bits256 last_trans_hash = td::Bits256::zero();
  td::uint64 last_trans_lt{0};
};

// An error is the message of INVALID_REQUEST. Validates the Account before
// any address lookup, keeping malformed request cells apart from DB errors.
td::Result<AccountState> request_account(const std::string& shard_account_boc);

td::Ref<vm::Cell> shard_account_cell(const AccountState& state);

// The BOC of the ShardAccount, none over the input BOC limit: a returned
// state must be usable as input to another call.
td::Result<std::optional<std::string>> shard_account_boc(const AccountState& state);

// The standard address of an existing account, which its VM context is
// looked up by; none for account_none, or an address of another kind.
// Anycast addresses are not supported: this is the rewritten address, and
// TON derives the seed of an anycast account from its original one.
std::optional<block::StdAddress> account_address(const td::Ref<vm::Cell>& account);

// An account of TON's transaction code, at now. An error is a message, which
// the caller tells apart: a request account or a network one.
td::Result<block::Account> transaction_account(const AccountState& state, const block::StdAddress& address,
                                               const block::Config& config, td::uint32 now);

// The state as TON stores it after a transaction: an account that does not
// exist has no last transaction in ShardAccounts. Transaction::commit gives
// acc_nonexist for a deleted account too.
AccountState account_state(const block::Account& account);

// Whether TON prepares a transaction with this config, as the emulator does
// first (fetch_config_params), with the resolved nonzero block seed.
// An error is a message: one of the config.
td::Status check_transaction_config(const block::Config& config, const td::Ref<vm::Tuple>& prev_blocks_info,
                                    ton::WorkchainId workchain, td::uint32 now, td::Bits256 block_rand_seed);

// Request cells are parsed before accessing account/config cells. An error
// from either helper is the message of INVALID_REQUEST; database cell-load
// exceptions propagate to the request actor as infrastructure errors.
struct CallOverrides {
  std::optional<td::uint32> now;
  std::optional<td::uint64> lt;
  std::optional<td::Bits256> block_rand_seed;
  std::vector<std::pair<int, td::Ref<vm::Cell>>> config_params;
  td::Ref<vm::Cell> libraries;
};

struct CallContext {
  VmContext chain;
  // That of N, or a copy with the params of the call.
  std::shared_ptr<block::Config> config;
  bool config_overridden{false};
  td::Ref<vm::Cell> extra_libraries;

  // In the order of lookup: public, the account's own, then the call's.
  std::vector<td::Ref<vm::Cell>> libraries(const td::Ref<vm::Cell>& account_library) const;
  // Public and the call's in one dictionary, as TransactionEmulator takes them.
  td::Result<vm::Dictionary> merged_libraries() const;
  // TON's version-dependent seed derivation for a standard account address.
  // Transaction emulation and historical replay derive it inside TON itself.
  td::Bits256 vm_seed(const block::StdAddress& address, int global_version) const;
};

td::Result<CallOverrides> parse_overrides(const ton::tvm::v1::ContextOverrides& overrides, bool transaction = false);
// config_mode is what the copy with the params of the call is unpacked with:
// a get-method needs capabilities only, a transaction kTransactionConfig.
td::Result<CallContext> resolve_context(const VmContext& context, CallOverrides overrides,
                                        int config_mode = block::Config::needCapabilities);

// A get-method on an account, a cell of TL-B Account. An account that does not
// parse is INVALID_REQUEST when it came with the call and an Internal error
// otherwise. context is null only for an account without an address.
td::Result<ton::tvm::v1::CallResult> run_get_method(const ton::tvm::v1::GetMethod& call,
                                                    const td::Ref<vm::Cell>& account, bool from_request,
                                                    const VmContext* context);

struct EmulationMessage {
  td::Ref<vm::Cell> root;
  block::StdAddress destination;
  td::uint64 created_lt{0};
};

// An internal or external-in message of a request; an error is the message
// of INVALID_REQUEST.
td::Result<EmulationMessage> request_message(const std::string& boc);

// One ordinary transaction of the message on the state, in the context of N
// with the overrides of the call.
td::Result<ton::tvm::v1::CallResult> run_emulate(const ton::tvm::v1::Emulate& call, const EmulationMessage& message,
                                                 const AccountState& state, bool from_request,
                                                 const VmContext& context);

}  // namespace tvm_executor
