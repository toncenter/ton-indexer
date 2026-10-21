#pragma once
// The calls of ton_v1 apart from the actors that load their data: synchronous
// code on a TON worker, given the cells it needs.
#include <optional>
#include <string>
#include <vector>
#include "crypto/block/mc-config.h"
#include "crypto/vm/stack.hpp"
#include "ExecutorServer.h"

namespace tvm_executor {

// The constants of ton_v1 that decide answers, as in fireton/protocol/limits.go.
inline constexpr std::size_t kMaxInputBocSize = 1 << 20;
inline constexpr std::size_t kMaxCallResultSize = 8 << 20;
inline constexpr std::size_t kMaxVmLogSize = 64 << 10;
inline constexpr int kMaxStackDepth = 32;
inline constexpr std::size_t kMaxTupleSize = 255;
inline constexpr td::uint64 kDefaultGetMethodGas = 1'000'000;
inline constexpr td::uint64 kMaxGetMethodGas = 10'000'000;

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

// The VM context of block N for an account, before the overrides of a call:
// that of a next block after N (substreams-tvm.md, «Семантика»).
struct VmContext {
  // The masterchain state of N, with libraries, capabilities and prev blocks.
  const block::ConfigInfo* config;
  // gen_utime and gen_lt of the state of the shard of the account at N.
  td::uint32 now;
  td::uint64 lt;
  // Of that shard block, which the rand seed of the account is made of.
  td::Bits256 block_rand_seed;
};

// The Account of the ShardAccount BOC of a request; an error is the message
// of INVALID_REQUEST.
td::Result<td::Ref<vm::Cell>> request_account(const std::string& shard_account_boc);

// The standard address of an existing account, which its VM context is
// looked up by; none for account_none, or an address of another kind.
std::optional<block::StdAddress> account_address(const td::Ref<vm::Cell>& account);

// A get-method on an account, a cell of TL-B Account. An account that does not
// parse is INVALID_REQUEST when it came with the call and an Internal error
// otherwise. context is null only for an account without an address.
td::Result<ton::tvm::v1::CallResult> run_get_method(const ton::tvm::v1::GetMethod& call,
                                                    const td::Ref<vm::Cell>& account, bool from_request,
                                                    const VmContext* context);

}  // namespace tvm_executor
