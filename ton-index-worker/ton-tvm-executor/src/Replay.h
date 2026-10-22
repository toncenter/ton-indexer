#pragma once
#include "Calls.h"
#include "emulator/transaction-emulator.h"

namespace tvm_executor {

// What a shard block gives VM contexts and replays, from the validated block
// itself, never from public call overrides.
struct ShardBlockData {
  ton::BlockIdExt id;
  std::vector<ton::BlockIdExt> prev;
  // master_ref, or the previous block for a masterchain one.
  ton::BlockIdExt master;
  td::uint32 now;
  td::uint64 start_lt, end_lt;
  td::Bits256 rand_seed;
  td::Ref<vm::Cell> account_blocks;
};

td::Result<ShardBlockData> shard_block_data(const td::Ref<vm::Cell>& root, const ton::BlockIdExt& id);

// The predecessor on the shard chain of the address, across splits and merges.
td::Result<ton::BlockIdExt> account_predecessor(const ShardBlockData& block, const block::StdAddress& address);

struct ReplayTransaction {
  td::Ref<vm::Cell> root;
  td::uint64 lt;
  // The hashes of the account before and after it.
  td::Bits256 before, after;
};

// The transactions of the account in the block, by lt; none when it has none.
td::Result<std::vector<ReplayTransaction>> account_transactions(const ShardBlockData& block,
                                                               const td::Bits256& address);

// Native execution of the transactions of an account in one shard block, in
// the context of that block. The actor advances it by one transaction per
// turn, so cancellation and other requests run between transactions.
class Replay {
 public:
  // config is that of the masterchain state the block refers to.
  static td::Result<std::unique_ptr<Replay>> create(const AccountState& state, const block::StdAddress& address,
                                                   const ShardBlockData& block,
                                                   std::shared_ptr<block::ConfigInfo> config);
  // Empty on success; otherwise a deterministic REPLAY_MISMATCH explanation.
  // Cell-load exceptions are left to the actor as infrastructure failures.
  td::Result<std::string> advance(const ReplayTransaction& transaction);
  AccountState state() const;
  // The transaction the emulator made at the last advance, for a diagnosis of
  // a mismatch; null when it made none.
  const td::Ref<vm::Cell>& emulated() const {
    return emulated_;
  }

 private:
  Replay(std::shared_ptr<block::Config> config, block::Account account, td::uint32 now, td::uint64 block_lt)
      : emulator_(std::move(config)), account_(std::move(account)), now_(now), block_lt_(block_lt) {
  }
  emulator::TransactionEmulator emulator_;
  block::Account account_;
  td::uint32 now_;
  td::uint64 block_lt_;
  td::Ref<vm::Cell> emulated_;
};

}  // namespace tvm_executor
