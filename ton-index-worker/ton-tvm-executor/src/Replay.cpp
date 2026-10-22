#include "Replay.h"

#include <algorithm>
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"

namespace tvm_executor {

td::Result<ShardBlockData> shard_block_data(const td::Ref<vm::Cell>& root, const ton::BlockIdExt& id) {
  block::gen::Block::Record data;
  block::gen::BlockInfo::Record info;
  block::gen::BlockExtra::Record extra;
  if (!tlb::unpack_cell(root, data) || !tlb::unpack_cell(data.info, info) || !tlb::unpack_cell(data.extra, extra)) {
    return malformed(PSLICE() << "cannot unpack block " << id.to_str());
  }
  ShardBlockData out{id, {}, {}, info.gen_utime, info.start_lt, info.end_lt, extra.rand_seed, extra.account_blocks};
  bool after_split;
  auto status = block::unpack_block_prev_blk_ext(root, id, out.prev, out.master, after_split);
  if (status.is_error()) {
    return malformed(status.message());
  }
  return out;
}

td::Result<ton::BlockIdExt> account_predecessor(const ShardBlockData& block, const block::StdAddress& address) {
  auto prefix = ton::extract_addr_prefix(address.workchain, address.addr.cbits());
  for (const auto& id : block.prev) {
    if (ton::shard_contains(id.shard_full(), prefix)) {
      return id;
    }
  }
  return malformed("account is outside both predecessor shards");
}

td::Result<std::vector<ReplayTransaction>> account_transactions(const ShardBlockData& block,
                                                               const td::Bits256& address) {
  vm::AugmentedDictionary accounts{vm::load_cell_slice_ref(block.account_blocks), 256,
                                    block::tlb::aug_ShardAccountBlocks};
  auto value = accounts.lookup(address.cbits(), 256);
  if (value.is_null()) {
    return std::vector<ReplayTransaction>{};
  }
  block::gen::AccountBlock::Record account;
  if (!tlb::csr_unpack(value, account) || account.account_addr != address) {
    return malformed("cannot unpack replay AccountBlock");
  }
  vm::AugmentedDictionary transactions{vm::DictNonEmpty(), account.transactions, 64,
                                        block::tlb::aug_AccountTransactions};
  std::vector<ReplayTransaction> out;
  bool ok = transactions.check_for_each_extra([&](td::Ref<vm::CellSlice> value, td::Ref<vm::CellSlice>,
                                                  td::ConstBitPtr key, int bits) {
    auto root = value->prefetch_ref();
    block::gen::Transaction::Record tx;
    block::gen::HASH_UPDATE::Record update;
    if (!tlb::unpack_cell(root, tx) || tx.account_addr != address || bits != 64 || tx.lt != key.get_uint(64) ||
        tx.lt < block.start_lt || tx.lt >= block.end_lt || tx.now != block.now ||
        !tlb::type_unpack_cell(tx.state_update, block::gen::t_HASH_UPDATE_Account, update)) {
      return false;
    }
    out.push_back({std::move(root), tx.lt, update.old_hash, update.new_hash});
    return true;
  });
  if (!ok || out.empty()) {
    return malformed("invalid transaction dictionary of an account block");
  }
  std::sort(out.begin(), out.end(), [](const auto& a, const auto& b) { return a.lt < b.lt; });
  return out;
}

td::Result<std::unique_ptr<Replay>> Replay::create(const AccountState& state, const block::StdAddress& address,
                                                 const ShardBlockData& block,
                                                 std::shared_ptr<block::ConfigInfo> config) {
  if (block.rand_seed.is_zero()) {
    return malformed("zero block_rand_seed in replay block");
  }
  // All of the network: any error is Internal.
  TRY_RESULT(prev_blocks, prev_blocks_info(*config));
  auto config_status = check_transaction_config(*config, prev_blocks, address.workchain, block.now, block.rand_seed);
  if (config_status.is_error()) {
    return malformed(PSLICE() << "config of " << block.master.to_str() << ": " << config_status.message());
  }
  auto account = transaction_account(state, address, *config, block.now);
  if (account.is_error()) {
    return malformed(PSLICE() << "replay: " << account.error().message());
  }
  auto libraries = config->get_libraries_root();
  auto out = std::unique_ptr<Replay>(new Replay(std::move(config), account.move_as_ok(), block.now, block.start_lt));
  auto seed = block.rand_seed;
  out->emulator_.set_rand_seed(seed);
  out->emulator_.set_libs(vm::Dictionary{libraries, 256});
  if (prev_blocks.not_null()) {
    out->emulator_.set_prev_blocks_info(prev_blocks);
  }
  return out;
}

td::Result<std::string> Replay::advance(const ReplayTransaction& transaction) {
  auto mismatch = [&](td::Slice reason) -> std::string {
    return PSTRING() << "replay lt " << transaction.lt << ": " << reason;
  };
  block::gen::Transaction::Record tx;
  if (!tlb::unpack_cell(transaction.root, tx)) {
    return malformed("cannot unpack replay transaction");
  }
  if (tx.lt - tx.lt % block::ConfigInfo::get_lt_align() != block_lt_) {
    return mismatch("native BLOCKLT differs from the original block start_lt");
  }
  if (transaction.before != account_.total_state->get_hash().bits() || tx.prev_trans_lt != account_.last_trans_lt_ ||
      tx.prev_trans_hash != account_.last_trans_hash_) {
    return mismatch("initial state or previous transaction differs");
  }
  int type;
  auto tag = block::gen::t_TransactionDescr.get_tag(vm::load_cell_slice(tx.description));
  if (tag == block::gen::TransactionDescr::trans_ord) {
    type = block::transaction::Transaction::tr_ord;
  } else if (tag == block::gen::TransactionDescr::trans_tick_tock) {
    block::gen::TransactionDescr::Record_trans_tick_tock description;
    if (!tlb::unpack_cell(tx.description, description)) {
      return malformed("cannot unpack tick/tock description");
    }
    type = description.is_tock ? block::transaction::Transaction::tr_tock : block::transaction::Transaction::tr_tick;
  } else {
    return mismatch("transaction type is not supported by the native emulator");
  }
  // The config is checked by create(): an error is of this transaction. The
  // account is moved into the emulator; after a mismatch the replay is over.
  emulated_ = {};
  auto result = emulator_.emulate_transaction(std::move(account_), tx.r1.in_msg->prefetch_ref(), now_, tx.lt, type);
  if (result.is_error()) {
    return mismatch(result.error().message());
  }
  auto* executed = dynamic_cast<emulator::TransactionEmulator::EmulationSuccess*>(result.ok().get());
  if (executed == nullptr) {
    auto* rejected = dynamic_cast<emulator::TransactionEmulator::EmulationExternalNotAccepted*>(result.ok().get());
    return mismatch(PSTRING() << "original transaction was not accepted, exit code "
                              << (rejected ? rejected->vm_exit_code : 0));
  }
  emulated_ = executed->transaction;
  if (executed->transaction->get_hash() != transaction.root->get_hash()) {
    return mismatch("transaction hash differs");
  }
  if (transaction.after != executed->account.total_state->get_hash().bits()) {
    return mismatch("resulting state hash differs");
  }
  account_ = std::move(executed->account);
  return std::string{};
}

AccountState Replay::state() const {
  return account_state(account_);
}

}  // namespace tvm_executor
