#pragma once
#include "IndexData.h"

td::Result<td::Bits256> ext_in_msg_get_normalized_hash(td::Ref<vm::Cell> ext_in_msg_cell);

class ParseQuery: public td::actor::Actor {
private:
  const int mc_seqno_;
  MasterchainBlockDataState mc_block_;
  std::shared_ptr<vm::CellDbReader> cell_db_reader_;
  ParsedBlockPtr result;
  td::Promise<ParsedBlockPtr> promise_;
public:
  ParseQuery(int mc_seqno, MasterchainBlockDataState mc_block, std::shared_ptr<vm::CellDbReader> cell_db_reader, td::Promise<ParsedBlockPtr> promise)
    : mc_seqno_(mc_seqno), mc_block_(std::move(mc_block)), cell_db_reader_(std::move(cell_db_reader)), result(std::make_shared<ParsedBlock>()), promise_(std::move(promise)) {}

  void start_up() override;

private:
  td::Status parse_impl();

  schema::Block parse_block(const td::Ref<vm::Cell>& root_cell, const ton::BlockIdExt& blk_id, block::gen::Block::Record& blk, const block::gen::BlockInfo::Record& info, 
                            const block::gen::BlockExtra::Record& extra, const td::optional<schema::Block> &mc_block);
  schema::MasterchainBlockShard parse_shard_state(const schema::Block& mc_block, const ton::BlockIdExt& shard_blk_id);
  static td::Result<schema::CurrencyCollection> parse_currency_collection(td::Ref<vm::CellSlice> csr);
  td::Result<schema::Message> parse_message(td::Ref<vm::Cell> msg_cell);
  td::Result<schema::TrStoragePhase> parse_tr_storage_phase(vm::CellSlice& cs);
  td::Result<schema::TrCreditPhase> parse_tr_credit_phase(vm::CellSlice& cs);
  td::Result<schema::TrComputePhase> parse_tr_compute_phase(vm::CellSlice& cs);
  td::Result<schema::StorageUsed> parse_storage_used(vm::CellSlice& cs);
  td::Result<schema::TrActionPhase> parse_tr_action_phase(vm::CellSlice& cs);
  td::Result<schema::TrBouncePhase> parse_tr_bounce_phase(vm::CellSlice& cs);
  td::Result<schema::SplitMergeInfo> parse_split_merge_info(td::Ref<vm::CellSlice>& cs);
  td::Result<schema::TransactionDescr> process_transaction_descr(vm::CellSlice& td_cs);

  struct AccountStateShort {
    td::Bits256 account_cell_hash;
    uint64_t last_transaction_lt;
    td::Bits256 last_transaction_hash;
  };
  td::Result<std::vector<schema::Transaction>> parse_transactions(const ton::BlockIdExt& blk_id, const block::gen::Block::Record &block, 
                                const block::gen::BlockInfo::Record &info, const block::gen::BlockExtra::Record &extra,
                                std::map<td::Bits256, AccountStateShort> &account_states);
  td::Result<std::vector<schema::AccountState>> parse_account_states_new(ton::WorkchainId workchain_id, uint32_t gen_utime, std::map<td::Bits256, AccountStateShort> &account_states);
  td::Result<schema::AccountState> parse_none_account(td::Ref<vm::Cell> account_root, block::StdAddress address, uint32_t gen_utime, td::Bits256 last_trans_hash, uint64_t last_trans_lt);

public: //TODO: refactor
  static td::Result<schema::AccountState> parse_account(td::Ref<vm::Cell> account_root, uint32_t gen_utime, td::Bits256 last_trans_hash, uint64_t last_trans_lt);
};


class ParseManager: public td::actor::Actor {
public:
    ParseManager() {}

    void parse(int mc_seqno, MasterchainBlockDataState mc_block, std::shared_ptr<vm::CellDbReader> cell_db_reader, td::Promise<ParsedBlockPtr> promise) {
      td::actor::create_actor<ParseQuery>("parsequery", mc_seqno, std::move(mc_block), cell_db_reader, std::move(promise)).release();
    }
};