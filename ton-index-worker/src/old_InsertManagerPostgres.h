#pragma once
#include <queue>
#include <pqxx/pqxx>
#include "InsertManagerBase.h"


class InsertBatchPostgres;

class InsertManagerPostgres: public InsertManagerBase {
public:
  struct Credential {
    std::string host = "127.0.0.1";
    int port = 5432;
    std::string user;
    std::string password;
    std::string dbname = "ton_index";
  };
private:
  Credential credential_;
public:
  InsertManagerPostgres(Credential credential);
  
  void start_up() override;
  void alarm() override;

  void get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise) override;
  void insert(std::uint32_t mc_seqno, ParsedBlockPtr block_ds, td::Promise<QueueStatus> queued_promise, td::Promise<td::Unit> inserted_promise) override;
  void upsert_jetton_wallet(JettonWalletData jetton_wallet, td::Promise<td::Unit> promise) override;
  void get_jetton_wallet(std::string address, td::Promise<JettonWalletData> promise) override;
  void upsert_jetton_master(JettonMasterData jetton_wallet, td::Promise<td::Unit> promise) override;
  void get_jetton_master(std::string address, td::Promise<JettonMasterData> promise) override;
  void upsert_nft_collection(NFTCollectionData nft_collection, td::Promise<td::Unit> promise) override;
  void get_nft_collection(std::string address, td::Promise<NFTCollectionData> promise) override;
  void upsert_nft_item(NFTItemData nft_item, td::Promise<td::Unit> promise) override;
  void get_nft_item(std::string address, td::Promise<NFTItemData> promise) override;
};


class InsertBatchPostgres: public td::actor::Actor {
public:
  InsertBatchMcSeqnos(InsertManagerPostgres::Credential credential, std::vector<ParsedBlockPtr> mc_blocks, td::Promise<td::Unit>&& promise) :
    credential_(std::move(credential)), mc_blocks_(std::move(mc_blocks)), promise_(std::move(promise)) {}
  
  void start_up();
private:
  InsertManagerPostgres::Credential credential_;
  std::string connection_string_;
  std::vector<ParsedBlockPtr> mc_blocks_;
  td::Promise<td::Unit> promise_;

  struct TxMsg {
    td::uint64 tenant_id;
    std::string tx_hash;
    std::string msg_hash;
    std::string direction; // in or out
  };

  struct MsgBody {
    td::uint64 tenant_id;
    std::string hash;
    std::string body;
  };

  std::string stringify(schema::ComputeSkipReason compute_skip_reason);
  std::string stringify(schema::AccStatusChange acc_status_change);
  std::string stringify(schema::AccountStatus account_status);
  std::string jsonify(const schema::SplitMergeInfo& info);
  std::string jsonify(const schema::StorageUsedShort& s);
  std::string jsonify(const schema::TrStoragePhase& s);
  std::string jsonify(const schema::TrCreditPhase& c);
  std::string jsonify(const schema::TrActionPhase& action);
  std::string jsonify(const schema::TrBouncePhase& bounce);
  std::string jsonify(const schema::TrComputePhase& compute);
  std::string jsonify(schema::TransactionDescr descr);

  void insert_blocks(pqxx::work &transaction, const std::vector<ParsedBlockPtr>& mc_blocks);
  void insert_transactions(pqxx::work &transaction, const std::vector<ParsedBlockPtr>& mc_blocks);
  void insert_messsages(pqxx::work &transaction, const std::vector<schema::Message> &messages, const std::vector<MsgBody>& msg_bodies, const std::vector<TxMsg> &tx_msgs);
  void insert_messages_contents(const std::vector<MsgBody>& msg_bodies, pqxx::work& transaction);
  void insert_messages_impl(const std::vector<schema::Message>& messages, pqxx::work& transaction);
  void insert_messages_txs(const std::vector<TxMsg>& messages, pqxx::work& transaction);
  void insert_account_states(pqxx::work &transaction, const std::vector<ParsedBlockPtr>& mc_blocks);
  void insert_jetton_transfers(pqxx::work &transaction, const std::vector<ParsedBlockPtr>& mc_blocks);
  void insert_jetton_burns(pqxx::work &transaction, const std::vector<ParsedBlockPtr>& mc_blocks);
  void insert_nft_transfers(pqxx::work &transaction, const std::vector<ParsedBlockPtr>& mc_blocks);
};
