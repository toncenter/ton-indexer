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

    std::string get_connection_string();
  };
  struct Options {
    
  };
private:
  InsertManagerPostgres::Credential credential_;
  InsertManagerPostgres::Options options_;
public:
  InsertManagerPostgres(InsertManagerPostgres::Credential credential) : credential_(credential) {}
  
  void create_insert_actor(std::vector<InsertTaskStruct> insert_tasks, td::Promise<td::Unit> promise) override;
  void get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise) override;

  void upsert_jetton_wallet(JettonWalletData jetton_wallet, td::Promise<td::Unit> promise) override;
  void upsert_jetton_master(JettonMasterData jetton_wallet, td::Promise<td::Unit> promise) override;
  void upsert_nft_collection(NFTCollectionData nft_collection, td::Promise<td::Unit> promise) override;
  void upsert_nft_item(NFTItemData nft_item, td::Promise<td::Unit> promise) override;
};


class InsertBatchPostgres: public td::actor::Actor {
public:
  InsertBatchPostgres(InsertManagerPostgres::Credential credential, std::vector<InsertTaskStruct> insert_tasks, td::Promise<td::Unit>&& promise) :
    credential_(std::move(credential)), insert_tasks_(std::move(insert_tasks)), promise_(std::move(promise)) {}
  
  void start_up();
private:
  InsertManagerPostgres::Credential credential_;
  std::string connection_string_;
  std::vector<InsertTaskStruct> insert_tasks_;
  td::Promise<td::Unit> promise_;

  struct TxMsg {
    std::string tx_hash;
    std::string msg_hash;
    std::string direction; // in or out
  };

  struct MsgBody {
    td::Bits256 hash;
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
  std::string jsonify(const schema::BlockReference& block_ref);
  std::string jsonify(const std::vector<schema::BlockReference>& prev_blocks);

  void insert_blocks(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks_);
  void insert_shard_state(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks_);
  void insert_transactions(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks_);
  void insert_messsages(pqxx::work &transaction, const std::vector<schema::Message> &messages, const std::vector<MsgBody>& msg_bodies, const std::vector<TxMsg> &tx_msgs);
  void insert_messages_contents(const std::vector<MsgBody>& msg_bodies, pqxx::work& transaction);
  void insert_messages_impl(const std::vector<schema::Message>& messages, pqxx::work& transaction);
  void insert_messages_txs(const std::vector<TxMsg>& messages, pqxx::work& transaction);
  void insert_account_states(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks_);
  void insert_latest_account_states(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks_);
  void insert_jetton_transfers(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks_);
  void insert_jetton_burns(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks_);
  void insert_nft_transfers(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks_);
};
