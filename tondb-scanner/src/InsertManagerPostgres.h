#pragma once
#include <queue>
#include <pqxx/pqxx>
#include "InsertManager.h"

class InsertBatchMcSeqnos;

class InsertManagerPostgres: public InsertManagerInterface {
private:
  std::queue<ParsedBlockPtr> insert_queue_;
  std::queue<td::Promise<td::Unit>> promise_queue_;

  int batch_blocks_count_{512};
  int batch_tx_count_{50000};
  int max_parallel_insert_actors_{3};
  std::atomic<int> parallel_insert_actors_{0};

  struct PostgresCredential {
    std::string host = "127.0.0.1";
    int port = 5432;
    std::string user;
    std::string password;
    std::string dbname = "ton_index";

    std::string getConnectionString();
  } credential;

  std::atomic<uint> inserted_count_;
  std::chrono::system_clock::time_point start_time_;
  std::chrono::system_clock::time_point last_verbose_time_;
public:
  InsertManagerPostgres();

  void set_host(std::string value) { credential.host = std::move(value); }
  void set_port(int value) { credential.port = value; }
  void set_user(std::string value) { credential.user = std::move(value); }
  void set_password(std::string value) { credential.password = std::move(value); }
  void set_dbname(std::string value) { credential.dbname = std::move(value); }

  void set_batch_blocks_count(int value) { batch_blocks_count_ = value; }
  void set_parallel_inserts_actors(int value) { max_parallel_insert_actors_ = value; }

  void start_up() override;
  void alarm() override;

  void report_statistics();

  void get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise) override;
  void insert(ParsedBlockPtr block_ds, td::Promise<td::Unit> promise) override;
  void upsert_jetton_wallet(JettonWalletData jetton_wallet, td::Promise<td::Unit> promise) override;
  void get_jetton_wallet(std::string address, td::Promise<JettonWalletData> promise) override;
  void upsert_jetton_master(JettonMasterData jetton_wallet, td::Promise<td::Unit> promise) override;
  void get_jetton_master(std::string address, td::Promise<JettonMasterData> promise) override;
  void upsert_nft_collection(NFTCollectionData nft_collection, td::Promise<td::Unit> promise) override;
  void get_nft_collection(std::string address, td::Promise<NFTCollectionData> promise) override;
  void upsert_nft_item(NFTItemData nft_item, td::Promise<td::Unit> promise) override;
  void get_nft_item(std::string address, td::Promise<NFTItemData> promise) override;
};

class InsertBatchMcSeqnos: public td::actor::Actor {
public:
  InsertBatchMcSeqnos(std::string connection_string, std::vector<ParsedBlockPtr> mc_blocks, td::Promise<td::Unit>&& promise) :
    connection_string_(std::move(connection_string)), mc_blocks_(std::move(mc_blocks)), promise_(std::move(promise)) {}
  
  void start_up();
private:
  std::string connection_string_;
  std::vector<ParsedBlockPtr> mc_blocks_;
  td::Promise<td::Unit> promise_;

  struct TxMsg {
    std::string tx_hash;
    std::string msg_hash;
    std::string direction; // in or out
  };

  struct MsgBody {
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

  int transactions_count_{0};
  int messages_count_{0};
  int blocks_count_{0};
};
