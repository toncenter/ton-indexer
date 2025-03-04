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

    std::string get_connection_string(std::string dbname = "") const;
  };
private:
  InsertManagerPostgres::Credential credential_;
  bool custom_types_{false};
  bool create_indexes_{false};
  bool run_migrations_{false};
  std::int32_t max_data_depth_{0};
  std::int32_t out_of_sync_seqno_{0};
public:
  InsertManagerPostgres(InsertManagerPostgres::Credential credential, bool custom_types, bool create_indexes, bool run_migrations) : 
    credential_(credential), custom_types_(custom_types), create_indexes_(create_indexes), run_migrations_(run_migrations) {}

  void start_up() override;

  void set_max_data_depth(std::int32_t value);

  void create_insert_actor(std::vector<InsertTaskStruct> insert_tasks, td::Promise<td::Unit> promise) override;
  void get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise, std::int32_t from_seqno = 0, std::int32_t to_seqno = 0) override;
};


class InsertBatchPostgres: public td::actor::Actor {
public:
  InsertBatchPostgres(InsertManagerPostgres::Credential credential, std::vector<InsertTaskStruct> insert_tasks, td::Promise<td::Unit> promise, std::int32_t max_data_depth = 12) :
    credential_(std::move(credential)), insert_tasks_(std::move(insert_tasks)), promise_(std::move(promise)), max_data_depth_(max_data_depth) {
      // sorting in descending seqno order for easier processing of interfaces
      std::sort(insert_tasks_.begin(), insert_tasks_.end(), [](const auto& a, const auto& b) {
        return a.mc_seqno_ > b.mc_seqno_;
      });
  }

  void start_up() override;
  void alarm() override;
private:
  InsertManagerPostgres::Credential credential_;
  std::string connection_string_;
  std::vector<InsertTaskStruct> insert_tasks_;
  td::Promise<td::Unit> promise_;
  std::int32_t max_data_depth_;
  bool with_copy_{true};

  std::string stringify(schema::ComputeSkipReason compute_skip_reason);
  std::string stringify(schema::AccStatusChange acc_status_change);
  std::string stringify(schema::AccountStatus account_status);
  std::string stringify(schema::Trace::State state);

  void insert_blocks(pqxx::work &txn, bool with_copy);
  void insert_shard_state(pqxx::work &txn, bool with_copy);
  void insert_transactions(pqxx::work &txn, bool with_copy);
  void insert_messages(pqxx::work &txn, bool with_copy);
  void insert_account_states(pqxx::work &txn, bool with_copy);
  std::string insert_latest_account_states(pqxx::work &txn);
  void insert_jetton_transfers(pqxx::work &txn, bool with_copy);
  void insert_jetton_burns(pqxx::work &txn, bool with_copy);
  void insert_nft_transfers(pqxx::work &txn, bool with_copy);
  std::string insert_jetton_masters(pqxx::work &txn);
  std::string insert_jetton_wallets(pqxx::work &txn);
  std::string insert_nft_collections(pqxx::work &txn);
  std::string insert_nft_items(pqxx::work &txn);
  std::string insert_getgems_nft_auctions(pqxx::work &txn);
  std::string insert_getgems_nft_sales(pqxx::work &txn);
  void insert_traces(pqxx::work &txn, bool with_copy);
};
