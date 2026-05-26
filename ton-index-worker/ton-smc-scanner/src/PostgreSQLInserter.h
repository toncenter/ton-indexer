#pragma once
#include <td/actor/actor.h>
#include <td/utils/base64.h>
#include <pqxx/pqxx>
#include <sw/redis++/redis++.h>
#include "IndexData.h"
#include "KvrocksState.h"


using InsertData = kvrocks_state::PointStateData;

struct ScannerInsertConfig {
  std::string postgres_connection_string;
  bool postgres_enabled{false};
  kvrocks_state::KvrocksConfig kvrocks_config;
  std::uint32_t source_mc_seqno{0};
  std::int32_t max_data_depth{0};
};

class PostgreSQLInserter : public td::actor::Actor {
public:
    PostgreSQLInserter(ScannerInsertConfig config,
                       std::shared_ptr<sw::redis::Redis> kvrocks,
                       std::vector<InsertData> data,
                       td::Promise<td::Unit> promise)
      : config_(std::move(config)), kvrocks_(std::move(kvrocks)), data_(std::move(data)), promise_(std::move(promise)) {}

    void start_up() override;
private:
    void insert_postgres(const kvrocks_state::StateBatch& batch);
    void insert_kvrocks(const kvrocks_state::StateBatch& batch);
    void insert_latest_account_states(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);
    void insert_jetton_masters(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);
    void insert_jetton_wallets(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);
    void insert_nft_items(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);
    void insert_nft_collections(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);
    void insert_getgems_nft_sales(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);
    void insert_getgems_nft_auctions(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);
    void insert_multisig_contracts(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);
    void insert_multisig_orders(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);
    void insert_vesting_contracts(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);
    void insert_nominator_pools(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);
    void insert_telemint_contracts(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);
    void insert_dedust_pools(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);
    void lock_nft_real_owner_reconciliation(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);
    void reconcile_nft_real_owners(pqxx::work &transaction, const kvrocks_state::StateBatch& batch);

    ScannerInsertConfig config_;
    std::shared_ptr<sw::redis::Redis> kvrocks_;
    std::vector<InsertData> data_;
    td::Promise<td::Unit> promise_;
};

class PostgreSQLInsertManager : public td::actor::Actor {
public:
  PostgreSQLInsertManager(ScannerInsertConfig config, std::int32_t batch_size)
    : config_(std::move(config)), batch_size_(batch_size) {}
  void start_up() override;
  void insert_data(std::vector<InsertData> data, td::Promise<td::Unit> promise);
  void insert_done(size_t cnt);
private:

  ScannerInsertConfig config_;
  std::shared_ptr<sw::redis::Redis> kvrocks_;
  std::int32_t batch_size_;
  std::vector<std::tuple<InsertData, td::Promise<>>> queue_;

  std::int32_t inserted_count_{0};
  std::int32_t in_progress_{0};
};
