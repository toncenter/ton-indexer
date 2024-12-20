#pragma once
#include <td/actor/actor.h>
#include <td/utils/base64.h>
#include <td/utils/JsonBuilder.h>
#include <crypto/vm/cells/CellHash.h>
#include <pqxx/pqxx>
// #include <semaphore>
#include "IndexData.h"



using InsertData = std::variant<schema::AccountState, JettonMasterDataV2, JettonWalletDataV2, NFTItemDataV2, NFTCollectionDataV2>;

class PostgreSQLInserter : public td::actor::Actor {
public:
    PostgreSQLInserter(std::string connection_string, std::vector<InsertData> data, td::Promise<td::Unit> promise)
      : connection_string_(connection_string), data_(std::move(data)), promise_(std::move(promise)) {}

    void start_up() override;
private:
    void insert_latest_account_states(pqxx::work &transaction);
    void insert_jetton_masters(pqxx::work &transaction);
    void insert_jetton_wallets(pqxx::work &transaction);
    void insert_nft_items(pqxx::work &transaction);
    void insert_nft_collections(pqxx::work &transaction);

    std::string connection_string_;
    std::vector<InsertData> data_;
    td::Promise<td::Unit> promise_;
};

class PostgreSQLInsertManager : public td::actor::Actor {
public:
  PostgreSQLInsertManager(std::string connection_string, std::int32_t batch_size)
    : connection_string_(connection_string), batch_size_(batch_size) {}
  void start_up() override;
  void alarm() override;
  void insert_data(std::vector<InsertData> data);
  void insert_done(size_t cnt);
  void checkpoint(ton::ShardIdFull shard, td::Bits256 cur_addr_);
  void checkpoint_read(ton::ShardIdFull shard, td::Promise<td::Bits256> promise);
  void checkpoint_reset(ton::ShardIdFull shard);
private:
  void check_queue(bool force = false);

  std::string connection_string_;
  std::int32_t batch_size_;
  std::vector<InsertData> queue_;

  std::int32_t inserted_count_{0};
  std::int32_t in_progress_{0};
};
