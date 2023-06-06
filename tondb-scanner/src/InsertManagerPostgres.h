#pragma once
#include <queue>
#include "InsertManager.h"

class InsertManagerPostgres: public InsertManagerInterface {
private:
  std::queue<ParsedBlockPtr> insert_queue_;
  std::queue<td::Promise<td::Unit>> promise_queue_;

  int batch_size{2048};

  struct PostgresCredential {
    std::string host = "127.0.0.1";
    int port = 5432;
    std::string user;
    std::string password;
    std::string dbname = "ton_index";

    std::string getConnectionString();
  } credential;

  unsigned long inserted_count_;
  std::chrono::system_clock::time_point start_time_;
  std::chrono::system_clock::time_point last_verbose_time_;
public:
  InsertManagerPostgres();

  void set_host(std::string value) { credential.host = std::move(value); }
  void set_port(int value) { credential.port = value; }
  void set_user(std::string value) { credential.user = std::move(value); }
  void set_password(std::string value) { credential.password = std::move(value); }
  void set_dbname(std::string value) { credential.dbname = std::move(value); }

  void set_batch_size(int value) { batch_size = value; }

  void start_up() override;
  void alarm() override;

  void report_statistics();

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
