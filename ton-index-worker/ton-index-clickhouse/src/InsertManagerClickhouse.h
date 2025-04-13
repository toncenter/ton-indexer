#pragma once
#include <string>
#include <queue>
#include <clickhouse/client.h>
#include "InsertManagerBase.h"


class InsertBatchClickhouse;

class InsertManagerClickhouse: public InsertManagerBase {
public:
  struct Credential {
    std::string host = "127.0.0.1";
    int port = 9000;
    std::string user = "default";
    std::string password = "";
    std::string dbname = "default";

    clickhouse::ClientOptions get_clickhouse_options();
  };
  struct Options {};
private:
  InsertManagerClickhouse::Credential credential_;
public:
  InsertManagerClickhouse(Credential credential) : credential_(credential) {}

  void start_up() override;

  void create_insert_actor(std::vector<InsertTaskStruct> insert_tasks, td::Promise<td::Unit> promise) override;
  void get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise, std::int32_t from_seqno = 0, std::int32_t to_seqno = 0) override;
};


class InsertBatchClickhouse: public td::actor::Actor {
public:
  InsertBatchClickhouse(clickhouse::ClientOptions client_options, std::vector<InsertTaskStruct> insert_tasks, td::Promise<td::Unit> promise) 
    : client_options_(std::move(client_options)), insert_tasks_(std::move(insert_tasks)), promise_(std::move(promise)) {}

  void start_up() override;
private:
  InsertManagerClickhouse::Credential credential_;
  clickhouse::ClientOptions client_options_;
  std::vector<InsertTaskStruct> insert_tasks_;
  td::Promise<td::Unit> promise_;

  struct MsgBody {
    td::Bits256 hash;
    std::string body;
  };

  void insert_jettons(clickhouse::Client& client);
  void insert_nfts(clickhouse::Client& client);
  void insert_transactions(clickhouse::Client& client);
  void insert_messages(clickhouse::Client& client);
  void insert_account_states(clickhouse::Client& client);
  void insert_shard_state(clickhouse::Client& client);
  void insert_blocks(clickhouse::Client& client);
};
