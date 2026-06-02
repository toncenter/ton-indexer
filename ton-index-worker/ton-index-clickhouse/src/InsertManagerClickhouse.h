#pragma once
#include <string>
#include <queue>
#include <clickhouse/client.h>
#include "InsertManagerBase.h"


class InsertManagerClickhouse: public InsertManagerBase {
public:
  struct Credential {
    std::string conn_str;
    std::string host = "127.0.0.1";
    int port = 9000;
    std::string user = "default";
    std::string password = "";
    std::string dbname = "default";

    void set_conn_str(const std::string& conn_str);
    clickhouse::ClientOptions get_clickhouse_options();
  };
  struct Options {};
private:
  InsertManagerClickhouse::Credential credential_;
public:
  InsertManagerClickhouse(Credential credential) : credential_(credential) {}

  void start_up() override;

  void create_insert_actor(std::vector<InsertTaskStruct> insert_tasks,
                           td::Promise<InsertManagerInterface::InsertResult> promise) override;
  void get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise, std::int32_t from_seqno = 0, std::int32_t to_seqno = 0) override;
};
