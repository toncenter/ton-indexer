#pragma once
#include <queue>
#include <utility>
#include "InsertManagerBase.h"


class InsertManagerPostgres: public InsertManagerBase {
public:
  struct Credential {
    std::string host = "127.0.0.1";
    int port = 5432;
    std::string user;
    std::string password;
    std::string dbname = "ton_index";

    std::string conn_str;

    std::string get_connection_string() const;
  };
private:
  Credential credential_;
  std::int32_t max_data_depth_{0};
public:
  InsertManagerPostgres(Credential  credential) :
    credential_(std::move(credential)) {}

  void start_up() override;

  void set_max_data_depth(std::int32_t value);

  void create_insert_actor(std::vector<InsertTaskStruct> insert_tasks, td::Promise<td::Unit> promise) override;
  void get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise, std::int32_t from_seqno = 0, std::int32_t to_seqno = 0) override;
};
