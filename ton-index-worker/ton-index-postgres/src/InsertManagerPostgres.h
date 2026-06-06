#pragma once
#include <memory>
#include <queue>
#include <utility>
#include <cstdint>
#include "InsertManagerBase.h"
#include "KvrocksClient.h"
#include "PartitionManagerPostgres.h"

class InsertBatchPostgres;

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
  KvrocksConfig kvrocks_config_;
  PartitionManagerConfig partition_config_;
  std::shared_ptr<sw::redis::Redis> kvrocks_;
  td::actor::ActorOwn<> leader_heartbeat_;
  std::string worker_id_;
  std::int32_t max_data_depth_{0};
  std::int32_t latest_states_prepare_parallelism_{4};
  std::int32_t latest_states_prepare_chunk_size_{128};
  bool no_leader_{false};
  bool disable_progress_advance_{false};
  bool kvrocks_skip_current_tables_{false};
public:
  InsertManagerPostgres(Credential credential, KvrocksConfig kvrocks_config = {}, PartitionManagerConfig partition_config = {},
                        bool no_leader = false, bool disable_progress_advance = false,
                        bool kvrocks_skip_current_tables = false) :
    credential_(credential), kvrocks_config_(std::move(kvrocks_config)), partition_config_(partition_config),
    no_leader_(no_leader), disable_progress_advance_(disable_progress_advance),
    kvrocks_skip_current_tables_(kvrocks_skip_current_tables) {}

  void start_up() override;

  void set_max_data_depth(std::int32_t value);
  void set_latest_states_prepare_parallelism(std::int32_t value);
  void set_latest_states_prepare_chunk_size(std::int32_t value);

  void create_insert_actor(std::vector<InsertTaskStruct> insert_tasks,
                           td::Promise<InsertManagerInterface::InsertResult> promise) override;
  void get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise, std::int32_t from_seqno = 0, std::int32_t to_seqno = 0) override;
  void ensure_resume_state_initialized(td::Promise<bool> promise, std::int32_t from_seqno = 0) override;
  void get_resume_seqno(td::Promise<InsertManagerInterface::ResumeState> promise, std::int32_t from_seqno = 0, std::int32_t to_seqno = 0) override;
};
