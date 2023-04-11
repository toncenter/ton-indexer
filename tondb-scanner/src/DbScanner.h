#pragma once
#include <queue>
#include "validator/manager-disk.h"
#include "validator/db/rootdb.hpp"

#include "InsertManagerPostgres.h"


class DbScanner: public td::actor::Actor {
private:
  td::actor::ActorOwn<ton::validator::ValidatorManagerInterface> validator_manager_;
  td::actor::ActorOwn<ton::validator::RootDb> db_;
  td::actor::ActorId<InsertManagerInterface> insert_manager_;

  std::string db_root_;
  
  std::queue<int> seqnos_to_process_;
  std::set<int> seqnos_in_progress_;
  int max_parallel_fetch_actors_{1};
  int last_known_seqno_{-1};

public:
  DbScanner(td::actor::ActorId<InsertManagerInterface> insert_manager) 
      : insert_manager_(insert_manager) {
  }

  void set_db_root(std::string db_root) {
    db_root_ = db_root;
  }

  void set_last_known_seqno(int seqno) {
    last_known_seqno_ = seqno;
  }

  void start_up() override;

  void alarm() override;

  void run();

private:
  void update_last_mc_seqno();
  void set_last_mc_seqno(int mc_seqno);
  void catch_up_with_primary();
  void schedule_for_processing();
  void seqno_fetched(int mc_seqno, td::Result<std::vector<BlockDataState>> blocks_data_state);
};