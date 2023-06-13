#pragma once
#include <queue>
#include "validator/manager-disk.h"
#include "validator/db/rootdb.hpp"

#include "IndexData.h"
#include "InsertManagerPostgres.h"
#include "DataParser.h"
#include "EventProcessor.h"

class DbScanner: public td::actor::Actor {
private:
  td::actor::ActorOwn<ton::validator::ValidatorManagerInterface> validator_manager_;
  td::actor::ActorOwn<ton::validator::RootDb> db_;
  td::actor::ActorOwn<EventProcessor> event_processor_;
  td::actor::ActorId<InsertManagerInterface> insert_manager_;
  td::actor::ActorId<ParseManager> parse_manager_;

  std::string db_root_;
  
  std::queue<std::uint32_t> seqnos_to_process_;
  std::set<std::uint32_t> seqnos_in_progress_;
  std::set<std::uint32_t> existing_mc_seqnos_;
  int max_parallel_fetch_actors_{2048};
  std::uint32_t last_known_seqno_{0};

public:
  DbScanner(td::actor::ActorId<InsertManagerInterface> insert_manager, td::actor::ActorId<ParseManager> parse_manager) 
      : insert_manager_(insert_manager), parse_manager_(parse_manager) {
  }

  void set_db_root(std::string db_root) {
    db_root_ = db_root;
  }

  void set_last_known_seqno(int seqno) {
    last_known_seqno_ = seqno;
  }

  void set_max_parallel_fetch_actors(int max_parallel_fetch_actors) {
    max_parallel_fetch_actors_ = max_parallel_fetch_actors;
  }

  void start_up() override;

  void alarm() override;

  void run();

private:
  void update_last_mc_seqno();
  void set_last_mc_seqno(int mc_seqno);
  void catch_up_with_primary();
  void schedule_for_processing();
  void seqno_fetched(int mc_seqno, td::Result<MasterchainBlockDataState> blocks_data_state);
  void seqno_parsed(int mc_seqno, td::Result<ParsedBlockPtr> parsed_block);
  void interfaces_processed(int mc_seqno, ParsedBlockPtr parsed_block, td::Result<td::Unit> result);
  void got_existing_seqnos(td::Result<std::vector<std::uint32_t>> R);
  void reschedule_seqno(int mc_seqno);
};
