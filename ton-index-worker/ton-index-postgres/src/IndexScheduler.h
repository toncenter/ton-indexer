#pragma once
#include <queue>
#include "td/actor/actor.h"

#include "IndexData.h"
#include "DbScanner.h"
#include "EventProcessor.h"
#include "InsertManager.h"
#include "DataParser.h"


class IndexScheduler: public td::actor::Actor {
private:
  td::actor::ActorId<DbScanner> db_scanner_;
  td::actor::ActorId<InsertManagerInterface> insert_manager_;
  td::actor::ActorId<ParseManager> parse_manager_;
  td::actor::ActorOwn<EventProcessor> event_processor_;

  const uint32_t max_active_tasks_{32};
  const uint32_t from_seqno_{0};
  
  uint32_t last_known_seqno_{0};
  uint32_t last_indexed_seqno_{0};

  std::queue<uint32_t> queued_seqnos_;
  std::set<uint32_t> processing_seqnos_;
  std::set<uint32_t> existing_seqnos_;

  double avg_tps_{0};
  uint32_t last_existing_seqno_count_{0};

  const QueueState max_queue_{30000, 30000, 500000, 500000};
  QueueState cur_queue_state_;

  const uint32_t stats_timeout_{10};
  td::Timestamp next_print_stats_;
public:
  IndexScheduler(td::actor::ActorId<DbScanner> db_scanner, td::actor::ActorId<InsertManagerInterface> insert_manager,
      td::actor::ActorId<ParseManager> parse_manager, uint32_t from_seqno = 0,
      uint32_t max_active_tasks = 32, QueueState max_queue = QueueState{30000, 30000, 500000, 500000}, int32_t stats_timeout = 10)
    : db_scanner_(db_scanner), insert_manager_(insert_manager), parse_manager_(parse_manager), 
      from_seqno_(from_seqno), last_known_seqno_(from_seqno), max_active_tasks_(max_active_tasks),
      max_queue_(std::move(max_queue)), stats_timeout_(stats_timeout) {
    event_processor_ = td::actor::create_actor<EventProcessor>("event_processor", insert_manager_);
  };

  void alarm() override;
  void run();
private:
  void schedule_next_seqnos();

  void schedule_seqno(uint32_t mc_seqno);
  void reschedule_seqno(uint32_t mc_seqno);
  void seqno_fetched(uint32_t mc_seqno, MasterchainBlockDataState block_data_state);
  void seqno_parsed(uint32_t mc_seqno, ParsedBlockPtr parsed_block);
  void seqno_interfaces_processed(uint32_t mc_seqno, ParsedBlockPtr parsed_block);
  void seqno_queued_to_insert(uint32_t mc_seqno, QueueState status);
  void seqno_inserted(uint32_t mc_seqno, td::Unit result);

  void got_existing_seqnos(td::Result<std::vector<uint32_t>> R);
  void got_last_known_seqno(uint32_t last_known_seqno);

  void got_insert_queue_state(QueueState status);

  void print_stats();
};
