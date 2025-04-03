#pragma once
#include <queue>
#include "td/actor/actor.h"

#include "IndexData.h"
#include "DbScanner.h"
#include "EventProcessor.h"
#include "TraceAssembler.h"
#include "InsertManager.h"
#include "DataParser.h"
#include "smc-interfaces/InterfacesDetector.h"

using Detector = InterfacesDetector<JettonWalletDetectorR, JettonMasterDetectorR, 
                                      NftItemDetectorR, NftCollectionDetectorR,
                                      GetGemsNftFixPriceSale, GetGemsNftAuction>;

class IndexScheduler: public td::actor::Actor {
private:
  std::deque<std::uint32_t> queued_seqnos_;
  std::set<std::uint32_t> processing_seqnos_;
  std::set<std::uint32_t> indexed_seqnos_;

  td::actor::ActorId<DbScanner> db_scanner_;
  td::actor::ActorId<InsertManagerInterface> insert_manager_;
  td::actor::ActorId<ParseManager> parse_manager_;
  td::actor::ActorOwn<TraceAssembler> trace_assembler_;
  std::shared_ptr<td::Destructor> watcher_;

  std::string working_dir_;

  std::uint32_t max_active_tasks_{32};
  std::int32_t last_known_seqno_{0};
  std::int32_t last_indexed_seqno_{0};
  std::int32_t from_seqno_{0};
  std::int32_t to_seqno_{0};
  bool force_index_{false};
  bool is_in_sync_{false};

  td::Timestamp last_alarm_timestamp_;
  std::double_t avg_tps_{0};
  std::int64_t last_indexed_seqno_count_{0};

  QueueState max_queue_{30000, 30000, 500000, 500000};
  QueueState cur_queue_state_;

  std::int32_t stats_timeout_{10};
  td::Timestamp next_print_stats_;
  td::Timestamp next_statistics_flush_;

  std::unordered_map<ton::BlockSeqno, td::Timer> timers_;
public:
  IndexScheduler(td::actor::ActorId<DbScanner> db_scanner, td::actor::ActorId<InsertManagerInterface> insert_manager,
      td::actor::ActorId<ParseManager> parse_manager, std::string working_dir, std::int32_t from_seqno = 0, std::int32_t to_seqno = 0, bool force_index = false,
      std::uint32_t max_active_tasks = 32, QueueState max_queue = QueueState{30000, 30000, 500000, 500000}, std::int32_t stats_timeout = 10,
      std::shared_ptr<td::Destructor> watcher = nullptr)
    : db_scanner_(db_scanner), insert_manager_(insert_manager), parse_manager_(parse_manager), working_dir_(std::move(working_dir)),
      from_seqno_(from_seqno), to_seqno_(to_seqno), force_index_(force_index), max_active_tasks_(max_active_tasks),
      max_queue_(std::move(max_queue)), stats_timeout_(stats_timeout), watcher_(watcher) {};

  void start_up() override;
  void alarm() override;
  void run();
private:
  void schedule_next_seqnos();

  void schedule_seqno(std::uint32_t mc_seqno);
  void reschedule_seqno(std::uint32_t mc_seqno, bool silent = false);
  void seqno_fetched(std::uint32_t mc_seqno, MasterchainBlockDataState block_data_state);
  void seqno_parsed(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block);
  void seqno_traces_assembled(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block);
  void seqno_interfaces_processed(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block);
  void seqno_actions_processed(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block);
  void seqno_queued_to_insert(std::uint32_t mc_seqno, QueueState status);
  void seqno_inserted(std::uint32_t mc_seqno, td::Unit result);

  void process_existing_seqnos(td::Result<std::vector<std::uint32_t>> R);
  void handle_missing_ta_state(ton::BlockSeqno next_seqno);
  void handle_valid_ta_state(ton::BlockSeqno last_state_seqno);
  void got_newest_mc_seqno(std::uint32_t newest_mc_seqno);

  void got_insert_queue_state(QueueState status);

  void print_stats();
};
