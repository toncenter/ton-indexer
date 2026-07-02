#pragma once
#include <memory>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include "td/actor/actor.h"

#include "IndexData.h"
#include "DbScanner.h"
#include "ActionDetector.h"
#include "TraceAssembler.h"
#include "InsertManager.h"
#include "DataParser.h"
#include "smc-interfaces/InterfacesDetector.h"
#include "Otel.h"

using Detector = InterfacesDetector<JettonWalletDetectorR, JettonMasterDetectorR,
                                      NftItemDetectorR, NftCollectionDetectorR,
                                      GetGemsNftFixPriceSale, GetGemsNftFixPriceSaleV4,
                                      GetGemsNftAuction, VestingContract,
                                      NominatorPoolContract, TelemintContract>;

class IndexScheduler: public td::actor::Actor {
private:
  std::deque<std::uint32_t> queued_seqnos_;
  std::set<std::uint32_t> processing_seqnos_;
  std::set<std::uint32_t> inserting_seqnos_;
  std::set<std::uint32_t> indexed_seqnos_;
  std::unordered_set<std::uint32_t> skip_insert_seqnos_;

  td::actor::ActorId<DbScanner> db_scanner_;
  td::actor::ActorId<InsertManagerInterface> insert_manager_;
  td::actor::ActorId<ParseManager> parse_manager_;
  td::actor::ActorOwn<TraceAssembler> trace_assembler_;
  std::shared_ptr<td::Destructor> watcher_;

  std::string working_dir_;

  std::uint32_t max_active_tasks_{32};
  std::int32_t last_known_seqno_{0};
  std::int32_t last_indexed_seqno_{0};
  // Highest seqno such that all seqnos from the current start point up to it
  // are acknowledged (inserted or intentionally skipped). Unlike
  // last_indexed_seqno_ it never runs ahead across in-flight gaps, so it is a
  // safe upper bound for the failover watchdog checks.
  std::int32_t contiguous_indexed_seqno_{0};
  std::int32_t from_seqno_{0};
  std::int32_t read_from_seqno_{0};
  std::int32_t to_seqno_{0};
  bool force_index_{false};
  std::uint32_t prewarm_count_{50};
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
  struct SeqnoOtelTraceState {
    std::uint32_t attempt_{0};
    bool is_in_sync_{false};
    std::unique_ptr<OtelSpan> otel_root_span_;
    std::unique_ptr<OtelSpan> active_otel_stage_span_;
  };
  std::unordered_map<std::uint32_t, SeqnoOtelTraceState> seqno_otel_traces_;
  std::unordered_map<std::uint32_t, std::uint32_t> seqno_attempts_;
public:
  IndexScheduler(td::actor::ActorId<DbScanner> db_scanner, td::actor::ActorId<InsertManagerInterface> insert_manager,
      td::actor::ActorId<ParseManager> parse_manager, std::string working_dir, std::int32_t from_seqno = 0, std::int32_t to_seqno = 0, bool force_index = false,
      std::uint32_t max_active_tasks = 32, QueueState max_queue = QueueState{30000, 30000, 500000, 500000}, std::int32_t stats_timeout = 10,
      std::shared_ptr<td::Destructor> watcher = nullptr, std::uint32_t prewarm_count = 50)
    : db_scanner_(db_scanner), insert_manager_(insert_manager), parse_manager_(parse_manager), working_dir_(std::move(working_dir)),
      from_seqno_(from_seqno), to_seqno_(to_seqno), force_index_(force_index), max_active_tasks_(max_active_tasks),
      prewarm_count_(prewarm_count), max_queue_(std::move(max_queue)), stats_timeout_(stats_timeout), watcher_(watcher) {};

  void start_up() override;
  void alarm() override;
  void run();
  void get_contiguous_indexed_seqno(td::Promise<std::int32_t> promise);
private:
  void advance_contiguous_indexed_seqno();
  void schedule_next_seqnos();

  void schedule_seqno(std::uint32_t mc_seqno);
  void reschedule_seqno(std::uint32_t mc_seqno, bool silent = false);
  void seqno_fetched(std::uint32_t mc_seqno, schema::MasterchainBlockDataState block_data_state);
  void seqno_parsed(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block);
  void seqno_traces_assembled(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block);
  void seqno_interfaces_processed(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block);
  void seqno_actions_processed(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block);
  void seqno_queued_to_insert(std::uint32_t mc_seqno, QueueState status);
  void set_seqno_otel_attribute(std::uint32_t mc_seqno, const std::string& key,
                                const OtelStageSpan::AttributeValue& value, bool include_stage = true);
  void seqno_inserted(std::uint32_t mc_seqno);
  void handle_seqno_failure(std::uint32_t mc_seqno, std::string error_type, td::Status error, bool silent);

  void process_force_resume_state_initialized(td::Result<bool> R, ton::BlockSeqno start_seqno);
  void process_resume_seqno(td::Result<InsertManagerInterface::ResumeState> R);
  void process_archive_existing_seqnos(td::Result<std::vector<std::uint32_t>> R);
  void start_from_seqno(ton::BlockSeqno seqno, bool reset_trace_assembler);
  void got_newest_mc_seqno(std::uint32_t newest_mc_seqno);
  bool is_bounded_archive_range() const;
  bool should_insert_seqno(std::uint32_t mc_seqno) const;
  ton::BlockSeqno prewarm_start_seqno(ton::BlockSeqno seqno) const;
  void seqno_processed_without_insert(std::uint32_t mc_seqno, const char* reason);
  void maybe_finish_bounded_range();

  void got_insert_queue_state(QueueState status);

  void print_stats();
  void start_seqno_otel_trace(std::uint32_t mc_seqno, bool is_in_sync);
  void start_seqno_otel_stage(std::uint32_t mc_seqno, const std::string& stage_name);
  void finish_seqno_otel_stage(std::uint32_t mc_seqno);
  void finish_seqno_otel_trace(std::uint32_t mc_seqno);
  void fail_seqno_otel_trace(std::uint32_t mc_seqno, const std::string& error_type, const td::Status& error, bool silent);
  void apply_otel_processing_lag(std::uint32_t mc_seqno, const ParsedBlock& parsed_block);
};
