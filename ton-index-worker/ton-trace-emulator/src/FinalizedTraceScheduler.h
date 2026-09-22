#pragma once

#include <map>
#include <set>

#include "BlockEmulator.h"
#include "DbScanner.h"
#include "DbEventListener.h"
#include "RedisMaterializer.h"
#include "TraceProcessor.h"

// Both producers apply the same sequence locally; Redis accepts each block once.
// Restart rebuilds local trace state from the shared bootstrap block, without FLUSHDB.
class FinalizedTraceScheduler : public td::actor::Actor {
  friend struct FinalizedTraceSchedulerTest;
 public:
  FinalizedTraceScheduler(td::actor::ActorId<DbScanner> scanner, td::actor::ActorId<TraceProcessor> processor,
                          RedisConnectionOptions redis,
                          ton::BlockSeqno from = 0, ton::BlockSeqno to = 0, std::string db_event_fifo = {});

 private:
  td::actor::ActorId<DbScanner> scanner_;
  td::actor::ActorId<TraceProcessor> processor_;
  td::actor::ActorOwn<RedisMaterializer> writer_;
  std::string db_event_fifo_;
  td::actor::ActorOwn<DbEventListener> db_event_listener_;
  ton::BlockSeqno notified_head_{0};
  ton::BlockSeqno requested_from_{0}, to_{0}, next_{0}, head_{0};
  std::uint32_t block_time_{0};
  static constexpr std::size_t kMaxWrites = 64;
  struct PendingTrace {
    RedisWritePlan plan;
    bool in_flight{false};
    td::Timestamp retry_at;
  };
  bool block_active_{false};
  bool preparation_finished_{false}, control_in_flight_{false};
  std::set<std::string> received_traces_;
  std::map<std::string, PendingTrace> pending_traces_;
  std::size_t in_flight_{0}, completed_traces_{0};
  td::Timestamp control_retry_at_;
  bool busy_{false};

  void start_up() override;
  void alarm() override;
  void handle_db_event(ton::tl_object_ptr<ton::ton_api::db_Event> event);
  void got_head(td::Result<ton::BlockSeqno> result);
  void initialized(td::Result<std::int64_t> result);
  void fetched(td::Result<schema::MasterchainBlockDataState> result);
  void parsed(td::Result<FinalizedBlockResult> result);
  void prepared(td::Result<RedisWriteBatch> result);
  void trace_prepared(ton::BlockSeqno seqno, RedisWritePlan plan);
  void trace_written(std::string trace_key, td::Result<std::int64_t> result);
  void drive_block();
  void committed(td::Result<std::int64_t> result);
};
