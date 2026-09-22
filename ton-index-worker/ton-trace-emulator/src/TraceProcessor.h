#pragma once

#include <memory>
#include <functional>
#include <string>
#include <vector>

#include "crypto/common/bitstring.h"
#include "emu/EmuClassifierActor.h"
#include "td/actor/actor.h"
#include "td/utils/Status.h"

#include "ConfirmedTraceSnapshot.h"
#include "Measurement.h"
#include "TraceEmulator.h"
#include "TraceLifecycle.h"
#include "TraceUpdate.h"

struct RedisWriteBatch;
struct RedisWritePlan;
struct RedisConnectionOptions;

enum class TraceCleanupMode {
  Retention,
  PendingTimeout,
  ReplacedConfirmedTimeout,
  Invalidation,
  Oversized,
};

class ITraceProcessor : public td::actor::Actor {
 public:
  // One update is one observable trace transition. Its disconnected block
  // fragments are merged before classification and Redis publication.
  virtual void process_trace_update(TraceUpdate update, td::Promise<td::Unit> promise) = 0;
  virtual void process_confirmed_trace_update(TraceUpdate update, td::Promise<ConfirmedTraceSnapshot> promise) = 0;
  // Scheduler closes blocks before sending this notification and filters any
  // later confirmed arrivals. Discard queued work for all versions of these ids.
  virtual void discard_confirmed_updates(std::vector<ton::BlockId> block_ids) = 0;
  // Only promotes matching nodes already present when their queued operation
  // runs. An unavailable promotion requests ordinary finalized emulation;
  // the promise completes after all attempted Redis writes have finished.
  virtual void promote_confirmed(std::vector<ConfirmedTraceSnapshot> snapshots, ton::BlockSeqno mc_seqno,
                                 td::Promise<td::Unit> promise) = 0;
  virtual void invalidate(std::vector<td::Bits256> trace_hashes) = 0;
  virtual void mark_confirmed_roots_replaced(std::vector<td::Bits256> trace_hashes) = 0;
};

class TraceProcessor : public ITraceProcessor {
  friend struct TraceProcessorTest;
  struct Impl;
  std::unique_ptr<Impl> impl_;

  void start_next_operations();
  void schedule_trace(const std::string& trace_key);
  void request_cleanup(const std::string& trace_key, TraceCleanupMode mode);
  void start_replaced_confirmed_root_ttl(const std::string& trace_key);
  void update_lifecycle(const std::string& trace_key);
  bool touch_oversized_trace(const std::string& trace_key);
  td::Status drop_oversized_finalized_trace(const std::string& trace_key, const TraceUpdate& update);
  void enqueue_trace_update(TraceUpdate update, bool confirmed, td::Promise<td::Unit> regular_promise,
                            td::Promise<ConfirmedTraceSnapshot> confirmed_promise);
  void start_up() override;
  void alarm() override;
  void tear_down() override;
  void classification_finished(std::string trace_key, td::Result<mch::EmuClassifyResult> result);
  void classification_ready(std::string trace_key, mch::EmuActionPayload payload);
  void materialize_classified_trace(std::string trace_key);
  void write_finished(std::string trace_key, td::Status status, RedisWriteBatch batch);
  void finalized_update_prepared(td::Result<td::Unit> result);
  void prepare_finalized_impl(ton::BlockSeqno seqno, std::uint32_t unix_time, std::vector<TraceUpdate> updates,
      std::function<void(RedisWritePlan)> on_plan, td::Promise<RedisWriteBatch> promise);

 public:
  TraceProcessor(RedisConnectionOptions redis_options, TraceRetentionConfig retention,
                 mch::EmuClassifierConfig classifier_config = {}, bool finalized_only = false);
  ~TraceProcessor() override;

  // Applies every update locally and returns complete Redis snapshots, without writing.
  // One block at a time. Retain/retry the returned batch on Redis failures.
  void prepare_finalized_block(ton::BlockSeqno seqno, std::uint32_t unix_time,
                               std::vector<TraceUpdate> updates, td::Promise<RedisWriteBatch> promise);
  // Emit each trace as soon as its classification finishes; complete after all updates.
  void prepare_finalized_block_streaming(ton::BlockSeqno seqno, std::uint32_t unix_time,
      std::vector<TraceUpdate> updates,
      std::function<void(RedisWritePlan)> on_plan, td::Promise<RedisWriteBatch> promise);

  void process_trace_update(TraceUpdate update, td::Promise<td::Unit> promise) override;
  void process_confirmed_trace_update(TraceUpdate update, td::Promise<ConfirmedTraceSnapshot> promise) override;
  void discard_confirmed_updates(std::vector<ton::BlockId> block_ids) override;
  void promote_confirmed(std::vector<ConfirmedTraceSnapshot> snapshots, ton::BlockSeqno mc_seqno,
                         td::Promise<td::Unit> promise) override;
  void invalidate(std::vector<td::Bits256> trace_hashes) override;
  void mark_confirmed_roots_replaced(std::vector<td::Bits256> trace_hashes) override;
};
