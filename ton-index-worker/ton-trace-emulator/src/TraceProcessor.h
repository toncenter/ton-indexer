#pragma once

#include <memory>
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
  virtual void promote_confirmed(std::vector<ConfirmedTraceSnapshot> snapshots, ton::BlockSeqno mc_seqno,
                                 td::Promise<td::Unit> promise) = 0;
  virtual void invalidate(std::vector<td::Bits256> trace_hashes) = 0;
  virtual void mark_confirmed_roots_replaced(std::vector<td::Bits256> trace_hashes) = 0;
};

class TraceProcessor : public ITraceProcessor {
  struct Impl;
  std::unique_ptr<Impl> impl_;

  void start_next_operations();
  void schedule_trace(const std::string& trace_key);
  void request_cleanup(const std::string& trace_key, TraceCleanupMode mode);
  void start_replaced_confirmed_root_ttl(const std::string& trace_key);
  void update_lifecycle(const std::string& trace_key);
  bool touch_oversized_trace(const std::string& trace_key);
  void enqueue_trace_update(TraceUpdate update, bool confirmed, td::Promise<td::Unit> regular_promise,
                            td::Promise<ConfirmedTraceSnapshot> confirmed_promise);
  void start_up() override;
  void alarm() override;
  void tear_down() override;
  void classification_finished(std::string trace_key, td::Result<mch::EmuClassifyResult> result);
  void classification_ready(std::string trace_key, mch::EmuActionPayload payload);
  void materialize_classified_trace(std::string trace_key);
  void write_finished(std::string trace_key, td::Status status, RedisWriteBatch batch);

 public:
  TraceProcessor(const std::string& redis_dsn, TraceRetentionConfig retention,
                 mch::EmuClassifierConfig classifier_config = {});
  ~TraceProcessor() override;

  void process_trace_update(TraceUpdate update, td::Promise<td::Unit> promise) override;
  void process_confirmed_trace_update(TraceUpdate update, td::Promise<ConfirmedTraceSnapshot> promise) override;
  void promote_confirmed(std::vector<ConfirmedTraceSnapshot> snapshots, ton::BlockSeqno mc_seqno,
                         td::Promise<td::Unit> promise) override;
  void invalidate(std::vector<td::Bits256> trace_hashes) override;
  void mark_confirmed_roots_replaced(std::vector<td::Bits256> trace_hashes) override;
};
