#pragma once

#include "RedisMaterializer.h"
#include "StreamingHints.h"
#include "TraceAssembler.h"

struct DetectedAccounts;

// Data preparation shared by the emulator and finalized processor. No actor state or I/O.
namespace trace_materialization {
inline constexpr std::size_t kMaxCachedTraceNodes = 1000;
inline constexpr const char* kStreamingTransactionsChannel = "streaming_transactions";
inline constexpr const char* kStreamingActionsChannel = "streaming_actions";
inline constexpr const char* kActionsStateField = "mch_classify_state";
inline constexpr const char* kActionsField = "actions";
inline constexpr const char* kActionsFinalityField = "actions_finality";
inline constexpr const char* kAaiPrefix = "_aai:";

struct PreparedActionUpdate {
  ActionState state;
  bool actions_updated{false};
  std::vector<std::pair<std::string, std::string>> fields_to_set;
  std::vector<TraceStateIndexRef> removed_index_refs;
  std::vector<TraceStateIndexRef> added_index_refs;
};

struct PreparedTraceUpdate {
  bool needs_redis_write{false};
  ActiveTrace next_trace;
  RedisWritePlan redis;
  std::vector<AcceptedNode> accepted_nodes;
};

std::string account_key(const block::StdAddress& address);
const char* classification_outcome_name(mch::EmuClassifyOutcome outcome);
std::uint8_t trace_finality(const ActiveTrace& trace);
StreamingUpdateFinality streaming_update_finality(FinalityState finality);
void append_streaming_transaction_hint(RedisWritePlan&, const ActiveTrace&, const std::string&,
                                       StreamingUpdateFinality);
void append_streaming_actions_hint(RedisWritePlan&, const ActiveTrace&, const std::string&, StreamingUpdateFinality,
                                   bool);
PreparedActionUpdate prepare_action_update(const ActionState&, const mch::EmuActionPayload&);
td::Status append_account_state_writes(RedisWritePlan&, const Trace&);
td::Status append_account_state_writes(RedisWritePlan&, const TraceUpdate&);
td::Status append_account_state_writes(RedisWritePlan&, const DetectedAccounts&, FinalityState);
td::Result<RedisWritePlan> build_redis_plan(const TraceTransition&, const PreparedActionUpdate&, const TraceUpdate&,
                                            const std::string&);
void fill_classification_measurement(const mch::EmuClassifyResult&, const MeasurementPtr&);
void append_otel_propagation(const MeasurementPtr&, RedisWritePlan&);
td::Result<PreparedTraceUpdate> prepare_trace_materialization(const ActiveTrace&, TraceTransition, const TraceUpdate&,
                                                              const mch::EmuActionPayload&, const std::string&,
                                                              const MeasurementPtr&);
std::vector<TraceStateIndexRef> collect_trace_index_refs(const ActiveTrace&);
bool finalized_trace_is_open(const ActiveTrace&);
RedisWritePlan build_finalized_snapshot(const std::string&, const ActiveTrace&, std::uint32_t ttl);
RedisWritePlan build_trace_cleanup(const std::string&, const ActiveTrace&);
}  // namespace trace_materialization
