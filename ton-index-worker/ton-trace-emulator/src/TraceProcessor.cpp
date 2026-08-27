#include <algorithm>
#include <cstdint>
#include <deque>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <optional>
#include <queue>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "td/utils/Timer.h"

#include "RedisMaterializer.h"
#include "Serializer.hpp"
#include "Statistics.h"
#include "StreamingHints.h"
#include "TraceAssembler.h"
#include "TraceLifecycle.h"
#include "TraceProcessor.h"
#include "TraceState.h"

namespace {

constexpr std::size_t kMaxConcurrentWrites = 16;
constexpr std::size_t kMaxPendingTraceUpdates = 10000;
constexpr std::size_t kMaxCachedTraceNodes = 1000;
constexpr double kCleanupRetrySeconds = 1.0;
constexpr double kExpirySweepSeconds = 1.0;
constexpr double kQueueFullLogIntervalSeconds = 5.0;
constexpr double kQueueStatsLogIntervalSeconds = 10.0;

constexpr const char* kInvalidatedTraceChannel = "invalidated_traces";
constexpr const char* kStreamingTransactionsChannel = "streaming_transactions";
constexpr const char* kStreamingActionsChannel = "streaming_actions";
constexpr const char* kActionsStateField = "mch_classify_state";
constexpr const char* kActionsField = "actions";
constexpr const char* kActionsFinalityField = "actions_finality";
constexpr const char* kAaiPrefix = "_aai:";

std::string finality_name(FinalityState finality) {
  switch (finality) {
    case FinalityState::Emulated:
      return "pending";
    case FinalityState::Confirmed:
      return "confirmed";
    case FinalityState::Finalized:
      return "finalized";
  }
  return "pending";
}

std::string trace_emulator_operation(FinalityState finality) {
  return finality == FinalityState::Emulated ? "emulate" : "read_finalized";
}

const char* classification_outcome_name(mch::EmuClassifyOutcome outcome) {
  switch (outcome) {
    case mch::EmuClassifyOutcome::classified:
      return "classified";
    case mch::EmuClassifyOutcome::classify_failed:
      return "classify_failed";
    case mch::EmuClassifyOutcome::convert_failed:
      return "convert_failed";
  }
  return "unknown";
}

bool contains_real_root(const Trace& trace) {
  return trace.contains_root_transaction() && trace.root->finality_state != FinalityState::Emulated;
}

std::string account_key(const block::StdAddress& address) {
  return std::to_string(address.workchain) + ":" + address.addr.to_hex();
}

std::uint8_t trace_finality(const ActiveTrace& trace) {
  auto finality = TraceStateFinality::Finalized;
  if (trace.nodes.nodes().empty()) {
    return static_cast<std::uint8_t>(trace.finality);
  }
  for (const auto& [_, node] : trace.nodes.nodes()) {
    finality = std::min(finality, node.finality);
  }
  return static_cast<std::uint8_t>(finality);
}

std::vector<std::string> transaction_accounts(const ActiveTrace& trace,
                                              std::optional<TraceStateFinality> only_finality = std::nullopt) {
  std::set<std::string> accounts;
  for (const auto& [_, node] : trace.nodes.nodes()) {
    if (only_finality && node.finality != *only_finality) {
      continue;
    }
    for (const auto& index : node.index_refs) {
      accounts.insert(index.index_key);
    }
  }
  return {accounts.begin(), accounts.end()};
}

StreamingUpdateFinality streaming_update_finality(FinalityState finality) {
  return static_cast<StreamingUpdateFinality>(static_cast<std::uint8_t>(finality));
}

void append_streaming_transaction_hint(RedisWritePlan& plan, const ActiveTrace& trace, const std::string& trace_key,
                                       StreamingUpdateFinality update_finality) {
  const auto only_finality =
      update_finality == StreamingUpdateFinality::Pending ? std::optional{TraceStateFinality::Emulated} : std::nullopt;
  auto accounts = transaction_accounts(trace, only_finality);
  if (accounts.empty()) {
    return;
  }
  const auto snapshot_finality = update_finality == StreamingUpdateFinality::Pending
                                     ? static_cast<std::uint8_t>(TraceStateFinality::Emulated)
                                     : trace_finality(trace);
  plan.publications.emplace_back(kStreamingTransactionsChannel, pack_streaming_hint(StreamingTransactionHint{
                                                                    .trace_key = trace_key,
                                                                    .update_seq = trace.update_seq,
                                                                    .update_finality =
                                                                        static_cast<std::uint8_t>(update_finality),
                                                                    .trace_finality = snapshot_finality,
                                                                    .accounts = std::move(accounts),
                                                                }));
}

void append_streaming_actions_hint(RedisWritePlan& plan, const ActiveTrace& trace, const std::string& trace_key,
                                   StreamingUpdateFinality update_finality, bool actions_updated) {
  StreamingActionsHint hint{
      .trace_key = trace_key,
      .update_seq = trace.update_seq,
      .update_finality = static_cast<std::uint8_t>(update_finality),
      .trace_finality = trace_finality(trace),
      .actions_updated = actions_updated,
  };
  if (actions_updated) {
    hint.action_types_and_accounts.reserve(trace.actions.routes.size());
    for (const auto& route : trace.actions.routes) {
      hint.action_types_and_accounts.push_back(StreamingActionRoute{
          .type = route.type,
          .accounts = route.accounts,
      });
    }
  }
  plan.publications.emplace_back(kStreamingActionsChannel, pack_streaming_hint(hint));
}

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

struct CachedConfirmedTrace {
  std::map<std::string, TraceStateNode> nodes;
  std::vector<AccountStateWrite> account_states;
};

}  // namespace

struct ConfirmedTraceSnapshotData {
  std::string trace_key;
  std::shared_ptr<const ActiveTrace> state;
  CachedConfirmedTrace confirmed;
};

namespace {

struct InsertCompletion {
  bool confirmed{false};
  td::Promise<td::Unit> regular_promise;
  td::Promise<ConfirmedTraceSnapshot> confirmed_promise;

  void set_error(td::Status error) {
    if (confirmed) {
      confirmed_promise.set_error(std::move(error));
    } else {
      regular_promise.set_error(std::move(error));
    }
  }

  void set_value(ConfirmedTraceSnapshot snapshot = {}) {
    if (confirmed) {
      confirmed_promise.set_value(std::move(snapshot));
    } else {
      regular_promise.set_value(td::Unit());
    }
  }
};

struct InsertRequest {
  Trace trace;
  InsertCompletion completion;
  MeasurementPtr measurement;
  bool contains_real_root{false};
};

struct ClassificationWork {
  Trace trace;
  TraceTransition transition;
  InsertCompletion completion;
  MeasurementPtr measurement;
  bool contains_real_root{false};
  std::optional<mch::EmuActionPayload> payload;
  std::optional<td::Timer> classification_timer;
  td::Timer insert_timer{true};
};

struct PromoteConfirmedRequest {
  std::shared_ptr<const ActiveTrace> fallback_state;
  CachedConfirmedTrace trace;
  ton::BlockSeqno mc_seqno;
  td::Promise<td::Unit> promise;
};

struct ConfirmedPromotionCompletion {
  std::size_t remaining{0};
  std::optional<td::Status> first_error;
  td::Promise<td::Unit> promise;

  void one_finished(td::Result<td::Unit> result) {
    if (result.is_error() && !first_error) {
      first_error = result.move_as_error();
    }
    if (--remaining != 0) {
      return;
    }
    if (first_error) {
      promise.set_error(std::move(*first_error));
    } else {
      promise.set_value(td::Unit());
    }
  }
};

struct ConfirmedRootReplacedRequest {};

using TraceRequest = std::variant<InsertRequest, PromoteConfirmedRequest, ConfirmedRootReplacedRequest>;

std::deque<TraceRequest> resolve_terminal_queue(std::deque<TraceRequest> queued, std::size_t& pending_updates,
                                                TraceCleanupMode mode) {
  std::deque<TraceRequest> surviving;
  while (!queued.empty()) {
    auto work = std::move(queued.front());
    queued.pop_front();

    if (auto* request = std::get_if<InsertRequest>(&work)) {
      --pending_updates;
      if (mode == TraceCleanupMode::Invalidation && request->completion.confirmed) {
        request->completion.set_error(td::Status::Error("Confirmed trace was invalidated before insertion"));
      } else {
        if (mode == TraceCleanupMode::Oversized && request->measurement) {
          request->measurement->set_otel_attribute("ton.trace_state.oversized", true);
        }
        request->completion.set_value();
      }
      continue;
    }

    if (auto* promotion = std::get_if<PromoteConfirmedRequest>(&work)) {
      if (mode == TraceCleanupMode::Invalidation) {
        surviving.push_back(std::move(work));
      } else {
        promotion->promise.set_value(td::Unit());
      }
    }
  }
  return surviving;
}

enum class InFlightKind {
  Update,
  Cleanup,
};

enum class TraceReadyQueue {
  None,
  General,
  Write,
};

struct InFlightWork {
  InFlightKind kind{InFlightKind::Update};
  ActiveTrace next_trace;
  InsertCompletion completion;
  TraceCleanupMode cleanup_mode{TraceCleanupMode::Retention};
  bool contains_real_root{false};
  bool counted_update{false};
  std::optional<CachedConfirmedTrace> confirmed_trace;
};

struct TraceSlot {
  std::shared_ptr<const ActiveTrace> current{std::make_shared<const ActiveTrace>()};
  RedisWriteBatch dirty;

  std::deque<TraceRequest> queued;
  std::optional<ClassificationWork> classification;
  std::optional<InFlightWork> in_flight;
  TraceReadyQueue scheduled_queue{TraceReadyQueue::None};
  bool cleanup_requested{false};

  TraceLifecycle lifecycle{TraceLifecycle::UnknownRoot};
  td::Timestamp deadline;
  TraceCleanupMode cleanup_mode{TraceCleanupMode::Retention};
};

TraceReadyQueue next_ready_queue(const TraceSlot& slot) {
  if (slot.in_flight || (slot.classification && !slot.classification->payload)) {
    return TraceReadyQueue::None;
  }
  if (slot.classification || slot.cleanup_requested) {
    return TraceReadyQueue::Write;
  }
  if (slot.queued.empty()) {
    return TraceReadyQueue::None;
  }
  return std::holds_alternative<PromoteConfirmedRequest>(slot.queued.front()) ? TraceReadyQueue::Write
                                                                              : TraceReadyQueue::General;
}

TraceReadyQueue next_queue_to_drain(std::size_t active_writes, bool has_ready_writes,
                                    std::size_t general_ready_count, bool has_ready_traces) {
  if (active_writes < kMaxConcurrentWrites && has_ready_writes) {
    return TraceReadyQueue::Write;
  }
  if (general_ready_count > 0 && has_ready_traces) {
    return TraceReadyQueue::General;
  }
  return TraceReadyQueue::None;
}

struct TraceProcessorQueueSnapshot {
  std::size_t queued_updates{0};
  std::size_t classifying{0};
  std::size_t classified_waiting_write{0};
  std::size_t in_flight_updates{0};
  std::size_t in_flight_cleanups{0};
  std::size_t cleanup_requested{0};
  std::size_t promotions_waiting_write{0};
  std::size_t scheduled_general{0};
  std::size_t scheduled_writes{0};
  std::size_t max_slot_queue{0};
  std::string max_slot_trace;
};

TraceProcessorQueueSnapshot collect_queue_snapshot(const std::unordered_map<std::string, TraceSlot>& traces) {
  TraceProcessorQueueSnapshot snapshot;
  for (const auto& [trace_key, slot] : traces) {
    std::size_t slot_updates = 0;
    for (const auto& request : slot.queued) {
      if (std::holds_alternative<InsertRequest>(request)) {
        ++slot_updates;
      }
    }
    snapshot.queued_updates += slot_updates;
    if (slot_updates > snapshot.max_slot_queue) {
      snapshot.max_slot_queue = slot_updates;
      snapshot.max_slot_trace = trace_key;
    }

    if (slot.classification) {
      if (slot.classification->payload) {
        ++snapshot.classified_waiting_write;
      } else {
        ++snapshot.classifying;
      }
    }
    if (slot.in_flight) {
      if (slot.in_flight->kind == InFlightKind::Cleanup) {
        ++snapshot.in_flight_cleanups;
      } else if (slot.in_flight->counted_update) {
        ++snapshot.in_flight_updates;
      }
    }
    if (slot.cleanup_requested) {
      ++snapshot.cleanup_requested;
    }
    if (!slot.classification && !slot.cleanup_requested && !slot.queued.empty() &&
        std::holds_alternative<PromoteConfirmedRequest>(slot.queued.front())) {
      ++snapshot.promotions_waiting_write;
    }
    if (slot.scheduled_queue == TraceReadyQueue::General) {
      ++snapshot.scheduled_general;
    } else if (slot.scheduled_queue == TraceReadyQueue::Write) {
      ++snapshot.scheduled_writes;
    }
  }
  return snapshot;
}

std::string format_queue_snapshot(const TraceProcessorQueueSnapshot& snapshot, std::size_t pending_updates,
                                  std::size_t trace_slots, std::size_t active_writes,
                                  std::size_t ready_general_entries, std::size_t ready_write_entries) {
  const auto accounted_updates = snapshot.queued_updates + snapshot.classifying +
                                 snapshot.classified_waiting_write + snapshot.in_flight_updates;
  std::ostringstream result;
  result << "pending_updates=" << pending_updates
         << " accounted_updates=" << accounted_updates
         << " trace_slots=" << trace_slots
         << " queued_updates=" << snapshot.queued_updates
         << " classifying=" << snapshot.classifying
         << " classified_waiting_write=" << snapshot.classified_waiting_write
         << " in_flight_updates=" << snapshot.in_flight_updates
         << " in_flight_cleanups=" << snapshot.in_flight_cleanups
         << " cleanup_requested=" << snapshot.cleanup_requested
         << " promotions_waiting_write=" << snapshot.promotions_waiting_write
         << " active_writes=" << active_writes
         << " scheduled_general=" << snapshot.scheduled_general
         << " scheduled_writes=" << snapshot.scheduled_writes
         << " ready_general_entries=" << ready_general_entries
         << " ready_write_entries=" << ready_write_entries
         << " max_slot_queue=" << snapshot.max_slot_queue
         << " max_slot_trace=" << snapshot.max_slot_trace;
  return result.str();
}

void real_root_applied(TraceSlot& slot) {
  if (slot.cleanup_mode != TraceCleanupMode::ReplacedConfirmedTimeout) {
    return;
  }
  slot.cleanup_requested = false;
  slot.cleanup_mode = TraceCleanupMode::Retention;
}

void remember_failed_write(TraceSlot& slot, ActiveTrace next_trace, RedisWriteBatch batch) {
  // The logical patch is needed by later partial updates. Its Redis data
  // commands are replayed with the next patch, but its time-sensitive trace
  // notifications are not.
  batch.discard_trace_publications();
  slot.current = std::make_shared<const ActiveTrace>(std::move(next_trace));
  slot.dirty = std::move(batch);
}

PreparedActionUpdate prepare_action_update(const ActionState& current, const mch::EmuActionPayload& payload,
                                           const std::string& trace_key) {
  PreparedActionUpdate prepared;
  prepared.state = current;
  prepared.state.blob_is_current = false;
  if (payload.state == nullptr) {
    return prepared;
  }
  if (current.blob_finality && *current.blob_finality > payload.finality) {
    LOG(DEBUG) << "skipping stale actions write for " << trace_key << ": stored finality "
               << static_cast<int>(*current.blob_finality) << " > emission finality "
               << static_cast<int>(payload.finality);
    return prepared;
  }

  prepared.fields_to_set.emplace_back(kActionsStateField, payload.state);
  prepared.state.classify_state = payload.state;
  if (payload.actions_blob.empty()) {
    return prepared;
  }

  prepared.fields_to_set.emplace_back(kActionsField, payload.actions_blob);
  prepared.fields_to_set.emplace_back(kActionsFinalityField, std::to_string(static_cast<int>(payload.finality)));
  prepared.state.blob = payload.actions_blob;
  prepared.state.routes = payload.routes;
  prepared.state.blob_is_current = true;
  prepared.actions_updated = true;

  std::vector<TraceStateIndexRef> resulting_refs;
  resulting_refs.reserve(payload.aai.size());
  for (const auto& [account, member] : payload.aai) {
    resulting_refs.push_back(TraceStateIndexRef{
        .index_key = std::string(kAaiPrefix) + account,
        .member = member,
        .score = static_cast<std::uint64_t>(payload.aai_score),
    });
  }
  std::sort(resulting_refs.begin(), resulting_refs.end());
  resulting_refs.erase(std::unique(resulting_refs.begin(), resulting_refs.end()), resulting_refs.end());

  std::set_difference(current.aai_refs.begin(), current.aai_refs.end(), resulting_refs.begin(), resulting_refs.end(),
                      std::back_inserter(prepared.removed_index_refs));
  std::set_difference(resulting_refs.begin(), resulting_refs.end(), current.aai_refs.begin(), current.aai_refs.end(),
                      std::back_inserter(prepared.added_index_refs));
  prepared.state.blob_finality = payload.finality;
  prepared.state.aai_refs = std::move(resulting_refs);
  return prepared;
}

td::Result<std::string> rewrite_actions_finality(std::string_view blob, std::uint8_t finality) {
  try {
    auto unpacked = msgpack::unpack(blob.data(), blob.size());
    const auto& actions = unpacked.get();
    if (actions.type != msgpack::type::ARRAY) {
      return td::Status::Error("Actions payload is not a msgpack array");
    }

    for (std::uint32_t row_index = 0; row_index < actions.via.array.size; ++row_index) {
      auto& row = actions.via.array.ptr[row_index];
      if (row.type != msgpack::type::MAP) {
        return td::Status::Error("Actions payload row is not a msgpack map");
      }

      bool found_finality = false;
      for (std::uint32_t field_index = 0; field_index < row.via.map.size; ++field_index) {
        auto& field = row.via.map.ptr[field_index];
        if (field.key.type != msgpack::type::STR ||
            std::string_view(field.key.via.str.ptr, field.key.via.str.size) != "finality") {
          continue;
        }
        if (field.val.type != msgpack::type::POSITIVE_INTEGER && field.val.type != msgpack::type::NEGATIVE_INTEGER) {
          return td::Status::Error("Actions payload finality is not a msgpack integer");
        }
        field.val = msgpack::object(finality);
        found_finality = true;
      }
      if (!found_finality) {
        return td::Status::Error("Actions payload row has no finality field");
      }
    }

    std::stringstream buffer;
    msgpack::pack(buffer, actions);
    return buffer.str();
  } catch (const std::exception& error) {
    return td::Status::Error("Failed to rewrite actions finality: " + std::string(error.what()));
  }
}

struct PreparedActionPromotion {
  bool actions_updated{false};
  std::vector<std::pair<std::string, std::string>> fields_to_set;
};

td::Result<PreparedActionPromotion> prepare_promoted_action_update(const ActionState& current, ActionState& next,
                                                                   std::uint8_t finality,
                                                                   const std::string& trace_key) {
  PreparedActionPromotion prepared;
  next.blob_is_current = false;
  if (!current.blob_finality) {
    return prepared;
  }
  if (*current.blob_finality > finality) {
    LOG(DEBUG) << "skipping stale promoted actions write for " << trace_key << ": stored finality "
               << static_cast<int>(*current.blob_finality) << " > promoted trace finality "
               << static_cast<int>(finality);
    return prepared;
  }
  if (!current.blob) {
    return prepared;
  }
  if (!current.blob_is_current) {
    return prepared;
  }

  TRY_RESULT(rewritten, rewrite_actions_finality(*current.blob, finality));
  prepared.fields_to_set.emplace_back(kActionsField, rewritten);
  prepared.fields_to_set.emplace_back(kActionsFinalityField, std::to_string(static_cast<int>(finality)));
  next.blob = std::move(rewritten);
  next.blob_finality = finality;
  next.blob_is_current = true;
  prepared.actions_updated = true;
  return prepared;
}

td::Status append_account_state_writes(RedisWritePlan& plan, const Trace& trace) {
  for (const auto& [address, account] : trace.committed_accounts) {
    auto redis_account_result = parse_account(account);
    if (redis_account_result.is_error()) {
      return redis_account_result.move_as_error_prefix("Failed to parse account: ");
    }

    std::stringstream state_buffer;
    msgpack::pack(state_buffer, redis_account_result.move_as_ok());

    std::stringstream interfaces_buffer;
    auto interfaces = trace.committed_interfaces.find(address);
    if (interfaces != trace.committed_interfaces.end()) {
      msgpack::pack(interfaces_buffer, parse_interfaces(interfaces->second));
    }

    switch (trace.root->finality_state) {
      case FinalityState::Finalized:
        break;
      case FinalityState::Confirmed:
        break;
      case FinalityState::Emulated:
        return td::Status::Error("Emulated trace contains committed account states");
    }
    auto account_address = account_key(address);
    plan.account_states.push_back(AccountStateWrite{
        .account = std::move(account_address),
        .lt = account.last_trans_lt_,
        .finality = trace.root->finality_state,
        .state = state_buffer.str(),
        .interfaces = interfaces_buffer.str(),
    });
  }
  return td::Status::OK();
}

void append_publications(RedisWritePlan& plan, const TraceTransition& transition, const Trace& trace,
                         const std::string& trace_key, bool actions_updated) {
  bool has_committed_transactions = false;
  bool has_pending_transactions = false;
  for (const auto& accepted : transition.accepted_nodes) {
    if (accepted.finality == FinalityState::Emulated) {
      has_pending_transactions = true;
      continue;
    }
    has_committed_transactions = true;
  }

  if (has_committed_transactions) {
    const auto finalized = trace.root->finality_state == FinalityState::Finalized;
    auto update_finality = finalized ? StreamingUpdateFinality::Finalized : StreamingUpdateFinality::Confirmed;
    append_streaming_transaction_hint(plan, transition.next_trace, trace_key, update_finality);
  }
  if (has_pending_transactions) {
    append_streaming_transaction_hint(plan, transition.next_trace, trace_key, StreamingUpdateFinality::Pending);
  }
  append_streaming_actions_hint(plan, transition.next_trace, trace_key,
                                streaming_update_finality(trace.root->finality_state), actions_updated);
}

td::Result<RedisWritePlan> build_redis_plan(const TraceTransition& transition,
                                            const PreparedActionUpdate& action_update, const Trace& trace,
                                            const std::string& trace_key) {
  RedisWritePlan plan;
  plan.trace_key = trace_key;
  plan.node_fields_to_delete = transition.node_delta.removed_node_keys;
  plan.indexes_to_remove = transition.node_delta.removed_index_refs;
  plan.indexes_to_add = transition.node_delta.added_index_refs;
  plan.indexes_to_remove.insert(plan.indexes_to_remove.end(), action_update.removed_index_refs.begin(),
                                action_update.removed_index_refs.end());
  plan.indexes_to_add.insert(plan.indexes_to_add.end(), action_update.added_index_refs.begin(),
                             action_update.added_index_refs.end());
  plan.raw_external_message_hash = transition.raw_external_message_hash;

  plan.fields_to_set.reserve(transition.node_delta.upserted_nodes.size() + transition.metadata_patch.size() +
                             action_update.fields_to_set.size() + 1);
  for (const auto& node : transition.node_delta.upserted_nodes) {
    if (!node.serialized) {
      return td::Status::Error("Cannot materialize trace node without serialized payload");
    }
    plan.fields_to_set.emplace_back(node.key, *node.serialized);
  }
  for (const auto& [field, value] : transition.metadata_patch) {
    plan.fields_to_set.emplace_back(field, value);
  }
  if (!transition.node_delta.empty() || !transition.metadata_patch.empty()) {
    plan.fields_to_set.emplace_back("update_seq", std::to_string(transition.next_trace.update_seq));
  }
  plan.fields_to_set.insert(plan.fields_to_set.end(), action_update.fields_to_set.begin(),
                            action_update.fields_to_set.end());

  auto account_states_status = append_account_state_writes(plan, trace);
  if (account_states_status.is_error()) {
    return account_states_status;
  }
  append_publications(plan, transition, trace, trace_key, action_update.actions_updated);
  return plan;
}

void fill_measurement(const TraceTransition& transition, const Trace& trace, const MeasurementPtr& measurement) {
  if (!measurement) {
    return;
  }

  measurement->set_otel_attribute("ton.trace_state.accepted_nodes_count",
                                  static_cast<std::int64_t>(transition.accepted_nodes.size()));
  if (!trace.root) {
    return;
  }
  measurement->set_otel_attribute("ton.trace_state.cached_nodes_count",
                                  static_cast<std::int64_t>(transition.cached_nodes_count));
  measurement->set_otel_attribute("ton.trace_state.reused_serializations_count",
                                  static_cast<std::int64_t>(transition.reused_serializations));
  if (!transition.needs_redis_write) {
    return;
  }

  measurement->set_finality(finality_name(trace.root->finality_state));
  measurement->set_operation(trace_emulator_operation(trace.root->finality_state));
  measurement->set_out_channel(kStreamingActionsChannel);
  measurement->set_otel_attribute("ton.trace_state.upserted_nodes_count",
                                  static_cast<std::int64_t>(transition.node_delta.upserted_nodes.size()));
  measurement->set_otel_attribute("ton.trace_state.removed_nodes_count",
                                  static_cast<std::int64_t>(transition.node_delta.removed_node_keys.size()));
  measurement->set_otel_attribute("ton.trace_state.update_seq",
                                  static_cast<std::int64_t>(transition.next_trace.update_seq));
}

void fill_classification_measurement(const mch::EmuClassifyResult& result, const MeasurementPtr& measurement) {
  if (!measurement) {
    return;
  }

  measurement->set_otel_attribute("ton.trace.classification.outcome", classification_outcome_name(result.outcome));
  measurement->set_otel_attribute("ton.trace.classification.failed", result.failure);
  measurement->set_otel_attribute("ton.trace.classification.used_fallback", result.used_fallback);
  measurement->set_otel_attribute("ton.trace.classification.queue_us", result.queue_us);
  measurement->set_otel_attribute("ton.trace.classification.duration_us", result.classify_us);
  measurement->set_otel_attribute("ton.trace.classification.serialize_us", result.serialize_us);
  measurement->set_otel_attribute("ton.actions.count", static_cast<std::int64_t>(result.payload.action_count));
}

void append_otel_propagation(const MeasurementPtr& measurement, RedisWritePlan& redis_plan) {
  if (!measurement) {
    return;
  }
  for (const auto& [field, value] : measurement->otel_propagation_fields()) {
    redis_plan.fields_to_set.emplace_back(field, value);
  }
}

td::Result<PreparedTraceUpdate> prepare_trace_materialization(const ActiveTrace& current, TraceTransition transition,
                                                              const Trace& trace, const mch::EmuActionPayload& payload,
                                                              const std::string& trace_key,
                                                              const MeasurementPtr& measurement) {
  if (payload.state != nullptr && measurement) {
    measurement->set_otel_attribute("ton.actions.count", static_cast<std::int64_t>(payload.action_count));
  }

  if (!transition.needs_redis_write) {
    return PreparedTraceUpdate{};
  }

  auto action_update = prepare_action_update(current.actions, payload, trace_key);
  transition.next_trace.actions = action_update.state;
  auto redis_result = build_redis_plan(transition, action_update, trace, trace_key);
  if (redis_result.is_error()) {
    return redis_result.move_as_error();
  }
  auto redis = redis_result.move_as_ok();
  append_otel_propagation(measurement, redis);

  PreparedTraceUpdate prepared;
  prepared.needs_redis_write = true;
  prepared.next_trace = std::move(transition.next_trace);
  prepared.redis = std::move(redis);
  prepared.accepted_nodes = std::move(transition.accepted_nodes);
  return prepared;
}

td::Result<CachedConfirmedTrace> collect_confirmed_data(const ActiveTrace& resulting_trace,
                                                        const PreparedTraceUpdate& prepared) {
  CachedConfirmedTrace confirmed;
  for (const auto& accepted : prepared.accepted_nodes) {
    if (accepted.finality != FinalityState::Confirmed) {
      continue;
    }
    const auto* node = resulting_trace.nodes.find(accepted.key);
    if (!node || node->finality != TraceStateFinality::Confirmed || !node->serialized) {
      return td::Status::Error("Prepared confirmed node is missing from trace state");
    }
    confirmed.nodes.insert_or_assign(accepted.key, *node);
  }
  for (const auto& account : prepared.redis.account_states) {
    confirmed.account_states.push_back(account);
  }
  return confirmed;
}

ConfirmedTraceSnapshot make_confirmed_snapshot(const std::string& trace_key, std::shared_ptr<const ActiveTrace> state,
                                               CachedConfirmedTrace confirmed) {
  return std::make_shared<const ConfirmedTraceSnapshotData>(ConfirmedTraceSnapshotData{
      .trace_key = trace_key,
      .state = std::move(state),
      .confirmed = std::move(confirmed),
  });
}

td::Result<TraceStateNode> finalize_cached_node(const TraceStateNode& cached, ton::BlockSeqno mc_seqno) {
  if (!cached.serialized) {
    return td::Status::Error("Cached confirmed node has no serialized payload");
  }

  try {
    RedisTraceNode redis_node;
    msgpack::unpack(cached.serialized->data(), cached.serialized->size()).get().convert(redis_node);
    redis_node.emulated = false;
    redis_node.finality = FinalityState::Finalized;
    redis_node.mc_block_seqno = mc_seqno;

    std::stringstream buffer;
    msgpack::pack(buffer, redis_node);

    auto finalized = cached;
    finalized.finality = TraceStateFinality::Finalized;
    finalized.fingerprint = trace_node_fingerprint(redis_node);
    finalized.serialized = std::make_shared<const std::string>(buffer.str());
    finalized.mc_seqno = mc_seqno;
    return finalized;
  } catch (const std::exception& error) {
    return td::Status::Error("Failed to deserialize cached confirmed node: " + std::string(error.what()));
  }
}

td::Result<std::vector<TraceStateNode>> prepare_finalized_nodes(const ActiveTrace& current,
                                                                const CachedConfirmedTrace& cached,
                                                                ton::BlockSeqno mc_seqno) {
  std::vector<TraceStateNode> finalized_nodes;
  finalized_nodes.reserve(cached.nodes.size());
  for (const auto& [key, cached_node] : cached.nodes) {
    if (cached_node.finality != TraceStateFinality::Confirmed || !cached_node.serialized) {
      return td::Status::Error("Snapshot contains an invalid confirmed node");
    }
    const auto* current_node = current.nodes.find(key);
    if (current_node && current_node->finality == TraceStateFinality::Finalized) {
      continue;
    }
    TRY_RESULT(finalized, finalize_cached_node(cached_node, mc_seqno));
    finalized_nodes.push_back(std::move(finalized));
  }
  return finalized_nodes;
}

td::Status append_promoted_account_states(RedisWritePlan& plan, const CachedConfirmedTrace& cached) {
  for (const auto& cached_account : cached.account_states) {
    if (cached_account.finality != FinalityState::Confirmed) {
      return td::Status::Error("Cached account state is not confirmed");
    }
    auto account = cached_account;
    account.finality = FinalityState::Finalized;
    plan.account_states.push_back(std::move(account));
  }
  return td::Status::OK();
}

void append_promoted_publications(RedisWritePlan& plan, const CachedConfirmedTrace& cached,
                                  const ActiveTrace& next_trace, const std::string& trace_key, bool actions_updated) {
  if (!cached.nodes.empty()) {
    append_streaming_transaction_hint(plan, next_trace, trace_key, StreamingUpdateFinality::Finalized);
  }
  const auto has_pending_nodes =
      std::any_of(next_trace.nodes.nodes().begin(), next_trace.nodes.nodes().end(),
                  [](const auto& entry) { return entry.second.finality == TraceStateFinality::Emulated; });
  if (has_pending_nodes) {
    append_streaming_transaction_hint(plan, next_trace, trace_key, StreamingUpdateFinality::Pending);
  }
  append_streaming_actions_hint(plan, next_trace, trace_key, StreamingUpdateFinality::Finalized, actions_updated);
}

td::Status append_full_trace_state(RedisWritePlan& plan, const ActiveTrace& trace) {
  std::set<TraceStateIndexRef> indexes;
  for (const auto& [key, node] : trace.nodes.nodes()) {
    if (!node.serialized) {
      return td::Status::Error("Trace snapshot contains a node without serialized payload");
    }
    plan.fields_to_set.emplace_back(key, *node.serialized);
    indexes.insert(node.index_refs.begin(), node.index_refs.end());
  }
  plan.indexes_to_add.assign(indexes.begin(), indexes.end());
  for (const auto& [field, value] : trace.metadata) {
    plan.fields_to_set.emplace_back(field, value);
  }
  plan.fields_to_set.emplace_back("update_seq", std::to_string(trace.update_seq));
  return td::Status::OK();
}

td::Result<PreparedTraceUpdate> prepare_confirmed_promotion(const ActiveTrace& current,
                                                            const CachedConfirmedTrace& cached,
                                                            const std::string& trace_key, ton::BlockSeqno mc_seqno,
                                                            bool materialize_full_state) {
  TRY_RESULT(finalized_nodes, prepare_finalized_nodes(current, cached, mc_seqno));

  PreparedTraceUpdate prepared;
  prepared.next_trace = current;
  auto state_change = current.nodes.upsert_nodes(std::move(finalized_nodes));
  auto node_delta = std::move(state_change.delta);
  prepared.next_trace.nodes.apply(std::move(state_change));

  if (!node_delta.empty()) {
    if (current.update_seq == std::numeric_limits<std::uint64_t>::max()) {
      return td::Status::Error("Trace update_seq overflow");
    }
    prepared.next_trace.update_seq = current.update_seq + 1;
  }
  if (const auto root_key = trace_metadata_value(current, "root_node")) {
    if (const auto* root = prepared.next_trace.nodes.find(*root_key)) {
      prepared.next_trace.finality = static_cast<FinalityState>(static_cast<std::uint8_t>(root->finality));
    }
  }

  const auto promoted_finality = trace_finality(prepared.next_trace);
  TRY_RESULT(action_update, prepare_promoted_action_update(current.actions, prepared.next_trace.actions,
                                                           promoted_finality, trace_key));

  auto& plan = prepared.redis;
  plan.trace_key = trace_key;
  plan.raw_external_message_hash = trace_metadata_value(prepared.next_trace, "root_node").value_or(std::string{});
  if (plan.raw_external_message_hash.empty()) {
    return td::Status::Error("Cannot finalize a trace without root_node metadata");
  }

  if (materialize_full_state) {
    TRY_STATUS(append_full_trace_state(plan, prepared.next_trace));
    if (current.actions.classify_state) {
      plan.fields_to_set.emplace_back(kActionsStateField, *current.actions.classify_state);
    }
    if (action_update.actions_updated) {
      plan.indexes_to_add.insert(plan.indexes_to_add.end(), current.actions.aai_refs.begin(),
                                 current.actions.aai_refs.end());
    }
  } else {
    plan.node_fields_to_delete = node_delta.removed_node_keys;
    plan.indexes_to_remove = node_delta.removed_index_refs;
    plan.indexes_to_add = node_delta.added_index_refs;
    for (const auto& node : node_delta.upserted_nodes) {
      if (!node.serialized) {
        return td::Status::Error("Finalized node has no serialized payload");
      }
      plan.fields_to_set.emplace_back(node.key, *node.serialized);
    }
    if (!node_delta.empty()) {
      plan.fields_to_set.emplace_back("update_seq", std::to_string(prepared.next_trace.update_seq));
    }
  }
  plan.fields_to_set.insert(plan.fields_to_set.end(), action_update.fields_to_set.begin(),
                            action_update.fields_to_set.end());

  TRY_STATUS(append_promoted_account_states(plan, cached));
  append_promoted_publications(plan, cached, prepared.next_trace, trace_key, action_update.actions_updated);
  prepared.needs_redis_write = true;
  return prepared;
}

std::vector<TraceStateIndexRef> collect_cleanup_index_refs(const TraceSlot& slot) {
  std::set<TraceStateIndexRef> refs;
  for (const auto& [_, node] : slot.current->nodes.nodes()) {
    refs.insert(node.index_refs.begin(), node.index_refs.end());
  }
  refs.insert(slot.current->actions.aai_refs.begin(), slot.current->actions.aai_refs.end());
  // A failed patch may have removed an index only from the logical state.
  // Redis still contains that old member until the dirty batch is replayed,
  // so expiry must clean both sides of every carried delta.
  for (const auto& plan : slot.dirty.plans) {
    refs.insert(plan.indexes_to_remove.begin(), plan.indexes_to_remove.end());
    refs.insert(plan.indexes_to_add.begin(), plan.indexes_to_add.end());
  }
  return {refs.begin(), refs.end()};
}

constexpr bool publishes_invalidation(TraceCleanupMode mode) {
  return mode == TraceCleanupMode::PendingTimeout || mode == TraceCleanupMode::ReplacedConfirmedTimeout ||
         mode == TraceCleanupMode::Invalidation;
}

RedisWriteBatch build_cleanup_batch(const std::string& trace_key, const TraceSlot& slot, TraceCleanupMode mode) {
  RedisWritePlan plan;
  plan.trace_key = trace_key;
  plan.erase_trace = true;
  plan.indexes_to_remove = collect_cleanup_index_refs(slot);
  plan.raw_external_message_hash = trace_metadata_value(*slot.current, "root_node").value_or(std::string{});
  if (publishes_invalidation(mode)) {
    plan.publications.emplace_back(kInvalidatedTraceChannel, trace_key);
  }
  return RedisWriteBatch{.plans = {std::move(plan)}};
}

constexpr bool cleanup_is_terminal(TraceCleanupMode mode) {
  return mode == TraceCleanupMode::Invalidation || mode == TraceCleanupMode::Oversized;
}

constexpr bool cleanup_waits_for_current_updates(TraceCleanupMode mode) {
  return !cleanup_is_terminal(mode);
}

static_assert(cleanup_is_terminal(TraceCleanupMode::Oversized));
static_assert(!publishes_invalidation(TraceCleanupMode::Oversized));

}  // namespace

struct TraceProcessor::Impl {
  Impl(const std::string& redis_dsn, TraceRetentionConfig retention_config, mch::EmuClassifierConfig classifier_config)
      : materializer(redis_dsn, kMaxConcurrentWrites)
      , retention(std::move(retention_config))
      , classifier_config(std::move(classifier_config)) {
    if (this->classifier_config.prep) {
      const auto worker_count = std::max(1, this->classifier_config.workers);
      classifiers.reserve(worker_count);
      for (int idx = 0; idx < worker_count; ++idx) {
        classifiers.emplace_back(td::actor::create_actor<mch::EmuClassifierActor>(
            "MchEmuClassifier-w" + std::to_string(idx), this->classifier_config, static_cast<std::size_t>(idx)));
      }
    }
  }

  RedisMaterializer materializer;
  TraceRetentionConfig retention;
  mch::EmuClassifierConfig classifier_config;
  std::vector<td::actor::ActorOwn<mch::EmuClassifierActor>> classifiers;
  std::size_t next_classifier{0};
  std::unordered_map<std::string, TraceSlot> traces;
  std::unordered_map<std::string, td::Timestamp> oversized_traces;
  CompetingTraceSet candidates;
  std::deque<std::string> ready_traces;
  std::deque<std::string> ready_writes;
  std::size_t pending_updates{0};
  std::size_t active_writes{0};
  td::Timestamp next_queue_full_log;
  td::Timestamp next_queue_stats_log;
};

TraceProcessor::TraceProcessor(const std::string& redis_dsn, TraceRetentionConfig retention,
                               mch::EmuClassifierConfig classifier_config)
    : impl_(std::make_unique<Impl>(redis_dsn, std::move(retention), std::move(classifier_config))) {
}

TraceProcessor::~TraceProcessor() = default;

void TraceProcessor::start_up() {
  alarm_timestamp() = td::Timestamp::in(kExpirySweepSeconds);
}

bool TraceProcessor::touch_oversized_trace(const std::string& trace_key) {
  auto it = impl_->oversized_traces.find(trace_key);
  if (it == impl_->oversized_traces.end()) {
    return false;
  }
  it->second = td::Timestamp::in(impl_->retention.open_seconds);
  return true;
}

void TraceProcessor::schedule_trace(const std::string& trace_key) {
  auto it = impl_->traces.find(trace_key);
  if (it == impl_->traces.end()) {
    return;
  }
  auto& slot = it->second;
  const auto queue = next_ready_queue(slot);
  if (slot.scheduled_queue == queue) {
    return;
  }
  slot.scheduled_queue = queue;
  if (queue == TraceReadyQueue::General) {
    impl_->ready_traces.push_back(trace_key);
  } else if (queue == TraceReadyQueue::Write) {
    impl_->ready_writes.push_back(trace_key);
  }
}

void TraceProcessor::request_cleanup(const std::string& trace_key, TraceCleanupMode mode) {
  auto& slot = impl_->traces[trace_key];

  if (slot.current->root_account) {
    impl_->candidates.forget(*slot.current->root_account, trace_key);
  }

  if (!slot.cleanup_requested || cleanup_is_terminal(mode)) {
    // Terminal cleanup always wins over a normal TTL cleanup.
    slot.cleanup_mode = mode;
  }

  slot.cleanup_requested = true;
  schedule_trace(trace_key);
}

void TraceProcessor::update_lifecycle(const std::string& trace_key) {
  auto it = impl_->traces.find(trace_key);
  if (it == impl_->traces.end()) {
    return;
  }
  auto& slot = it->second;
  const auto previous_lifecycle = slot.lifecycle;
  const auto root_node = trace_metadata_value(*slot.current, "root_node").value_or(std::string{});
  slot.lifecycle = classify_trace_lifecycle(slot.current->nodes, root_node);

  // A terminally dropped trace is already on its way out. Updating its
  // lifecycle would leave stale retention state behind.
  if (cleanup_is_terminal(slot.cleanup_mode)) {
    return;
  }

  if (slot.current->root_account) {
    impl_->candidates.forget(*slot.current->root_account, trace_key);
  }

  const auto code_hash = trace_metadata_value(*slot.current, "root_account_code_hash").value_or(std::string{});

  // Continuation patches do not make a replaced root real again. Keep the
  // dedicated deadline until another confirmed/finalized root patch arrives.
  if (slot.cleanup_mode == TraceCleanupMode::ReplacedConfirmedTimeout) {
    if (slot.current->root_account && wallet_external_messages_compete(code_hash)) {
      impl_->candidates.remember(*slot.current->root_account, trace_key);
    }
    // The serialized root is kept during the grace period, but for
    // lifecycle transitions it is unresolved until another block accepts it.
    slot.lifecycle = TraceLifecycle::UnknownRoot;
    return;
  }

  const bool competing_candidate = slot.lifecycle == TraceLifecycle::RootPending &&
                                   slot.current->root_account.has_value() &&
                                   wallet_external_messages_compete(code_hash);

  std::vector<std::string> traces_to_invalidate;
  if (competing_candidate) {
    impl_->candidates.remember(*slot.current->root_account, trace_key);
  } else if (trace_root_became_real(previous_lifecycle, slot.lifecycle) && slot.current->root_account &&
             wallet_external_messages_compete(code_hash)) {
    traces_to_invalidate = impl_->candidates.accept(*slot.current->root_account, trace_key);
  }

  const auto next_cleanup_mode = competing_candidate ? TraceCleanupMode::PendingTimeout : TraceCleanupMode::Retention;

  // A pending root uses an absolute deadline. Repeated emulation updates
  // cannot keep an external message alive forever.
  const bool keep_pending_deadline = slot.lifecycle == TraceLifecycle::RootPending &&
                                     previous_lifecycle == TraceLifecycle::RootPending && slot.deadline;
  slot.cleanup_mode = next_cleanup_mode;
  if (!keep_pending_deadline) {
    slot.deadline = td::Timestamp::in(trace_retention_seconds(slot.lifecycle, impl_->retention));
  }

  for (const auto& candidate : traces_to_invalidate) {
    request_cleanup(candidate, TraceCleanupMode::Invalidation);
  }
}

void TraceProcessor::process_trace_patch(Trace trace, td::Promise<td::Unit> promise, MeasurementPtr measurement) {
  enqueue_trace_patch(std::move(trace), false, std::move(promise), {}, std::move(measurement));
}

void TraceProcessor::process_confirmed_trace_patch(Trace trace, td::Promise<ConfirmedTraceSnapshot> promise,
                                                   MeasurementPtr measurement) {
  enqueue_trace_patch(std::move(trace), true, {}, std::move(promise), std::move(measurement));
}

void TraceProcessor::enqueue_trace_patch(Trace trace, bool confirmed, td::Promise<td::Unit> regular_promise,
                                         td::Promise<ConfirmedTraceSnapshot> confirmed_promise,
                                         MeasurementPtr measurement) {
  InsertCompletion completion{
      .confirmed = confirmed,
      .regular_promise = std::move(regular_promise),
      .confirmed_promise = std::move(confirmed_promise),
  };
  auto trace_key = td::base64_encode(trace.ext_in_msg_hash_norm.as_slice());
  if (touch_oversized_trace(trace_key)) {
    if (measurement) {
      measurement->set_otel_attribute("ton.trace_state.oversized", true);
    }
    completion.set_value();
    return;
  }

  const bool real_root = contains_real_root(trace);
  auto slot_it = impl_->traces.find(trace_key);
  if (slot_it != impl_->traces.end() && cleanup_is_terminal(slot_it->second.cleanup_mode)) {
    if (confirmed) {
      completion.set_error(td::Status::Error("Confirmed trace is already being invalidated"));
    } else {
      completion.set_value();
    }
    return;
  }
  if (impl_->pending_updates >= kMaxPendingTraceUpdates) {
    g_statistics.record_count(TRACE_PROCESSOR_QUEUE_FULL);
    if (!impl_->next_queue_full_log || impl_->next_queue_full_log.is_in_past()) {
      impl_->next_queue_full_log = td::Timestamp::in(kQueueFullLogIntervalSeconds);
      const auto snapshot = collect_queue_snapshot(impl_->traces);
      LOG(ERROR) << "Trace processor queue is full: "
                 << format_queue_snapshot(snapshot, impl_->pending_updates, impl_->traces.size(),
                                          impl_->active_writes, impl_->ready_traces.size(),
                                          impl_->ready_writes.size());
    }
    completion.set_error(
        td::Status::Error("Trace processor queue is full (" + std::to_string(kMaxPendingTraceUpdates) + ")"));
    return;
  }

  auto& slot = impl_->traces[trace_key];
  if (real_root && slot.cleanup_mode == TraceCleanupMode::ReplacedConfirmedTimeout &&
      (!slot.in_flight || slot.in_flight->kind != InFlightKind::Cleanup)) {
    // Let the root patch run before an expiry that was only queued by the
    // periodic sweep.
    slot.cleanup_requested = false;
  }
  slot.queued.push_back(InsertRequest{
      .trace = std::move(trace),
      .completion = std::move(completion),
      .measurement = std::move(measurement),
      .contains_real_root = real_root,
  });
  ++impl_->pending_updates;
  schedule_trace(trace_key);
  start_next_operations();
}

void TraceProcessor::start_next_operations() {
  // Redis-ready traces stay in their own queue while all write slots are
  // occupied. General work can still assemble and enter classification
  // without repeatedly rotating the entire Redis backlog.
  auto general_ready_count = impl_->ready_traces.size();
  while (true) {
    const auto source_queue = next_queue_to_drain(impl_->active_writes, !impl_->ready_writes.empty(),
                                                  general_ready_count, !impl_->ready_traces.empty());
    std::string trace_key;
    if (source_queue == TraceReadyQueue::Write) {
      trace_key = std::move(impl_->ready_writes.front());
      impl_->ready_writes.pop_front();
    } else if (source_queue == TraceReadyQueue::General) {
      --general_ready_count;
      trace_key = std::move(impl_->ready_traces.front());
      impl_->ready_traces.pop_front();
    } else {
      break;
    }

    auto slot_it = impl_->traces.find(trace_key);
    if (slot_it == impl_->traces.end()) {
      continue;
    }
    auto& slot = slot_it->second;
    if (slot.scheduled_queue != source_queue) {
      continue;
    }
    slot.scheduled_queue = TraceReadyQueue::None;
    if ((slot.classification && !slot.classification->payload) || slot.in_flight ||
        (!slot.classification && slot.queued.empty() && !slot.cleanup_requested)) {
      continue;
    }

    if (slot.classification) {
      if (impl_->active_writes >= kMaxConcurrentWrites) {
        schedule_trace(trace_key);
        continue;
      }
      materialize_classified_trace(std::move(trace_key));
      continue;
    }

    if (slot.cleanup_requested) {
      if (impl_->active_writes >= kMaxConcurrentWrites) {
        schedule_trace(trace_key);
        continue;
      }
      auto batch = build_cleanup_batch(trace_key, slot, slot.cleanup_mode);
      auto completion = [self = actor_id(this), trace_key](td::Status status, RedisWriteBatch finished_batch) mutable {
        td::actor::send_closure(self, &TraceProcessor::write_finished, std::move(trace_key), std::move(status),
                                std::move(finished_batch));
      };

      slot.in_flight.emplace(InFlightWork{
          .kind = InFlightKind::Cleanup,
          .cleanup_mode = slot.cleanup_mode,
      });
      ++impl_->active_writes;
      impl_->materializer.write(std::move(batch), std::move(completion), td::Timer());
      continue;
    }

    auto work = std::move(slot.queued.front());
    slot.queued.pop_front();
    if (std::holds_alternative<ConfirmedRootReplacedRequest>(work)) {
      start_replaced_confirmed_root_ttl(trace_key);
      schedule_trace(trace_key);
      continue;
    }

    if (std::holds_alternative<PromoteConfirmedRequest>(work) && impl_->active_writes >= kMaxConcurrentWrites) {
      slot.queued.push_front(std::move(work));
      schedule_trace(trace_key);
      continue;
    }

    InsertCompletion insert_completion;
    MeasurementPtr request_measurement;
    td::Timer request_timer;
    bool contains_real_root = false;
    bool promotion = false;
    bool materialize_full_state = false;
    td::Result<PreparedTraceUpdate> prepared_result;
    if (auto* request = std::get_if<InsertRequest>(&work)) {
      auto trace = std::move(request->trace);
      auto completion = std::move(request->completion);
      auto measurement = std::move(request->measurement);
      const bool real_root = request->contains_real_root;

      td::Result<TraceTransition> transition_result;
      try {
        transition_result = TraceAssembler().apply(*slot.current, trace, trace_key);
      } catch (const vm::VmError& error) {
        transition_result = td::Status::Error("Got VmError while assembling trace: " + std::string(error.get_msg()));
      } catch (const std::exception& error) {
        transition_result = td::Status::Error("Got exception while assembling trace: " + std::string(error.what()));
      }

      if (transition_result.is_error()) {
        --impl_->pending_updates;
        completion.set_error(transition_result.move_as_error());
        schedule_trace(trace_key);
        continue;
      }

      auto transition = transition_result.move_as_ok();
      fill_measurement(transition, trace, measurement);
      if (!transition.needs_redis_write) {
        --impl_->pending_updates;
        if (real_root) {
          real_root_applied(slot);
          update_lifecycle(trace_key);
        }
        if (completion.confirmed) {
          completion.set_value(make_confirmed_snapshot(trace_key, slot.current, CachedConfirmedTrace{}));
        } else {
          completion.set_value();
        }
        schedule_trace(trace_key);
        continue;
      }

      const auto node_count = transition.next_trace.nodes.nodes().size();
      if (node_count > kMaxCachedTraceNodes) {
        LOG(WARNING) << "Dropping oversized trace " << trace_key << " with " << node_count << " nodes; limit is "
                     << kMaxCachedTraceNodes;
        if (measurement) {
          measurement->set_otel_attribute("ton.trace_state.oversized", true);
        }
        impl_->oversized_traces.insert_or_assign(trace_key, td::Timestamp::in(impl_->retention.open_seconds));
        slot.queued =
            resolve_terminal_queue(std::move(slot.queued), impl_->pending_updates, TraceCleanupMode::Oversized);
        request_cleanup(trace_key, TraceCleanupMode::Oversized);
        --impl_->pending_updates;
        completion.set_value();
        continue;
      }

      auto full_trace_result = TraceAssembler().build_full_trace(transition.next_trace, trace_key, trace);
      if (full_trace_result.is_error()) {
        --impl_->pending_updates;
        completion.set_error(full_trace_result.move_as_error());
        schedule_trace(trace_key);
        continue;
      }
      auto full_trace = full_trace_result.move_as_ok();
      const auto full_trace_finality = mch::view_finality(full_trace);
      const auto update_seq = full_trace.update_seq;

      slot.classification.emplace(ClassificationWork{
          .trace = std::move(trace),
          .transition = std::move(transition),
          .completion = std::move(completion),
          .measurement = std::move(measurement),
          .contains_real_root = real_root,
      });

      if (impl_->classifiers.empty()) {
        if (slot.classification->measurement) {
          slot.classification->measurement->set_otel_attribute("ton.trace.classification.outcome", "disabled");
        }
        mch::EmuActionPayload payload;
        payload.finality = static_cast<std::uint8_t>(full_trace_finality);
        payload.update_seq = update_seq;
        td::actor::send_closure(actor_id(this), &TraceProcessor::classification_ready, trace_key, std::move(payload));
        continue;
      }

      slot.classification->classification_timer.emplace();
      if (slot.classification->measurement) {
        slot.classification->measurement->start_otel_child_span("classify_trace");
      }
      const auto enqueued_us = mch::emu_now_us();
      auto classification_promise = td::PromiseCreator::lambda(
          [self = actor_id(this), trace_key](td::Result<mch::EmuClassifyResult> result) mutable {
            td::actor::send_closure(self, &TraceProcessor::classification_finished, std::move(trace_key),
                                    std::move(result));
          });
      auto& classifier = impl_->classifiers[impl_->next_classifier++ % impl_->classifiers.size()];
      td::actor::send_closure(classifier, &mch::EmuClassifierActor::classify, std::move(full_trace),
                              enqueued_us, std::move(classification_promise));
      continue;
    } else {
      auto promotion_request = std::move(std::get<PromoteConfirmedRequest>(work));
      insert_completion.regular_promise = std::move(promotion_request.promise);
      promotion = true;
      if (slot.current->nodes.nodes().empty() && slot.current->metadata.empty()) {
        if (!promotion_request.fallback_state) {
          LOG(FATAL) << "Confirmed snapshot has no fallback state for trace " << trace_key;
        }
        slot.current = std::move(promotion_request.fallback_state);
        materialize_full_state = true;
      }
      const auto root_key = trace_metadata_value(*slot.current, "root_node");
      contains_real_root = root_key && promotion_request.trace.nodes.count(*root_key) != 0;
      try {
        prepared_result = prepare_confirmed_promotion(*slot.current, promotion_request.trace, trace_key,
                                                      promotion_request.mc_seqno, materialize_full_state);
      } catch (const std::exception& error) {
        prepared_result =
            td::Status::Error("Got exception while promoting confirmed trace: " + std::string(error.what()));
      }
    }

    if (prepared_result.is_error()) {
      auto error = prepared_result.move_as_error();
      if (promotion) {
        LOG(FATAL) << "Failed to promote confirmed snapshot for trace " << trace_key << ": " << error;
      }
      insert_completion.set_error(std::move(error));
      g_statistics.record_time(INSERT_TRACE, request_timer.elapsed() * 1e3);
      schedule_trace(trace_key);
      continue;
    }

    auto prepared = prepared_result.move_as_ok();
    const auto resulting_nodes =
        prepared.needs_redis_write ? prepared.next_trace.nodes.nodes().size() : slot.current->nodes.nodes().size();
    if (resulting_nodes > kMaxCachedTraceNodes) {
      LOG(WARNING) << "Dropping oversized trace " << trace_key << " with " << resulting_nodes << " nodes; limit is "
                   << kMaxCachedTraceNodes;
      if (request_measurement) {
        request_measurement->set_otel_attribute("ton.trace_state.oversized", true);
      }
      impl_->oversized_traces.insert_or_assign(trace_key, td::Timestamp::in(impl_->retention.open_seconds));
      slot.queued = resolve_terminal_queue(std::move(slot.queued), impl_->pending_updates, TraceCleanupMode::Oversized);
      request_cleanup(trace_key, TraceCleanupMode::Oversized);
      insert_completion.set_value();
      g_statistics.record_time(INSERT_TRACE, request_timer.elapsed() * 1e3);
      continue;
    }

    std::optional<CachedConfirmedTrace> confirmed_trace;
    if (insert_completion.confirmed) {
      const auto& resulting_trace = prepared.needs_redis_write ? prepared.next_trace : *slot.current;
      auto confirmed_result = collect_confirmed_data(resulting_trace, prepared);
      if (confirmed_result.is_error()) {
        insert_completion.set_error(confirmed_result.move_as_error());
        g_statistics.record_time(INSERT_TRACE, request_timer.elapsed() * 1e3);
        schedule_trace(trace_key);
        continue;
      }
      confirmed_trace = confirmed_result.move_as_ok();
    }
    if (!prepared.needs_redis_write) {
      if (contains_real_root) {
        real_root_applied(slot);
        update_lifecycle(trace_key);
      }
      if (confirmed_trace) {
        insert_completion.set_value(make_confirmed_snapshot(trace_key, slot.current, std::move(*confirmed_trace)));
      } else {
        insert_completion.set_value();
      }
      g_statistics.record_time(INSERT_TRACE, request_timer.elapsed() * 1e3);
      schedule_trace(trace_key);
      continue;
    }

    if (request_measurement) {
      request_measurement->set_otel_attribute("ton.trace_state.carried_redis_writes_count",
                                              static_cast<std::int64_t>(slot.dirty.plans.size()));
    }
    RedisWriteBatch batch = std::move(slot.dirty);
    batch.plans.push_back(std::move(prepared.redis));

    auto completion = [self = actor_id(this), trace_key](td::Status status, RedisWriteBatch finished_batch) mutable {
      td::actor::send_closure(self, &TraceProcessor::write_finished, std::move(trace_key), std::move(status),
                              std::move(finished_batch));
    };

    slot.in_flight.emplace(InFlightWork{
        .kind = InFlightKind::Update,
        .next_trace = std::move(prepared.next_trace),
        .completion = std::move(insert_completion),
        .contains_real_root = contains_real_root,
        .confirmed_trace = std::move(confirmed_trace),
    });
    ++impl_->active_writes;
    impl_->materializer.write(std::move(batch), std::move(completion), request_timer);
  }
}

void TraceProcessor::classification_finished(std::string trace_key, td::Result<mch::EmuClassifyResult> result) {
  auto slot_it = impl_->traces.find(trace_key);
  if (slot_it == impl_->traces.end() || !slot_it->second.classification) {
    LOG(FATAL) << "Got classification result for unknown trace " << trace_key;
  }

  auto& work = *slot_it->second.classification;
  if (result.is_error()) {
    if (work.measurement) {
      work.measurement->set_otel_attribute("ton.trace.classification.outcome", "response_lost");
      work.measurement->set_otel_attribute("ton.trace.classification.failed", true);
    }
    auto error = result.move_as_error();
    LOG(WARNING) << "[mch-emu] classify response lost for trace " << trace_key << ": " << error;
    mch::EmuActionPayload payload;
    payload.state = "response_lost";
    payload.finality = trace_finality(work.transition.next_trace);
    payload.update_seq = work.transition.next_trace.update_seq;
    classification_ready(std::move(trace_key), std::move(payload));
    return;
  }

  auto classified = result.move_as_ok();
  fill_classification_measurement(classified, work.measurement);
  if (classified.trace_id != trace_key) {
    LOG(FATAL) << "Classifier returned trace " << classified.trace_id << " while processing " << trace_key;
  }
  classification_ready(std::move(trace_key), std::move(classified.payload));
}

void TraceProcessor::classification_ready(std::string trace_key, mch::EmuActionPayload payload) {
  auto slot_it = impl_->traces.find(trace_key);
  if (slot_it == impl_->traces.end() || !slot_it->second.classification) {
    LOG(FATAL) << "Got actions for unknown trace " << trace_key;
  }

  auto& slot = slot_it->second;
  if (slot.classification->classification_timer) {
    g_statistics.record_time(CLASSIFY_TRACE, slot.classification->classification_timer->elapsed() * 1e6);
    slot.classification->classification_timer.reset();
  }
  const auto expected_update_seq = slot.classification->transition.next_trace.update_seq;
  const auto expected_finality = trace_finality(slot.classification->transition.next_trace);
  if (payload.update_seq != expected_update_seq || payload.finality != expected_finality) {
    if (slot.classification->measurement) {
      slot.classification->measurement->set_otel_attribute("ton.trace.classification.outcome", "invalid_result");
      slot.classification->measurement->set_otel_attribute("ton.trace.classification.failed", true);
    }
    auto work = std::move(*slot.classification);
    slot.classification.reset();
    --impl_->pending_updates;
    work.completion.set_error(td::Status::Error("Classifier result does not match the assembled full trace"));
    schedule_trace(trace_key);
    start_next_operations();
    return;
  }

  slot.classification->insert_timer.resume();
  if (slot.classification->measurement) {
    slot.classification->measurement->end_otel_child_span("classify_trace");
    // The producer callback ends this span after attaching any insertion error.
    slot.classification->measurement->start_otel_child_span("insert_trace");
  }
  slot.classification->payload = std::move(payload);
  schedule_trace(trace_key);
  start_next_operations();
}

void TraceProcessor::materialize_classified_trace(std::string trace_key) {
  auto slot_it = impl_->traces.find(trace_key);
  if (slot_it == impl_->traces.end() || !slot_it->second.classification || !slot_it->second.classification->payload) {
    LOG(FATAL) << "Tried to write an unclassified trace " << trace_key;
  }

  auto& slot = slot_it->second;
  auto work = std::move(*slot.classification);
  slot.classification.reset();
  auto payload = std::move(*work.payload);

  td::Result<PreparedTraceUpdate> prepared_result;
  try {
    prepared_result = prepare_trace_materialization(*slot.current, std::move(work.transition), work.trace, payload,
                                                    trace_key, work.measurement);
  } catch (const vm::VmError& error) {
    prepared_result = td::Status::Error("Got VmError while materializing trace: " + std::string(error.get_msg()));
  } catch (const std::exception& error) {
    prepared_result = td::Status::Error("Got exception while materializing trace: " + std::string(error.what()));
  }

  if (prepared_result.is_error()) {
    --impl_->pending_updates;
    work.completion.set_error(prepared_result.move_as_error());
    g_statistics.record_time(INSERT_TRACE, work.insert_timer.elapsed() * 1e3);
    schedule_trace(trace_key);
    start_next_operations();
    return;
  }

  auto prepared = prepared_result.move_as_ok();
  std::optional<CachedConfirmedTrace> confirmed_trace;
  if (work.completion.confirmed) {
    const auto& resulting_trace = prepared.needs_redis_write ? prepared.next_trace : *slot.current;
    auto confirmed_result = collect_confirmed_data(resulting_trace, prepared);
    if (confirmed_result.is_error()) {
      --impl_->pending_updates;
      work.completion.set_error(confirmed_result.move_as_error());
      g_statistics.record_time(INSERT_TRACE, work.insert_timer.elapsed() * 1e3);
      schedule_trace(trace_key);
      start_next_operations();
      return;
    }
    confirmed_trace = confirmed_result.move_as_ok();
  }

  if (!prepared.needs_redis_write) {
    --impl_->pending_updates;
    if (work.contains_real_root) {
      real_root_applied(slot);
      update_lifecycle(trace_key);
    }
    if (confirmed_trace) {
      work.completion.set_value(make_confirmed_snapshot(trace_key, slot.current, std::move(*confirmed_trace)));
    } else {
      work.completion.set_value();
    }
    g_statistics.record_time(INSERT_TRACE, work.insert_timer.elapsed() * 1e3);
    schedule_trace(trace_key);
    start_next_operations();
    return;
  }

  if (work.measurement) {
    work.measurement->set_otel_attribute("ton.trace_state.carried_redis_writes_count",
                                         static_cast<std::int64_t>(slot.dirty.plans.size()));
  }
  RedisWriteBatch batch = std::move(slot.dirty);
  batch.plans.push_back(std::move(prepared.redis));

  auto completion = [self = actor_id(this), trace_key](td::Status status, RedisWriteBatch finished_batch) mutable {
    td::actor::send_closure(self, &TraceProcessor::write_finished, std::move(trace_key), std::move(status),
                            std::move(finished_batch));
  };

  slot.in_flight.emplace(InFlightWork{
      .kind = InFlightKind::Update,
      .next_trace = std::move(prepared.next_trace),
      .completion = std::move(work.completion),
      .contains_real_root = work.contains_real_root,
      .counted_update = true,
      .confirmed_trace = std::move(confirmed_trace),
  });
  ++impl_->active_writes;
  impl_->materializer.write(std::move(batch), std::move(completion), work.insert_timer);
}

void TraceProcessor::write_finished(std::string trace_key, td::Status status, RedisWriteBatch batch) {
  auto slot_it = impl_->traces.find(trace_key);
  if (slot_it == impl_->traces.end() || !slot_it->second.in_flight) {
    LOG(FATAL) << "Got completion for unknown trace write " << trace_key;
  }
  --impl_->active_writes;

  auto& slot = slot_it->second;
  auto in_flight = std::move(*slot.in_flight);
  slot.in_flight.reset();

  if (in_flight.kind == InFlightKind::Cleanup) {
    if (status.is_error()) {
      LOG(ERROR) << "Redis cleanup failed for trace " << trace_key
                 << "; retrying while the cleanup is still relevant: " << status;
      slot.cleanup_requested = false;
      slot.deadline = td::Timestamp::in(kCleanupRetrySeconds);
      if (cleanup_waits_for_current_updates(slot.cleanup_mode)) {
        schedule_trace(trace_key);
      }
    } else {
      if (slot.cleanup_requested && publishes_invalidation(slot.cleanup_mode) &&
          !publishes_invalidation(in_flight.cleanup_mode)) {
        // Invalidation can arrive while a normal retention cleanup is
        // already in Redis. Run one small follow-up cleanup so its
        // notification is not lost.
        schedule_trace(trace_key);
        start_next_operations();
        return;
      }
      auto queued = std::move(slot.queued);
      if (cleanup_is_terminal(slot.cleanup_mode)) {
        queued = resolve_terminal_queue(std::move(queued), impl_->pending_updates, slot.cleanup_mode);
      }

      if (queued.empty()) {
        impl_->traces.erase(slot_it);
      } else {
        TraceSlot replacement;
        replacement.queued = std::move(queued);
        slot = std::move(replacement);
        schedule_trace(trace_key);
      }
    }
    start_next_operations();
    return;
  }

  if (in_flight.counted_update) {
    --impl_->pending_updates;
  }

  if (status.is_error()) {
    auto error = status.to_string();
    LOG(ERROR) << "Redis write failed for trace " << trace_key
               << "; carrying its data changes into the next patch: " << error;
    remember_failed_write(slot, std::move(in_flight.next_trace), std::move(batch));
    if (in_flight.contains_real_root) {
      real_root_applied(slot);
    }
    update_lifecycle(trace_key);
    in_flight.completion.set_error(std::move(status));
  } else {
    slot.current = std::make_shared<const ActiveTrace>(std::move(in_flight.next_trace));
    slot.dirty = RedisWriteBatch{};
    if (in_flight.contains_real_root) {
      real_root_applied(slot);
    }
    update_lifecycle(trace_key);
    if (in_flight.confirmed_trace) {
      in_flight.completion.set_value(
          make_confirmed_snapshot(trace_key, slot.current, std::move(*in_flight.confirmed_trace)));
    } else {
      in_flight.completion.set_value();
    }
  }

  schedule_trace(trace_key);
  start_next_operations();
}

void TraceProcessor::promote_confirmed(std::vector<ConfirmedTraceSnapshot> snapshots, ton::BlockSeqno mc_seqno,
                                       td::Promise<td::Unit> promise) {
  struct TracePromotion {
    std::shared_ptr<const ActiveTrace> fallback_state;
    CachedConfirmedTrace confirmed;
  };

  std::map<std::string, TracePromotion> promotions;
  for (const auto& snapshot : snapshots) {
    if (!snapshot || !snapshot->state) {
      LOG(FATAL) << "Got an empty confirmed snapshot for mc " << mc_seqno;
    }
    if (touch_oversized_trace(snapshot->trace_key)) {
      continue;
    }
    auto& promotion = promotions[snapshot->trace_key];
    if (!promotion.fallback_state || snapshot->state->update_seq > promotion.fallback_state->update_seq) {
      promotion.fallback_state = snapshot->state;
    }
    for (const auto& [key, node] : snapshot->confirmed.nodes) {
      promotion.confirmed.nodes.insert_or_assign(key, node);
    }
    promotion.confirmed.account_states.insert(promotion.confirmed.account_states.end(),
                                              snapshot->confirmed.account_states.begin(),
                                              snapshot->confirmed.account_states.end());
  }

  if (promotions.empty()) {
    promise.set_value(td::Unit());
    return;
  }
  auto completion = std::make_shared<ConfirmedPromotionCompletion>(ConfirmedPromotionCompletion{
      .remaining = promotions.size(),
      .promise = std::move(promise),
  });
  for (auto& [trace_key, promotion] : promotions) {
    auto& slot = impl_->traces[trace_key];
    if (slot.current->root_account) {
      impl_->candidates.forget(*slot.current->root_account, trace_key);
    }
    // An exact finalized block is canonical. It supersedes a queued TTL
    // cleanup or invalidation, and its snapshot can rebuild an evicted
    // trace after an already-running cleanup finishes.
    slot.cleanup_requested = false;
    slot.cleanup_mode = TraceCleanupMode::Retention;
    slot.queued.push_back(PromoteConfirmedRequest{
        .fallback_state = std::move(promotion.fallback_state),
        .trace = std::move(promotion.confirmed),
        .mc_seqno = mc_seqno,
        .promise = td::PromiseCreator::lambda(
            [completion](td::Result<td::Unit> result) mutable { completion->one_finished(std::move(result)); }),
    });
    schedule_trace(trace_key);
  }
  start_next_operations();
}

void TraceProcessor::alarm() {
  auto now = td::Timestamp::now();
  if (!impl_->next_queue_stats_log || impl_->next_queue_stats_log.is_in_past(now)) {
    impl_->next_queue_stats_log = td::Timestamp::in(kQueueStatsLogIntervalSeconds);
    const auto snapshot = collect_queue_snapshot(impl_->traces);
    LOG(INFO) << "Trace processor queues: "
              << format_queue_snapshot(snapshot, impl_->pending_updates, impl_->traces.size(), impl_->active_writes,
                                       impl_->ready_traces.size(), impl_->ready_writes.size());
  }

  std::vector<std::pair<std::string, TraceCleanupMode>> expired;
  for (const auto& [trace_key, slot] : impl_->traces) {
    if (slot.cleanup_requested || !slot.deadline || !slot.deadline.is_in_past(now)) {
      continue;
    }
    if (cleanup_waits_for_current_updates(slot.cleanup_mode) &&
        (slot.classification || slot.in_flight || !slot.queued.empty())) {
      continue;
    }
    expired.emplace_back(trace_key, slot.cleanup_mode);
  }
  for (const auto& [trace_key, mode] : expired) {
    request_cleanup(trace_key, mode);
  }

  for (auto it = impl_->oversized_traces.begin(); it != impl_->oversized_traces.end();) {
    if (it->second.is_in_past(now) && impl_->traces.count(it->first) == 0) {
      it = impl_->oversized_traces.erase(it);
    } else {
      ++it;
    }
  }

  alarm_timestamp() = td::Timestamp::in(kExpirySweepSeconds);
  start_next_operations();
}

void TraceProcessor::invalidate(std::vector<td::Bits256> trace_hashes) {
  for (const auto& hash : trace_hashes) {
    auto trace_key = td::base64_encode(hash.as_slice());
    request_cleanup(trace_key, TraceCleanupMode::Invalidation);
  }
  start_next_operations();
}

void TraceProcessor::start_replaced_confirmed_root_ttl(const std::string& trace_key) {
  auto it = impl_->traces.find(trace_key);
  if (it == impl_->traces.end()) {
    return;
  }
  auto& slot = it->second;
  const auto root_key = trace_metadata_value(*slot.current, "root_node").value_or(std::string{});
  const auto* root = slot.current->nodes.find(root_key);
  const bool already_waiting = slot.cleanup_mode == TraceCleanupMode::ReplacedConfirmedTimeout;
  if (cleanup_is_terminal(slot.cleanup_mode) ||
      (root && root->finality != TraceStateFinality::Confirmed && !already_waiting)) {
    return;
  }

  if (slot.current->root_account) {
    impl_->candidates.forget(*slot.current->root_account, trace_key);
  }
  slot.cleanup_mode = TraceCleanupMode::ReplacedConfirmedTimeout;
  if (!already_waiting) {
    slot.deadline = td::Timestamp::in(impl_->retention.root_replaced_confirmed_seconds);
  }
  slot.cleanup_requested = false;
  update_lifecycle(trace_key);
  LOG(INFO) << "Confirmed root of trace " << trace_key << " was replaced; waiting "
            << impl_->retention.root_replaced_confirmed_seconds << "s for another inclusion";
}

void TraceProcessor::mark_confirmed_roots_replaced(std::vector<td::Bits256> trace_hashes) {
  for (const auto& trace_hash : trace_hashes) {
    auto trace_key = td::base64_encode(trace_hash.as_slice());
    auto& slot = impl_->traces[trace_key];
    if (cleanup_is_terminal(slot.cleanup_mode)) {
      continue;
    }
    // The marker is ordered with trace updates. Old confirmed patches run
    // first; a later real root runs after it and cancels the grace period.
    slot.queued.emplace_back(ConfirmedRootReplacedRequest{});
    schedule_trace(trace_key);
  }
  start_next_operations();
}

void TraceProcessor::tear_down() {
  for (auto& [_, slot] : impl_->traces) {
    if (slot.classification) {
      slot.classification->completion.set_error(
          td::Status::Error("TraceProcessor stopped during trace classification"));
    }
    if (slot.in_flight && slot.in_flight->kind == InFlightKind::Update) {
      slot.in_flight->completion.set_error(td::Status::Error("TraceProcessor stopped during trace write"));
    }
    for (auto& work : slot.queued) {
      if (auto* request = std::get_if<InsertRequest>(&work)) {
        request->completion.set_error(td::Status::Error("TraceProcessor stopped before trace write"));
      } else if (auto* promotion = std::get_if<PromoteConfirmedRequest>(&work)) {
        promotion->promise.set_error(td::Status::Error("TraceProcessor stopped before confirmed state promotion"));
      }
    }
  }
  impl_->traces.clear();
  impl_->oversized_traces.clear();
  impl_->ready_traces.clear();
  impl_->ready_writes.clear();
  impl_->pending_updates = 0;
}
