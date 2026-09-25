#include "TraceMaterialization.h"

#include <algorithm>
#include <set>
#include <sstream>

#include "Serializer.hpp"
#include "TraceInterfaceDetector.h"

namespace trace_materialization {
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
  plan.publications.emplace_back(kStreamingTransactionsChannel,
                                 pack_streaming_hint(StreamingTransactionHint{
                                     .trace_key = trace_key,
                                     .update_seq = trace.update_seq,
                                     .update_finality = static_cast<std::uint8_t>(update_finality),
                                     .trace_finality = snapshot_finality,
                                     .accounts = std::move(accounts),
                                 }));
}

void append_streaming_actions_hint(RedisWritePlan& plan, const ActiveTrace& trace, const std::string& trace_key,
                                   StreamingUpdateFinality update_finality, bool actions_updated) {
  // Replay must not expose a retained action blob from a failed classification.
  plan.fields_to_set.emplace_back("streaming_actions_updated", actions_updated ? "1" : "0");
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

PreparedActionUpdate prepare_action_update(const ActionState& current, const mch::EmuActionPayload& payload) {
  PreparedActionUpdate prepared;
  prepared.state = current;
  prepared.state.blob_is_current = false;
  if (payload.state == nullptr) {
    return prepared;
  }
  // This classification belongs to the accepted graph in the serialized trace
  // queue. A new canonical branch may have a pending tail even when the old
  // branch was complete; its fresh actions must replace the old branch's blob.

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

namespace {
td::Status append_account(RedisWritePlan& plan, const block::StdAddress& address, const block::Account& account,
                          const std::vector<Trace::Detector::DetectedInterface>* interfaces, FinalityState finality) {
  auto redis_account_result = parse_account(account);
  if (redis_account_result.is_error()) {
    return redis_account_result.move_as_error_prefix("Failed to parse account: ");
  }

  std::stringstream state_buffer;
  msgpack::pack(state_buffer, redis_account_result.move_as_ok());

  std::stringstream interfaces_buffer;
  if (interfaces) {
    msgpack::pack(interfaces_buffer, parse_interfaces(*interfaces));
  }

  switch (finality) {
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
      .finality = finality,
      .state = state_buffer.str(),
      .interfaces = interfaces_buffer.str(),
  });
  return td::Status::OK();
}
}  // namespace

td::Status append_account_state_writes(RedisWritePlan& plan, const Trace& trace) {
  for (const auto& [address, account] : trace.committed_accounts) {
    auto it = trace.committed_interfaces.find(address);
    TRY_STATUS(append_account(plan, address, account, it == trace.committed_interfaces.end() ? nullptr : &it->second,
                              trace.root->finality_state));
  }
  return td::Status::OK();
}

td::Status append_account_state_writes(RedisWritePlan& plan, const DetectedAccounts& accounts, FinalityState finality) {
  for (const auto& [address, account] : accounts.states) {
    auto it = accounts.interfaces.find(address);
    TRY_STATUS(
        append_account(plan, address, account, it == accounts.interfaces.end() ? nullptr : &it->second, finality));
  }
  return td::Status::OK();
}

td::Status append_account_state_writes(RedisWritePlan& plan, const TraceUpdate& update) {
  for (const auto& fragment : update.fragments) {
    TRY_STATUS(append_account_state_writes(plan, fragment));
  }
  return td::Status::OK();
}

void append_publications(RedisWritePlan& plan, const TraceTransition& transition, FinalityState update_finality,
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
    const auto finalized = update_finality == FinalityState::Finalized;
    auto streaming_finality = finalized ? StreamingUpdateFinality::Finalized : StreamingUpdateFinality::Confirmed;
    append_streaming_transaction_hint(plan, transition.next_trace, trace_key, streaming_finality);
  }
  if (has_pending_transactions) {
    append_streaming_transaction_hint(plan, transition.next_trace, trace_key, StreamingUpdateFinality::Pending);
  }
  append_streaming_actions_hint(plan, transition.next_trace, trace_key, streaming_update_finality(update_finality),
                                actions_updated);
}

td::Result<RedisWritePlan> build_redis_plan(const TraceTransition& transition,
                                            const PreparedActionUpdate& action_update, const TraceUpdate& update,
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

  auto account_states_status = append_account_state_writes(plan, update);
  if (account_states_status.is_error()) {
    return account_states_status;
  }
  append_publications(plan, transition, update.fragments.front().root->finality_state, trace_key,
                      action_update.actions_updated);
  return plan;
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
                                                              const TraceUpdate& update,
                                                              const mch::EmuActionPayload& payload,
                                                              const std::string& trace_key,
                                                              const MeasurementPtr& measurement) {
  if (payload.state != nullptr && measurement) {
    measurement->set_otel_attribute("ton.actions.count", static_cast<std::int64_t>(payload.action_count));
  }

  if (!transition.needs_redis_write) {
    return PreparedTraceUpdate{};
  }

  auto action_update = prepare_action_update(current.actions, payload);
  transition.next_trace.actions = action_update.state;
  auto redis_result = build_redis_plan(transition, action_update, update, trace_key);
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

bool finalized_trace_is_open(const ActiveTrace& trace) {
  if (!trace.root()) {
    return true;
  }
  for (const auto& [_, node] : trace.nodes.nodes()) {
    for (const auto& child : node.internal_child_keys) {
      if (!trace.nodes.find(child)) {
        return true;
      }
    }
  }
  return false;
}

RedisWritePlan build_finalized_snapshot(const std::string& trace_key, const ActiveTrace& trace, std::uint32_t ttl) {
  RedisWritePlan plan;
  plan.trace_key = trace_key;
  plan.replace_trace = true;
  plan.expire_seconds = ttl;
  plan.raw_external_message_hash = trace_metadata_value(trace, "root_node").value_or(std::string{});
  for (const auto& [field, value] : trace.metadata) {
    plan.fields_to_set.emplace_back(field, value);
  }
  plan.fields_to_set.emplace_back("update_seq", std::to_string(trace.update_seq));
  // Only ton-finalized-streamer: finalized subscriptions must wait for all internal continuations.
  plan.fields_to_set.emplace_back("trace_complete", finalized_trace_is_open(trace) ? "0" : "1");
  for (const auto& [key, node] : trace.nodes.nodes()) {
    CHECK(node.serialized);
    plan.fields_to_set.emplace_back(key, *node.serialized);
    plan.indexes_to_add.insert(plan.indexes_to_add.end(), node.index_refs.begin(), node.index_refs.end());
  }
  if (trace.actions.blob) {
    plan.fields_to_set.emplace_back(kActionsField, *trace.actions.blob);
  }
  if (trace.actions.classify_state) {
    plan.fields_to_set.emplace_back(kActionsStateField, *trace.actions.classify_state);
  }
  if (trace.actions.blob_finality) {
    plan.fields_to_set.emplace_back(kActionsFinalityField, std::to_string(*trace.actions.blob_finality));
  }
  plan.indexes_to_add.insert(plan.indexes_to_add.end(), trace.actions.aai_refs.begin(), trace.actions.aai_refs.end());
  append_streaming_transaction_hint(plan, trace, trace_key, StreamingUpdateFinality::Finalized);
  append_streaming_actions_hint(plan, trace, trace_key, StreamingUpdateFinality::Finalized,
                                trace.actions.blob_is_current);
  return plan;
}

std::vector<TraceStateIndexRef> collect_trace_index_refs(const ActiveTrace& trace) {
  std::set<TraceStateIndexRef> refs;
  for (const auto& [_, node] : trace.nodes.nodes()) {
    refs.insert(node.index_refs.begin(), node.index_refs.end());
  }
  refs.insert(trace.actions.aai_refs.begin(), trace.actions.aai_refs.end());
  return {refs.begin(), refs.end()};
}

RedisWritePlan build_trace_cleanup(const std::string& key, const ActiveTrace& trace) {
  RedisWritePlan plan;
  plan.trace_key = key;
  plan.erase_trace = true;
  plan.indexes_to_remove = collect_trace_index_refs(trace);
  plan.raw_external_message_hash = trace_metadata_value(trace, "root_node").value_or(std::string{});
  return plan;
}
}  // namespace trace_materialization
