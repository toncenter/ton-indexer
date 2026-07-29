#include "TraceInserter.h"

#include "Serializer.hpp"
#include "Statistics.h"
#include "TraceLifecycle.h"
#include "TraceState.h"
#include "td/utils/Timer.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
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

struct AccountStateWrite {
    std::string key;
    std::string lt;
    std::string state;
    std::string interfaces;
};

struct RedisWritePlan {
    std::string trace_key;
    bool erase_trace{false};
    std::vector<std::string> node_fields_to_delete;
    std::vector<TraceStateIndexRef> indexes_to_remove;
    std::vector<TraceStateIndexRef> indexes_to_add;
    std::vector<std::pair<std::string, std::string>> fields_to_set;
    std::vector<AccountStateWrite> account_states;
    std::string raw_external_message_hash;
    std::vector<std::pair<std::string, std::string>> publications;
};

struct RedisWriteBatch {
    std::vector<RedisWritePlan> plans;

    void discard_trace_publications() {
        for (auto& plan : plans) {
            plan.publications.clear();
        }
    }
};

namespace {

constexpr bool kPipelineTransactionCommands = false;
constexpr bool kCreateDedicatedTransactionConnection = false;
constexpr std::size_t kMaxConcurrentWrites = 4;
constexpr std::size_t kMaxQueuedTraceUpdates = 10000;
constexpr std::size_t kMaxCachedTraceNodes = 1000;
constexpr double kCleanupRetrySeconds = 1.0;
constexpr double kExpirySweepSeconds = 1.0;

constexpr const char* kNewTraceChannel = "new_trace";
constexpr const char* kNewPendingTraceChannel = "new_pending_trace";
constexpr const char* kInvalidatedTraceChannel = "invalidated_traces";

constexpr const char* kUpdateAccountStateScript = R"(
    local cur = redis.call('HGET', KEYS[1], 'lt')
    local cur_num = tonumber(cur)
    local new_num = tonumber(ARGV[1])
    if (not cur_num) or (new_num > cur_num) then
        redis.call('HSET', KEYS[1], 'lt', ARGV[1], 'state', ARGV[2], 'interfaces', ARGV[3])
        redis.call('PUBLISH', 'new_account_state', KEYS[1])
    end
    redis.call('EXPIRE', KEYS[1], 60)
    return 1
)";

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

TraceStateFinality to_state_finality(FinalityState finality) {
    return static_cast<TraceStateFinality>(static_cast<std::uint8_t>(finality));
}

bool is_more_final(FinalityState left, FinalityState right) {
    return static_cast<std::uint8_t>(left) > static_cast<std::uint8_t>(right);
}

bool contains_real_root(const Trace& trace) {
    return trace.contains_root_transaction() &&
           trace.root->finality_state != FinalityState::Emulated;
}

std::string node_key(const TraceNode& node) {
    return td::base64_encode(node.node_id.as_slice());
}

std::string account_key(const block::StdAddress& address) {
    return std::to_string(address.workchain) + ":" + address.addr.to_hex();
}

std::string node_fingerprint(const TraceNode& node) {
    if (node.transaction_root.is_null()) {
        return {};
    }

    auto fingerprint = td::base64_encode(node.transaction_root->get_hash().as_slice());
    fingerprint += ":" + account_key(node.address);
    fingerprint += ":" + std::to_string(static_cast<std::uint8_t>(node.finality_state));
    fingerprint += ":" + std::to_string(node.mc_block_seqno);
    fingerprint += ":" + std::to_string(node.block_id.workchain);
    fingerprint += ":" + std::to_string(node.block_id.shard);
    fingerprint += ":" + std::to_string(node.block_id.seqno);
    return fingerprint;
}

std::string node_fingerprint(const RedisTraceNode& node) {
    auto fingerprint = td::base64_encode(node.transaction.hash.as_slice());
    fingerprint += ":" + account_key(node.transaction.account);
    fingerprint += ":" + std::to_string(static_cast<std::uint8_t>(node.finality));
    fingerprint += ":" + std::to_string(node.mc_block_seqno);
    fingerprint += ":" + std::to_string(node.block_id.workchain);
    fingerprint += ":" + std::to_string(node.block_id.shard);
    fingerprint += ":" + std::to_string(node.block_id.seqno);
    return fingerprint;
}

sw::redis::Redis create_redis(const std::string& redis_dsn) {
    sw::redis::Uri uri(redis_dsn);
    auto connection_options = uri.connection_options();
    if (connection_options.connect_timeout == std::chrono::milliseconds{0}) {
        connection_options.connect_timeout = std::chrono::seconds{2};
    }
    if (connection_options.socket_timeout == std::chrono::milliseconds{0}) {
        connection_options.socket_timeout = std::chrono::seconds{5};
    }

    auto pool_options = uri.connection_pool_options();
    pool_options.size = std::max(pool_options.size, kMaxConcurrentWrites);
    if (pool_options.wait_timeout == std::chrono::milliseconds{0}) {
        pool_options.wait_timeout = std::chrono::seconds{5};
    }
    return sw::redis::Redis(connection_options, pool_options);
}

struct AcceptedNode {
    std::string key;
    FinalityState finality{FinalityState::Emulated};
    std::string transaction_hash;
};

using TraceMetadata = std::map<std::string, std::string>;

struct ActiveTrace {
    TraceState nodes;
    TraceMetadata metadata;
    std::uint64_t update_seq{0};
    std::optional<std::string> root_account;
    FinalityState finality{FinalityState::Emulated};
};

struct TraceTransition {
    bool needs_redis_write{false};
    ActiveTrace next_trace;
    TraceStateDelta node_delta;
    TraceMetadata metadata_patch;
    std::vector<AcceptedNode> accepted_nodes;
    std::size_t cached_nodes_count{0};
    std::size_t reused_serializations{0};
    std::string raw_external_message_hash;
};

struct PreparedNodeUpdate {
    TraceStateUpdate state_update;
    std::vector<AcceptedNode> accepted_nodes;
    std::size_t reused_serializations{0};
};

struct PreparedTraceUpdate {
    bool needs_redis_write{false};
    ActiveTrace next_trace;
    RedisWritePlan redis;
    std::vector<AcceptedNode> accepted_nodes;
};

struct CachedConfirmedNode {
    TraceStateNode state;
    std::string transaction_hash;
};

struct CachedConfirmedTrace {
    std::map<std::string, CachedConfirmedNode> nodes;
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
    td::Timer timer;
    bool contains_real_root{false};
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

struct ConfirmedRootReplacedRequest {
};

using TraceRequest =
    std::variant<InsertRequest, PromoteConfirmedRequest, ConfirmedRootReplacedRequest>;

std::deque<TraceRequest> resolve_terminal_queue(
    std::deque<TraceRequest> queued,
    std::size_t& queued_updates,
    TraceCleanupMode mode) {
    std::deque<TraceRequest> surviving;
    while (!queued.empty()) {
        auto work = std::move(queued.front());
        queued.pop_front();

        if (auto* request = std::get_if<InsertRequest>(&work)) {
            --queued_updates;
            g_statistics.record_time(INSERT_TRACE, request->timer.elapsed() * 1e3);
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

struct InFlightWork {
    InFlightKind kind{InFlightKind::Update};
    ActiveTrace next_trace;
    InsertCompletion completion;
    TraceCleanupMode cleanup_mode{TraceCleanupMode::Retention};
    bool contains_real_root{false};
    std::optional<CachedConfirmedTrace> confirmed_trace;
};

struct TraceSlot {
    std::shared_ptr<const ActiveTrace> current{
        std::make_shared<const ActiveTrace>()};
    RedisWriteBatch dirty;

    std::deque<TraceRequest> queued;
    std::optional<InFlightWork> in_flight;
    bool scheduled{false};
    bool cleanup_requested{false};

    TraceLifecycle lifecycle{TraceLifecycle::UnknownRoot};
    td::Timestamp deadline;
    TraceCleanupMode cleanup_mode{TraceCleanupMode::Retention};
};

void real_root_applied(TraceSlot& slot) {
    if (slot.cleanup_mode != TraceCleanupMode::ReplacedConfirmedTimeout) {
        return;
    }
    slot.cleanup_requested = false;
    slot.cleanup_mode = TraceCleanupMode::Retention;
}

void remember_failed_write(TraceSlot& slot,
                           ActiveTrace next_trace,
                           RedisWriteBatch batch) {
    // The logical patch is needed by later partial updates. Its Redis data
    // commands are replayed with the next patch, but its time-sensitive trace
    // notifications are not.
    batch.discard_trace_publications();
    slot.current =
        std::make_shared<const ActiveTrace>(std::move(next_trace));
    slot.dirty = std::move(batch);
}

void add_metadata_change(const ActiveTrace& current,
                         TraceTransition& transition,
                         std::string field,
                         std::string value) {
    auto cached = current.metadata.find(field);
    if (cached == current.metadata.end() || cached->second != value) {
        transition.metadata_patch.emplace(field, value);
        transition.next_trace.metadata.insert_or_assign(std::move(field), std::move(value));
    }
}

std::vector<std::string> actual_child_keys(const TraceNode& node) {
    std::vector<std::string> result;
    result.reserve(node.children.size());
    for (const auto& child : node.children) {
        if (child) {
            result.push_back(node_key(*child));
        }
    }
    return result;
}

td::Result<TraceStateNode> prepare_state_node(const TraceNode& node,
                                             const std::string& key,
                                             const std::string& fingerprint,
                                             const TraceStateNode* cached,
                                             const std::string& trace_key,
                                             std::size_t& reused_serializations) {
    if (cached && cached->fingerprint == fingerprint) {
        ++reused_serializations;
        return *cached;
    }

    auto redis_node_result = parse_trace_node(node);
    if (redis_node_result.is_error()) {
        return redis_node_result.move_as_error_prefix("Failed to parse trace node: ");
    }
    auto redis_node = redis_node_result.move_as_ok();
    if (!redis_node.transaction.in_msg) {
        return td::Status::Error("Trace transaction has no inbound message");
    }

    auto parsed_key = td::base64_encode(redis_node.transaction.in_msg->hash.as_slice());
    if (parsed_key != key) {
        return td::Status::Error("Trace node_id does not match transaction inbound message hash");
    }

    std::stringstream buffer;
    msgpack::pack(buffer, redis_node);

    std::vector<std::string> out_message_keys;
    out_message_keys.reserve(redis_node.transaction.out_msgs.size());
    for (const auto& out_message : redis_node.transaction.out_msgs) {
        out_message_keys.push_back(td::base64_encode(out_message.hash.as_slice()));
    }

    auto index = TraceStateIndexRef{
        .index_key = account_key(redis_node.transaction.account),
        .member = trace_key + ":" + key,
        .score = redis_node.transaction.lt,
    };
    return TraceStateNode{
        .key = key,
        .finality = to_state_finality(redis_node.finality),
        .fingerprint = fingerprint,
        .serialized = std::make_shared<const std::string>(buffer.str()),
        .child_keys = std::move(out_message_keys),
        .index_refs = {std::move(index)},
    };
}

td::Result<PreparedNodeUpdate> prepare_node_update(const ActiveTrace& current,
                                                   const Trace& trace,
                                                   const std::string& trace_key) {
    PreparedNodeUpdate prepared;
    if (!trace.root) {
        return prepared;
    }

    prepared.state_update.root_key = node_key(*trace.root);

    std::queue<TraceNode*> queue;
    queue.push(trace.root.get());
    std::set<std::string> seen;

    while (!queue.empty()) {
        auto* node = queue.front();
        queue.pop();
        if (!node) {
            continue;
        }

        auto key = node_key(*node);
        if (!seen.insert(key).second) {
            return td::Status::Error("Incoming trace contains duplicate node_id");
        }
        auto fingerprint = node_fingerprint(*node);
        if (fingerprint.empty()) {
            return td::Status::Error("Trace node has no transaction cell");
        }

        auto* cached = current.nodes.find(key);
        auto child_keys = actual_child_keys(*node);
        if (cached && cached->finality > to_state_finality(node->finality_state)) {
            prepared.state_update.nodes.push_back(TraceStateNode{
                .key = key,
                .finality = to_state_finality(node->finality_state),
                .fingerprint = std::move(fingerprint),
                .child_keys = std::move(child_keys),
            });
            continue;
        }

        auto state_node_result =
            prepare_state_node(*node,
                               key,
                               fingerprint,
                               cached,
                               trace_key,
                               prepared.reused_serializations);
        if (state_node_result.is_error()) {
            return state_node_result.move_as_error();
        }
        auto state_node = state_node_result.move_as_ok();

        for (const auto& child_key : child_keys) {
            if (std::find(state_node.child_keys.begin(), state_node.child_keys.end(), child_key) ==
                state_node.child_keys.end()) {
                return td::Status::Error("Trace child is not present in parent transaction out messages");
            }
        }
        prepared.state_update.nodes.push_back(std::move(state_node));
        prepared.accepted_nodes.push_back(AcceptedNode{
            .key = key,
            .finality = node->finality_state,
            .transaction_hash = td::base64_encode(node->transaction_root->get_hash().as_slice()),
        });

        for (auto& child : node->children) {
            if (child) {
                queue.push(child.get());
            }
        }
    }
    return prepared;
}

td::Result<TraceTransition> prepare_trace_transition(const ActiveTrace& current,
                                                     const Trace& trace,
                                                     const std::string& trace_key) {
    TraceTransition transition;
    transition.cached_nodes_count = current.nodes.nodes().size();

    auto node_update_result = prepare_node_update(current, trace, trace_key);
    if (node_update_result.is_error()) {
        return node_update_result.move_as_error();
    }
    auto node_update = node_update_result.move_as_ok();
    transition.accepted_nodes = std::move(node_update.accepted_nodes);
    transition.reused_serializations = node_update.reused_serializations;

    if (transition.accepted_nodes.empty()) {
        return transition;
    }

    auto state_change = current.nodes.prepare(node_update.state_update);
    transition.node_delta = std::move(state_change.delta);
    transition.next_trace.nodes.apply(std::move(state_change));
    transition.next_trace.metadata = current.metadata;
    transition.next_trace.update_seq = current.update_seq;
    transition.next_trace.root_account = current.root_account;
    transition.next_trace.finality = current.finality;

    for (const auto& [address, interfaces] : trace.interfaces) {
        auto redis_interfaces = parse_interfaces(interfaces);
        std::stringstream buffer;
        msgpack::pack(buffer, redis_interfaces);
        add_metadata_change(current, transition, account_key(address), buffer.str());
    }

    auto root_account = account_key(trace.root->address);
    transition.raw_external_message_hash = td::base64_encode(trace.ext_in_msg_hash.as_slice());
    if (node_update.state_update.root_key == transition.raw_external_message_hash) {
        transition.next_trace.root_account = std::move(root_account);
    }
    auto root_account_it = trace.emulated_accounts.find(trace.root->address);
    if (root_account_it != trace.emulated_accounts.end() && root_account_it->second.code.not_null()) {
        add_metadata_change(
            current,
            transition,
            "root_account_code_hash",
            td::base64_encode(root_account_it->second.code->get_hash().as_slice()));
    }
    add_metadata_change(
        current, transition, "root_node", transition.raw_external_message_hash);
    add_metadata_change(current,
                        transition,
                        "depth_limit_exceeded",
                        trace.tx_limit_exceeded ? "1" : "0");

    const auto has_state_change =
        !transition.node_delta.empty() || !transition.metadata_patch.empty();
    if (has_state_change) {
        if (current.update_seq == std::numeric_limits<std::uint64_t>::max()) {
            return td::Status::Error("Trace update_seq overflow");
        }
        transition.next_trace.update_seq = current.update_seq + 1;
    }

    transition.next_trace.finality = trace.root->finality_state;
    if (is_more_final(current.finality, transition.next_trace.finality)) {
        transition.next_trace.finality = current.finality;
    }
    // Exact duplicates still refresh legacy TTLs and notifications.
    transition.needs_redis_write = true;
    return transition;
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

        std::string prefix;
        switch (trace.root->finality_state) {
            case FinalityState::Finalized:
                prefix = "account_finalized:";
                break;
            case FinalityState::Confirmed:
                prefix = "account_confirmed:";
                break;
            case FinalityState::Emulated:
                return td::Status::Error("Emulated trace contains committed account states");
        }
        plan.account_states.push_back(AccountStateWrite{
            .key = prefix + account_key(address),
            .lt = std::to_string(account.last_trans_lt_),
            .state = state_buffer.str(),
            .interfaces = interfaces_buffer.str(),
        });
    }
    return td::Status::OK();
}

void append_publications(RedisWritePlan& plan,
                         const TraceTransition& transition,
                         const Trace& trace,
                         const std::string& trace_key) {
    std::string committed_transactions = trace_key + ":";
    bool has_committed_transactions = false;
    bool has_pending_transactions = false;
    for (const auto& accepted : transition.accepted_nodes) {
        if (accepted.finality == FinalityState::Emulated) {
            has_pending_transactions = true;
            continue;
        }
        if (has_committed_transactions) {
            committed_transactions += ",";
        }
        committed_transactions += accepted.transaction_hash;
        has_committed_transactions = true;
    }

    if (has_committed_transactions) {
        auto channel = trace.root->finality_state == FinalityState::Finalized
                           ? "new_finalized_txs"
                           : "new_confirmed_txs";
        plan.publications.emplace_back(channel, committed_transactions);
        plan.publications.emplace_back("new_commited_txs", committed_transactions);
    }
    if (has_pending_transactions) {
        plan.publications.emplace_back(kNewPendingTraceChannel, trace_key);
    }
    plan.publications.emplace_back(kNewTraceChannel, trace_key);
}

td::Result<RedisWritePlan> build_redis_plan(const TraceTransition& transition,
                                            const Trace& trace,
                                            const std::string& trace_key) {
    RedisWritePlan plan;
    plan.trace_key = trace_key;
    plan.node_fields_to_delete = transition.node_delta.removed_node_keys;
    plan.indexes_to_remove = transition.node_delta.removed_index_refs;
    plan.indexes_to_add = transition.node_delta.added_index_refs;
    plan.raw_external_message_hash = transition.raw_external_message_hash;

    plan.fields_to_set.reserve(transition.node_delta.upserted_nodes.size() +
                               transition.metadata_patch.size() + 1);
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
        plan.fields_to_set.emplace_back(
            "update_seq", std::to_string(transition.next_trace.update_seq));
    }

    auto account_states_status = append_account_state_writes(plan, trace);
    if (account_states_status.is_error()) {
        return account_states_status;
    }
    append_publications(plan, transition, trace, trace_key);
    return plan;
}

void fill_measurement(const TraceTransition& transition,
                      const Trace& trace,
                      const MeasurementPtr& measurement) {
    if (!measurement) {
        return;
    }

    measurement->set_otel_attribute(
        "ton.trace_state.accepted_nodes_count",
        static_cast<std::int64_t>(transition.accepted_nodes.size()));
    if (!trace.root) {
        return;
    }
    measurement->set_otel_attribute(
        "ton.trace_state.cached_nodes_count",
        static_cast<std::int64_t>(transition.cached_nodes_count));
    measurement->set_otel_attribute(
        "ton.trace_state.reused_serializations_count",
        static_cast<std::int64_t>(transition.reused_serializations));
    if (!transition.needs_redis_write) {
        return;
    }

    measurement->set_finality(finality_name(trace.root->finality_state));
    measurement->set_operation(trace_emulator_operation(trace.root->finality_state));
    measurement->set_out_channel(kNewTraceChannel);
    measurement->set_otel_attribute(
        "ton.trace_state.upserted_nodes_count",
        static_cast<std::int64_t>(transition.node_delta.upserted_nodes.size()));
    measurement->set_otel_attribute(
        "ton.trace_state.removed_nodes_count",
        static_cast<std::int64_t>(transition.node_delta.removed_node_keys.size()));
    measurement->set_otel_attribute(
        "ton.trace_state.update_seq",
        static_cast<std::int64_t>(transition.next_trace.update_seq));
}

void append_otel_propagation(const MeasurementPtr& measurement, RedisWritePlan& redis_plan) {
    if (!measurement) {
        return;
    }
    for (const auto& [field, value] : measurement->otel_propagation_fields()) {
        redis_plan.fields_to_set.emplace_back(field, value);
    }
}

td::Result<PreparedTraceUpdate> prepare_trace_update(const ActiveTrace& current,
                                                     const Trace& trace,
                                                     const std::string& trace_key,
                                                     const MeasurementPtr& measurement) {
    auto transition_result = prepare_trace_transition(current, trace, trace_key);
    if (transition_result.is_error()) {
        return transition_result.move_as_error();
    }
    auto transition = transition_result.move_as_ok();
    fill_measurement(transition, trace, measurement);

    if (!transition.needs_redis_write) {
        return PreparedTraceUpdate{};
    }

    auto redis_result = build_redis_plan(transition, trace, trace_key);
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

std::optional<std::string> metadata_value(const ActiveTrace& trace,
                                          const std::string& field) {
    auto it = trace.metadata.find(field);
    if (it == trace.metadata.end()) {
        return std::nullopt;
    }
    return it->second;
}

td::Result<CachedConfirmedTrace> collect_confirmed_data(
    const ActiveTrace& resulting_trace,
    const PreparedTraceUpdate& prepared) {
    CachedConfirmedTrace confirmed;
    for (const auto& accepted : prepared.accepted_nodes) {
        if (accepted.finality != FinalityState::Confirmed) {
            continue;
        }
        const auto* node = resulting_trace.nodes.find(accepted.key);
        if (!node || node->finality != TraceStateFinality::Confirmed ||
            !node->serialized) {
            return td::Status::Error(
                "Prepared confirmed node is missing from trace state");
        }
        confirmed.nodes.insert_or_assign(
            accepted.key,
            CachedConfirmedNode{
                .state = *node,
                .transaction_hash = accepted.transaction_hash,
            });
    }
    for (const auto& account : prepared.redis.account_states) {
        confirmed.account_states.push_back(account);
    }
    return confirmed;
}

ConfirmedTraceSnapshot make_confirmed_snapshot(
    const std::string& trace_key,
    std::shared_ptr<const ActiveTrace> state,
    CachedConfirmedTrace confirmed) {
    return std::make_shared<const ConfirmedTraceSnapshotData>(
        ConfirmedTraceSnapshotData{
            .trace_key = trace_key,
            .state = std::move(state),
            .confirmed = std::move(confirmed),
        });
}

td::Result<TraceStateNode> finalize_cached_node(
    const CachedConfirmedNode& cached,
    ton::BlockSeqno mc_seqno) {
    if (!cached.state.serialized) {
        return td::Status::Error(
            "Cached confirmed node has no serialized payload");
    }

    try {
        RedisTraceNode redis_node;
        msgpack::unpack(
            cached.state.serialized->data(),
            cached.state.serialized->size())
            .get()
            .convert(redis_node);
        redis_node.emulated = false;
        redis_node.finality = FinalityState::Finalized;
        redis_node.mc_block_seqno = mc_seqno;

        std::stringstream buffer;
        msgpack::pack(buffer, redis_node);

        auto finalized = cached.state;
        finalized.finality = TraceStateFinality::Finalized;
        finalized.fingerprint = node_fingerprint(redis_node);
        finalized.serialized =
            std::make_shared<const std::string>(buffer.str());
        return finalized;
    } catch (const std::exception& error) {
        return td::Status::Error(
            "Failed to deserialize cached confirmed node: " +
            std::string(error.what()));
    }
}

td::Result<std::vector<TraceStateNode>> prepare_finalized_nodes(
    const ActiveTrace& current,
    const CachedConfirmedTrace& cached,
    ton::BlockSeqno mc_seqno) {
    std::vector<TraceStateNode> finalized_nodes;
    finalized_nodes.reserve(cached.nodes.size());
    for (const auto& [key, cached_node] : cached.nodes) {
        if (cached_node.state.finality !=
                TraceStateFinality::Confirmed ||
            !cached_node.state.serialized) {
            return td::Status::Error(
                "Snapshot contains an invalid confirmed node");
        }
        const auto* current_node = current.nodes.find(key);
        if (current_node &&
            current_node->finality == TraceStateFinality::Finalized) {
            continue;
        }
        TRY_RESULT(finalized, finalize_cached_node(cached_node, mc_seqno));
        finalized_nodes.push_back(std::move(finalized));
    }
    return finalized_nodes;
}

td::Status append_promoted_account_states(
    RedisWritePlan& plan,
    const CachedConfirmedTrace& cached) {
    constexpr std::string_view confirmed_prefix = "account_confirmed:";
    for (const auto& cached_account : cached.account_states) {
        if (!cached_account.key.starts_with(confirmed_prefix)) {
            return td::Status::Error(
                "Cached account state is not confirmed");
        }
        auto account = cached_account;
        account.key =
            "account_finalized:" +
            account.key.substr(confirmed_prefix.size());
        plan.account_states.push_back(std::move(account));
    }
    return td::Status::OK();
}

void append_promoted_publications(
    RedisWritePlan& plan,
    const CachedConfirmedTrace& cached,
    const ActiveTrace& next_trace,
    const std::string& trace_key) {
    std::string finalized_transactions = trace_key + ":";
    bool has_finalized_transactions = false;
    for (const auto& [_, node] : cached.nodes) {
        if (has_finalized_transactions) {
            finalized_transactions += ",";
        }
        finalized_transactions += node.transaction_hash;
        has_finalized_transactions = true;
    }
    if (has_finalized_transactions) {
        plan.publications.emplace_back(
            "new_finalized_txs", finalized_transactions);
        plan.publications.emplace_back(
            "new_commited_txs", finalized_transactions);
    }
    const auto has_pending_nodes = std::any_of(
        next_trace.nodes.nodes().begin(),
        next_trace.nodes.nodes().end(),
        [](const auto& entry) {
            return entry.second.finality == TraceStateFinality::Emulated;
        });
    if (has_pending_nodes) {
        plan.publications.emplace_back(
            kNewPendingTraceChannel, trace_key);
    }
    plan.publications.emplace_back(kNewTraceChannel, trace_key);
}

td::Status append_full_trace_state(
    RedisWritePlan& plan,
    const ActiveTrace& trace) {
    std::set<TraceStateIndexRef> indexes;
    for (const auto& [key, node] : trace.nodes.nodes()) {
        if (!node.serialized) {
            return td::Status::Error(
                "Trace snapshot contains a node without serialized payload");
        }
        plan.fields_to_set.emplace_back(key, *node.serialized);
        indexes.insert(node.index_refs.begin(), node.index_refs.end());
    }
    plan.indexes_to_add.assign(indexes.begin(), indexes.end());
    for (const auto& [field, value] : trace.metadata) {
        plan.fields_to_set.emplace_back(field, value);
    }
    plan.fields_to_set.emplace_back(
        "update_seq", std::to_string(trace.update_seq));
    return td::Status::OK();
}

td::Result<PreparedTraceUpdate> prepare_confirmed_promotion(
    const ActiveTrace& current,
    const CachedConfirmedTrace& cached,
    const std::string& trace_key,
    ton::BlockSeqno mc_seqno,
    bool materialize_full_state) {
    TRY_RESULT(
        finalized_nodes,
        prepare_finalized_nodes(current, cached, mc_seqno));

    PreparedTraceUpdate prepared;
    prepared.next_trace = current;
    auto state_change =
        current.nodes.upsert_nodes(std::move(finalized_nodes));
    auto node_delta = std::move(state_change.delta);
    prepared.next_trace.nodes.apply(std::move(state_change));

    if (!node_delta.empty()) {
        if (current.update_seq ==
            std::numeric_limits<std::uint64_t>::max()) {
            return td::Status::Error("Trace update_seq overflow");
        }
        prepared.next_trace.update_seq = current.update_seq + 1;
    }
    if (const auto root_key = metadata_value(current, "root_node")) {
        if (const auto* root = prepared.next_trace.nodes.find(*root_key)) {
            prepared.next_trace.finality =
                static_cast<FinalityState>(
                    static_cast<std::uint8_t>(root->finality));
        }
    }

    auto& plan = prepared.redis;
    plan.trace_key = trace_key;
    plan.raw_external_message_hash =
        metadata_value(prepared.next_trace, "root_node")
            .value_or(std::string{});
    if (plan.raw_external_message_hash.empty()) {
        return td::Status::Error(
            "Cannot finalize a trace without root_node metadata");
    }

    if (materialize_full_state) {
        TRY_STATUS(append_full_trace_state(
            plan, prepared.next_trace));
    } else {
        plan.node_fields_to_delete = node_delta.removed_node_keys;
        plan.indexes_to_remove = node_delta.removed_index_refs;
        plan.indexes_to_add = node_delta.added_index_refs;
        for (const auto& node : node_delta.upserted_nodes) {
            if (!node.serialized) {
                return td::Status::Error(
                    "Finalized node has no serialized payload");
            }
            plan.fields_to_set.emplace_back(node.key, *node.serialized);
        }
        if (!node_delta.empty()) {
            plan.fields_to_set.emplace_back(
                "update_seq",
                std::to_string(prepared.next_trace.update_seq));
        }
    }

    TRY_STATUS(append_promoted_account_states(plan, cached));
    append_promoted_publications(
        plan, cached, prepared.next_trace, trace_key);
    prepared.needs_redis_write = true;
    return prepared;
}

std::vector<TraceStateIndexRef> collect_cleanup_index_refs(const TraceSlot& slot) {
    std::set<TraceStateIndexRef> refs;
    for (const auto& [_, node] : slot.current->nodes.nodes()) {
        refs.insert(node.index_refs.begin(), node.index_refs.end());
    }
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
    return mode == TraceCleanupMode::PendingTimeout ||
           mode == TraceCleanupMode::ReplacedConfirmedTimeout ||
           mode == TraceCleanupMode::Invalidation;
}

RedisWriteBatch build_cleanup_batch(const std::string& trace_key,
                                    const TraceSlot& slot,
                                    TraceCleanupMode mode) {
    RedisWritePlan plan;
    plan.trace_key = trace_key;
    plan.erase_trace = true;
    plan.indexes_to_remove = collect_cleanup_index_refs(slot);
    plan.raw_external_message_hash =
        metadata_value(*slot.current, "root_node")
            .value_or(std::string{});
    if (publishes_invalidation(mode)) {
        plan.publications.emplace_back(kInvalidatedTraceChannel, trace_key);
    }
    return RedisWriteBatch{.plans = {std::move(plan)}};
}

constexpr bool cleanup_is_terminal(TraceCleanupMode mode) {
    return mode == TraceCleanupMode::Invalidation ||
           mode == TraceCleanupMode::Oversized;
}

constexpr bool cleanup_waits_for_current_updates(TraceCleanupMode mode) {
    return !cleanup_is_terminal(mode);
}

static_assert(cleanup_is_terminal(TraceCleanupMode::Oversized));
static_assert(!publishes_invalidation(TraceCleanupMode::Oversized));

void append_redis_commands(sw::redis::Transaction& transaction, const RedisWritePlan& plan) {
    if (plan.erase_trace) {
        for (const auto& index : plan.indexes_to_remove) {
            transaction.zrem(index.index_key, index.member);
        }
        transaction.unlink(plan.trace_key);
        if (!plan.raw_external_message_hash.empty()) {
            transaction.del("tr_in_msg:" + plan.raw_external_message_hash);
        }
        for (const auto& [channel, message] : plan.publications) {
            transaction.publish(channel, message);
        }
        return;
    }

    if (!plan.node_fields_to_delete.empty()) {
        transaction.hdel(plan.trace_key,
                         plan.node_fields_to_delete.begin(),
                         plan.node_fields_to_delete.end());
    }
    for (const auto& index : plan.indexes_to_remove) {
        transaction.zrem(index.index_key, index.member);
    }
    for (const auto& index : plan.indexes_to_add) {
        transaction.zadd(index.index_key, index.member, index.score);
    }
    if (!plan.fields_to_set.empty()) {
        transaction.hset(plan.trace_key,
                         plan.fields_to_set.begin(),
                         plan.fields_to_set.end());
    }
    for (const auto& account : plan.account_states) {
        transaction.eval(kUpdateAccountStateScript,
                         {account.key},
                         {account.lt, account.state, account.interfaces});
    }

    auto message_key = "tr_in_msg:" + plan.raw_external_message_hash;
    transaction.set(message_key, plan.trace_key);
    transaction.expire(message_key, 600);

    for (const auto& [channel, message] : plan.publications) {
        transaction.publish(channel, message);
    }
}

class RedisTraceWriter : public td::actor::Actor {
public:
    using Completion = std::function<void(td::Status, RedisWriteBatch)>;

    RedisTraceWriter(sw::redis::Transaction&& transaction,
                     RedisWriteBatch batch,
                     Completion completion,
                     td::Timer timer)
        : transaction_(std::move(transaction))
        , batch_(std::move(batch))
        , completion_(std::move(completion))
        , timer_(timer) {
    }

private:
    sw::redis::Transaction transaction_;
    RedisWriteBatch batch_;
    Completion completion_;
    td::Timer timer_;

    void start_up() override {
        auto status = td::Status::OK();
        try {
            for (const auto& plan : batch_.plans) {
                append_redis_commands(transaction_, plan);
            }

            auto replies = transaction_.exec();
            for (std::size_t index = 0; index < replies.size(); ++index) {
                // Accessing each reply makes redis-plus-plus surface errors
                // returned by individual commands inside EXEC.
                replies.get(index);
            }
        } catch (const std::exception& error) {
            status = td::Status::Error("Failed to write trace to Redis: " +
                                       std::string(error.what()));
        } catch (...) {
            status = td::Status::Error("Failed to write trace to Redis: unknown error");
        }
        g_statistics.record_time(INSERT_TRACE, timer_.elapsed() * 1e3);
        completion_(std::move(status), std::move(batch_));
        stop();
    }
};

}  // namespace

td::Status flush_pending_redis_database(const std::string& redis_dsn) {
    try {
        auto redis = create_redis(redis_dsn);
        redis.flushdb();
        return td::Status::OK();
    } catch (const std::exception& error) {
        return td::Status::Error(
            "Failed to flush pending Redis database: " +
            std::string(error.what()));
    } catch (...) {
        return td::Status::Error(
            "Failed to flush pending Redis database: unknown error");
    }
}

struct RedisInsertManager::Impl {
    Impl(const std::string& redis_dsn, TraceRetentionConfig retention_config)
        : redis(create_redis(redis_dsn))
        , retention(std::move(retention_config)) {
    }

    sw::redis::Redis redis;
    TraceRetentionConfig retention;
    std::unordered_map<std::string, TraceSlot> traces;
    std::unordered_map<std::string, td::Timestamp> oversized_traces;
    CompetingTraceSet candidates;
    std::deque<std::string> ready_traces;
    std::size_t queued_updates{0};
    std::size_t active_writes{0};
};

RedisInsertManager::RedisInsertManager(const std::string& redis_dsn,
                                       TraceRetentionConfig retention)
    : impl_(std::make_unique<Impl>(redis_dsn, std::move(retention))) {
}

RedisInsertManager::~RedisInsertManager() = default;

void RedisInsertManager::start_up() {
    alarm_timestamp() = td::Timestamp::in(kExpirySweepSeconds);
}

bool RedisInsertManager::touch_oversized_trace(
    const std::string& trace_key) {
    auto it = impl_->oversized_traces.find(trace_key);
    if (it == impl_->oversized_traces.end()) {
        return false;
    }
    it->second = td::Timestamp::in(impl_->retention.open_seconds);
    return true;
}

void RedisInsertManager::schedule_trace(const std::string& trace_key) {
    auto it = impl_->traces.find(trace_key);
    if (it == impl_->traces.end()) {
        return;
    }
    auto& slot = it->second;
    if (slot.scheduled || slot.in_flight ||
        (slot.queued.empty() && !slot.cleanup_requested)) {
        return;
    }
    slot.scheduled = true;
    impl_->ready_traces.push_back(trace_key);
}

void RedisInsertManager::request_cleanup(const std::string& trace_key,
                                         TraceCleanupMode mode) {
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

void RedisInsertManager::update_lifecycle(const std::string& trace_key) {
    auto it = impl_->traces.find(trace_key);
    if (it == impl_->traces.end()) {
        return;
    }
    auto& slot = it->second;
    const auto previous_lifecycle = slot.lifecycle;
    const auto root_node =
        metadata_value(*slot.current, "root_node")
            .value_or(std::string{});
    slot.lifecycle =
        classify_trace_lifecycle(slot.current->nodes, root_node);

    // A terminally dropped trace is already on its way out. Updating its
    // lifecycle would leave stale retention state behind.
    if (cleanup_is_terminal(slot.cleanup_mode)) {
        return;
    }

    if (slot.current->root_account) {
        impl_->candidates.forget(*slot.current->root_account, trace_key);
    }

    const auto code_hash =
        metadata_value(*slot.current, "root_account_code_hash")
            .value_or(std::string{});

    // Continuation patches do not make a replaced root real again. Keep the
    // dedicated deadline until another confirmed/finalized root patch arrives.
    if (slot.cleanup_mode == TraceCleanupMode::ReplacedConfirmedTimeout) {
        if (slot.current->root_account &&
            wallet_external_messages_compete(code_hash)) {
            impl_->candidates.remember(*slot.current->root_account, trace_key);
        }
        // The serialized root is kept during the grace period, but for
        // lifecycle transitions it is unresolved until another block accepts it.
        slot.lifecycle = TraceLifecycle::UnknownRoot;
        return;
    }

    const bool competing_candidate =
        slot.lifecycle == TraceLifecycle::RootPending &&
        slot.current->root_account.has_value() &&
        wallet_external_messages_compete(code_hash);

    std::vector<std::string> traces_to_invalidate;
    if (competing_candidate) {
        impl_->candidates.remember(*slot.current->root_account, trace_key);
    } else if (trace_root_became_real(previous_lifecycle, slot.lifecycle) &&
               slot.current->root_account &&
               wallet_external_messages_compete(code_hash)) {
        traces_to_invalidate =
            impl_->candidates.accept(*slot.current->root_account, trace_key);
    }

    const auto next_cleanup_mode =
        competing_candidate
            ? TraceCleanupMode::PendingTimeout
            : TraceCleanupMode::Retention;

    // A pending root uses an absolute deadline. Repeated emulation updates
    // cannot keep an external message alive forever.
    const bool keep_pending_deadline =
        slot.lifecycle == TraceLifecycle::RootPending &&
        previous_lifecycle == TraceLifecycle::RootPending &&
        slot.deadline;
    slot.cleanup_mode = next_cleanup_mode;
    if (!keep_pending_deadline) {
        slot.deadline = td::Timestamp::in(
            trace_retention_seconds(slot.lifecycle, impl_->retention));
    }

    for (const auto& candidate : traces_to_invalidate) {
        request_cleanup(candidate, TraceCleanupMode::Invalidation);
    }
}

void RedisInsertManager::insert(Trace trace,
                                td::Promise<td::Unit> promise,
                                MeasurementPtr measurement) {
    insert_impl(
        std::move(trace),
        false,
        std::move(promise),
        {},
        std::move(measurement));
}

void RedisInsertManager::insert_confirmed(
    Trace trace,
    td::Promise<ConfirmedTraceSnapshot> promise,
    MeasurementPtr measurement) {
    insert_impl(
        std::move(trace),
        true,
        {},
        std::move(promise),
        std::move(measurement));
}

void RedisInsertManager::insert_impl(
    Trace trace,
    bool confirmed,
    td::Promise<td::Unit> regular_promise,
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
            measurement->set_otel_attribute(
                "ton.trace_state.oversized", true);
        }
        completion.set_value();
        return;
    }

    const bool real_root = contains_real_root(trace);
    auto slot_it = impl_->traces.find(trace_key);
    if (slot_it != impl_->traces.end() &&
        cleanup_is_terminal(slot_it->second.cleanup_mode)) {
        if (confirmed) {
            completion.set_error(td::Status::Error(
                "Confirmed trace is already being invalidated"));
        } else {
            completion.set_value();
        }
        return;
    }
    if (impl_->queued_updates >= kMaxQueuedTraceUpdates) {
        completion.set_error(td::Status::Error(
            "Redis trace writer queue is full (" + std::to_string(kMaxQueuedTraceUpdates) + ")"));
        return;
    }

    auto& slot = impl_->traces[trace_key];
    if (real_root &&
        slot.cleanup_mode == TraceCleanupMode::ReplacedConfirmedTimeout &&
        (!slot.in_flight || slot.in_flight->kind != InFlightKind::Cleanup)) {
        // Let the root patch run before an expiry that was only queued by the
        // periodic sweep.
        slot.cleanup_requested = false;
    }
    slot.queued.push_back(InsertRequest{
        .trace = std::move(trace),
        .completion = std::move(completion),
        .measurement = std::move(measurement),
        .timer = td::Timer(),
        .contains_real_root = real_root,
    });
    ++impl_->queued_updates;
    schedule_trace(trace_key);
    start_next_writes();
}

void RedisInsertManager::start_next_writes() {
    while (impl_->active_writes < kMaxConcurrentWrites && !impl_->ready_traces.empty()) {
        auto trace_key = std::move(impl_->ready_traces.front());
        impl_->ready_traces.pop_front();

        auto slot_it = impl_->traces.find(trace_key);
        if (slot_it == impl_->traces.end()) {
            continue;
        }
        auto& slot = slot_it->second;
        slot.scheduled = false;
        if (slot.in_flight ||
            (slot.queued.empty() && !slot.cleanup_requested)) {
            continue;
        }

        if (slot.cleanup_requested) {
            auto batch =
                build_cleanup_batch(trace_key, slot, slot.cleanup_mode);
            std::optional<sw::redis::Transaction> transaction;
            try {
                transaction.emplace(impl_->redis.transaction(
                    kPipelineTransactionCommands,
                    kCreateDedicatedTransactionConnection));
            } catch (const std::exception& error) {
                LOG(ERROR) << "Failed to create Redis cleanup transaction for trace "
                           << trace_key << ": " << error.what();
                slot.cleanup_requested = false;
                slot.deadline = td::Timestamp::in(kCleanupRetrySeconds);
                continue;
            }

            auto completion =
                [self = actor_id(this), trace_key](
                    td::Status status,
                    RedisWriteBatch finished_batch) mutable {
                    td::actor::send_closure(
                        self,
                        &RedisInsertManager::write_finished,
                        std::move(trace_key),
                        std::move(status),
                        std::move(finished_batch));
                };

            slot.in_flight.emplace(InFlightWork{
                .kind = InFlightKind::Cleanup,
                .cleanup_mode = slot.cleanup_mode,
            });
            ++impl_->active_writes;
            td::actor::create_actor<RedisTraceWriter>(
                "RedisTraceCleanup",
                std::move(*transaction),
                std::move(batch),
                std::move(completion),
                td::Timer())
                .release();
            continue;
        }

        auto work = std::move(slot.queued.front());
        slot.queued.pop_front();
        if (std::holds_alternative<ConfirmedRootReplacedRequest>(work)) {
            start_replaced_confirmed_root_ttl(trace_key);
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
            --impl_->queued_updates;
            insert_completion = std::move(request->completion);
            request_measurement = std::move(request->measurement);
            request_timer = request->timer;
            contains_real_root = request->contains_real_root;
            try {
                prepared_result = prepare_trace_update(
                    *slot.current,
                    request->trace,
                    trace_key,
                    request_measurement);
            } catch (const vm::VmError& error) {
                prepared_result = td::Status::Error(
                    "Got VmError while preparing trace: " +
                    std::string(error.get_msg()));
            } catch (const std::exception& error) {
                prepared_result = td::Status::Error(
                    "Got exception while preparing trace: " +
                    std::string(error.what()));
            }
        } else {
            auto promotion_request =
                std::move(std::get<PromoteConfirmedRequest>(work));
            insert_completion.regular_promise =
                std::move(promotion_request.promise);
            promotion = true;
            if (slot.current->nodes.nodes().empty() &&
                slot.current->metadata.empty()) {
                if (!promotion_request.fallback_state) {
                    LOG(FATAL) << "Confirmed snapshot has no fallback state for trace "
                               << trace_key;
                }
                slot.current =
                    std::move(promotion_request.fallback_state);
                materialize_full_state = true;
            }
            const auto root_key =
                metadata_value(*slot.current, "root_node");
            contains_real_root =
                root_key &&
                promotion_request.trace.nodes.count(*root_key) != 0;
            try {
                prepared_result = prepare_confirmed_promotion(
                    *slot.current,
                    promotion_request.trace,
                    trace_key,
                    promotion_request.mc_seqno,
                    materialize_full_state);
            } catch (const std::exception& error) {
                prepared_result = td::Status::Error(
                    "Got exception while promoting confirmed trace: " +
                    std::string(error.what()));
            }
        }

        if (prepared_result.is_error()) {
            auto error = prepared_result.move_as_error();
            if (promotion) {
                LOG(FATAL) << "Failed to promote confirmed snapshot for trace "
                           << trace_key << ": " << error;
            }
            insert_completion.set_error(std::move(error));
            g_statistics.record_time(
                INSERT_TRACE, request_timer.elapsed() * 1e3);
            schedule_trace(trace_key);
            continue;
        }

        auto prepared = prepared_result.move_as_ok();
        const auto resulting_nodes =
            prepared.needs_redis_write
                ? prepared.next_trace.nodes.nodes().size()
                : slot.current->nodes.nodes().size();
        if (resulting_nodes > kMaxCachedTraceNodes) {
            LOG(WARNING) << "Dropping oversized trace " << trace_key
                         << " with " << resulting_nodes
                         << " nodes; limit is " << kMaxCachedTraceNodes;
            if (request_measurement) {
                request_measurement->set_otel_attribute(
                    "ton.trace_state.oversized", true);
            }
            impl_->oversized_traces.insert_or_assign(
                trace_key,
                td::Timestamp::in(impl_->retention.open_seconds));
            slot.queued = resolve_terminal_queue(
                std::move(slot.queued),
                impl_->queued_updates,
                TraceCleanupMode::Oversized);
            request_cleanup(trace_key, TraceCleanupMode::Oversized);
            insert_completion.set_value();
            g_statistics.record_time(
                INSERT_TRACE, request_timer.elapsed() * 1e3);
            continue;
        }

        std::optional<CachedConfirmedTrace> confirmed_trace;
        if (insert_completion.confirmed) {
            const auto& resulting_trace =
                prepared.needs_redis_write
                    ? prepared.next_trace
                    : *slot.current;
            auto confirmed_result =
                collect_confirmed_data(resulting_trace, prepared);
            if (confirmed_result.is_error()) {
                insert_completion.set_error(
                    confirmed_result.move_as_error());
                g_statistics.record_time(
                    INSERT_TRACE, request_timer.elapsed() * 1e3);
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
                insert_completion.set_value(make_confirmed_snapshot(
                    trace_key,
                    slot.current,
                    std::move(*confirmed_trace)));
            } else {
                insert_completion.set_value();
            }
            g_statistics.record_time(
                INSERT_TRACE, request_timer.elapsed() * 1e3);
            schedule_trace(trace_key);
            continue;
        }

        if (request_measurement) {
            request_measurement->set_otel_attribute(
                "ton.trace_state.carried_redis_writes_count",
                static_cast<std::int64_t>(slot.dirty.plans.size()));
        }
        RedisWriteBatch batch = std::move(slot.dirty);
        batch.plans.push_back(std::move(prepared.redis));

        std::optional<sw::redis::Transaction> transaction;
        try {
            transaction.emplace(impl_->redis.transaction(kPipelineTransactionCommands,
                                                         kCreateDedicatedTransactionConnection));
        } catch (const std::exception& error) {
            auto status = td::Status::Error(
                "Failed to create Redis transaction for trace " + trace_key + ": " + error.what());
            LOG(ERROR) << status;

            remember_failed_write(
                slot, std::move(prepared.next_trace), std::move(batch));
            if (contains_real_root) {
                real_root_applied(slot);
            }
            update_lifecycle(trace_key);
            insert_completion.set_error(std::move(status));
            g_statistics.record_time(
                INSERT_TRACE, request_timer.elapsed() * 1e3);
            schedule_trace(trace_key);
            continue;
        }

        auto completion =
            [self = actor_id(this), trace_key](td::Status status, RedisWriteBatch finished_batch) mutable {
                td::actor::send_closure(self,
                                        &RedisInsertManager::write_finished,
                                        std::move(trace_key),
                                        std::move(status),
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

        td::actor::create_actor<RedisTraceWriter>("RedisTraceWriter",
                                                   std::move(*transaction),
                                                   std::move(batch),
                                                   std::move(completion),
                                                   request_timer)
            .release();
    }
}

void RedisInsertManager::write_finished(std::string trace_key,
                                        td::Status status,
                                        RedisWriteBatch batch) {
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
                       << "; retrying while the cleanup is still relevant: "
                       << status;
            slot.cleanup_requested = false;
            slot.deadline = td::Timestamp::in(kCleanupRetrySeconds);
            if (cleanup_waits_for_current_updates(slot.cleanup_mode)) {
                schedule_trace(trace_key);
            }
        } else {
            if (slot.cleanup_requested &&
                publishes_invalidation(slot.cleanup_mode) &&
                !publishes_invalidation(in_flight.cleanup_mode)) {
                // Invalidation can arrive while a normal retention cleanup is
                // already in Redis. Run one small follow-up cleanup so its
                // notification is not lost.
                schedule_trace(trace_key);
                start_next_writes();
                return;
            }
            auto queued = std::move(slot.queued);
            if (cleanup_is_terminal(slot.cleanup_mode)) {
                queued = resolve_terminal_queue(
                    std::move(queued),
                    impl_->queued_updates,
                    slot.cleanup_mode);
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
        start_next_writes();
        return;
    }

    if (status.is_error()) {
        auto error = status.to_string();
        LOG(ERROR) << "Redis write failed for trace " << trace_key
                   << "; carrying its data changes into the next patch: " << error;
        remember_failed_write(
            slot, std::move(in_flight.next_trace), std::move(batch));
        if (in_flight.contains_real_root) {
            real_root_applied(slot);
        }
        update_lifecycle(trace_key);
        in_flight.completion.set_error(std::move(status));
    } else {
        slot.current = std::make_shared<const ActiveTrace>(
            std::move(in_flight.next_trace));
        slot.dirty = RedisWriteBatch{};
        if (in_flight.contains_real_root) {
            real_root_applied(slot);
        }
        update_lifecycle(trace_key);
        if (in_flight.confirmed_trace) {
            in_flight.completion.set_value(make_confirmed_snapshot(
                trace_key,
                slot.current,
                std::move(*in_flight.confirmed_trace)));
        } else {
            in_flight.completion.set_value();
        }
    }

    schedule_trace(trace_key);
    start_next_writes();
}

void RedisInsertManager::promote_confirmed(
    std::vector<ConfirmedTraceSnapshot> snapshots,
    ton::BlockSeqno mc_seqno,
    td::Promise<td::Unit> promise) {
    struct TracePromotion {
        std::shared_ptr<const ActiveTrace> fallback_state;
        CachedConfirmedTrace confirmed;
    };

    std::map<std::string, TracePromotion> promotions;
    for (const auto& snapshot : snapshots) {
        if (!snapshot || !snapshot->state) {
            LOG(FATAL) << "Got an empty confirmed snapshot for mc "
                       << mc_seqno;
        }
        if (touch_oversized_trace(snapshot->trace_key)) {
            continue;
        }
        auto& promotion = promotions[snapshot->trace_key];
        if (!promotion.fallback_state ||
            snapshot->state->update_seq >
                promotion.fallback_state->update_seq) {
            promotion.fallback_state = snapshot->state;
        }
        for (const auto& [key, node] :
             snapshot->confirmed.nodes) {
            promotion.confirmed.nodes.insert_or_assign(key, node);
        }
        promotion.confirmed.account_states.insert(
            promotion.confirmed.account_states.end(),
            snapshot->confirmed.account_states.begin(),
            snapshot->confirmed.account_states.end());
    }

    if (promotions.empty()) {
        promise.set_value(td::Unit());
        return;
    }
    auto completion = std::make_shared<ConfirmedPromotionCompletion>(
        ConfirmedPromotionCompletion{
            .remaining = promotions.size(),
            .promise = std::move(promise),
        });
    for (auto& [trace_key, promotion] : promotions) {
        auto& slot = impl_->traces[trace_key];
        if (slot.current->root_account) {
            impl_->candidates.forget(
                *slot.current->root_account, trace_key);
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
            .promise = td::PromiseCreator::lambda([
                completion
            ](td::Result<td::Unit> result) mutable {
                completion->one_finished(std::move(result));
            }),
        });
        schedule_trace(trace_key);
    }
    start_next_writes();
}

void RedisInsertManager::alarm() {
    auto now = td::Timestamp::now();
    std::vector<std::pair<std::string, TraceCleanupMode>> expired;
    for (const auto& [trace_key, slot] : impl_->traces) {
        if (slot.cleanup_requested ||
            !slot.deadline ||
            !slot.deadline.is_in_past(now)) {
            continue;
        }
        if (cleanup_waits_for_current_updates(slot.cleanup_mode) &&
            (slot.in_flight || !slot.queued.empty())) {
            continue;
        }
        expired.emplace_back(trace_key, slot.cleanup_mode);
    }
    for (const auto& [trace_key, mode] : expired) {
        request_cleanup(trace_key, mode);
    }

    for (auto it = impl_->oversized_traces.begin();
         it != impl_->oversized_traces.end();) {
        if (it->second.is_in_past(now) &&
            impl_->traces.count(it->first) == 0) {
            it = impl_->oversized_traces.erase(it);
        } else {
            ++it;
        }
    }

    alarm_timestamp() = td::Timestamp::in(kExpirySweepSeconds);
    start_next_writes();
}

void RedisInsertManager::invalidate(std::vector<td::Bits256> trace_hashes) {
    for (const auto& hash : trace_hashes) {
        auto trace_key = td::base64_encode(hash.as_slice());
        request_cleanup(trace_key, TraceCleanupMode::Invalidation);
    }
    start_next_writes();
}

void RedisInsertManager::start_replaced_confirmed_root_ttl(
    const std::string& trace_key) {
    auto it = impl_->traces.find(trace_key);
    if (it == impl_->traces.end()) {
        return;
    }
    auto& slot = it->second;
    const auto root_key =
        metadata_value(*slot.current, "root_node")
            .value_or(std::string{});
    const auto* root = slot.current->nodes.find(root_key);
    const bool already_waiting =
        slot.cleanup_mode == TraceCleanupMode::ReplacedConfirmedTimeout;
    if (cleanup_is_terminal(slot.cleanup_mode) ||
        (root &&
         root->finality != TraceStateFinality::Confirmed &&
         !already_waiting)) {
        return;
    }

    if (slot.current->root_account) {
        impl_->candidates.forget(*slot.current->root_account, trace_key);
    }
    slot.cleanup_mode = TraceCleanupMode::ReplacedConfirmedTimeout;
    if (!already_waiting) {
        slot.deadline = td::Timestamp::in(
            impl_->retention.root_replaced_confirmed_seconds);
    }
    slot.cleanup_requested = false;
    update_lifecycle(trace_key);
    LOG(INFO) << "Confirmed root of trace " << trace_key
              << " was replaced; waiting "
              << impl_->retention.root_replaced_confirmed_seconds
              << "s for another inclusion";
}

void RedisInsertManager::mark_confirmed_roots_replaced(
    std::vector<td::Bits256> trace_hashes) {
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
    start_next_writes();
}

void RedisInsertManager::tear_down() {
    for (auto& [_, slot] : impl_->traces) {
        if (slot.in_flight && slot.in_flight->kind == InFlightKind::Update) {
            slot.in_flight->completion.set_error(
                td::Status::Error("RedisInsertManager stopped during trace write"));
        }
        for (auto& work : slot.queued) {
            if (auto* request = std::get_if<InsertRequest>(&work)) {
                request->completion.set_error(
                    td::Status::Error(
                        "RedisInsertManager stopped before trace write"));
            } else if (auto* promotion =
                           std::get_if<PromoteConfirmedRequest>(&work)) {
                promotion->promise.set_error(
                    td::Status::Error(
                        "RedisInsertManager stopped before confirmed state promotion"));
            }
        }
    }
    impl_->traces.clear();
    impl_->oversized_traces.clear();
    impl_->ready_traces.clear();
    impl_->queued_updates = 0;
}
