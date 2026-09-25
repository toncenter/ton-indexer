#include <algorithm>
#include <iterator>
#include <limits>
#include <queue>
#include <set>
#include <sstream>
#include <utility>

#include "emu/EmuClassifierBridge.h"
#include "vm/boc.h"

#include "Serializer.hpp"
#include "TraceAssembler.h"
#include "BlockParser.h"
#include "TraceInterfaceDetector.h"

namespace {

TraceStateFinality to_state_finality(FinalityState finality) {
  return static_cast<TraceStateFinality>(static_cast<std::uint8_t>(finality));
}

bool is_more_final(FinalityState left, FinalityState right) {
  return static_cast<std::uint8_t>(left) > static_cast<std::uint8_t>(right);
}

std::string node_key(const TraceNode& node) {
  return td::base64_encode(node.node_id.as_slice());
}

bool stale_root_update(const ActiveTrace& current, const Trace& patch) {
  const auto* root = current.root();
  if (!root || !patch.root || root->finality <= to_state_finality(patch.root->finality_state)) {
    return false;
  }
  // A continuation of this execution may legitimately be less final than its
  // ancestor. A late root, or a continuation of a superseded execution, may not.
  return patch.contains_root_transaction() || root->key != td::base64_encode(patch.ext_in_msg_hash.as_slice()) ||
         root->transaction_hash() != td::base64_encode(patch.root_tx_hash.as_slice());
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

td::Result<TraceStateNode> prepare_state_node(const TraceNode& node, const std::string& key,
                                              const std::string& fingerprint, const TraceStateNode* cached,
                                              const std::string& trace_key, std::size_t& reused_serializations) {
  if (cached && cached->fingerprint == fingerprint) {
    if (!cached->transaction_boc) {
      return td::Status::Error("Cached trace node has no classifier transaction BOC");
    }
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

  auto transaction_boc_result = vm::std_boc_serialize(node.transaction_root, 0);
  if (transaction_boc_result.is_error()) {
    return transaction_boc_result.move_as_error_prefix("Failed to serialize trace transaction for classifier: ");
  }

  std::stringstream buffer;
  msgpack::pack(buffer, redis_node);

  std::vector<std::string> out_message_keys;
  std::vector<std::string> internal_message_keys;
  out_message_keys.reserve(redis_node.transaction.out_msgs.size());
  for (const auto& out_message : redis_node.transaction.out_msgs) {
    out_message_keys.push_back(td::base64_encode(out_message.hash.as_slice()));
    if (out_message.destination) internal_message_keys.push_back(out_message_keys.back());
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
      .transaction_boc = std::make_shared<const std::string>(transaction_boc_result.move_as_ok().as_slice().str()),
      .workchain = node.address.workchain,
      .mc_seqno = node.mc_block_seqno,
      .child_keys = std::move(out_message_keys),
      .internal_child_keys = std::move(internal_message_keys),
      .index_refs = {std::move(index)},
  };
}

struct PreparedNodeUpdate {
  TraceStateUpdate state_update;
  std::vector<AcceptedNode> accepted_nodes;
  std::size_t reused_serializations{0};
};

td::Result<PreparedNodeUpdate> prepare_node_update(const ActiveTrace& current, const Trace& patch,
                                                   const std::string& trace_key) {
  PreparedNodeUpdate prepared;
  if (!patch.root) {
    return prepared;
  }

  prepared.state_update.root_key = node_key(*patch.root);

  std::queue<TraceNode*> queue;
  queue.push(patch.root.get());
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
        prepare_state_node(*node, key, fingerprint, cached, trace_key, prepared.reused_serializations);
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
    });

    for (auto& child : node->children) {
      if (child) {
        queue.push(child.get());
      }
    }
  }
  return prepared;
}

}  // namespace

std::string trace_node_fingerprint(const RedisTraceNode& node) {
  auto fingerprint = td::base64_encode(node.transaction.hash.as_slice());
  fingerprint += ":" + account_key(node.transaction.account);
  fingerprint += ":" + std::to_string(static_cast<std::uint8_t>(node.finality));
  fingerprint += ":" + std::to_string(node.mc_block_seqno);
  fingerprint += ":" + std::to_string(node.block_id.workchain);
  fingerprint += ":" + std::to_string(node.block_id.shard);
  fingerprint += ":" + std::to_string(node.block_id.seqno);
  return fingerprint;
}

std::optional<std::string> trace_metadata_value(const ActiveTrace& trace, const std::string& field) {
  auto it = trace.metadata.find(field);
  if (it == trace.metadata.end()) {
    return std::nullopt;
  }
  return it->second;
}

namespace {

using ClassifierInterfaces = mch::ParsedBlockLookupSource::InterfaceMap;

td::Status apply_fragment(TraceTransition& transition, const Trace& patch, const std::string& trace_key,
                          std::shared_ptr<ClassifierInterfaces>& mutable_interfaces) {
  auto& next = transition.next_trace;
  if (stale_root_update(next, patch)) {
    return td::Status::OK();
  }

  auto node_update_result = prepare_node_update(next, patch, trace_key);
  if (node_update_result.is_error()) {
    return node_update_result.move_as_error();
  }
  auto node_update = node_update_result.move_as_ok();
  transition.reused_serializations += node_update.reused_serializations;

  if (node_update.accepted_nodes.empty()) {
    return td::Status::OK();
  }
  transition.accepted_nodes.insert(transition.accepted_nodes.end(),
                                   std::make_move_iterator(node_update.accepted_nodes.begin()),
                                   std::make_move_iterator(node_update.accepted_nodes.end()));

  const bool had_root = next.root() != nullptr;
  const auto previous_root_key = patch.contains_root_transaction()
                                     ? trace_metadata_value(next, "root_node").value_or(std::string{})
                                     : std::string{};
  next.nodes.apply_update(node_update.state_update, previous_root_key);

  for (const auto& [address, interfaces] : patch.interfaces) {
    auto redis_interfaces = parse_interfaces(interfaces);
    std::stringstream buffer;
    msgpack::pack(buffer, redis_interfaces);
    next.metadata.insert_or_assign(account_key(address), buffer.str());
  }

  if (!patch.interfaces.empty()) {
    if (!mutable_interfaces) {
      mutable_interfaces = std::make_shared<ClassifierInterfaces>(*next.classifier_interfaces);
      next.classifier_interfaces = mutable_interfaces;
    }
    // An explicitly observed empty final interface set must also clear a
    // value cached by an older trace update.
    for (const auto& [account, _] : patch.interfaces) {
      mutable_interfaces->erase(account);
    }
    auto patch_interfaces = mch::make_interface_map(patch);
    for (auto& [account, interfaces] : patch_interfaces) {
      mutable_interfaces->insert_or_assign(std::move(account), std::move(interfaces));
    }
  }

  transition.raw_external_message_hash = td::base64_encode(patch.ext_in_msg_hash.as_slice());
  if (patch.contains_root_transaction()) {
    next.root_account = account_key(patch.root->address);
    auto root_accounts = patch.emulated_accounts.equal_range(patch.root->address);
    if (root_accounts.first != root_accounts.second) {
      const auto& final_root_account = std::prev(root_accounts.second)->second;
      if (final_root_account.code.not_null()) {
        next.metadata.insert_or_assign("root_account_code_hash",
                                       td::base64_encode(final_root_account.code->get_hash().as_slice()));
      }
    }
  }
  if (patch.contains_root_transaction() || !had_root) {
    next.metadata.insert_or_assign("root_node", transition.raw_external_message_hash);
  }
  next.metadata.insert_or_assign("depth_limit_exceeded", patch.tx_limit_exceeded ? "1" : "0");

  next.tx_limit_exceeded = patch.tx_limit_exceeded;
  if (is_more_final(patch.root->finality_state, next.finality)) {
    next.finality = patch.root->finality_state;
  }
  // Exact duplicates still refresh Redis TTLs and notifications.
  transition.needs_redis_write = true;
  return td::Status::OK();
}

td::Result<TraceTransition> finish_transition(const ActiveTrace& current, TraceTransition transition) {
  if (!transition.needs_redis_write) {
    transition.next_trace = ActiveTrace{};
    return transition;
  }
  auto& next = transition.next_trace;
  transition.node_delta = current.nodes.delta_to(next.nodes);
  for (const auto& [field, value] : next.metadata) {
    auto cached = current.metadata.find(field);
    if (cached == current.metadata.end() || cached->second != value) {
      transition.metadata_patch.emplace(field, value);
    }
  }
  next.update_seq = current.update_seq;
  if (!transition.node_delta.empty() || !transition.metadata_patch.empty()) {
    if (current.update_seq == std::numeric_limits<std::uint64_t>::max()) {
      return td::Status::Error("Trace update_seq overflow");
    }
    ++next.update_seq;
  }
  return transition;
}

}  // namespace

td::Result<TraceTransition> TraceAssembler::apply_update(const ActiveTrace& current, TraceUpdate& update,
                                                         const std::string& trace_key) const {
  normalize_trace_update_interfaces(update);

  TraceTransition combined;
  combined.cached_nodes_count = current.nodes.nodes().size();
  if (update.empty()) {
    return combined;
  }

  // Establish the accepted root before applying disconnected continuations.
  // This also makes rejecting a stale root reject its entire TraceUpdate.
  auto root_fragment = std::find_if(update.fragments.begin(), update.fragments.end(),
                                    [](const Trace& fragment) { return fragment.contains_root_transaction(); });
  if (root_fragment != update.fragments.end()) {
    std::rotate(update.fragments.begin(), root_fragment, std::next(root_fragment));
  }
  if (stale_root_update(current, update.fragments.front())) {
    return combined;
  }

  combined.next_trace = current;
  std::shared_ptr<ClassifierInterfaces> mutable_interfaces;
  bool tx_limit_exceeded = false;
  std::set<std::string> accepted_keys;
  for (const auto& patch : update.fragments) {
    tx_limit_exceeded = tx_limit_exceeded || patch.tx_limit_exceeded;
    auto status = apply_fragment(combined, patch, trace_key, mutable_interfaces);
    if (status.is_error()) {
      return status.move_as_error_prefix("Failed to apply trace update fragment: ");
    }
    for (auto& accepted : combined.accepted_nodes) {
      accepted_keys.insert(std::move(accepted.key));
    }
    combined.accepted_nodes.clear();
  }

  if (combined.needs_redis_write) {
    combined.next_trace.tx_limit_exceeded = tx_limit_exceeded;
    combined.next_trace.metadata.insert_or_assign("depth_limit_exceeded", tx_limit_exceeded ? "1" : "0");
    for (const auto& key : accepted_keys) {
      const auto* node = combined.next_trace.nodes.find(key);
      if (node) {
        combined.accepted_nodes.push_back(AcceptedNode{
            .key = key,
            .finality = static_cast<FinalityState>(static_cast<std::uint8_t>(node->finality)),
        });
      }
    }
  }
  return finish_transition(current, std::move(combined));
}

namespace {
td::Result<mch::EmuTraceView> build_trace_view(const ActiveTrace& trace, const std::string& trace_key,
    const AllShardStates& shard_states, const std::shared_ptr<block::ConfigInfo>& config,
    const TraceNode* fallback_root) {
  mch::EmuTraceView view;
  view.trace_id = trace_key;
  view.tx_limit_exceeded = trace.tx_limit_exceeded;
  view.interfaces = trace.classifier_interfaces;
  view.shard_states = shard_states;
  view.config = config;
  view.update_seq = trace.update_seq;
  view.nodes.reserve(trace.nodes.nodes().size());

  std::vector<std::string> ordered_keys;
  std::set<std::string> visited;
  auto append_subtree = [&](const std::string& root_key) {
    std::vector<std::string> pending{root_key};
    while (!pending.empty()) {
      auto key = std::move(pending.back());
      pending.pop_back();
      if (!visited.insert(key).second) {
        continue;
      }
      const auto* node = trace.nodes.find(key);
      if (!node) {
        continue;
      }
      ordered_keys.push_back(key);
      for (auto it = node->child_keys.rbegin(); it != node->child_keys.rend(); ++it) {
        pending.push_back(*it);
      }
    }
  };

  if (auto root_key = trace_metadata_value(trace, "root_node")) {
    append_subtree(*root_key);
  } else if (fallback_root) {
    append_subtree(node_key(*fallback_root));
  }
  // Unknown-root continuation patches and any temporarily disconnected nodes
  // still belong to the full trace.
  for (const auto& [key, _] : trace.nodes.nodes()) {
    append_subtree(key);
  }

  for (const auto& key : ordered_keys) {
    const auto& node = *trace.nodes.find(key);
    if (!node.transaction_boc) {
      return td::Status::Error("Full trace contains a node without transaction BOC");
    }
    mch::EmuTxRef full_trace_node;
    full_trace_node.address.workchain = node.workchain;
    full_trace_node.tx_boc = node.transaction_boc;
    full_trace_node.mc_seqno = node.mc_seqno;
    full_trace_node.finality = static_cast<mch::EmuFinality>(static_cast<std::uint8_t>(node.finality));
    view.nodes.push_back(std::move(full_trace_node));
  }
  return view;
}

}  // namespace

td::Result<mch::EmuTraceView> TraceAssembler::build_full_trace(const ActiveTrace& trace, const std::string& key,
    const Trace& context) const {
  return build_trace_view(trace, key, context.shard_states, context.config, context.root.get());
}

td::Result<mch::EmuTraceView> TraceAssembler::build_full_trace(const ActiveTrace& trace, const std::string& key,
    const AllShardStates& states, const std::shared_ptr<block::ConfigInfo>& config) const {
  return build_trace_view(trace, key, states, config, nullptr);
}

td::Result<TraceStateNode> prepare_finalized_node(const TransactionInfo& tx, const std::string& trace_key) {
  // A non-owning leaf adapter for the common node serializer; no tree is built.
  TraceNode node;
  node.node_id = tx.in_msg_hash;
  node.address = tx.account;
  node.transaction_root = tx.root;
  node.mc_block_seqno = tx.mc_block_seqno;
  node.block_id = tx.block_id;
  node.finality_state = FinalityState::Finalized;
  std::size_t reused = 0;
  return prepare_state_node(node, node_key(node), node_fingerprint(node), nullptr, trace_key, reused);
}

void apply_detected_accounts(ActiveTrace& trace, const DetectedAccounts& accounts) {
  auto interfaces = std::make_shared<ClassifierInterfaces>(*trace.classifier_interfaces);
  for (const auto& [address, detected] : accounts.interfaces) {
    std::stringstream buffer;
    msgpack::pack(buffer, parse_interfaces(detected));
    trace.metadata.insert_or_assign(account_key(address), buffer.str());
    interfaces->erase(address);
  }
  for (auto& [address, detected] : mch::make_interface_map(accounts)) {
    interfaces->insert_or_assign(std::move(address), std::move(detected));
  }
  trace.classifier_interfaces = std::move(interfaces);
}
