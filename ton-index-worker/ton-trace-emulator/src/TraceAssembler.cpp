#include <algorithm>
#include <limits>
#include <queue>
#include <set>
#include <sstream>
#include <utility>

#include "emu/EmuClassifierBridge.h"
#include "vm/boc.h"

#include "Serializer.hpp"
#include "TraceAssembler.h"

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

void add_metadata_change(const ActiveTrace& current, TraceTransition& transition, std::string field,
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
      .transaction_boc = std::make_shared<const std::string>(transaction_boc_result.move_as_ok().as_slice().str()),
      .workchain = node.address.workchain,
      .mc_seqno = node.mc_block_seqno,
      .child_keys = std::move(out_message_keys),
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

td::Result<TraceTransition> TraceAssembler::apply(const ActiveTrace& current, const Trace& patch,
                                                  const std::string& trace_key) const {
  TraceTransition transition;
  transition.cached_nodes_count = current.nodes.nodes().size();

  auto node_update_result = prepare_node_update(current, patch, trace_key);
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
  transition.next_trace = current;
  transition.next_trace.nodes.apply(std::move(state_change));

  for (const auto& [address, interfaces] : patch.interfaces) {
    auto redis_interfaces = parse_interfaces(interfaces);
    std::stringstream buffer;
    msgpack::pack(buffer, redis_interfaces);
    add_metadata_change(current, transition, account_key(address), buffer.str());
  }

  auto patch_interfaces = mch::make_interface_map(patch);
  if (!patch_interfaces.empty()) {
    auto next_interfaces =
        std::make_shared<mch::ParsedBlockLookupSource::InterfaceMap>(*current.classifier_interfaces);
    for (auto& [account, interfaces] : patch_interfaces) {
      next_interfaces->insert_or_assign(std::move(account), std::move(interfaces));
    }
    transition.next_trace.classifier_interfaces = std::move(next_interfaces);
  }

  auto root_account = account_key(patch.root->address);
  transition.raw_external_message_hash = td::base64_encode(patch.ext_in_msg_hash.as_slice());
  if (node_update.state_update.root_key == transition.raw_external_message_hash) {
    transition.next_trace.root_account = std::move(root_account);
  }
  auto root_account_it = patch.emulated_accounts.find(patch.root->address);
  if (root_account_it != patch.emulated_accounts.end() && root_account_it->second.code.not_null()) {
    add_metadata_change(current, transition, "root_account_code_hash",
                        td::base64_encode(root_account_it->second.code->get_hash().as_slice()));
  }
  add_metadata_change(current, transition, "root_node", transition.raw_external_message_hash);
  add_metadata_change(current, transition, "depth_limit_exceeded", patch.tx_limit_exceeded ? "1" : "0");

  transition.next_trace.tx_limit_exceeded = patch.tx_limit_exceeded;
  const auto has_state_change = !transition.node_delta.empty() || !transition.metadata_patch.empty();
  if (has_state_change) {
    if (current.update_seq == std::numeric_limits<std::uint64_t>::max()) {
      return td::Status::Error("Trace update_seq overflow");
    }
    transition.next_trace.update_seq = current.update_seq + 1;
  }

  transition.next_trace.finality = patch.root->finality_state;
  if (is_more_final(current.finality, transition.next_trace.finality)) {
    transition.next_trace.finality = current.finality;
  }
  // Exact duplicates still refresh Redis TTLs and notifications.
  transition.needs_redis_write = true;
  return transition;
}

td::Result<mch::EmuTraceView> TraceAssembler::build_full_trace(const ActiveTrace& trace, const std::string& trace_key,
                                                               const Trace& lookup_context) const {
  mch::EmuTraceView view;
  view.trace_id = trace_key;
  view.tx_limit_exceeded = trace.tx_limit_exceeded;
  view.interfaces = trace.classifier_interfaces;
  view.shard_states = lookup_context.shard_states;
  view.config = lookup_context.config;
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
  } else if (lookup_context.root) {
    append_subtree(node_key(*lookup_context.root));
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
