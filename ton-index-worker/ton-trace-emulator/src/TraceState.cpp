#include <algorithm>
#include <iterator>
#include <set>
#include <stdexcept>
#include <utility>

#include "TraceState.h"

namespace {

using NodeMap = std::map<std::string, TraceStateNode>;
using IndexSet = std::set<TraceStateIndexRef>;

bool is_more_final(TraceStateFinality cached, TraceStateFinality incoming) {
    return cached > incoming;
}

TraceStateNode normalize_node(TraceStateNode node) {
    if (node.key.empty()) {
        throw std::invalid_argument("TraceState node key must not be empty");
    }
    std::sort(node.index_refs.begin(), node.index_refs.end());
    node.index_refs.erase(
        std::unique(node.index_refs.begin(), node.index_refs.end()), node.index_refs.end());
    return node;
}

NodeMap normalize_update(const TraceStateUpdate& update) {
    if (update.root_key.empty()) {
        throw std::invalid_argument("TraceState update root key must not be empty");
    }

    NodeMap incoming;
    for (auto node : update.nodes) {
        node = normalize_node(std::move(node));
        auto [_, inserted] = incoming.emplace(node.key, std::move(node));
        if (!inserted) {
            throw std::invalid_argument("TraceState update contains duplicate node keys");
        }
    }
    if (incoming.find(update.root_key) == incoming.end()) {
        throw std::invalid_argument("TraceState update does not contain its root node");
    }

    return incoming;
}

IndexSet collect_index_refs(const NodeMap& nodes) {
    IndexSet refs;
    for (const auto& [_, node] : nodes) {
        refs.insert(node.index_refs.begin(), node.index_refs.end());
    }
    return refs;
}

void collect_subtree(const NodeMap& nodes, const std::string& root, std::set<std::string>& visited) {
    std::vector<std::string> pending{root};
    while (!pending.empty()) {
        auto key = std::move(pending.back());
        pending.pop_back();
        auto it = nodes.find(key);
        if (it == nodes.end() || !visited.insert(key).second) {
            continue;
        }
        pending.insert(pending.end(), it->second.child_keys.begin(), it->second.child_keys.end());
    }
}

void replace_node(const NodeMap& current, NodeMap& result, TraceStateNode node, std::set<std::string>& detached) {
    auto cached = current.find(node.key);
    if (cached != current.end()) {
        for (const auto& child : cached->second.child_keys) {
            if (std::find(node.child_keys.begin(), node.child_keys.end(), child) == node.child_keys.end()) {
                collect_subtree(current, child, detached);
            }
        }
    }
    result.insert_or_assign(node.key, std::move(node));
}

void prune_detached(NodeMap& result, const std::set<std::string>& detached,
                    const std::set<std::string>& explicit_roots = {}) {
    if (detached.empty()) {
        return;
    }
    // Only a removed edge (or a replaced root) proves a branch obsolete.
    // Keep shared descendants reachable through the resulting graph, including
    // partial fragments outside that branch. An omitted child is not a deletion.
    std::set<std::string> retained;
    for (const auto& [key, _] : result) {
        if (detached.count(key) == 0 || explicit_roots.count(key) != 0) {
            collect_subtree(result, key, retained);
        }
    }
    for (const auto& key : detached) {
        if (retained.count(key) == 0) {
            result.erase(key);
        }
    }
}

NodeMap merge_update(const NodeMap& current, const TraceStateUpdate& update, const std::string& previous_root_key) {
    const auto incoming = normalize_update(update);
    auto previous_root = current.find(previous_root_key.empty() ? update.root_key : previous_root_key);
    if (previous_root != current.end() &&
        is_more_final(previous_root->second.finality, incoming.at(update.root_key).finality)) {
        return current;
    }

    NodeMap result = current;
    std::set<std::string> detached;
    std::vector<std::string> pending{update.root_key};

    while (!pending.empty()) {
        auto key = std::move(pending.back());
        pending.pop_back();
        const auto& incoming_node = incoming.at(key);
        auto cached = current.find(key);
        if (cached != current.end() &&
            is_more_final(cached->second.finality, incoming_node.finality)) {
            continue;
        }

        if (key == update.root_key && !previous_root_key.empty() && previous_root_key != key) {
            collect_subtree(current, previous_root_key, detached);
        }
        replace_node(current, result, incoming_node, detached);

        for (const auto& child_key : incoming_node.child_keys) {
            if (incoming.count(child_key) != 0) {
                pending.push_back(child_key);
            }
        }
    }

    prune_detached(result, detached, {update.root_key});
    return result;
}

TraceStateDelta make_delta(const NodeMap& current, const NodeMap& resulting) {
    TraceStateDelta delta;
    for (const auto& [key, _] : current) {
        if (resulting.count(key) == 0) {
            delta.removed_node_keys.push_back(key);
        }
    }
    for (const auto& [key, resulting_node] : resulting) {
        auto cached = current.find(key);
        if (cached == current.end() || cached->second != resulting_node) {
            delta.upserted_nodes.push_back(resulting_node);
        }
    }

  const auto cached_refs = collect_index_refs(current);
  const auto resulting_refs = collect_index_refs(resulting);
  std::set_difference(cached_refs.begin(), cached_refs.end(), resulting_refs.begin(), resulting_refs.end(),
                      std::back_inserter(delta.removed_index_refs));
  std::set_difference(resulting_refs.begin(), resulting_refs.end(), cached_refs.begin(), cached_refs.end(),
                      std::back_inserter(delta.added_index_refs));
  return delta;
}

TraceStateChange make_change(const NodeMap& current, NodeMap resulting) {
  TraceStateChange change;
  change.delta = make_delta(current, resulting);
  change.resulting_nodes = std::move(resulting);
  return change;
}

}  // namespace

bool TraceStateDelta::empty() const {
    return removed_node_keys.empty() && upserted_nodes.empty() && removed_index_refs.empty() &&
           added_index_refs.empty();
}

TraceStateChange TraceState::prepare(const TraceStateUpdate& update, const std::string& previous_root_key) const {
    return make_change(nodes_, merge_update(nodes_, update, previous_root_key));
}

TraceStateChange TraceState::upsert_nodes(
    std::vector<TraceStateNode> nodes) const {
    NodeMap result = nodes_;
    std::set<std::string> seen;
    for (auto node : nodes) {
        node = normalize_node(std::move(node));
        if (!seen.insert(node.key).second) {
            throw std::invalid_argument(
                "TraceState upsert contains duplicate node keys");
        }
        result.insert_or_assign(node.key, std::move(node));
    }
    return make_change(nodes_, std::move(result));
}

void TraceState::apply(TraceStateChange&& change) noexcept {
    nodes_.swap(change.resulting_nodes);
}

TraceStateDelta TraceState::delta_to(const TraceState& resulting) const {
  return make_delta(nodes_, resulting.nodes_);
}

const TraceStateNode* TraceState::find(const std::string& key) const {
    auto it = nodes_.find(key);
    return it == nodes_.end() ? nullptr : &it->second;
}

const std::map<std::string, TraceStateNode>& TraceState::nodes() const {
    return nodes_;
}
