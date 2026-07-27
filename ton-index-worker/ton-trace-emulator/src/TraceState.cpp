#include "TraceState.h"

#include <algorithm>
#include <iterator>
#include <set>
#include <stdexcept>
#include <utility>

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

void erase_subtree(NodeMap& nodes, const std::string& key) {
    auto it = nodes.find(key);
    if (it == nodes.end()) {
        return;
    }

    const auto child_keys = it->second.child_keys;
    nodes.erase(it);
    for (const auto& child_key : child_keys) {
        erase_subtree(nodes, child_key);
    }
}

NodeMap merge_update(const NodeMap& current, const TraceStateUpdate& update) {
    const auto incoming = normalize_update(update);
    NodeMap result = current;
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

        if (cached != current.end() &&
            incoming_node.finality != TraceStateFinality::Emulated) {
            erase_subtree(result, key);
        }
        result.insert_or_assign(key, incoming_node);

        for (const auto& child_key : incoming_node.child_keys) {
            if (incoming.count(child_key) != 0) {
                pending.push_back(child_key);
            }
        }
    }
    return result;
}

}  // namespace

bool TraceStateDelta::empty() const {
    return removed_node_keys.empty() && upserted_nodes.empty() && removed_index_refs.empty() &&
           added_index_refs.empty();
}

TraceStateChange TraceState::prepare(const TraceStateUpdate& update) const {
    TraceStateChange change;
    change.resulting_nodes = merge_update(nodes_, update);
    auto& delta = change.delta;

    for (const auto& entry : nodes_) {
        const auto& key = entry.first;
        auto resulting_it = change.resulting_nodes.find(key);
        if (resulting_it == change.resulting_nodes.end()) {
            delta.removed_node_keys.push_back(key);
        }
    }
    for (const auto& [key, resulting_node] : change.resulting_nodes) {
        auto cached_it = nodes_.find(key);
        if (cached_it == nodes_.end() || cached_it->second != resulting_node) {
            delta.upserted_nodes.push_back(resulting_node);
        }
    }

    const auto cached_refs = collect_index_refs(nodes_);
    const auto resulting_refs = collect_index_refs(change.resulting_nodes);
    std::set_difference(cached_refs.begin(), cached_refs.end(),
                        resulting_refs.begin(), resulting_refs.end(),
                        std::back_inserter(delta.removed_index_refs));
    std::set_difference(resulting_refs.begin(), resulting_refs.end(),
                        cached_refs.begin(), cached_refs.end(),
                        std::back_inserter(delta.added_index_refs));
    return change;
}

void TraceState::apply(TraceStateChange&& change) noexcept {
    nodes_.swap(change.resulting_nodes);
}

const TraceStateNode* TraceState::find(const std::string& key) const {
    auto it = nodes_.find(key);
    return it == nodes_.end() ? nullptr : &it->second;
}

const std::map<std::string, TraceStateNode>& TraceState::nodes() const {
    return nodes_;
}
