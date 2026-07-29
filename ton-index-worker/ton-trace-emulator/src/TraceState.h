#pragma once

#include <compare>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

enum class TraceStateFinality : std::uint8_t {
    Emulated = 0,
    Confirmed = 1,
    Finalized = 2,
};

struct TraceStateIndexRef {
    std::string index_key;
    std::string member;
    std::uint64_t score{0};

    auto operator<=>(const TraceStateIndexRef&) const = default;
};

struct TraceStateNode {
    std::string key;
    TraceStateFinality finality{TraceStateFinality::Emulated};
    std::string fingerprint;
    std::shared_ptr<const std::string> serialized;
    std::vector<std::string> child_keys;
    std::vector<TraceStateIndexRef> index_refs;

    bool operator==(const TraceStateNode&) const = default;
};

// Nodes are a flat representation of one incoming trace subtree. child_keys may
// refer to messages which have no node in this update.
struct TraceStateUpdate {
    std::string root_key;
    std::vector<TraceStateNode> nodes;
};

struct TraceStateDelta {
    // All collections are sorted, making Redis writes and tests deterministic.
    std::vector<std::string> removed_node_keys;
    std::vector<TraceStateNode> upserted_nodes;
    std::vector<TraceStateIndexRef> removed_index_refs;
    std::vector<TraceStateIndexRef> added_index_refs;

    bool empty() const;
};

// One prepared update contains both the small Redis delta and the complete
// state which becomes current after Redis accepts that delta.
struct TraceStateChange {
    TraceStateDelta delta;
    std::map<std::string, TraceStateNode> resulting_nodes;
};

class TraceState {
public:
    // Prepares an update without changing the current in-memory state.
    TraceStateChange prepare(const TraceStateUpdate& update) const;

    // Inserts or replaces only the given nodes without touching descendants
    // or sibling branches.
    TraceStateChange upsert_nodes(std::vector<TraceStateNode> nodes) const;

    // Replaces this object with the resulting state of a prepared change.
    void apply(TraceStateChange&& change) noexcept;

    const TraceStateNode* find(const std::string& key) const;
    const std::map<std::string, TraceStateNode>& nodes() const;

private:
    std::map<std::string, TraceStateNode> nodes_;
};
