#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

#include "td/utils/tests.h"

#include "TraceState.h"

namespace {

using Finality = TraceStateFinality;

TraceStateIndexRef index_for(const std::string& key, std::uint64_t score = 100) {
    return TraceStateIndexRef{
        .index_key = "account:" + key,
        .member = "trace:" + key,
        .score = score,
    };
}

TraceStateNode node(std::string key,
                    Finality finality,
                    std::vector<std::string> children = {},
                    std::string content = {}) {
    if (content.empty()) {
        content = key + "-v1";
    }
    auto index = index_for(key);
    return TraceStateNode{
        .key = std::move(key),
        .finality = finality,
        .fingerprint = content,
        .serialized = std::make_shared<const std::string>("msgpack:" + content),
        .child_keys = std::move(children),
        .index_refs = {std::move(index)},
    };
}

TraceStateUpdate update(std::string root, std::initializer_list<TraceStateNode> nodes) {
    return TraceStateUpdate{
        .root_key = std::move(root),
        .nodes = nodes,
    };
}

TraceStateDelta apply(TraceState& state, const TraceStateUpdate& next) {
    auto change = state.prepare(next);
    auto delta = change.delta;
    state.apply(std::move(change));
    return delta;
}

}  // namespace

TEST(TraceState, new_trace_produces_sorted_upserts_and_exact_indexes) {
    TraceState state;

    auto change = state.prepare(update("root", {
        node("root", Finality::Emulated, {"tail"}),
        node("tail", Finality::Emulated),
    }));
    const auto& delta = change.delta;

    ASSERT_EQ(2u, delta.upserted_nodes.size());
    ASSERT_EQ("root", delta.upserted_nodes[0].key);
    ASSERT_EQ("tail", delta.upserted_nodes[1].key);
    ASSERT_EQ(2u, delta.added_index_refs.size());
    ASSERT_EQ(index_for("root"), delta.added_index_refs[0]);
    ASSERT_EQ(index_for("tail"), delta.added_index_refs[1]);
}

TEST(TraceState, duplicate_emulated_update_is_a_noop_and_keeps_omitted_tail) {
    TraceState state;
    apply(state, update("root", {
        node("root", Finality::Emulated, {"tail"}),
        node("tail", Finality::Emulated),
    }));

    auto change = state.prepare(update("root", {
        *state.find("root"),
    }));

    ASSERT_TRUE(change.delta.empty());
    ASSERT_TRUE(state.find("tail") != nullptr);
    ASSERT_EQ("msgpack:root-v1", *state.find("root")->serialized);
}

TEST(TraceState, lower_finality_node_cannot_change_anything_below_it) {
    TraceState state;
    apply(state, update("root", {
        node("root", Finality::Finalized, {"child"}, "root-final"),
        node("child", Finality::Finalized, {}, "child-original"),
    }));

    auto change = state.prepare(update("root", {
        node("root", Finality::Confirmed, {"child"}, "root-downgrade"),
        node("child", Finality::Confirmed, {}, "child-should-not-change"),
    }));

    ASSERT_TRUE(change.delta.empty());
    ASSERT_EQ("child-original", state.find("child")->fingerprint);
}

TEST(TraceState, confirmed_node_keeps_its_still_referenced_omitted_pending_tail) {
    TraceState state;
    apply(state, update("root", {
        node("root", Finality::Confirmed, {"tail"}, "same-root"),
        node("tail", Finality::Emulated),
    }));

    auto change = state.prepare(update("root", {
        node("root", Finality::Confirmed, {"tail"}, "same-root"),
    }));
    const auto& delta = change.delta;

    ASSERT_TRUE(delta.removed_node_keys.empty());
    ASSERT_TRUE(delta.removed_index_refs.empty());
    state.apply(std::move(change));
    ASSERT_TRUE(state.find("tail") != nullptr);
}

TEST(TraceState, partial_update_keeps_the_sibling_branch) {
    TraceState state;
    apply(state, update("root", {
        node("root", Finality::Emulated, {"left", "right"}),
        node("left", Finality::Emulated, {"left-tail"}),
        node("left-tail", Finality::Emulated),
        node("right", Finality::Emulated),
    }));

    apply(state, update("left", {
        node("left", Finality::Confirmed, {"left-tail"}),
    }));

    ASSERT_TRUE(state.find("root") != nullptr);
    ASSERT_TRUE(state.find("right") != nullptr);
    ASSERT_TRUE(state.find("left-tail") != nullptr);
}

TEST(TraceState, changed_pending_out_messages_remove_only_the_detached_branch) {
    TraceState state;
    apply(state, update("root", {
                                    node("root", Finality::Emulated, {"old", "keep"}),
                                    node("old", Finality::Emulated, {"old-tail"}),
                                    node("old-tail", Finality::Emulated),
                                    node("keep", Finality::Emulated),
                                }));
    apply(state, update("unrelated", {node("unrelated", Finality::Confirmed)}));
    auto change = state.prepare(update("root", {node("root", Finality::Emulated, {"keep"}, "new-root")}));
    ASSERT_EQ(std::vector<std::string>({"old", "old-tail"}), change.delta.removed_node_keys);
    ASSERT_EQ(2u, change.delta.removed_index_refs.size());
    state.apply(std::move(change));
    ASSERT_TRUE(state.find("keep") != nullptr);
    ASSERT_TRUE(state.find("unrelated") != nullptr);
}

TEST(TraceState, replacing_raw_root_keeps_shared_descendants_and_unrelated_fragments) {
    TraceState state;
    apply(state, update("A", {
                                 node("A", Finality::Emulated, {"shared", "old"}),
                                 node("shared", Finality::Confirmed, {"tail"}),
                                 node("tail", Finality::Emulated),
                                 node("old", Finality::Emulated),
                             }));
    apply(state, update("unrelated", {node("unrelated", Finality::Confirmed)}));
    auto change = state.prepare(update("B", {node("B", Finality::Confirmed, {"shared"})}), "A");
    ASSERT_EQ(std::vector<std::string>({"A", "old"}), change.delta.removed_node_keys);
    state.apply(std::move(change));
    ASSERT_TRUE(state.find("shared") != nullptr);
    ASSERT_TRUE(state.find("tail") != nullptr);
    ASSERT_TRUE(state.find("unrelated") != nullptr);
}

TEST(TraceState, late_pending_raw_root_cannot_replace_a_finalized_root) {
    TraceState state;
    apply(state, update("B", {node("B", Finality::Finalized, {"child"}), node("child", Finality::Emulated)}));
    auto change = state.prepare(update("A", {node("A", Finality::Emulated)}), "B");
    ASSERT_TRUE(change.delta.empty());
}

TEST(TraceState, ancestor_update_preserves_a_more_final_child_and_its_tail) {
    TraceState state;
    apply(state, update("A", {
                                 node("A", Finality::Emulated, {"B"}),
                                 node("B", Finality::Finalized, {"C"}),
                                 node("C", Finality::Emulated),
                             }));
    apply(state, update("A", {node("A", Finality::Confirmed, {"B"}), node("B", Finality::Emulated)}));
    ASSERT_EQ(Finality::Finalized, state.find("B")->finality);
    ASSERT_TRUE(state.find("C") != nullptr);
}

TEST(TraceState, detached_branch_keeps_descendants_referenced_by_another_parent) {
    TraceState state;
    apply(state, update("A", {
                                 node("A", Finality::Emulated, {"B", "C"}),
                                 node("B", Finality::Emulated, {"D"}),
                                 node("C", Finality::Emulated, {"D"}),
                                 node("D", Finality::Emulated),
                             }));
    auto delta = apply(state, update("A", {node("A", Finality::Confirmed, {"C"})}));
    ASSERT_EQ(std::vector<std::string>({"B"}), delta.removed_node_keys);
    ASSERT_TRUE(state.find("D") != nullptr);
}

TEST(TraceState, next_patch_builds_on_a_patch_whose_redis_write_failed) {
    TraceState state;
    apply(state, update("A", {
        node("A", Finality::Finalized, {"B"}),
        node("B", Finality::Confirmed, {"C"}),
        node("C", Finality::Emulated),
    }));

    // Redis did not confirm this write, but the patch is still part of the
    // logical trace state on which future partial updates must build.
    auto failed_redis_write = state.prepare(update("B", {
        node("B", Finality::Finalized, {"C"}),
        node("C", Finality::Confirmed),
    }));
    state.apply(std::move(failed_redis_write));

    apply(state, update("C", {
        node("C", Finality::Finalized),
    }));

    ASSERT_EQ(Finality::Finalized, state.find("A")->finality);
    ASSERT_EQ(Finality::Finalized, state.find("B")->finality);
    ASSERT_EQ(Finality::Finalized, state.find("C")->finality);
}

TEST(TraceState, prepare_does_not_change_state_until_apply) {
    TraceState state;
    auto change = state.prepare(update("root", {
        node("root", Finality::Emulated),
    }));

    ASSERT_TRUE(state.find("root") == nullptr);

    state.apply(std::move(change));
    ASSERT_TRUE(state.find("root") != nullptr);
}

TEST(TraceState, changed_index_score_emits_exact_remove_and_add) {
    TraceState state;
    apply(state, update("root", {
        node("root", Finality::Emulated, {}, "v1"),
    }));

    auto changed = node("root", Finality::Emulated, {}, "v2");
    changed.index_refs = {index_for("root", 200)};
    auto change = state.prepare(update("root", {changed}));
    const auto& delta = change.delta;

    ASSERT_TRUE(delta.removed_node_keys.empty());
    ASSERT_EQ(1u, delta.upserted_nodes.size());
    ASSERT_EQ(1u, delta.removed_index_refs.size());
    ASSERT_EQ(1u, delta.added_index_refs.size());
    ASSERT_EQ(index_for("root", 100), delta.removed_index_refs[0]);
    ASSERT_EQ(index_for("root", 200), delta.added_index_refs[0]);
}

TEST(TraceState, finalizing_known_nodes_keeps_pending_descendants) {
    TraceState state;
    apply(state, update("A", {
        node("A", Finality::Confirmed, {"B"}, "A-confirmed"),
        node("B", Finality::Confirmed, {"C"}, "B-confirmed"),
        node("C", Finality::Emulated, {}, "C-pending"),
    }));

    auto finalized_a = node(
        "A", Finality::Finalized, {"B"}, "A-finalized");
    auto change = state.upsert_nodes({finalized_a});

    ASSERT_TRUE(change.delta.removed_node_keys.empty());
    ASSERT_EQ(1u, change.delta.upserted_nodes.size());
    ASSERT_TRUE(change.delta.removed_index_refs.empty());
    ASSERT_TRUE(change.delta.added_index_refs.empty());
    state.apply(std::move(change));

    ASSERT_EQ(Finality::Finalized, state.find("A")->finality);
    ASSERT_EQ(Finality::Confirmed, state.find("B")->finality);
    ASSERT_EQ(Finality::Emulated, state.find("C")->finality);
}

TEST(TraceState, delta_to_collapses_intermediate_changes) {
    TraceState initial;
    apply(initial, update("root", {
                                      node("root", Finality::Emulated, {"old"}),
                                      node("old", Finality::Emulated),
                                  }));

    TraceState resulting = initial;
    apply(resulting, update("root", {
                                        node("root", Finality::Confirmed, {"new"}, "root-confirmed"),
                                        node("new", Finality::Confirmed),
                                    }));
    apply(resulting, update("new", {
                                       node("new", Finality::Finalized, {}, "new-finalized"),
                                   }));

    auto delta = initial.delta_to(resulting);

    ASSERT_EQ(1u, delta.removed_node_keys.size());
    ASSERT_EQ("old", delta.removed_node_keys.front());
    ASSERT_EQ(2u, delta.upserted_nodes.size());
    ASSERT_EQ("new", delta.upserted_nodes[0].key);
    ASSERT_EQ(Finality::Finalized, delta.upserted_nodes[0].finality);
    ASSERT_EQ("root", delta.upserted_nodes[1].key);
}

TEST(TraceState, large_trace_leaf_update_is_still_a_single_node_delta) {
    TraceState state;
    std::vector<TraceStateNode> nodes;
    std::vector<std::string> children;
    for (int i = 0; i < 1000; ++i) {
        auto key = "leaf-" + std::to_string(i);
        children.push_back(key);
        nodes.push_back(node(key, Finality::Emulated));
    }
    nodes.push_back(node("root", Finality::Confirmed, std::move(children)));
    state.apply(state.upsert_nodes(std::move(nodes)));

    auto change = state.prepare(update("leaf-500", {node("leaf-500", Finality::Confirmed, {}, "new-leaf")}));
    ASSERT_EQ(1u, change.delta.upserted_nodes.size());
    ASSERT_EQ("leaf-500", change.delta.upserted_nodes.front().key);
    ASSERT_TRUE(change.delta.removed_node_keys.empty());
    ASSERT_TRUE(change.delta.added_index_refs.empty());
    ASSERT_TRUE(change.delta.removed_index_refs.empty());
}
