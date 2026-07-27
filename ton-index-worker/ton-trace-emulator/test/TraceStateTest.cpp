#include "TraceState.h"

#include "td/utils/tests.h"

#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

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

TEST(TraceState, confirmed_node_replaces_its_omitted_pending_tail) {
    TraceState state;
    apply(state, update("root", {
        node("root", Finality::Confirmed, {"tail"}, "same-root"),
        node("tail", Finality::Emulated),
    }));

    auto change = state.prepare(update("root", {
        node("root", Finality::Confirmed, {"tail"}, "same-root"),
    }));
    const auto& delta = change.delta;

    ASSERT_EQ(1u, delta.removed_node_keys.size());
    ASSERT_EQ("tail", delta.removed_node_keys[0]);
    ASSERT_EQ(index_for("tail"), delta.removed_index_refs[0]);
    state.apply(std::move(change));
    ASSERT_TRUE(state.find("tail") == nullptr);
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
    ASSERT_TRUE(state.find("left-tail") == nullptr);
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
