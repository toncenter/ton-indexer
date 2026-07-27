#include "TraceLifecycle.h"

#include "td/utils/tests.h"

#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

namespace {

TraceStateNode lifecycle_node(std::string key, TraceStateFinality finality) {
    return TraceStateNode{
        .key = std::move(key),
        .finality = finality,
        .fingerprint = "fingerprint",
        .serialized = std::make_shared<const std::string>("serialized"),
    };
}

TraceState lifecycle_state(std::string root,
                           std::initializer_list<TraceStateNode> nodes) {
    std::vector<TraceStateNode> connected_nodes(nodes);
    for (auto& node : connected_nodes) {
        if (node.key != root) {
            connected_nodes.front().child_keys.push_back(node.key);
        }
    }

    TraceState state;
    auto change = state.prepare(TraceStateUpdate{
        .root_key = std::move(root),
        .nodes = std::move(connected_nodes),
    });
    state.apply(std::move(change));
    return state;
}

}  // namespace

TEST(TraceLifecycle, pending_root_is_short_lived) {
    auto state = lifecycle_state("root", {
        lifecycle_node("root", TraceStateFinality::Emulated),
        lifecycle_node("tail", TraceStateFinality::Emulated),
    });

    ASSERT_EQ(TraceLifecycle::RootPending,
              classify_trace_lifecycle(state, "root"));
}

TEST(TraceLifecycle, real_root_with_pending_tail_stays_open) {
    auto state = lifecycle_state("root", {
        lifecycle_node("root", TraceStateFinality::Finalized),
        lifecycle_node("middle", TraceStateFinality::Confirmed),
        lifecycle_node("tail", TraceStateFinality::Emulated),
    });

    ASSERT_EQ(TraceLifecycle::Open,
              classify_trace_lifecycle(state, "root"));
}

TEST(TraceLifecycle, confirmed_nodes_wait_for_finalization) {
    auto state = lifecycle_state("root", {
        lifecycle_node("root", TraceStateFinality::Finalized),
        lifecycle_node("tail", TraceStateFinality::Confirmed),
    });

    ASSERT_EQ(TraceLifecycle::AwaitingFinalization,
              classify_trace_lifecycle(state, "root"));
}

TEST(TraceLifecycle, fully_finalized_trace_is_complete) {
    auto state = lifecycle_state("root", {
        lifecycle_node("root", TraceStateFinality::Finalized),
        lifecycle_node("tail", TraceStateFinality::Finalized),
    });

    ASSERT_EQ(TraceLifecycle::Finalized,
              classify_trace_lifecycle(state, "root"));
}

TEST(TraceLifecycle, missing_canonical_root_is_not_synthetic) {
    auto state = lifecycle_state("partial", {
        lifecycle_node("partial", TraceStateFinality::Emulated),
    });

    ASSERT_EQ(TraceLifecycle::UnknownRoot,
              classify_trace_lifecycle(state, "canonical-root"));
}

TEST(TraceLifecycle, retention_depends_on_trace_stage) {
    TraceRetentionConfig retention{
        .root_pending_seconds = 10.0,
        .open_seconds = 20.0,
        .completed_seconds = 30.0,
    };

    ASSERT_EQ(10.0,
              trace_retention_seconds(TraceLifecycle::RootPending, retention));
    ASSERT_EQ(20.0,
              trace_retention_seconds(TraceLifecycle::Open, retention));
    ASSERT_EQ(20.0,
              trace_retention_seconds(TraceLifecycle::UnknownRoot, retention));
    ASSERT_EQ(
        30.0,
        trace_retention_seconds(TraceLifecycle::AwaitingFinalization, retention));
    ASSERT_EQ(30.0,
              trace_retention_seconds(TraceLifecycle::Finalized, retention));
}

TEST(TraceLifecycle, competitors_are_cleared_only_when_root_becomes_real) {
    ASSERT_TRUE(trace_root_became_real(
        TraceLifecycle::RootPending, TraceLifecycle::Open));
    ASSERT_TRUE(trace_root_became_real(
        TraceLifecycle::UnknownRoot, TraceLifecycle::Finalized));

    ASSERT_TRUE(!trace_root_became_real(
        TraceLifecycle::RootPending, TraceLifecycle::RootPending));
    ASSERT_TRUE(!trace_root_became_real(
        TraceLifecycle::Open, TraceLifecycle::Finalized));
}

TEST(TraceLifecycle, highload_wallet_messages_do_not_compete) {
    ASSERT_TRUE(!wallet_external_messages_compete(
        "EayteVWEQJDyg78ji8FEmHH3g+fMCXlAjT9IWUg+hSU="));
    ASSERT_TRUE(wallet_external_messages_compete("ordinary-wallet-code"));
    ASSERT_TRUE(wallet_external_messages_compete(""));
}

TEST(TraceLifecycle, accepting_one_message_invalidates_only_its_competitors) {
    CompetingTraceSet candidates;
    candidates.remember("wallet", "E1");
    candidates.remember("wallet", "E2");
    candidates.remember("wallet", "E3");
    candidates.remember("other-wallet", "E4");

    auto invalidated = candidates.accept("wallet", "E2");

    ASSERT_EQ(2u, invalidated.size());
    ASSERT_EQ("E1", invalidated[0]);
    ASSERT_EQ("E3", invalidated[1]);
    ASSERT_TRUE(!candidates.contains("wallet", "E1"));
    ASSERT_TRUE(!candidates.contains("wallet", "E2"));
    ASSERT_TRUE(candidates.contains("other-wallet", "E4"));
}

TEST(TraceLifecycle, expired_candidate_is_forgotten_without_touching_others) {
    CompetingTraceSet candidates;
    candidates.remember("wallet", "E1");
    candidates.remember("wallet", "E2");

    candidates.forget("wallet", "E1");

    ASSERT_TRUE(!candidates.contains("wallet", "E1"));
    ASSERT_TRUE(candidates.contains("wallet", "E2"));
}
