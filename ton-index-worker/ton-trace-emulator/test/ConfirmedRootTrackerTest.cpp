#include "ConfirmedRootTracker.h"

#include "td/utils/tests.h"

#include <string>

namespace {

td::Bits256 hash(char digit) {
    td::Bits256 result;
    ASSERT_EQ(256, result.from_hex(std::string(64, digit)));
    return result;
}

ton::BlockIdExt block(ton::BlockSeqno seqno, char variant) {
    return ton::BlockIdExt{
        ton::basechainId,
        ton::shardIdAll,
        seqno,
        hash(variant),
        hash(variant),
    };
}

}  // namespace

TEST(ConfirmedRootTracker, finalized_confirmed_variant_keeps_its_root) {
    ConfirmedRootTracker tracker;
    const auto confirmed = block(10, '1');
    const auto trace = hash('a');

    tracker.add_confirmed_root(confirmed, trace);
    auto replaced = tracker.finalize_block(confirmed);

    ASSERT_TRUE(replaced.empty());
}

TEST(ConfirmedRootTracker, empty_finalized_variant_replaces_confirmed_root) {
    ConfirmedRootTracker tracker;
    const auto confirmed = block(10, '1');
    const auto finalized = block(10, '2');
    const auto trace = hash('a');

    tracker.add_confirmed_root(confirmed, trace);
    auto replaced = tracker.finalize_block(finalized);

    ASSERT_EQ(1u, replaced.size());
    ASSERT_EQ(trace, replaced.front());
}

TEST(ConfirmedRootTracker, root_in_finalized_variant_is_not_replaced) {
    ConfirmedRootTracker tracker;
    const auto confirmed = block(10, '1');
    const auto finalized = block(10, '2');
    const auto trace = hash('a');

    tracker.add_confirmed_root(confirmed, trace);
    tracker.add_finalized_root(finalized, trace);
    auto replaced = tracker.finalize_block(finalized);

    ASSERT_TRUE(replaced.empty());
}

TEST(ConfirmedRootTracker, later_confirmed_inclusion_supersedes_discarded_one) {
    ConfirmedRootTracker tracker;
    const auto old_confirmed = block(10, '1');
    const auto finalized = block(10, '2');
    const auto later_confirmed = block(11, '3');
    const auto trace = hash('a');

    // Block emulators run concurrently, so the newer block may finish first.
    tracker.add_confirmed_root(later_confirmed, trace);
    tracker.add_confirmed_root(old_confirmed, trace);
    auto replaced = tracker.finalize_block(finalized);

    ASSERT_TRUE(replaced.empty());
}

TEST(ConfirmedRootTracker, canonical_candidate_wins_regardless_of_callback_order) {
    ConfirmedRootTracker tracker;
    const auto discarded = block(10, '1');
    const auto canonical = block(10, '2');
    const auto trace = hash('a');

    tracker.add_confirmed_root(discarded, trace);
    tracker.add_confirmed_root(canonical, trace);
    auto replaced = tracker.finalize_block(canonical);

    ASSERT_TRUE(replaced.empty());
}

TEST(ConfirmedRootTracker, duplicate_fork_variants_report_root_once) {
    ConfirmedRootTracker tracker;
    const auto first = block(10, '1');
    const auto second = block(10, '2');
    const auto finalized = block(10, '3');
    const auto trace = hash('a');

    tracker.add_confirmed_root(first, trace);
    tracker.add_confirmed_root(second, trace);
    auto replaced = tracker.finalize_block(finalized);

    ASSERT_EQ(1u, replaced.size());
    ASSERT_EQ(trace, replaced.front());
}
