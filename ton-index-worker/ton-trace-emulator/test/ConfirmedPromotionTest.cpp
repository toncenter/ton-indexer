#include "td/utils/tests.h"
#include "validator/block-handle.hpp"

#include "TraceScheduler.h"

namespace {

ton::BlockIdExt test_block(ton::BlockSeqno seqno, char variant, ton::WorkchainId workchain = 0) {
  td::Bits256 hash;
  CHECK(hash.from_hex(std::string(64, variant)) == 256);
  return {workchain, ton::shardIdAll, seqno, hash, hash};
}

class TestBlockData : public ton::validator::BlockData {
  ton::BlockSeqno seqno_;

 public:
  explicit TestBlockData(ton::BlockSeqno seqno) : seqno_(seqno) {
  }
  td::BufferSlice data() const override {
    return {};
  }
  ton::FileHash file_hash() const override {
    return block_id().file_hash;
  }
  ton::BlockIdExt block_id() const override {
    return test_block(seqno_, 'a', ton::masterchainId);
  }
  td::Ref<vm::Cell> root_cell() const override {
    return {};
  }
};

schema::MasterchainBlockDataState empty_test_block(ton::BlockSeqno seqno) {
  schema::MasterchainBlockDataState data;
  data.shard_blocks_.push_back(schema::BlockDataState{
      .block_data = td::make_ref<TestBlockData>(seqno),
      .handle = std::make_shared<ton::validator::BlockHandleImpl>(test_block(seqno, 'a', ton::masterchainId)),
  });
  return data;
}

// Exercise scheduler ordering without a Redis service. Processor-level tests
// separately cover real writes and compatibility checks in each trace queue.
class TestProcessor : public ITraceProcessor {
  std::function<void(ton::BlockSeqno, td::Promise<td::Unit>)> started_;

 public:
  explicit TestProcessor(decltype(started_) started) : started_(std::move(started)) {
  }
  void process_trace_update(TraceUpdate, td::Promise<td::Unit> promise) override {
    started_(0, std::move(promise));
  }
  void process_confirmed_trace_update(TraceUpdate, td::Promise<ConfirmedTraceSnapshot>) override {
    UNREACHABLE();
  }
  void promote_confirmed(std::vector<ConfirmedTraceSnapshot>, ton::BlockSeqno seqno,
                         td::Promise<td::Unit> promise) override {
    started_(seqno, std::move(promise));
  }
  void invalidate(std::vector<td::Bits256>) override {
  }
  void mark_confirmed_roots_replaced(std::vector<td::Bits256>) override {
  }
};

}  // namespace

struct TraceSchedulerTest : TraceEmulatorScheduler {
  int promotion_error;
  td::Timestamp deadline;
  td::Promise<td::Unit> first_request;
  std::vector<ton::BlockSeqno> resolved, attempted, completed;
  std::vector<td::actor::ActorId<McBlockEmulator>> emulators;
  unsigned ordinary_writes{0};

  explicit TraceSchedulerTest(int error = 0)
      : TraceEmulatorScheduler({}, {}, "", "", "redis://127.0.0.1:1", {}, "", ""), promotion_error(error) {
  }

  void check_observed_versions() {
    // Prevent DB fetches: only test event bookkeeping, including events that
    // have not yet yielded a snapshot (or may eventually fail).
    db_catch_up_in_progress_ = true;
    auto a = test_block(10, 'a'), b = test_block(10, 'b');
    handle_block_signed(a);
    handle_block_signed(a);
    ASSERT_EQ(1u, confirmed_block_versions_.size());
    ASSERT_TRUE(confirmed_block_versions_.at(a.id) == a);
    ASSERT_TRUE(!can_reuse_confirmed_block(a));
    confirmed_block_snapshots_[a] = {};
    ASSERT_TRUE(can_reuse_confirmed_block(a));
    handle_block_signed(b);
    handle_block_signed(a);
    ASSERT_TRUE(!confirmed_block_versions_.at(a.id));
    confirmed_block_snapshots_[b] = {};
    ASSERT_TRUE(!can_reuse_confirmed_block(a));
    ASSERT_TRUE(!can_reuse_confirmed_block(b));

    auto next = test_block(11, 'c');
    auto other_shard = a;
    other_shard.id.shard = ton::shardIdAll / 2;
    handle_block_signed(next);
    handle_block_signed(other_shard);
    ASSERT_TRUE(confirmed_block_versions_.at(next.id) == next);
    ASSERT_TRUE(confirmed_block_versions_.at(other_shard.id) == other_shard);
    confirmed_block_snapshots_[next] = {};
    confirmed_block_snapshots_[other_shard] = {};
    ASSERT_TRUE(can_reuse_confirmed_block(next));
    ASSERT_TRUE(can_reuse_confirmed_block(other_shard));

    close_confirmed_block(a.id);
    discard_confirmed_snapshots({a});
    handle_block_signed(test_block(10, 'd'));
    ASSERT_EQ(0u, confirmed_block_versions_.count(a.id));
    ASSERT_EQ(2u, confirmed_block_versions_.size());
  }

  void start_up() override {
    last_started_finalized_seqno_ = last_fetched_seqno_ = 11;
    finalized_blocks_in_pipeline_ = 2;
    finalized_ready_.reset(10);
    trace_processor_ = td::actor::create_actor<TestProcessor>(
        "TestProcessor", [self = actor_id(this)](ton::BlockSeqno seqno, td::Promise<td::Unit> promise) mutable {
          td::actor::send_closure(self, &TraceSchedulerTest::processor_started, seqno, std::move(promise));
        });
    start_mc(10);
    deadline = td::Timestamp::in(3);
    alarm_timestamp() = td::Timestamp::in(0.001);
  }

  void check_regular_result_waits() {
    last_started_finalized_seqno_ = last_fetched_seqno_ = 11;
    finalized_blocks_in_pipeline_ = 2;
    finalized_ready_.reset(11);
    finalized_commit_.emplace(FinalizedCommitState{.seqno = 10, .pending_writes = 1});
    finalized_block_emulated(11, FinalizedBlockResult{.mc_seqno = 11});
    ASSERT_EQ(10u, finalized_commit_->seqno);
    finalized_trace_write_finished(10);
    ASSERT_TRUE(!finalized_commit_);
    ASSERT_EQ(0u, finalized_blocks_in_pipeline_);
  }

  void start_mc(ton::BlockSeqno seqno) {
    auto ids_ready = [self = actor_id(this)](ton::BlockSeqno seqno) {
      td::actor::send_closure(self, &TraceSchedulerTest::ids_resolved, seqno);
    };
    auto promote = [self = actor_id(this), seqno](td::Promise<td::Unit> promise) mutable {
      td::actor::send_closure(self, &TraceSchedulerTest::promotion_ready, seqno, std::move(promise));
    };
    auto result =
        td::PromiseCreator::lambda([self = actor_id(this), seqno](td::Result<FinalizedBlockResult> result) mutable {
          td::actor::send_closure(self, &TraceSchedulerTest::block_finished, seqno, std::move(result));
        });
    emulators.push_back(td::actor::create_actor<McBlockEmulator>("InlinePromotion", empty_test_block(seqno),
                                                                 std::move(ids_ready), std::move(promote),
                                                                 std::move(result))
                            .release());
  }

  void ids_resolved(ton::BlockSeqno seqno) {
    ASSERT_EQ(10u + resolved.size(), seqno);  // Once per block, in order.
    resolved.push_back(seqno);
  }

  void promotion_ready(ton::BlockSeqno seqno, td::Promise<td::Unit> promise) {
    ASSERT_TRUE(completed.empty());  // No provisional FinalizedBlockResult.
    if (seqno == 10) {
      first_request = std::move(promise);
      start_mc(11);
      return;
    }
    // Deliver readiness out of order while keeping trace-id resolution ordered.
    request_confirmed_promotion(11, {}, std::move(promise));
    ASSERT_TRUE(!finalized_commit_);
    ASSERT_TRUE(attempted.empty());
    request_confirmed_promotion(10, {}, std::move(first_request));
    ASSERT_EQ(10u, finalized_commit_->seqno);
  }

  void processor_started(ton::BlockSeqno seqno, td::Promise<td::Unit> promise) {
    ASSERT_TRUE(finalized_commit_.has_value());
    if (seqno == 0) {
      // Simulate completion of the ordinary data write after declined promotion.
      ASSERT_EQ(10u, finalized_commit_->seqno);
      ASSERT_EQ(1u, finalized_commit_->pending_writes);
      ASSERT_EQ(std::vector<ton::BlockSeqno>{10}, attempted);
      ++ordinary_writes;
    } else {
      ASSERT_EQ(10u + attempted.size(), seqno);
      ASSERT_EQ(seqno, finalized_commit_->seqno);
      ASSERT_EQ(seqno - 10, completed.size());
      if (seqno == 11) {
        ASSERT_EQ(promotion_error ? 1u : 0u, ordinary_writes);
      }
      attempted.push_back(seqno);
      if (seqno == 10 && promotion_error) {
        promise.set_error(td::Status::Error(promotion_error, "test decline"));
        return;
      }
    }
    promise.set_value(td::Unit());
  }

  void block_finished(ton::BlockSeqno seqno, td::Result<FinalizedBlockResult> result) {
    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(10u + completed.size(), seqno);  // Exactly one result per actor.
    completed.push_back(seqno);
    auto block = result.move_as_ok();
    if (seqno == 10 && promotion_error) {
      // The empty test block has no transactions. Supply one processor request
      // to verify the barrier also waits for ordinary writes, not just emulation.
      TraceUpdate update;
      update.fragments.emplace_back();
      block.trace_updates.push_back(std::move(update));
    }
    finalized_block_emulated(seqno, std::move(block));
  }

  void alarm() override {
    ASSERT_TRUE(!deadline.is_in_past());
    if (completed.size() != 2 || finalized_commit_) {
      alarm_timestamp() = td::Timestamp::in(0.001);
      return;
    }
    ASSERT_EQ(0u, finalized_blocks_in_pipeline_);
    ASSERT_EQ(std::vector<ton::BlockSeqno>({10, 11}), resolved);
    ASSERT_EQ(resolved, attempted);
    for (const auto& emulator : emulators) {
      ASSERT_TRUE(!emulator.is_alive());
    }
    ASSERT_TRUE(!finalized_ready_.take_next());
    td::actor::SchedulerContext::get().stop();
  }
};

TEST(ConfirmedPromotion, multiple_observed_versions_disable_reuse_until_finalization) {
  TraceSchedulerTest test;
  test.check_observed_versions();
}

TEST(ConfirmedPromotion, promotion_is_ordered_and_finishes_each_actor_once) {
  for (int error : {0, kConfirmedPromotionUnavailable, 500}) {
    td::actor::Scheduler scheduler({1});
    td::actor::ActorOwn<TraceSchedulerTest> test;
    scheduler.run_in_context(
        [&] { test = td::actor::create_actor<TraceSchedulerTest>("PromotionBarrierTest", error); });
    scheduler.run();
    scheduler.run_in_context([&] { test.reset(); });
  }
}

TEST(ConfirmedPromotion, ready_regular_result_waits_for_preceding_write) {
  TraceSchedulerTest test;
  test.check_regular_result_waits();
}

TEST(ConfirmedPromotion, empty_block_without_promotion_finishes_once) {
  td::actor::Scheduler scheduler({1});
  unsigned resolved = 0, completed = 0;
  scheduler.run_in_context([&] {
    td::actor::create_actor<McBlockEmulator>(
        "OrdinaryEmptyBlock", empty_test_block(12),
        [&](ton::BlockSeqno seqno) {
          ASSERT_EQ(12u, seqno);
          ++resolved;
        },
        std::function<void(td::Promise<td::Unit>)>{},
        td::PromiseCreator::lambda([&](td::Result<FinalizedBlockResult> result) {
          ASSERT_TRUE(result.is_ok());
          ASSERT_EQ(12u, result.ok().mc_seqno);
          ASSERT_TRUE(result.ok().trace_updates.empty());
          ++completed;
          td::actor::SchedulerContext::get().stop();
        }))
        .release();
  });
  scheduler.run();
  ASSERT_EQ(1u, resolved);
  ASSERT_EQ(1u, completed);
}
