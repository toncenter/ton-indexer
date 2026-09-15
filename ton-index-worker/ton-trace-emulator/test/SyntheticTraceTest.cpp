#include "../bench/SyntheticTrace.h"
#include "TraceAssembler.h"
#include "Serializer.hpp"
#include "td/utils/tests.h"

TEST(SyntheticTrace, valid_transactions_accounts_and_incremental_finalization) {
  for (const auto count : {1, 2, 15, 1000}) {
    auto fixture = trace_bench::generate(1, count, 7, 256, 1000, true, 1);
    TraceAssembler assembler;
    ActiveTrace current;
    std::vector<TraceUpdate*> updates{&fixture.pending};
    for (auto& update : fixture.finalized) updates.push_back(&update);
    for (auto* update : updates) {
      if (update->empty()) continue;
      for (const auto& fragment : update->fragments) {
        const auto transaction = parse_trace_node(*fragment.root).move_as_ok().transaction;
        ASSERT_TRUE(!transaction.description.aborted);
        ASSERT_TRUE(std::get<TrComputePhase_vm>(transaction.description.compute_ph).success);
        ASSERT_TRUE(transaction.description.action->success);
        for (const auto& [address, account] : fragment.committed_accounts) {
          const auto parsed = parse_account(account);
          ASSERT_TRUE(parsed.is_ok());
        }
      }
      auto transition = assembler.apply_update(current, *update, fixture.key).move_as_ok();
      ASSERT_TRUE(transition.needs_redis_write);
      current = std::move(transition.next_trace);
      ASSERT_EQ(current.nodes.nodes().size(), static_cast<std::size_t>(count));
    }
    auto view = assembler.build_full_trace(current, fixture.key, fixture.finalized[0].fragments[0]).move_as_ok();
    ASSERT_EQ(view.nodes.size(), static_cast<std::size_t>(count));
    for (const auto& node : view.nodes) ASSERT_EQ(node.finality, mch::EmuFinality::finalized);
  }
}

TEST(SyntheticTrace, small_connected_fragments_grow_to_five_thousand_nodes) {
  for (auto fragment_txs : {5, 10}) {
    // A partial final fragment also exercises the boundary.
    auto fixture = trace_bench::generate_growing(1, 5003, fragment_txs, 17, 32, 1000, false, 1);
    ActiveTrace current;
    std::size_t nodes = 0;
    for (auto& update : fixture.finalized) {
      ASSERT_EQ(update.fragments.size(), 1u);
      const auto fragment_nodes = update.fragments[0].root->transactions_count();
      ASSERT_TRUE(fragment_nodes > 0 && fragment_nodes <= fragment_txs);
      auto transition = TraceAssembler().apply_update(current, update, fixture.key).move_as_ok();
      ASSERT_TRUE(transition.needs_redis_write);
      nodes += fragment_nodes;
      current = std::move(transition.next_trace);
      ASSERT_EQ(current.nodes.nodes().size(), nodes);
    }
    ASSERT_EQ(nodes, 5003u);
    auto view = TraceAssembler().build_full_trace(current, fixture.key, fixture.finalized[0].fragments[0]).move_as_ok();
    ASSERT_EQ(view.nodes.size(), nodes);
    for (const auto& node : view.nodes) ASSERT_EQ(node.finality, mch::EmuFinality::finalized);
    for (const auto& [key, node] : current.nodes.nodes())
      for (const auto& child : node.child_keys) ASSERT_TRUE(current.nodes.find(child) != nullptr);
  }
}
