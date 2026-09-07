#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "td/utils/tests.h"
#include "vm/boc.h"
#include "vm/cells/CellBuilder.h"

#include "TraceAssembler.h"
#include "TraceTestUtils.h"

namespace {

std::shared_ptr<const std::string> make_boc(std::uint8_t marker) {
  vm::CellBuilder builder;
  CHECK(builder.store_long_bool(marker, 8));
  auto result = vm::std_boc_serialize(builder.finalize(), 0);
  CHECK(result.is_ok());
  return std::make_shared<const std::string>(result.move_as_ok().as_slice().str());
}

TraceStateNode node(std::string key, std::int32_t workchain, TraceStateFinality finality,
                    std::vector<std::string> children, std::uint8_t marker) {
  return TraceStateNode{
      .key = std::move(key),
      .finality = finality,
      .transaction_boc = make_boc(marker),
      .workchain = workchain,
      .child_keys = std::move(children),
  };
}

void upsert(ActiveTrace& trace, std::vector<TraceStateNode> nodes) {
  auto change = trace.nodes.upsert_nodes(std::move(nodes));
  trace.nodes.apply(std::move(change));
}

}  // namespace

TEST(TraceAssembler, full_trace_is_ordered_and_owned) {
  ActiveTrace trace;
  trace.metadata.emplace("root_node", "A");
  trace.update_seq = 7;
  upsert(trace, {
                    node("A", 1, TraceStateFinality::Finalized, {"B"}, 0xA1),
                    node("B", 2, TraceStateFinality::Confirmed, {"C"}, 0xB1),
                });

  TraceAssembler assembler;
  Trace lookup_context;
  auto first_result = assembler.build_full_trace(trace, "trace-id", lookup_context);
  ASSERT_TRUE(first_result.is_ok());
  auto first = first_result.move_as_ok();

  ASSERT_EQ(2u, first.nodes.size());
  ASSERT_EQ(1, first.nodes[0].address.workchain);
  ASSERT_EQ(2, first.nodes[1].address.workchain);
  ASSERT_EQ(7u, first.update_seq);

  // A later patch changes B and supplies C. The already emitted full trace
  // remains an immutable two-node value with its own transaction BOCs.
  upsert(trace, {
                    node("B", 2, TraceStateFinality::Finalized, {"C"}, 0xB2),
                    node("C", 3, TraceStateFinality::Confirmed, {}, 0xC1),
                });
  trace.update_seq = 8;

  ASSERT_EQ(2u, first.nodes.size());
  ASSERT_EQ(mch::EmuFinality::confirmed, first.nodes[1].finality);
  ASSERT_TRUE(vm::std_boc_deserialize(*first.nodes[1].tx_boc).is_ok());

  auto second_result = assembler.build_full_trace(trace, "trace-id", lookup_context);
  ASSERT_TRUE(second_result.is_ok());
  auto second = second_result.move_as_ok();

  ASSERT_EQ(3u, second.nodes.size());
  ASSERT_EQ(1, second.nodes[0].address.workchain);
  ASSERT_EQ(2, second.nodes[1].address.workchain);
  ASSERT_EQ(3, second.nodes[2].address.workchain);
  ASSERT_EQ(mch::EmuFinality::finalized, second.nodes[1].finality);
  ASSERT_EQ(8u, second.update_seq);
}

TEST(TraceAssembler, raw_root_replacement_removes_old_tree_and_ignores_late_pending) {
  using namespace trace_test;
  auto a = message(1, true), b = message(2, true), old = message(3), tail = message(4);
  auto root_a = trace_test::node(a, {old}, FinalityState::Emulated);
  root_a->children.push_back(trace_test::node(old, {}, FinalityState::Emulated));
  ActiveTrace state;
  trace_test::apply(state, trace(std::move(root_a), a));

  auto root_b = trace_test::node(b, {tail}, FinalityState::Confirmed);
  root_b->children.push_back(trace_test::node(tail, {}, FinalityState::Emulated));
  auto replacement = trace_test::apply(state, trace(std::move(root_b), b));
  ASSERT_EQ(key(b), *trace_metadata_value(state, "root_node"));
  ASSERT_EQ(2u, state.nodes.nodes().size());
  ASSERT_TRUE(state.nodes.find(key(a)) == nullptr);
  ASSERT_TRUE(state.nodes.find(key(old)) == nullptr);
  ASSERT_EQ(2u, replacement.node_delta.removed_node_keys.size());
  ASSERT_EQ(2u, replacement.node_delta.removed_index_refs.size());

  auto late = trace_test::apply(state, trace(trace_test::node(a, {}, FinalityState::Emulated), a));
  ASSERT_TRUE(!late.needs_redis_write);
  ASSERT_EQ(key(b), *trace_metadata_value(state, "root_node"));
}

TEST(TraceAssembler, continuation_cannot_switch_root_but_can_extend_its_own_execution) {
  using namespace trace_test;
  auto a = message(1, true), other = message(2, true), child = message(3), tail = message(4);
  auto root = trace_test::node(a, {child}, FinalityState::Finalized);
  td::Bits256 root_hash = root->transaction_root->get_hash().bits();
  ActiveTrace state;
  trace_test::apply(state, trace(std::move(root), a));
  auto valid = trace_test::apply(state, trace(trace_test::node(child, {tail}, FinalityState::Confirmed), a, root_hash));
  ASSERT_TRUE(valid.needs_redis_write);
  ASSERT_EQ(key(a), *trace_metadata_value(state, "root_node"));
  auto stale = trace_test::apply(state, trace(trace_test::node(tail, {}, FinalityState::Confirmed), other));
  ASSERT_TRUE(!stale.needs_redis_write);
  ASSERT_TRUE(state.nodes.find(key(tail)) == nullptr);
}

TEST(TraceAssembler, rootless_fragment_does_not_create_or_replace_a_root) {
  using namespace trace_test;
  auto a = message(1, true), b = message(2, true), fragment = message(3);
  ActiveTrace state;
  trace_test::apply(state, trace(trace_test::node(fragment, {}, FinalityState::Confirmed), a));
  ASSERT_TRUE(state.root() == nullptr);
  ASSERT_EQ(key(a), *trace_metadata_value(state, "root_node"));
  trace_test::apply(state, trace(trace_test::node(b, {}, FinalityState::Confirmed), b));
  trace_test::apply(state, trace(trace_test::node(fragment, {}, FinalityState::Confirmed, 200), a));
  ASSERT_EQ(key(b), *trace_metadata_value(state, "root_node"));
  ASSERT_TRUE(state.nodes.find(key(fragment)) != nullptr);
}

TEST(TraceAssembler, grouped_fragments_have_one_delta_and_are_independent_of_root_order) {
  using namespace trace_test;
  auto a = message(1, true), b = message(2, true), child = message(3), tail = message(4);
  for (bool root_first : {false, true}) {
    ActiveTrace state;
    trace_test::apply(state, trace(trace_test::node(a, {}, FinalityState::Emulated), a));
    auto root = trace_test::node(b, {child}, FinalityState::Confirmed);
    td::Bits256 root_hash = root->transaction_root->get_hash().bits();
    TraceUpdate update;
    update.fragments.push_back(trace(trace_test::node(child, {tail}, FinalityState::Confirmed), b, root_hash));
    update.fragments.push_back(trace(std::move(root), b));
    if (root_first) {
      std::swap(update.fragments[0], update.fragments[1]);
    }
    auto result = TraceAssembler().apply_update(state, update, "trace");
    ASSERT_TRUE(result.is_ok());
    auto combined = result.move_as_ok();
    ASSERT_EQ(state.update_seq + 1, combined.next_trace.update_seq);
    ASSERT_EQ(key(b), *trace_metadata_value(combined.next_trace, "root_node"));
    ASSERT_EQ(std::vector<std::string>({key(a)}), combined.node_delta.removed_node_keys);
    ASSERT_TRUE(combined.next_trace.nodes.find(key(child)) != nullptr);
    ASSERT_EQ(2u, combined.node_delta.upserted_nodes.size());
  }
}
