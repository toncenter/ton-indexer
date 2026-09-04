#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "td/utils/tests.h"
#include "vm/boc.h"
#include "vm/cells/CellBuilder.h"

#include "TraceAssembler.h"

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
