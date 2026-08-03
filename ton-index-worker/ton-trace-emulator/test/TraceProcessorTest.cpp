#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "../src/TraceProcessor.cpp"
#include "td/utils/tests.h"

namespace {

std::string actions_blob(std::uint8_t finality) {
  msgpack::sbuffer buffer;
  msgpack::packer<msgpack::sbuffer> packer(&buffer);
  packer.pack_array(2);

  packer.pack_map(4);
  packer.pack(std::string("action_id"));
  packer.pack(std::string("first"));
  packer.pack(std::string("finality"));
  packer.pack(finality);
  packer.pack(std::string("success"));
  packer.pack(true);
  packer.pack(std::string("amount"));
  packer.pack(std::uint64_t{17});

  packer.pack_map(4);
  packer.pack(std::string("action_id"));
  packer.pack(std::string("second"));
  packer.pack(std::string("finality"));
  packer.pack(finality);
  packer.pack(std::string("success"));
  packer.pack(false);
  packer.pack(std::string("amount"));
  packer.pack(std::uint64_t{29});

  return {buffer.data(), buffer.size()};
}

const msgpack::object& map_field(const msgpack::object& row, std::string_view name) {
  for (std::uint32_t index = 0; index < row.via.map.size; ++index) {
    const auto& field = row.via.map.ptr[index];
    if (field.key.type == msgpack::type::STR &&
        std::string_view(field.key.via.str.ptr, field.key.via.str.size) == name) {
      return field.val;
    }
  }
  UNREACHABLE();
}

std::optional<std::string> redis_field(const RedisWritePlan& plan, std::string_view name) {
  for (const auto& [field, value] : plan.fields_to_set) {
    if (field == name) {
      return value;
    }
  }
  return std::nullopt;
}

ActiveTrace trace_with_actions(FinalityState trace_finality, std::optional<std::uint8_t> action_finality) {
  ActiveTrace trace;
  trace.finality = trace_finality;
  trace.metadata.emplace("root_node", "root");
  if (action_finality) {
    trace.actions.blob = actions_blob(*action_finality);
    trace.actions.blob_finality = *action_finality;
    trace.actions.classify_state = "ok";
  }
  return trace;
}

PreparedTraceUpdate promote(ActiveTrace current, bool materialize_full_state = false) {
  auto result = prepare_confirmed_promotion(current, CachedConfirmedTrace{}, "trace", 100, materialize_full_state);
  ASSERT_TRUE(result.is_ok());
  return result.move_as_ok();
}

}  // namespace

TEST(TraceProcessor, promotion_rewrites_all_action_finalities_and_keeps_content) {
  auto prepared = promote(trace_with_actions(FinalityState::Finalized, 1));

  auto rewritten = redis_field(prepared.redis, kActionsField);
  ASSERT_TRUE(rewritten.has_value());
  ASSERT_EQ(std::string("2"), *redis_field(prepared.redis, kActionsFinalityField));
  ASSERT_EQ(std::uint8_t{2}, *prepared.next_trace.actions.blob_finality);
  ASSERT_EQ(*rewritten, *prepared.next_trace.actions.blob);

  auto unpacked = msgpack::unpack(rewritten->data(), rewritten->size());
  const auto& rows = unpacked.get();
  ASSERT_EQ(2u, rows.via.array.size);
  ASSERT_EQ(std::string("first"), map_field(rows.via.array.ptr[0], "action_id").as<std::string>());
  ASSERT_TRUE(map_field(rows.via.array.ptr[0], "success").as<bool>());
  ASSERT_EQ(std::uint64_t{17}, map_field(rows.via.array.ptr[0], "amount").as<std::uint64_t>());
  ASSERT_EQ(std::uint64_t{2}, map_field(rows.via.array.ptr[0], "finality").as<std::uint64_t>());
  ASSERT_EQ(std::string("second"), map_field(rows.via.array.ptr[1], "action_id").as<std::string>());
  ASSERT_TRUE(!map_field(rows.via.array.ptr[1], "success").as<bool>());
  ASSERT_EQ(std::uint64_t{29}, map_field(rows.via.array.ptr[1], "amount").as<std::uint64_t>());
  ASSERT_EQ(std::uint64_t{2}, map_field(rows.via.array.ptr[1], "finality").as<std::uint64_t>());
}

TEST(TraceProcessor, promotion_without_actions_writes_no_action_fields) {
  auto prepared = promote(trace_with_actions(FinalityState::Finalized, std::nullopt));

  ASSERT_TRUE(!redis_field(prepared.redis, kActionsField).has_value());
  ASSERT_TRUE(!redis_field(prepared.redis, kActionsFinalityField).has_value());
  ASSERT_TRUE(!redis_field(prepared.redis, kActionsStateField).has_value());
}

TEST(TraceProcessor, promotion_at_equal_finality_still_rewrites_actions) {
  auto prepared = promote(trace_with_actions(FinalityState::Confirmed, 1));

  ASSERT_TRUE(redis_field(prepared.redis, kActionsField).has_value());
  ASSERT_EQ(std::string("1"), *redis_field(prepared.redis, kActionsFinalityField));
  ASSERT_EQ(std::uint8_t{1}, *prepared.next_trace.actions.blob_finality);
}

TEST(TraceProcessor, promotion_cannot_demote_actions) {
  auto current = trace_with_actions(FinalityState::Emulated, 1);
  auto original_blob = *current.actions.blob;
  auto prepared = promote(std::move(current));

  ASSERT_TRUE(!redis_field(prepared.redis, kActionsField).has_value());
  ASSERT_TRUE(!redis_field(prepared.redis, kActionsFinalityField).has_value());
  ASSERT_EQ(std::uint8_t{1}, *prepared.next_trace.actions.blob_finality);
  ASSERT_EQ(original_blob, *prepared.next_trace.actions.blob);
}

TEST(TraceProcessor, full_materialization_restores_action_marker_and_indexes) {
  auto current = trace_with_actions(FinalityState::Finalized, 1);
  current.actions.aai_refs.push_back(TraceStateIndexRef{
      .index_key = "_aai:account",
      .member = "trace:first",
      .score = 17,
  });
  auto prepared = promote(std::move(current), true);

  ASSERT_EQ(std::string("ok"), *redis_field(prepared.redis, kActionsStateField));
  ASSERT_TRUE(redis_field(prepared.redis, kActionsField).has_value());
  ASSERT_EQ(1u, prepared.redis.indexes_to_add.size());
  ASSERT_EQ(std::string("_aai:account"), prepared.redis.indexes_to_add[0].index_key);
  ASSERT_EQ(std::string("trace:first"), prepared.redis.indexes_to_add[0].member);
  ASSERT_EQ(std::uint64_t{17}, prepared.redis.indexes_to_add[0].score);
}

TEST(TraceProcessor, cleanup_removes_aai_refs) {
  const TraceStateIndexRef node_ref{
      .index_key = "account:node",
      .member = "trace:node",
      .score = 11,
  };
  const TraceStateIndexRef aai_ref{
      .index_key = "_aai:account",
      .member = "trace:action",
      .score = 17,
  };
  ActiveTrace current;
  auto node_change = current.nodes.upsert_nodes({TraceStateNode{
      .key = "node",
      .index_refs = {node_ref},
  }});
  current.nodes.apply(std::move(node_change));
  current.actions.aai_refs = {aai_ref};
  TraceSlot slot;
  slot.current = std::make_shared<const ActiveTrace>(std::move(current));

  const auto refs = collect_cleanup_index_refs(slot);

  ASSERT_EQ(2u, refs.size());
  ASSERT_TRUE(std::find(refs.begin(), refs.end(), node_ref) != refs.end());
  ASSERT_TRUE(std::find(refs.begin(), refs.end(), aai_ref) != refs.end());
}

TEST(TraceProcessor, cleanup_of_actionless_trace_unchanged) {
  const TraceStateIndexRef node_ref{
      .index_key = "account:node",
      .member = "trace:node",
      .score = 11,
  };
  ActiveTrace current;
  auto node_change = current.nodes.upsert_nodes({TraceStateNode{
      .key = "node",
      .index_refs = {node_ref},
  }});
  current.nodes.apply(std::move(node_change));
  TraceSlot slot;
  slot.current = std::make_shared<const ActiveTrace>(std::move(current));

  const auto refs = collect_cleanup_index_refs(slot);

  ASSERT_EQ(1u, refs.size());
  ASSERT_EQ(node_ref, refs[0]);
}
