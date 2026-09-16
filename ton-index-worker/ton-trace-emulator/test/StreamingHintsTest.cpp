#include <string>

#include "td/utils/tests.h"

#include "StreamingHints.h"

TEST(StreamingHints, transaction_hint_is_a_named_msgpack_map) {
  auto packed = pack_streaming_hint(StreamingTransactionHint{
      .trace_key = "trace",
      .update_seq = 42,
      .update_finality = static_cast<std::uint8_t>(StreamingUpdateFinality::Finalized),
      .trace_finality = 1,
      .accounts = {"0:AAAA", "0:BBBB"},
  });

  auto object_handle = msgpack::unpack(packed.data(), packed.size());
  auto object = object_handle.get();
  ASSERT_EQ(msgpack::type::MAP, object.type);

  StreamingTransactionHint decoded;
  object.convert(decoded);
  ASSERT_EQ("trace", decoded.trace_key);
  ASSERT_EQ(42u, decoded.update_seq);
  ASSERT_EQ(2u, decoded.update_finality);
  ASSERT_EQ(1u, decoded.trace_finality);
  ASSERT_EQ(2u, decoded.accounts.size());
  ASSERT_EQ("0:AAAA", decoded.accounts[0]);
  ASSERT_EQ("0:BBBB", decoded.accounts[1]);
}

TEST(StreamingHints, account_state_hint_contains_redis_version) {
  auto packed = pack_streaming_hint(StreamingAccountStateHint{
      .account = "0:AAAA",
      .lt = 123,
      .finality = 2,
  });

  StreamingAccountStateHint decoded;
  msgpack::unpack(packed.data(), packed.size()).get().convert(decoded);
  ASSERT_EQ("0:AAAA", decoded.account);
  ASSERT_EQ(123u, decoded.lt);
  ASSERT_EQ(2u, decoded.finality);
}

TEST(StreamingHints, actions_hint_contains_version_status_and_routes) {
  auto packed = pack_streaming_hint(StreamingActionsHint{
      .trace_key = "trace",
      .update_seq = 42,
      .update_finality = 2,
      .trace_finality = 1,
      .actions_updated = true,
      .action_types_and_accounts =
          {
              StreamingActionRoute{
                  .type = "jetton_transfer",
                  .accounts = {"0:AAAA", "0:BBBB"},
              },
          },
  });

  StreamingActionsHint decoded;
  msgpack::unpack(packed.data(), packed.size()).get().convert(decoded);
  ASSERT_EQ("trace", decoded.trace_key);
  ASSERT_EQ(42u, decoded.update_seq);
  ASSERT_EQ(2u, decoded.update_finality);
  ASSERT_EQ(1u, decoded.trace_finality);
  ASSERT_TRUE(decoded.actions_updated);
  ASSERT_EQ(1u, decoded.action_types_and_accounts.size());
  ASSERT_EQ("jetton_transfer", decoded.action_types_and_accounts[0].type);
  ASSERT_EQ(2u, decoded.action_types_and_accounts[0].accounts.size());
}
