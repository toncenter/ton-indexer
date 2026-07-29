#include "StreamingHints.h"

#include "td/utils/tests.h"

#include <string>

TEST(StreamingHints, transaction_hint_is_a_named_msgpack_map) {
    auto packed = pack_streaming_hint(StreamingTransactionHint{
        .trace_key = "trace",
        .update_seq = 42,
        .kind = static_cast<std::uint8_t>(StreamingTransactionKind::Finalized),
        .finality = 1,
        .accounts = {"0:AAAA", "0:BBBB"},
    });

    auto object = msgpack::unpack(packed.data(), packed.size()).get();
    ASSERT_EQ(msgpack::type::MAP, object.type);

    StreamingTransactionHint decoded;
    object.convert(decoded);
    ASSERT_EQ("trace", decoded.trace_key);
    ASSERT_EQ(42u, decoded.update_seq);
    ASSERT_EQ(2u, decoded.kind);
    ASSERT_EQ(1u, decoded.finality);
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
