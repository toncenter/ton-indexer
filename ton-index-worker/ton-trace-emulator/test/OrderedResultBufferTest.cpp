#include "OrderedResultBuffer.h"

#include "td/utils/tests.h"

#include <optional>
#include <string>

TEST(OrderedResultBuffer, later_result_waits_for_its_predecessor) {
    OrderedResultBuffer<std::string> buffer;
    buffer.reset(10);

    ASSERT_TRUE(buffer.insert(11, "eleven"));
    ASSERT_TRUE(!buffer.take_next());

    ASSERT_TRUE(buffer.insert(10, "ten"));
    auto ten = buffer.take_next();
    ASSERT_TRUE(ten.has_value());
    ASSERT_EQ(10u, ten->seqno);
    ASSERT_EQ("ten", ten->value);

    auto eleven = buffer.take_next();
    ASSERT_TRUE(eleven.has_value());
    ASSERT_EQ(11u, eleven->seqno);
    ASSERT_EQ("eleven", eleven->value);
}

TEST(OrderedResultBuffer, failed_slot_still_preserves_order) {
    OrderedResultBuffer<std::optional<std::string>> buffer;
    buffer.reset(20);

    ASSERT_TRUE(buffer.insert(21, std::string{"ready"}));
    ASSERT_TRUE(buffer.insert(20, std::nullopt));

    auto failed = buffer.take_next();
    ASSERT_TRUE(failed.has_value());
    ASSERT_EQ(20u, failed->seqno);
    ASSERT_TRUE(!failed->value);

    auto ready = buffer.take_next();
    ASSERT_TRUE(ready.has_value());
    ASSERT_EQ(21u, ready->seqno);
    ASSERT_EQ("ready", *ready->value);
}

TEST(OrderedResultBuffer, duplicate_sequence_is_rejected) {
    OrderedResultBuffer<std::string> buffer;
    buffer.reset(30);

    ASSERT_TRUE(buffer.insert(30, "first"));
    ASSERT_TRUE(!buffer.insert(30, "duplicate"));

    auto result = buffer.take_next();
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ("first", result->value);
    ASSERT_TRUE(!buffer.insert(30, "too late"));
}
