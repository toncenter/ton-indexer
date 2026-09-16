#include "RedisMaterializer.h"

#include "td/utils/tests.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace {

TraceStateIndexRef index_ref(std::string key,
                             std::string member,
                             std::uint64_t score = 0) {
    return TraceStateIndexRef{
        .index_key = std::move(key),
        .member = std::move(member),
        .score = score,
    };
}

}  // namespace

TEST(RedisMaterializer, groups_index_changes_by_redis_key) {
    auto writes = group_redis_index_writes(
        {
            index_ref("account-b", "trace-3"),
            index_ref("account-a", "trace-1"),
            index_ref("account-a", "trace-2"),
        },
        {
            index_ref("account-b", "trace-4", 40),
            index_ref("account-a", "trace-1", 10),
        });

    ASSERT_EQ(2u, writes.size());

    ASSERT_EQ("account-a", writes[0].index_key);
    ASSERT_EQ(std::vector<std::string>({"trace-1", "trace-2"}),
              writes[0].members_to_remove);
    ASSERT_EQ(1u, writes[0].members_to_add.size());
    ASSERT_EQ("trace-1", writes[0].members_to_add[0].first);
    ASSERT_EQ(10.0, writes[0].members_to_add[0].second);

    ASSERT_EQ("account-b", writes[1].index_key);
    ASSERT_EQ(std::vector<std::string>({"trace-3"}),
              writes[1].members_to_remove);
    ASSERT_EQ(1u, writes[1].members_to_add.size());
    ASSERT_EQ("trace-4", writes[1].members_to_add[0].first);
    ASSERT_EQ(40.0, writes[1].members_to_add[0].second);
}
