#pragma once

#include "TraceState.h"

#include <map>
#include <string>
#include <utility>
#include <vector>

struct RedisIndexWrite {
    std::string index_key;
    std::vector<std::string> members_to_remove;
    std::vector<std::pair<std::string, double>> members_to_add;
};

inline std::vector<RedisIndexWrite> group_redis_index_writes(
    const std::vector<TraceStateIndexRef>& removals,
    const std::vector<TraceStateIndexRef>& additions) {
    std::map<std::string, RedisIndexWrite> grouped;

    for (const auto& index : removals) {
        auto& write = grouped[index.index_key];
        write.index_key = index.index_key;
        write.members_to_remove.push_back(index.member);
    }
    for (const auto& index : additions) {
        auto& write = grouped[index.index_key];
        write.index_key = index.index_key;
        write.members_to_add.emplace_back(index.member, static_cast<double>(index.score));
    }

    std::vector<RedisIndexWrite> result;
    result.reserve(grouped.size());
    for (auto& [_, write] : grouped) {
        result.push_back(std::move(write));
    }
    return result;
}
