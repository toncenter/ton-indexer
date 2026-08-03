#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "td/utils/Status.h"
#include "td/utils/Timer.h"

#include "TraceEmulator.h"
#include "TraceState.h"

struct RedisIndexWrite {
  std::string index_key;
  std::vector<std::string> members_to_remove;
  std::vector<std::pair<std::string, double>> members_to_add;
};

inline std::vector<RedisIndexWrite> group_redis_index_writes(const std::vector<TraceStateIndexRef>& removals,
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

struct AccountStateWrite {
  std::string account;
  std::uint64_t lt{0};
  FinalityState finality{FinalityState::Confirmed};
  std::string state;
  std::string interfaces;

  std::string redis_key() const;
};

struct RedisWritePlan {
  std::string trace_key;
  bool erase_trace{false};
  std::vector<std::string> node_fields_to_delete;
  std::vector<TraceStateIndexRef> indexes_to_remove;
  std::vector<TraceStateIndexRef> indexes_to_add;
  std::vector<std::pair<std::string, std::string>> fields_to_set;
  std::vector<AccountStateWrite> account_states;
  std::string raw_external_message_hash;
  std::vector<std::pair<std::string, std::string>> publications;
};

struct RedisWriteBatch {
  std::vector<RedisWritePlan> plans;

  void discard_trace_publications();
};

// Executes an already prepared Redis batch. It owns no trace state and makes
// no retention or classification decisions.
class RedisMaterializer {
 public:
  using Completion = std::function<void(td::Status, RedisWriteBatch)>;

  RedisMaterializer(const std::string& redis_dsn, std::size_t connection_pool_size);
  ~RedisMaterializer();

  RedisMaterializer(const RedisMaterializer&) = delete;
  RedisMaterializer& operator=(const RedisMaterializer&) = delete;

  // Completion is called exactly once, including pipeline creation errors.
  void write(RedisWriteBatch batch, Completion completion, td::Timer timer);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// Clears the entire logical Redis database selected by redis_dsn.
td::Status flush_pending_redis_database(const std::string& redis_dsn);
