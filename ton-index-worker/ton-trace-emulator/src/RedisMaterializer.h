#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "td/actor/actor.h"
#include "td/utils/Status.h"
#include "td/utils/Timer.h"

#include "RedisConnectionActor.h"
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

// CPU actor: encodes prepared batches and owns the bounded connection pool.
// It owns no trace state and makes no retention or classification decisions.
class RedisMaterializer final : public td::actor::Actor {
 public:
  using Completion = std::function<void(td::Status, RedisWriteBatch)>;

  RedisMaterializer(RedisConnectionOptions options, std::size_t max_concurrent_batches);

  RedisMaterializer(const RedisMaterializer&) = delete;
  RedisMaterializer& operator=(const RedisMaterializer&) = delete;

  // Invoke with send_closure. Completion runs in this actor's scheduler
  // context and returns the original batch even on partial-write errors.
  void write(RedisWriteBatch batch, Completion completion, td::Timer timer);

 private:
  struct Pending {
    RedisWriteBatch batch;
    Completion completion;
    td::Timer timer;
  };
  struct Slot {
    td::actor::ActorOwn<RedisConnectionActor> connection;
    std::optional<Pending> pending;
  };

  RedisConnectionOptions options_;
  std::size_t limit_;
  std::vector<Slot> slots_;

  void finished(std::size_t index, td::Result<td::Unit> result);
  static void complete(Completion completion, td::Status status, RedisWriteBatch batch, td::Timer timer);
  void tear_down() override;
};

// Clears the entire logical Redis database selected by redis_dsn.
td::Status flush_pending_redis_database(const std::string& redis_dsn);
