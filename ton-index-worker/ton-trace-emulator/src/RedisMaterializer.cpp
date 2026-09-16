#include <algorithm>
#include <charconv>
#include <chrono>
#include <limits>
#include <stdexcept>
#include <sw/redis++/redis++.h>
#include <utility>

#include "td/actor/actor.h"

#include "RedisMaterializer.h"
#include "Statistics.h"
#include "StreamingHints.h"

namespace {

constexpr const char* kUpdateAccountStateScript = R"(
    local cur = redis.call('HGET', KEYS[1], 'lt')
    local cur_num = tonumber(cur)
    local new_num = tonumber(ARGV[1])
    if (not cur_num) or (new_num > cur_num) then
        redis.call('HSET', KEYS[1], 'lt', ARGV[1], 'state', ARGV[2], 'interfaces', ARGV[3])
        redis.call('PUBLISH', 'streaming_account_states', ARGV[4])
    end
    redis.call('EXPIRE', KEYS[1], 60)
    return 1
)";

// Used only by the explicit pre-scheduler startup FLUSHDB.
sw::redis::Redis create_startup_redis(const std::string& redis_dsn, std::size_t connection_pool_size) {
  sw::redis::Uri uri(redis_dsn);
  auto connection_options = uri.connection_options();
  if (connection_options.connect_timeout == std::chrono::milliseconds{0}) {
    connection_options.connect_timeout = std::chrono::seconds{2};
  }
  if (connection_options.socket_timeout == std::chrono::milliseconds{0}) {
    connection_options.socket_timeout = std::chrono::seconds{5};
  }

  auto pool_options = uri.connection_pool_options();
  pool_options.size = std::max(pool_options.size, connection_pool_size);
  if (pool_options.wait_timeout == std::chrono::milliseconds{0}) {
    pool_options.wait_timeout = std::chrono::seconds{5};
  }
  return sw::redis::Redis(connection_options, pool_options);
}

td::Status append_redis_data_commands(RedisPipeline& pipeline, const RedisWritePlan& plan, std::size_t max_bytes) {
  // Keep chronological plans separate: a carried delete followed by a later
  // reinsert of the same member must retain that order.
  auto index_writes = group_redis_index_writes(plan.indexes_to_remove, plan.indexes_to_add);

  if (plan.erase_trace) {
    for (const auto& index : index_writes) {
      if (!index.members_to_remove.empty()) {
        std::vector<td::Slice> args{"ZREM", index.index_key};
        for (const auto& member : index.members_to_remove)
          args.emplace_back(member);
        TRY_STATUS(pipeline.append(args, max_bytes));
      }
    }
    TRY_STATUS(pipeline.append({"UNLINK", plan.trace_key}, max_bytes));
    if (!plan.raw_external_message_hash.empty()) {
      TRY_STATUS(pipeline.append({"DEL", "tr_in_msg:" + plan.raw_external_message_hash}, max_bytes));
    }
    return td::Status::OK();
  }

  // Switch the payload, root pointer and update_seq in one HSET before removing
  // obsolete fields. A reader of an older hint then sees either the intact old
  // graph or the new version, never a pointer to a root we have just deleted.
  if (!plan.fields_to_set.empty()) {
    std::vector<td::Slice> args{"HSET", plan.trace_key};
    for (const auto& [field, value] : plan.fields_to_set) {
      args.emplace_back(field);
      args.emplace_back(value);
    }
    TRY_STATUS(pipeline.append(args, max_bytes));
  }
  if (!plan.node_fields_to_delete.empty()) {
    std::vector<td::Slice> args{"HDEL", plan.trace_key};
    for (const auto& field : plan.node_fields_to_delete)
      args.emplace_back(field);
    TRY_STATUS(pipeline.append(args, max_bytes));
  }
  for (const auto& index : index_writes) {
    if (!index.members_to_remove.empty()) {
      std::vector<td::Slice> args{"ZREM", index.index_key};
      for (const auto& member : index.members_to_remove)
        args.emplace_back(member);
      TRY_STATUS(pipeline.append(args, max_bytes));
    }
    if (!index.members_to_add.empty()) {
      std::vector<std::string> scores;
      scores.reserve(index.members_to_add.size());
      std::vector<td::Slice> args{"ZADD", index.index_key};
      for (const auto& [member, score] : index.members_to_add) {
        char buffer[64];
        auto formatted = std::to_chars(buffer, buffer + sizeof(buffer), score, std::chars_format::general,
                                       std::numeric_limits<double>::max_digits10);
        if (formatted.ec != std::errc{})
          return td::Status::Error("Cannot encode Redis index score");
        scores.emplace_back(buffer, formatted.ptr);
        args.emplace_back(scores.back());
        args.emplace_back(member);
      }
      TRY_STATUS(pipeline.append(args, max_bytes));
    }
  }
  for (const auto& account : plan.account_states) {
    auto hint = pack_streaming_hint(StreamingAccountStateHint{
        .account = account.account,
        .lt = account.lt,
        .finality = static_cast<std::uint8_t>(account.finality),
    });
    TRY_STATUS(pipeline.append({"EVAL", td::Slice(kUpdateAccountStateScript), "1", account.redis_key(),
                                std::to_string(account.lt), account.state, account.interfaces, hint},
                               max_bytes));
  }

  return pipeline.append({"SETEX", "tr_in_msg:" + plan.raw_external_message_hash, "600", plan.trace_key}, max_bytes);
}

td::Status append_redis_publications(RedisPipeline& pipeline, const RedisWriteBatch& batch, std::size_t max_bytes) {
  for (const auto& plan : batch.plans) {
    for (const auto& [channel, message] : plan.publications) {
      TRY_STATUS(pipeline.append({"PUBLISH", channel, message}, max_bytes));
    }
  }
  return td::Status::OK();
}

}  // namespace

std::string AccountStateWrite::redis_key() const {
  switch (finality) {
    case FinalityState::Confirmed:
      return "account_confirmed:" + account;
    case FinalityState::Finalized:
      return "account_finalized:" + account;
    case FinalityState::Emulated:
      throw std::logic_error("Emulated account state cannot be written to Redis");
  }
  throw std::logic_error("Unknown account state finality");
}

void RedisWriteBatch::discard_trace_publications() {
  for (auto& plan : plans) {
    plan.publications.clear();
  }
}

RedisMaterializer::RedisMaterializer(RedisConnectionOptions options, std::size_t max_concurrent_batches)
    : options_(std::move(options)), limit_(std::max(std::size_t{1}, max_concurrent_batches)) {
}

void RedisMaterializer::write(RedisWriteBatch batch, Completion completion, td::Timer timer) {
  auto index = std::size_t{0};
  while (index < slots_.size() && slots_[index].pending)
    ++index;
  if (index == slots_.size() && slots_.size() == limit_) {
    complete(std::move(completion), td::Status::Error("Redis materializer capacity exhausted"), std::move(batch),
             timer);
    return;
  }
  RedisPipeline data;
  RedisPipeline publications;
  auto status = td::Status::OK();
  try {
    for (const auto& plan : batch.plans) {
      status = append_redis_data_commands(data, plan, options_.max_batch_bytes);
      if (status.is_error())
        break;
    }
    if (status.is_ok()) {
      status = append_redis_publications(publications, batch, options_.max_batch_bytes - data.bytes().size());
    }
  } catch (...) {
    status = td::Status::Error("Failed to encode Redis batch");
  }
  if (status.is_error()) {
    complete(std::move(completion), std::move(status), std::move(batch), timer);
    return;
  }
  if (index == slots_.size()) {
    slots_.push_back(Slot{td::actor::create_actor<RedisConnectionActor>(
                              td::actor::ActorOptions().with_name("RedisConnection").with_poll(), options_),
                          {}});
  }
  slots_[index].pending.emplace(Pending{std::move(batch), std::move(completion), timer});
  auto promise = td::PromiseCreator::lambda([self = actor_id(this), index](td::Result<td::Unit> result) mutable {
    td::actor::send_closure(self, &RedisMaterializer::finished, index, std::move(result));
  });
  td::actor::send_closure(slots_[index].connection, &RedisConnectionActor::execute, std::move(data),
                          std::move(publications), std::move(promise));
}

void RedisMaterializer::finished(std::size_t index, td::Result<td::Unit> result) {
  CHECK(index < slots_.size() && slots_[index].pending);
  auto pending = std::move(*slots_[index].pending);
  slots_[index].pending.reset();
  complete(std::move(pending.completion), result.is_error() ? result.move_as_error() : td::Status::OK(),
           std::move(pending.batch), pending.timer);
}

void RedisMaterializer::complete(Completion completion, td::Status status, RedisWriteBatch batch, td::Timer timer) {
  g_statistics.record_time(INSERT_TRACE, timer.elapsed() * 1e3);
  completion(std::move(status), std::move(batch));
}

void RedisMaterializer::tear_down() {
  for (auto& slot : slots_) {
    if (!slot.pending)
      continue;
    auto pending = std::move(*slot.pending);
    slot.pending.reset();
    complete(std::move(pending.completion), td::Status::Error("Redis materializer stopped"), std::move(pending.batch),
             pending.timer);
  }
  slots_.clear();
}

td::Status flush_pending_redis_database(const std::string& redis_dsn) {
  try {
    auto redis = create_startup_redis(redis_dsn, 1);
    redis.flushdb();
    return td::Status::OK();
  } catch (const std::exception& error) {
    return td::Status::Error("Failed to flush pending Redis database: " + std::string(error.what()));
  } catch (...) {
    return td::Status::Error("Failed to flush pending Redis database: unknown error");
  }
}
