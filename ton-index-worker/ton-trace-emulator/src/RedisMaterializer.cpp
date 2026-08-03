#include <algorithm>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <sw/redis++/redis++.h>
#include <utility>

#include "td/actor/actor.h"

#include "RedisMaterializer.h"
#include "Statistics.h"
#include "StreamingHints.h"

namespace {

constexpr bool kCreateDedicatedPipelineConnection = false;

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

sw::redis::Redis create_redis(const std::string& redis_dsn, std::size_t connection_pool_size) {
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

void append_redis_data_commands(sw::redis::Pipeline& pipeline, const RedisWritePlan& plan) {
  // Keep chronological plans separate: a carried delete followed by a later
  // reinsert of the same member must retain that order.
  auto index_writes = group_redis_index_writes(plan.indexes_to_remove, plan.indexes_to_add);

  if (plan.erase_trace) {
    for (const auto& index : index_writes) {
      if (!index.members_to_remove.empty()) {
        pipeline.zrem(index.index_key, index.members_to_remove.begin(), index.members_to_remove.end());
      }
    }
    pipeline.unlink(plan.trace_key);
    if (!plan.raw_external_message_hash.empty()) {
      pipeline.del("tr_in_msg:" + plan.raw_external_message_hash);
    }
    return;
  }

  if (!plan.node_fields_to_delete.empty()) {
    pipeline.hdel(plan.trace_key, plan.node_fields_to_delete.begin(), plan.node_fields_to_delete.end());
  }
  for (const auto& index : index_writes) {
    if (!index.members_to_remove.empty()) {
      pipeline.zrem(index.index_key, index.members_to_remove.begin(), index.members_to_remove.end());
    }
    if (!index.members_to_add.empty()) {
      pipeline.zadd(index.index_key, index.members_to_add.begin(), index.members_to_add.end());
    }
  }
  if (!plan.fields_to_set.empty()) {
    pipeline.hset(plan.trace_key, plan.fields_to_set.begin(), plan.fields_to_set.end());
  }
  for (const auto& account : plan.account_states) {
    auto hint = pack_streaming_hint(StreamingAccountStateHint{
        .account = account.account,
        .lt = account.lt,
        .finality = static_cast<std::uint8_t>(account.finality),
    });
    pipeline.eval(kUpdateAccountStateScript, {account.redis_key()},
                  {std::to_string(account.lt), account.state, account.interfaces, std::move(hint)});
  }

  pipeline.setex("tr_in_msg:" + plan.raw_external_message_hash, 600, plan.trace_key);
}

bool append_redis_publications(sw::redis::Pipeline& pipeline, const RedisWriteBatch& batch) {
  bool appended = false;
  for (const auto& plan : batch.plans) {
    for (const auto& [channel, message] : plan.publications) {
      pipeline.publish(channel, message);
      appended = true;
    }
  }
  return appended;
}

void execute_pipeline(sw::redis::Pipeline& pipeline) {
  auto replies = pipeline.exec();
  for (std::size_t index = 0; index < replies.size(); ++index) {
    // Reading every reply makes redis-plus-plus surface command errors.
    replies.get(index);
  }
}

class RedisWriteActor final : public td::actor::Actor {
 public:
  RedisWriteActor(sw::redis::Pipeline&& pipeline, RedisWriteBatch batch, RedisMaterializer::Completion completion,
                  td::Timer timer)
      : pipeline_(std::move(pipeline)), batch_(std::move(batch)), completion_(std::move(completion)), timer_(timer) {
  }

 private:
  sw::redis::Pipeline pipeline_;
  RedisWriteBatch batch_;
  RedisMaterializer::Completion completion_;
  td::Timer timer_;

  void start_up() override {
    auto status = td::Status::OK();
    try {
      for (const auto& plan : batch_.plans) {
        append_redis_data_commands(pipeline_, plan);
      }
      execute_pipeline(pipeline_);

      // Publish only after all trace data is visible. Account-state
      // notifications remain atomic with their Lua update above.
      if (append_redis_publications(pipeline_, batch_)) {
        execute_pipeline(pipeline_);
      }
    } catch (const std::exception& error) {
      status = td::Status::Error("Failed to write trace to Redis: " + std::string(error.what()));
    } catch (...) {
      status = td::Status::Error("Failed to write trace to Redis: unknown error");
    }
    g_statistics.record_time(INSERT_TRACE, timer_.elapsed() * 1e3);
    completion_(std::move(status), std::move(batch_));
    stop();
  }
};

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

struct RedisMaterializer::Impl {
  Impl(const std::string& redis_dsn, std::size_t connection_pool_size)
      : redis(create_redis(redis_dsn, connection_pool_size)) {
  }

  sw::redis::Redis redis;
};

RedisMaterializer::RedisMaterializer(const std::string& redis_dsn, std::size_t connection_pool_size)
    : impl_(std::make_unique<Impl>(redis_dsn, connection_pool_size)) {
}

RedisMaterializer::~RedisMaterializer() = default;

void RedisMaterializer::write(RedisWriteBatch batch, Completion completion, td::Timer timer) {
  try {
    auto pipeline = impl_->redis.pipeline(kCreateDedicatedPipelineConnection);
    td::actor::create_actor<RedisWriteActor>("RedisMaterializer", std::move(pipeline), std::move(batch),
                                             std::move(completion), timer)
        .release();
  } catch (const std::exception& error) {
    g_statistics.record_time(INSERT_TRACE, timer.elapsed() * 1e3);
    completion(td::Status::Error("Failed to create Redis pipeline: " + std::string(error.what())), std::move(batch));
  } catch (...) {
    g_statistics.record_time(INSERT_TRACE, timer.elapsed() * 1e3);
    completion(td::Status::Error("Failed to create Redis pipeline: unknown error"), std::move(batch));
  }
}

td::Status flush_pending_redis_database(const std::string& redis_dsn) {
  try {
    auto redis = create_redis(redis_dsn, 1);
    redis.flushdb();
    return td::Status::OK();
  } catch (const std::exception& error) {
    return td::Status::Error("Failed to flush pending Redis database: " + std::string(error.what()));
  } catch (...) {
    return td::Status::Error("Failed to flush pending Redis database: unknown error");
  }
}
