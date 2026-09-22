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

// Shared by standalone account writes and the finalized trace script (Redis forbids nested EVAL).
constexpr const char* kUpdateAccountStateFunction = R"(
local function update_account(key, lt, state, interfaces, hint)
  local cur = redis.call('HGET', key, 'lt')
  -- A nonexist account has LT=0 and must replace the previous state.
  -- Compare decimal uint64 strings without rounding through Lua numbers.
  if not cur or lt == '0' or #lt > #cur or (#lt == #cur and lt > cur) then
    redis.call('HSET', key, 'lt', lt, 'state', state, 'interfaces', interfaces)
    redis.call('PUBLISH', 'streaming_account_states', hint)
  end
  redis.call('EXPIRE', key, 60)
  return 1
end
)";

const std::string kUpdateAccountStateScript = std::string(kUpdateAccountStateFunction) + R"(
return update_account(KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4])
)";

constexpr std::uint32_t kFinalizedVersionTtl = 600;

// Producers starting at different heads know different traces. Deduplicate per trace, not per block.
// Mark success last, after data and publications, and retain it beyond the ordinary replay TTL.
const std::string kWriteFinalizedTrace = std::string(kUpdateAccountStateFunction) + R"(
local last = redis.call('GET', KEYS[1])
if last and tonumber(ARGV[1]) <= tonumber(last) then return 0 end
for _, cmd in ipairs(cmsgpack.unpack(ARGV[3])) do
  if cmd[1] == 'ACCOUNT_STATE' then update_account(unpack(cmd, 2))
  else redis.call(unpack(cmd)) end
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return 1
)";

// Health reports the furthest completed block; it does not authorize or reject trace writes.
constexpr const char* kFinishFinalized = R"(
local last = tonumber(redis.call('HGET', KEYS[1], 'mc_seqno'))
local n = tonumber(ARGV[1])
if last and n <= last then return 0 end
redis.call('HSET', KEYS[1], 'mode', 'finalized', 'finalized_mc_block_time', ARGV[2],
  'mc_seqno', ARGV[1], 'updated_at', redis.call('TIME')[1])
redis.call('EXPIRE', KEYS[1], '20')
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

td::Status append_redis_account_commands(RedisPipeline& pipeline, const RedisWritePlan& plan, std::size_t max_bytes,
                                        bool inside_script) {
  for (const auto& account : plan.account_states) {
    auto hint = pack_streaming_hint(StreamingAccountStateHint{
        .account = account.account,
        .lt = account.lt,
        .finality = static_cast<std::uint8_t>(account.finality),
    });
    const auto key = account.redis_key(), lt = std::to_string(account.lt);
    std::vector<td::Slice> args = inside_script ? std::vector<td::Slice>{"ACCOUNT_STATE"}
        : std::vector<td::Slice>{"EVAL", td::Slice(kUpdateAccountStateScript), "1"};
    args.insert(args.end(), {key, lt, account.state, account.interfaces, hint});
    TRY_STATUS(pipeline.append(args, max_bytes));
  }
  return td::Status::OK();
}

td::Status append_redis_data_commands(RedisPipeline& pipeline, const RedisWritePlan& plan, std::size_t max_bytes,
                                     bool inside_script = false) {
  // Keep chronological plans separate: a carried delete followed by a later
  // reinsert of the same member must retain that order.
  auto index_writes = group_redis_index_writes(plan.indexes_to_remove, plan.indexes_to_add);

  if (plan.erase_trace) {
    for (const auto& index : index_writes) {
      if (!index.members_to_remove.empty()) {
        std::vector<td::Slice> args{"ZREM", index.index_key};
        for (const auto& member : index.members_to_remove) {
          args.emplace_back(member);
        }
        TRY_STATUS(pipeline.append(args, max_bytes));
      }
    }
    TRY_STATUS(pipeline.append({"UNLINK", plan.trace_key}, max_bytes));
    if (!plan.raw_external_message_hash.empty()) {
      TRY_STATUS(pipeline.append({"DEL", "tr_in_msg:" + plan.raw_external_message_hash}, max_bytes));
    }
    return append_redis_account_commands(pipeline, plan, max_bytes, inside_script);
  }

  if (plan.replace_trace) {
    TRY_STATUS(pipeline.append({"DEL", plan.trace_key}, max_bytes));
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
    for (const auto& field : plan.node_fields_to_delete) {
      args.emplace_back(field);
    }
    TRY_STATUS(pipeline.append(args, max_bytes));
  }
  for (const auto& index : index_writes) {
    if (!index.members_to_remove.empty()) {
      std::vector<td::Slice> args{"ZREM", index.index_key};
      for (const auto& member : index.members_to_remove) {
        args.emplace_back(member);
      }
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
        if (formatted.ec != std::errc{}) {
          return td::Status::Error("Cannot encode Redis index score");
        }
        scores.emplace_back(buffer, formatted.ptr);
        args.emplace_back(scores.back());
        args.emplace_back(member);
      }
      TRY_STATUS(pipeline.append(args, max_bytes));
    }
  }
  TRY_STATUS(append_redis_account_commands(pipeline, plan, max_bytes, inside_script));

  if (plan.expire_seconds) {
    TRY_STATUS(pipeline.append({"EXPIRE", plan.trace_key, std::to_string(plan.expire_seconds)}, max_bytes));
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
  for (const auto& plan : batch.plans) {
    if (plan.replace_trace) {
      complete(std::move(completion), td::Status::Error("Full replacement requires a finalized block commit"),
               std::move(batch), timer);
      return;
    }
  }
  auto available = free_slot();
  if (!available) {
    complete(std::move(completion), td::Status::Error("Redis materializer capacity exhausted"), std::move(batch),
             timer);
    return;
  }
  const auto index = *available;
  RedisPipeline data;
  RedisPipeline publications;
  auto status = td::Status::OK();
  try {
    for (const auto& plan : batch.plans) {
      status = append_redis_data_commands(data, plan, options_.max_batch_bytes);
      if (status.is_error()) {
        break;
      }
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
  ensure_slot(index);
  slots_[index].pending.emplace(Pending{std::move(batch), std::move(completion), timer});
  auto promise = td::PromiseCreator::lambda([self = actor_id(this), index](td::Result<td::Unit> result) mutable {
    td::actor::send_closure(self, &RedisMaterializer::finished, index, std::move(result));
  });
  td::actor::send_closure(slots_[index].connection, &RedisConnectionActor::execute, std::move(data),
                          std::move(publications), std::move(promise));
}

std::optional<std::size_t> RedisMaterializer::free_slot() const {
  for (std::size_t i = 0; i < slots_.size(); ++i) {
    if (!slots_[i].pending && !slots_[i].finalized) {
      return i;
    }
  }
  if (slots_.size() < limit_) {
    return slots_.size();
  }
  return std::nullopt;
}

void RedisMaterializer::ensure_slot(std::size_t index) {
  if (index == slots_.size()) {
    slots_.push_back(Slot{td::actor::create_actor<RedisConnectionActor>(
        td::actor::ActorOptions().with_name("RedisConnection").with_poll(), options_), {}, {}, td::Timer{}});
  }
}

void RedisMaterializer::finalized_finished(std::size_t index, td::Result<std::int64_t> result) {
  CHECK(index < slots_.size() && slots_[index].finalized);
  auto promise = std::move(*slots_[index].finalized);
  slots_[index].finalized.reset();
  g_statistics.record_time(INSERT_TRACE, slots_[index].finalized_timer.elapsed() * 1e3);
  promise.set_result(std::move(result));
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
    if (slot.finalized) {
      auto promise = std::move(*slot.finalized);
      slot.finalized.reset();
      promise.set_error(td::Status::Error("Redis materializer stopped"));
    }
    if (!slot.pending) {
      continue;
    }
    auto pending = std::move(*slot.pending);
    slot.pending.reset();
    complete(std::move(pending.completion), td::Status::Error("Redis materializer stopped"), std::move(pending.batch),
             pending.timer);
  }
  slots_.clear();
  control_.reset();
}

void RedisMaterializer::execute_control(RedisPipeline pipeline, td::Promise<std::int64_t> promise) {
  if (control_.empty()) {
    control_ = td::actor::create_actor<RedisConnectionActor>(
        td::actor::ActorOptions().with_name("RedisFinalizedCommit").with_poll(), options_);
  }
  td::actor::send_closure(control_, &RedisConnectionActor::execute_with_reply, std::move(pipeline),
                          RedisPipeline{}, std::move(promise));
}

void RedisMaterializer::write_finalized_trace(ton::BlockSeqno seqno, RedisWritePlan plan,
                                             td::Promise<std::int64_t> promise) {
  auto available = free_slot();
  if (!available) {
    promise.set_error(td::Status::Error("Redis materializer capacity exhausted"));
    return;
  }
  if (plan.trace_key.empty() || (!plan.replace_trace && !plan.erase_trace)) {
    promise.set_error(td::Status::Error("Finalized trace job requires one full snapshot or cleanup"));
    return;
  }
  RedisPipeline commands(true);
  auto status = append_redis_data_commands(commands, plan, options_.max_batch_bytes, true);
  if (status.is_error()) {
    promise.set_error(std::move(status));
    return;
  }
  for (const auto& [channel, message] : plan.publications) {
    status = commands.append({"PUBLISH", channel, message}, options_.max_batch_bytes);
    if (status.is_error()) {
      promise.set_error(std::move(status));
      return;
    }
  }
  // Leave room below Lua's 8000-slot C stack limit when calling unpack().
  for (const auto& command : commands.commands()) {
    if (command.size() > 7500) {
      promise.set_error(td::Status::Error("Redis command exceeds the Lua argument limit"));
      return;
    }
  }
  msgpack::sbuffer buffer;
  msgpack::pack(buffer, commands.commands());
  RedisPipeline pipeline;
  status = pipeline.append({"EVAL", td::Slice(kWriteFinalizedTrace), "1", "finalized:version:" + plan.trace_key,
                            std::to_string(seqno), std::to_string(std::max(kFinalizedVersionTtl, plan.expire_seconds)),
                            td::Slice(buffer.data(), buffer.size())},
                            options_.max_batch_bytes);
  if (status.is_error()) {
    promise.set_error(std::move(status));
    return;
  }
  const auto index = *available;
  ensure_slot(index);
  slots_[index].finalized.emplace(std::move(promise));
  slots_[index].finalized_timer = td::Timer{};
  auto done = td::PromiseCreator::lambda([self = actor_id(this), index](td::Result<std::int64_t> result) mutable {
    td::actor::send_closure(self, &RedisMaterializer::finalized_finished, index, std::move(result));
  });
  td::actor::send_closure(slots_[index].connection, &RedisConnectionActor::execute_with_reply,
                         std::move(pipeline), RedisPipeline{}, std::move(done));
}

void RedisMaterializer::finish_finalized(ton::BlockSeqno seqno, std::uint32_t unix_time,
                                        td::Promise<std::int64_t> promise) {
  RedisPipeline pipeline;
  auto status = pipeline.append({"EVAL", td::Slice(kFinishFinalized), "1", "health:ton-trace-emulator", std::to_string(seqno),
      std::to_string(unix_time)},
      options_.max_batch_bytes);
  if (status.is_error()) {
    promise.set_error(std::move(status));
    return;
  }
  execute_control(std::move(pipeline), std::move(promise));
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
