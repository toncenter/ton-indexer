#include <algorithm>
#include <hiredis/hiredis.h>
#include <hiredis/read.h>

#include "RedisConnectionActor.h"

namespace {
constexpr std::size_t kIoBytesPerTurn = 64 * 1024;
constexpr std::size_t kRepliesPerTurn = 256;
constexpr std::size_t kMaxReplyBytes = 64 * 1024;
}  // namespace

RedisConnectionActor::RedisConnectionActor(RedisConnectionOptions options) : options_(std::move(options)) {
}

RedisConnectionActor::~RedisConnectionActor() = default;

void RedisConnectionActor::ReaderDeleter::operator()(redisReader* reader) const {
  redisReaderFree(reader);
}

void RedisConnectionActor::execute(RedisPipeline data, RedisPipeline publications, td::Promise<td::Unit> promise) {
  if (request_) {
    promise.set_error(td::Status::Error("Redis connection already has an active batch"));
    return;
  }
  if (data.bytes().size() > options_.max_batch_bytes ||
      publications.bytes().size() > options_.max_batch_bytes - data.bytes().size()) {
    promise.set_error(td::Status::Error("Redis batch exceeds the encoded size limit"));
    return;
  }
  request_.emplace(Request{std::move(data), std::move(publications), std::move(promise)});
  deadline_ = td::Timestamp::in(options_.batch_timeout);
  alarm_timestamp() = deadline_;
  if (socket_.empty()) {
    auto status = connect();
    if (status.is_error()) {
      finish(std::move(status));
      return;
    }
  } else {
    start_phase(Phase::Data);
  }
  yield();
}

td::Status RedisConnectionActor::connect() {
  TRY_RESULT(socket, open_redis_socket(options_));
  socket_ = std::move(socket);
  reader_.reset(redisReaderCreate());
  if (!reader_) {
    return td::Status::Error("Cannot allocate Redis reply parser");
  }
  td::actor::SchedulerContext::get().get_poll().subscribe(socket_.get_poll_info().extract_pollable_fd(this),
                                                          td::PollFlags::ReadWrite());
  subscribed_ = true;
  connecting_ = true;
  alarm_timestamp().relax(td::Timestamp::in(options_.connect_timeout));

  setup_ = RedisPipeline{};
  if (options_.user != "default") {
    TRY_STATUS(setup_.append({"AUTH", options_.user, options_.password}, options_.max_batch_bytes));
  } else if (!options_.password.empty()) {
    TRY_STATUS(setup_.append({"AUTH", options_.password}, options_.max_batch_bytes));
  }
  if (options_.db != 0) {
    TRY_STATUS(setup_.append({"SELECT", std::to_string(options_.db)}, options_.max_batch_bytes));
  }
  start_phase(Phase::Setup);
  return td::Status::OK();
}

void RedisConnectionActor::notify() {
  // Poll observers run in scheduler context, but outside actor execution.
  td::actor::send_closure_later(self_, &RedisConnectionActor::loop);
}

void RedisConnectionActor::start_up() {
  self_ = actor_id(this);
}

const RedisPipeline& RedisConnectionActor::pipeline() const {
  if (phase_ == Phase::Setup)
    return setup_;
  if (phase_ == Phase::Data)
    return request_->data;
  return request_->publications;
}

void RedisConnectionActor::start_phase(Phase phase) {
  phase_ = phase;
  written_ = 0;
  remaining_replies_ = pipeline().replies();
  first_error_.reset();
}

td::Status RedisConnectionActor::drive() {
  if (socket_.empty())
    return td::Status::OK();
  td::sync_with_poll(socket_);
  TRY_STATUS(socket_.get_pending_error());
  if (!request_) {
    // A write-only connection must not receive unsolicited data. Close stale
    // idle sockets; the next batch will establish and authenticate a new one.
    if (td::can_read(socket_)) {
      char byte;
      TRY_RESULT(read, socket_.read(td::MutableSlice(&byte, 1)));
      if (read) {
        close_connection();
        return td::Status::OK();
      }
    }
    if (td::can_close(socket_))
      close_connection();
    return td::Status::OK();
  }
  if (deadline_.is_in_past())
    return td::Status::Error("Redis batch timed out");
  if (connecting_) {
    if (!td::can_write(socket_)) {
      if (td::can_close(socket_))
        return td::Status::Error("Redis connection closed while connecting");
      return td::Status::OK();
    }
    connecting_ = false;
    alarm_timestamp() = deadline_;
  }

  const auto& bytes = pipeline().bytes();
  std::size_t write_budget = kIoBytesPerTurn;
  while (written_ < bytes.size() && write_budget && td::can_write(socket_)) {
    auto size = std::min(write_budget, bytes.size() - written_);
    TRY_RESULT(written, socket_.write(td::Slice(bytes.data() + written_, size)));
    written_ += written;
    write_budget -= written;
  }

  std::size_t read_budget = kIoBytesPerTurn;
  std::size_t reply_budget = kRepliesPerTurn;
  while (remaining_replies_ && reply_budget) {
    void* raw = nullptr;
    if (redisReaderGetReply(reader_.get(), &raw) != REDIS_OK) {
      return td::Status::Error("Malformed Redis reply");
    }
    std::unique_ptr<redisReply, decltype(&freeReplyObject)> reply(static_cast<redisReply*>(raw), freeReplyObject);
    if (reply) {
      if (reply->len > kMaxReplyBytes)
        return td::Status::Error("Redis reply exceeds the size limit");
      if (reply->type == REDIS_REPLY_ERROR) {
        if (!first_error_) {
          // Setup errors deliberately omit server text, which might echo AUTH arguments.
          first_error_ = phase_ == Phase::Setup
                             ? td::Status::Error("Redis authentication or database selection failed")
                             : td::Status::Error("Redis command failed: " + std::string(reply->str, reply->len));
        }
      } else if ((phase_ == Phase::Setup && reply->type != REDIS_REPLY_STATUS) ||
                 (reply->type != REDIS_REPLY_INTEGER &&
                  !(reply->type == REDIS_REPLY_STATUS && td::Slice(reply->str, reply->len) == "OK"))) {
        return td::Status::Error("Unexpected Redis reply type for write command");
      }
      --remaining_replies_;
      --reply_budget;
      continue;
    }
    if (reader_->len - reader_->pos > kMaxReplyBytes) {
      return td::Status::Error("Incomplete Redis reply exceeds the size limit");
    }
    if (!read_budget || !td::can_read(socket_))
      break;
    char buffer[16384];
    TRY_RESULT(read, socket_.read(td::MutableSlice(buffer, std::min(sizeof(buffer), read_budget))));
    read_budget -= read;
    if (read && redisReaderFeed(reader_.get(), buffer, read) != REDIS_OK) {
      return td::Status::Error("Cannot buffer Redis reply");
    }
  }
  if (remaining_replies_ == 0) {
    if (written_ != bytes.size() || reader_->len != reader_->pos) {
      return td::Status::Error("Redis reply stream is out of sync");
    }
    phase_finished();
    return td::Status::OK();
  }
  if (td::can_close(socket_) && !td::can_read(socket_)) {
    return td::Status::Error("Redis connection closed before all replies arrived");
  }
  // Budget exhaustion is cooperative yielding, not a busy loop while waiting
  // for Redis: EAGAIN clears readiness and the poller supplies the next wakeup.
  if (!write_budget || !read_budget || !reply_budget)
    yield();
  return td::Status::OK();
}

void RedisConnectionActor::phase_finished() {
  if (first_error_) {
    finish(std::move(*first_error_));
    return;
  }
  if (phase_ == Phase::Setup) {
    setup_ = RedisPipeline{};
    start_phase(Phase::Data);
  } else if (phase_ == Phase::Data) {
    // Preserve the existing visibility barrier: publish trace notifications
    // only after every data command has successfully replied.
    start_phase(Phase::Publications);
  } else {
    finish(td::Status::OK());
    return;
  }
  yield();
}

void RedisConnectionActor::loop() {
  auto status = drive();
  if (status.is_error())
    finish(std::move(status));
}

void RedisConnectionActor::alarm() {
  if (request_) {
    finish(td::Status::Error(td::Slice(connecting_ ? "Redis connect timed out" : "Redis batch timed out")));
  }
}

void RedisConnectionActor::finish(td::Status status) {
  if (status.is_error())
    close_connection();
  first_error_.reset();
  alarm_timestamp() = td::Timestamp::never();
  if (!request_)
    return;
  auto promise = std::move(request_->promise);
  request_.reset();
  // No automatic replay after disconnect: some commands may already have
  // executed. The caller retains the original batch and its retry semantics.
  if (status.is_error())
    promise.set_error(std::move(status));
  else
    promise.set_value(td::Unit());
}

void RedisConnectionActor::close_connection() {
  if (subscribed_) {
    td::actor::SchedulerContext::get().get_poll().unsubscribe(socket_.get_poll_info().get_pollable_fd_ref());
    subscribed_ = false;
  }
  socket_.close();
  reader_.reset();
  setup_ = RedisPipeline{};
  connecting_ = false;
}

void RedisConnectionActor::tear_down() {
  finish(td::Status::Error("Redis connection actor stopped"));
}
