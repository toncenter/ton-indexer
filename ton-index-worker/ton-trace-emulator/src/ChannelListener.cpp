#include <algorithm>
#include <hiredis/hiredis.h>
#include <hiredis/read.h>

#include "ChannelListener.h"

namespace {
constexpr std::size_t kIoBytesPerTurn = 64 * 1024;
constexpr std::size_t kRepliesPerTurn = 256;
constexpr std::size_t kMaxReplyBytes = 1024 * 1024;

const redisReplyObjectFunctions& default_reply_functions() {
  static const auto functions = [] {
    std::unique_ptr<redisReader, decltype(&redisReaderFree)> reader(redisReaderCreate(), redisReaderFree);
    CHECK(reader);
    return *reader->fn;
  }();
  return functions;
}

// Reject oversized/nested arrays before hiredis allocates their elements.
// The template accommodates hiredis versions using either int or size_t.
template <class Size>
void* create_pubsub_array(const redisReadTask* task, Size elements) {
  if (task->parent || elements != 3)
    return nullptr;
  return default_reply_functions().createArray(task, elements);
}

redisReader* create_reader() {
  static auto functions = [] {
    auto functions = default_reply_functions();
    functions.createArray = create_pubsub_array;
    return functions;
  }();
  return redisReaderCreateWithFunctions(&functions);
}

bool is_string(const redisReply* reply, td::Slice value) {
  return reply->type == REDIS_REPLY_STRING && td::Slice(reply->str, reply->len) == value;
}
}  // namespace

ChannelListener::ChannelListener(RedisConnectionOptions options, std::string channel, Handler handler)
    : options_(std::move(options)), channel_(std::move(channel)), handler_(std::move(handler)) {
}

ChannelListener::~ChannelListener() = default;

void ChannelListener::ReaderDeleter::operator()(redisReader* reader) const {
  redisReaderFree(reader);
}

void ChannelListener::start_up() {
  self_ = actor_id(this);
  auto status = connect();
  if (status.is_error())
    reconnect(std::move(status));
}

td::Status ChannelListener::connect() {
  TRY_RESULT(socket, open_redis_socket(options_));
  socket_ = std::move(socket);
  reader_.reset(create_reader());
  if (!reader_)
    return td::Status::Error("Cannot allocate Redis subscriber parser");
  td::actor::SchedulerContext::get().get_poll().subscribe(socket_.get_poll_info().extract_pollable_fd(this),
                                                          td::PollFlags::ReadWrite());
  poll_subscribed_ = true;
  connecting_ = true;
  phase_ = Phase::Setup;
  setup_deadline_ = td::Timestamp::in(options_.batch_timeout);
  alarm_timestamp() = setup_deadline_;
  alarm_timestamp().relax(td::Timestamp::in(options_.connect_timeout));

  if (options_.user != "default") {
    TRY_STATUS(output_.append({"AUTH", options_.user, options_.password}, kMaxReplyBytes));
  } else if (!options_.password.empty()) {
    TRY_STATUS(output_.append({"AUTH", options_.password}, kMaxReplyBytes));
  }
  if (options_.db != 0)
    TRY_STATUS(output_.append({"SELECT", std::to_string(options_.db)}, kMaxReplyBytes));
  setup_replies_ = output_.replies();
  if (!setup_replies_)
    TRY_STATUS(subscribe());
  yield();
  return td::Status::OK();
}

td::Status ChannelListener::subscribe() {
  output_ = RedisPipeline{};
  written_ = 0;
  phase_ = Phase::Subscribe;
  return output_.append({"SUBSCRIBE", channel_}, kMaxReplyBytes);
}

void ChannelListener::notify() {
  td::actor::send_closure_later(self_, &ChannelListener::loop);
}

td::Status ChannelListener::process_reply(const redisReply& reply, std::vector<std::string>& messages) {
  if (phase_ == Phase::Setup) {
    if (reply.type != REDIS_REPLY_STATUS || td::Slice(reply.str, reply.len) != "OK")
      return td::Status::Error("Redis subscriber authentication or database selection failed");
    if (--setup_replies_ == 0) {
      TRY_STATUS(subscribe());
      yield();
    }
    return td::Status::OK();
  }
  if (reply.type != REDIS_REPLY_ARRAY || reply.elements != 3 || !is_string(reply.element[1], channel_))
    return td::Status::Error("Unexpected Redis subscription reply");
  if (phase_ == Phase::Subscribe) {
    if (!is_string(reply.element[0], "subscribe") || reply.element[2]->type != REDIS_REPLY_INTEGER ||
        reply.element[2]->integer != 1)
      return td::Status::Error("Invalid Redis subscription acknowledgement");
    phase_ = Phase::Listening;
    output_ = RedisPipeline{};
    written_ = 0;
    retry_delay_ = 0.1;
    alarm_timestamp() = td::Timestamp::never();
    return td::Status::OK();
  }
  if (!is_string(reply.element[0], "message") || reply.element[2]->type != REDIS_REPLY_STRING)
    return td::Status::Error("Unexpected Redis Pub/Sub message");
  messages.emplace_back(reply.element[2]->str, reply.element[2]->len);
  return td::Status::OK();
}

td::Status ChannelListener::drive(std::vector<std::string>& messages) {
  if (socket_.empty() || (phase_ == Phase::Listening && delivery_pending_))
    return td::Status::OK();
  td::sync_with_poll(socket_);
  TRY_STATUS(socket_.get_pending_error());
  if (phase_ != Phase::Listening && setup_deadline_.is_in_past())
    return td::Status::Error("Redis subscription setup timed out");
  if (connecting_) {
    if (!td::can_write(socket_)) {
      if (td::can_close(socket_))
        return td::Status::Error("Redis subscriber closed while connecting");
      return td::Status::OK();
    }
    connecting_ = false;
    alarm_timestamp() = setup_deadline_;
  }

  std::size_t write_budget = kIoBytesPerTurn;
  while (written_ < output_.bytes().size() && write_budget && td::can_write(socket_)) {
    auto size = std::min(write_budget, output_.bytes().size() - written_);
    TRY_RESULT(written, socket_.write(td::Slice(output_.bytes().data() + written_, size)));
    written_ += written;
    write_budget -= written;
  }

  std::size_t read_budget = kIoBytesPerTurn;
  std::size_t reply_budget = kRepliesPerTurn;
  while (reply_budget && !(phase_ == Phase::Listening && delivery_pending_)) {
    auto buffered = reader_->len - reader_->pos;
    void* raw = nullptr;
    if (redisReaderGetReply(reader_.get(), &raw) != REDIS_OK)
      return td::Status::Error("Malformed Redis subscription reply");
    std::unique_ptr<redisReply, decltype(&freeReplyObject)> reply(static_cast<redisReply*>(raw), freeReplyObject);
    reply_bytes_ += buffered - (reader_->len - reader_->pos);
    if (reply) {
      reply_bytes_ = 0;
      if (written_ != output_.bytes().size())
        return td::Status::Error("Redis subscription reply before command was sent");
      auto was_setup = phase_ == Phase::Setup;
      TRY_STATUS(process_reply(*reply, messages));
      --reply_budget;
      // AUTH/SELECT completed: send SUBSCRIBE on the next turn.
      if (was_setup && phase_ == Phase::Subscribe)
        return td::Status::OK();
      continue;
    }
    buffered = reader_->len - reader_->pos;
    if (reply_bytes_ + buffered >= kMaxReplyBytes)
      return td::Status::Error("Redis subscription reply exceeds the size limit");
    if (!read_budget || !td::can_read(socket_))
      break;
    char buffer[16384];
    auto size = std::min({sizeof(buffer), read_budget, kMaxReplyBytes - reply_bytes_ - buffered});
    TRY_RESULT(read, socket_.read(td::MutableSlice(buffer, size)));
    read_budget -= read;
    if (read && redisReaderFeed(reader_.get(), buffer, read) != REDIS_OK)
      return td::Status::Error("Cannot buffer Redis subscription reply");
  }
  // Drain complete buffered replies before reporting EOF. The next turn (or
  // the delivery acknowledgement) resumes even without another poll edge.
  if (!read_budget || !reply_budget || !write_budget) {
    yield();
  } else if (td::can_close(socket_) && !td::can_read(socket_) && !delivery_pending_) {
    return td::Status::Error("Redis subscription closed");
  }
  return td::Status::OK();
}

void ChannelListener::loop() {
  std::vector<std::string> messages;
  auto status = drive(messages);
  if (status.is_error())
    reconnect(std::move(status));
  if (!messages.empty()) {
    delivery_pending_ = true;
    handler_(std::move(messages), td::PromiseCreator::lambda([self = self_](td::Result<td::Unit> result) mutable {
               td::actor::send_closure(self, &ChannelListener::delivery_finished, std::move(result));
             }));
  }
}

void ChannelListener::delivery_finished(td::Result<td::Unit> result) {
  delivery_pending_ = false;
  if (result.is_error()) {
    stop();
    return;
  }
  yield();
}

void ChannelListener::reconnect(td::Status error) {
  LOG(WARNING) << "Redis subscriber: " << error << "; retrying in " << retry_delay_ << "s";
  close_connection();
  alarm_timestamp() = td::Timestamp::in(retry_delay_);
  retry_delay_ = std::min(retry_delay_ * 2, 5.0);
}

void ChannelListener::alarm() {
  if (socket_.empty()) {
    auto status = connect();
    if (status.is_error())
      reconnect(std::move(status));
  } else if (phase_ != Phase::Listening) {
    reconnect(td::Status::Error("Redis subscription setup timed out"));
  }
}

void ChannelListener::close_connection() {
  if (poll_subscribed_) {
    td::actor::SchedulerContext::get().get_poll().unsubscribe(socket_.get_poll_info().get_pollable_fd_ref());
    poll_subscribed_ = false;
  }
  socket_.close();
  reader_.reset();
  output_ = RedisPipeline{};
  written_ = reply_bytes_ = setup_replies_ = 0;
  connecting_ = false;
}

void ChannelListener::tear_down() {
  close_connection();
}
