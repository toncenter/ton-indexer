#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <hiredis/hiredis.h>
#include <hiredis/read.h>
#include <limits>
#include <sw/redis++/redis_uri.h>

#include "RedisConnectionActor.h"

#if TD_PORT_POSIX
#include <sys/socket.h>
#include <sys/un.h>
#endif

namespace {
constexpr std::size_t kIoBytesPerTurn = 64 * 1024;
constexpr std::size_t kRepliesPerTurn = 256;
constexpr std::size_t kMaxReplyBytes = 64 * 1024;

td::Result<td::SocketFd> open_socket(const RedisConnectionOptions& options) {
  if (options.unix_path.empty()) {
    return td::SocketFd::open(options.address);
  }
#if TD_PORT_POSIX
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  if (options.unix_path.size() >= sizeof(address.sun_path)) {
    return td::Status::Error("Redis UNIX socket path is too long");
  }
  std::memcpy(address.sun_path, options.unix_path.c_str(), options.unix_path.size() + 1);
  td::NativeFd fd{::socket(AF_UNIX, SOCK_STREAM, 0)};
  if (!fd) {
    return td::Status::PosixError(errno, "Cannot open Redis UNIX socket");
  }
  TRY_STATUS(fd.set_is_blocking_unsafe(false));
  if (::connect(fd.socket(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0 &&
      errno != EINPROGRESS) {
    return td::Status::PosixError(errno, "Cannot connect to Redis UNIX socket");
  }
  return td::SocketFd::from_native_fd(std::move(fd));
#else
  return td::Status::Error("Redis UNIX sockets are not supported on this platform");
#endif
}
}  // namespace

td::Result<RedisConnectionOptions> parse_redis_connection_options(const std::string& uri) {
  try {
    const auto parsed = sw::redis::Uri(uri).connection_options();
    if (parsed.resp != 2)
      return td::Status::Error("Redis materializer requires RESP2");
    if (parsed.db < 0)
      return td::Status::Error("Redis database must be nonnegative");
    RedisConnectionOptions options;
    options.user = parsed.user;
    options.password = parsed.password;
    options.db = parsed.db;
    if (parsed.connect_timeout.count())
      options.connect_timeout = parsed.connect_timeout.count() / 1000.0;
    if (parsed.socket_timeout.count())
      options.batch_timeout = parsed.socket_timeout.count() / 1000.0;
    if (!std::isfinite(options.connect_timeout) || options.connect_timeout <= 0 ||
        !std::isfinite(options.batch_timeout) || options.batch_timeout <= 0) {
      return td::Status::Error("Redis timeouts must be positive");
    }
    if (parsed.type == sw::redis::ConnectionType::UNIX) {
      if (parsed.path.empty())
        return td::Status::Error("Redis UNIX socket path is empty");
      options.unix_path = parsed.path;
    } else {
      TRY_STATUS(options.address.init_host_port(parsed.host, parsed.port));
    }
    return options;
  } catch (...) {
    // A URI parser exception may contain credentials from a malformed option.
    return td::Status::Error("Invalid Redis connection URI");
  }
}

td::Status RedisPipeline::append(const std::vector<td::Slice>& arguments, std::size_t max_bytes) {
  if (arguments.empty() || arguments.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    return td::Status::Error("Invalid Redis command argument count");
  }
  // Check size before asking hiredis to allocate the encoded command. Each
  // argument needs at most 32 additional bytes for its RESP length and CRLFs.
  std::size_t size_bound = 32;
  for (auto argument : arguments) {
    if (size_bound > max_bytes || max_bytes - size_bound < 32 || argument.size() > max_bytes - size_bound - 32) {
      return td::Status::Error("Redis batch exceeds the encoded size limit");
    }
    size_bound += argument.size() + 32;
  }
  if (bytes_.size() > max_bytes || size_bound > max_bytes - bytes_.size()) {
    return td::Status::Error("Redis batch exceeds the encoded size limit");
  }
  std::vector<const char*> argv;
  std::vector<std::size_t> lengths;
  argv.reserve(arguments.size());
  lengths.reserve(arguments.size());
  for (auto argument : arguments) {
    argv.push_back(argument.empty() ? "" : argument.data());
    lengths.push_back(argument.size());
  }
  char* raw = nullptr;
  auto size = redisFormatCommandArgv(&raw, static_cast<int>(argv.size()), argv.data(), lengths.data());
  std::unique_ptr<char, decltype(&redisFreeCommand)> command(raw, redisFreeCommand);
  if (size < 0 || !command) {
    return td::Status::Error("Cannot encode Redis command");
  }
  bytes_.append(command.get(), static_cast<std::size_t>(size));
  ++replies_;
  return td::Status::OK();
}

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
  TRY_RESULT(socket, open_socket(options_));
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
