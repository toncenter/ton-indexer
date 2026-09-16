#include <cerrno>
#include <cmath>
#include <cstring>
#include <hiredis/hiredis.h>
#include <limits>
#include <memory>
#include <sw/redis++/redis_uri.h>

#include "RedisTransport.h"

#if TD_PORT_POSIX
#include <sys/socket.h>
#include <sys/un.h>
#endif

td::Result<td::SocketFd> open_redis_socket(const RedisConnectionOptions& options) {
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

td::Result<RedisConnectionOptions> parse_redis_connection_options(const std::string& uri) {
  try {
    const auto parsed = sw::redis::Uri(uri).connection_options();
    if (parsed.resp != 2)
      return td::Status::Error("Redis transport requires RESP2");
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
