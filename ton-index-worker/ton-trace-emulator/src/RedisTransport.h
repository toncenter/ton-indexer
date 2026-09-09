#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "td/utils/Slice.h"
#include "td/utils/port/IPAddress.h"
#include "td/utils/port/SocketFd.h"

struct RedisConnectionOptions {
  td::IPAddress address;
  std::string unix_path;
  std::string user = "default";
  std::string password;
  int db = 0;
  double connect_timeout = 2.0;
  // Absolute deadline for a write batch, or for establishing a subscription.
  double batch_timeout = 5.0;
  std::size_t max_batch_bytes = 64 * 1024 * 1024;
};

// Resolves TCP hostnames. Call before starting the actor scheduler; reconnects
// reuse this endpoint and never perform synchronous DNS inside an actor.
td::Result<RedisConnectionOptions> parse_redis_connection_options(const std::string& uri);

// Binary-safe RESP encoding only: hiredis performs no socket I/O here.
class RedisPipeline {
 public:
  td::Status append(const std::vector<td::Slice>& arguments, std::size_t max_bytes);
  const std::string& bytes() const {
    return bytes_;
  }
  std::size_t replies() const {
    return replies_;
  }

 private:
  std::string bytes_;
  std::size_t replies_ = 0;
};

// Opens a nonblocking socket using the already resolved endpoint.
td::Result<td::SocketFd> open_redis_socket(const RedisConnectionOptions& options);
