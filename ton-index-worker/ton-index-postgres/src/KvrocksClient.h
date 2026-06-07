#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <sw/redis++/redis++.h>

struct KvrocksConfig {
  bool enabled{false};

  // Direct Redis/Kvrocks URI. Example: tcp://127.0.0.1:6666/0
  std::string uri;

  // Sentinel mode is enabled when sentinel_nodes is not empty.
  std::vector<std::pair<std::string, int>> sentinel_nodes;
  std::string sentinel_master_name;

  std::optional<std::string> user;
  std::optional<std::string> password;
  std::optional<int> db;

  std::optional<std::string> sentinel_user;
  std::optional<std::string> sentinel_password;

  std::size_t pool_size{128};
  std::chrono::milliseconds connect_timeout{1000};
  std::chrono::milliseconds socket_timeout{1000};
  std::chrono::milliseconds wait_timeout{1000};
  std::chrono::milliseconds sentinel_retry_interval{100};
  std::size_t sentinel_max_retry{2};

  bool use_sentinel() const;
  std::string describe() const;
};

std::vector<std::pair<std::string, int>> parse_kvrocks_sentinel_nodes(const std::string &nodes);

class KvrocksClient {
public:
  explicit KvrocksClient(KvrocksConfig config);

  const KvrocksConfig& config() const;
  std::shared_ptr<sw::redis::Redis> make_redis() const;
  void ping() const;

private:
  sw::redis::ConnectionOptions build_connection_options() const;
  sw::redis::ConnectionPoolOptions build_pool_options() const;
  sw::redis::SentinelOptions build_sentinel_options() const;

  KvrocksConfig config_;
};
