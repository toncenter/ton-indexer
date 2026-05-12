#include "KvrocksClient.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <stdexcept>

namespace {

std::string trim(std::string value) {
  auto is_space = [](unsigned char ch) {
    return std::isspace(ch) != 0;
  };
  value.erase(value.begin(), std::find_if(value.begin(), value.end(), [&](char ch) {
    return !is_space(static_cast<unsigned char>(ch));
  }));
  value.erase(std::find_if(value.rbegin(), value.rend(), [&](char ch) {
    return !is_space(static_cast<unsigned char>(ch));
  }).base(), value.end());
  return value;
}

std::vector<std::string> split(const std::string &value, char delimiter) {
  std::vector<std::string> result;
  std::stringstream ss(value);
  std::string item;
  while (std::getline(ss, item, delimiter)) {
    result.push_back(trim(item));
  }
  return result;
}

}  // namespace

bool KvrocksConfig::use_sentinel() const {
  return !sentinel_nodes.empty();
}

std::string KvrocksConfig::describe() const {
  std::stringstream ss;
  if (use_sentinel()) {
    ss << "sentinel master=" << sentinel_master_name << " sentinels=";
    bool first = true;
    for (const auto &[host, port] : sentinel_nodes) {
      if (!first) {
        ss << ",";
      }
      ss << host << ":" << port;
      first = false;
    }
  } else {
    ss << "direct uri=" << (uri.empty() ? "tcp://127.0.0.1:6379" : uri);
  }
  ss << " pool_size=" << pool_size;
  ss << " connect_timeout_ms=" << connect_timeout.count();
  ss << " socket_timeout_ms=" << socket_timeout.count();
  return ss.str();
}

std::vector<std::pair<std::string, int>> parse_kvrocks_sentinel_nodes(const std::string &nodes) {
  std::vector<std::pair<std::string, int>> result;
  for (const auto &raw_node : split(nodes, ',')) {
    if (raw_node.empty()) {
      continue;
    }

    auto colon_pos = raw_node.rfind(':');
    if (colon_pos == std::string::npos || colon_pos == 0 || colon_pos + 1 >= raw_node.size()) {
      throw std::invalid_argument("sentinel node must be host:port: " + raw_node);
    }

    auto host = trim(raw_node.substr(0, colon_pos));
    auto port_str = trim(raw_node.substr(colon_pos + 1));
    int port = 0;
    try {
      port = std::stoi(port_str);
    } catch (...) {
      throw std::invalid_argument("sentinel node port is not a number: " + raw_node);
    }
    if (port <= 0 || port >= 65536) {
      throw std::invalid_argument("sentinel node port is out of range: " + raw_node);
    }
    result.emplace_back(std::move(host), port);
  }

  if (result.empty()) {
    throw std::invalid_argument("sentinel node list is empty");
  }
  return result;
}

KvrocksClient::KvrocksClient(KvrocksConfig config) : config_(std::move(config)) {
  if (!config_.enabled) {
    throw std::invalid_argument("Kvrocks client cannot be constructed with disabled config");
  }
  if (config_.use_sentinel() && config_.sentinel_master_name.empty()) {
    throw std::invalid_argument("Kvrocks sentinel master name is required in sentinel mode");
  }
}

const KvrocksConfig& KvrocksClient::config() const {
  return config_;
}

std::shared_ptr<sw::redis::Redis> KvrocksClient::make_redis() const {
  auto connection_opts = build_connection_options();
  auto pool_opts = build_pool_options();

  if (config_.use_sentinel()) {
    auto sentinel = std::make_shared<sw::redis::Sentinel>(build_sentinel_options());
    return std::make_shared<sw::redis::Redis>(
        sentinel, config_.sentinel_master_name, sw::redis::Role::MASTER, connection_opts, pool_opts);
  }

  return std::make_shared<sw::redis::Redis>(connection_opts, pool_opts);
}

void KvrocksClient::ping() const {
  auto redis = make_redis();
  auto response = redis->ping();
  if (response != "PONG") {
    throw std::runtime_error("unexpected Kvrocks PING response: " + response);
  }
}

sw::redis::ConnectionOptions KvrocksClient::build_connection_options() const {
  sw::redis::ConnectionOptions opts;
  if (!config_.use_sentinel() && !config_.uri.empty()) {
    opts = sw::redis::Uri(config_.uri).connection_options();
  } else {
    opts.host = "127.0.0.1";
    opts.port = 6379;
  }

  if (config_.user.has_value()) {
    opts.user = *config_.user;
  }
  if (config_.password.has_value()) {
    opts.password = *config_.password;
  }
  if (config_.db.has_value()) {
    opts.db = *config_.db;
  }
  opts.keep_alive = true;
  opts.connect_timeout = config_.connect_timeout;
  opts.socket_timeout = config_.socket_timeout;
  return opts;
}

sw::redis::ConnectionPoolOptions KvrocksClient::build_pool_options() const {
  sw::redis::ConnectionPoolOptions opts;
  opts.size = config_.pool_size;
  opts.wait_timeout = config_.wait_timeout;
  return opts;
}

sw::redis::SentinelOptions KvrocksClient::build_sentinel_options() const {
  sw::redis::SentinelOptions opts;
  opts.nodes = config_.sentinel_nodes;
  if (config_.sentinel_user.has_value()) {
    opts.user = *config_.sentinel_user;
  }
  if (config_.sentinel_password.has_value()) {
    opts.password = *config_.sentinel_password;
  }
  opts.connect_timeout = config_.connect_timeout;
  opts.socket_timeout = config_.socket_timeout;
  opts.retry_interval = config_.sentinel_retry_interval;
  opts.max_retry = config_.sentinel_max_retry;
  return opts;
}
