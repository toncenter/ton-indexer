#pragma once

#include <cstdint>

namespace pqxx {
class connection;
}

struct PartitionManagerConfig {
  bool enabled{false};
  std::uint32_t partition_size_mc_seqnos{216000};
  std::uint32_t retention_mc_seqnos{0};
  std::uint32_t precreate_count{2};
};

class PartitionManagerPostgres {
public:
  explicit PartitionManagerPostgres(PartitionManagerConfig config) : config_(config) {}

  void ensure_partitions(pqxx::connection& c, std::uint32_t min_seqno, std::uint32_t max_seqno) const;
  void drop_old_partitions(pqxx::connection& c, std::uint32_t current_seqno) const;

private:
  PartitionManagerConfig config_;
};
