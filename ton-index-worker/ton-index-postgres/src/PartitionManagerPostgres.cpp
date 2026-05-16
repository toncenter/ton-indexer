#include "PartitionManagerPostgres.h"

#include "td/utils/logging.h"

#include <pqxx/pqxx>

#include <algorithm>
#include <array>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

struct TableSpec {
  std::string_view name;
  std::string_view partition_key;
};

struct PartitionRange {
  std::uint64_t start;
  std::uint64_t end;
};

struct PartitionToDrop {
  std::string name;
  PartitionRange range;
};

constexpr int partition_manager_lock_namespace = 0x544f4e;  // "TON"
constexpr int partition_manager_lock_id = 0x50415254;        // "PART"

constexpr std::array<TableSpec, 10> create_order{{
    {"blocks", "mc_block_seqno"},
    {"shard_state", "mc_seqno"},
    {"transactions", "mc_block_seqno"},
    {"messages", "mc_seqno"},
    {"jetton_transfers", "mc_seqno"},
    {"jetton_burns", "mc_seqno"},
    {"nft_transfers", "mc_seqno"},
    {"traces", "mc_seqno_end"},
    {"actions", "trace_mc_seqno_end"},
    {"action_accounts", "trace_mc_seqno_end"},
}};

constexpr std::array<TableSpec, 10> drop_order{{
    {"blocks", "mc_block_seqno"},
    {"shard_state", "mc_seqno"},
    {"transactions", "mc_block_seqno"},
    {"messages", "mc_seqno"},
    {"jetton_transfers", "mc_seqno"},
    {"jetton_burns", "mc_seqno"},
    {"nft_transfers", "mc_seqno"},
    {"traces", "mc_seqno_end"},
    {"actions", "trace_mc_seqno_end"},
    {"action_accounts", "trace_mc_seqno_end"},
}};

std::string qualified_name(std::string_view name) {
  return "public." + std::string{name};
}

std::string partition_name(std::string_view table_name, std::uint64_t start, std::uint64_t end) {
  return std::string{table_name} + "_p_" + std::to_string(start) + "_" + std::to_string(end);
}

std::string default_partition_name(std::string_view table_name) {
  return std::string{table_name} + "_default";
}

std::optional<std::pair<std::uint64_t, std::uint64_t>> parse_partition_name(
    std::string_view table_name, const std::string& partition_name) {
  const auto prefix = std::string{table_name} + "_p_";
  if (partition_name.rfind(prefix, 0) != 0) {
    return std::nullopt;
  }

  const auto range = partition_name.substr(prefix.size());
  const auto split = range.find('_');
  if (split == std::string::npos || split == 0 || split + 1 >= range.size()) {
    return std::nullopt;
  }

  try {
    std::size_t start_parsed = 0;
    std::size_t end_parsed = 0;
    const auto start_raw = range.substr(0, split);
    const auto end_raw = range.substr(split + 1);
    auto start = std::stoull(start_raw, &start_parsed);
    auto end = std::stoull(end_raw, &end_parsed);
    if (start_parsed != start_raw.size() || end_parsed != end_raw.size()) {
      return std::nullopt;
    }
    if (end <= start) {
      return std::nullopt;
    }
    return std::make_pair(start, end);
  } catch (...) {
    return std::nullopt;
  }
}

std::vector<PartitionRange> get_existing_partition_ranges(const std::unordered_set<std::string>& existing,
                                                          std::string_view table_name) {
  std::vector<PartitionRange> ranges;
  for (const auto& name : existing) {
    auto range = parse_partition_name(table_name, name);
    if (!range.has_value()) {
      continue;
    }
    ranges.push_back({range->first, range->second});
  }
  std::sort(ranges.begin(), ranges.end(), [](const auto& lhs, const auto& rhs) {
    return std::tie(lhs.start, lhs.end) < std::tie(rhs.start, rhs.end);
  });
  return ranges;
}

std::uint64_t skip_existing_covered_ranges(const std::vector<PartitionRange>& existing_ranges,
                                           std::uint64_t start) {
  auto next_start = start;
  for (const auto& range : existing_ranges) {
    if (range.end <= next_start) {
      continue;
    }
    if (range.start > next_start) {
      break;
    }
    next_start = range.end;
  }
  return next_start;
}

std::optional<PartitionRange> find_overlapping_range(const std::vector<PartitionRange>& existing_ranges,
                                                     std::uint64_t start, std::uint64_t end) {
  for (const auto& range : existing_ranges) {
    if (range.end <= start) {
      continue;
    }
    if (range.start >= end) {
      break;
    }
    return range;
  }
  return std::nullopt;
}

bool default_partition_has_rows_in_range(pqxx::work& txn, const TableSpec& table,
                                         const std::unordered_set<std::string>& existing,
                                         std::uint64_t start, std::uint64_t end) {
  const auto default_name = default_partition_name(table.name);
  if (existing.count(default_name) == 0) {
    return false;
  }

  std::stringstream query;
  query << "SELECT EXISTS (SELECT 1 FROM " << qualified_name(default_name)
        << " WHERE " << table.partition_key << " >= $1"
        << " AND " << table.partition_key << " < $2 LIMIT 1)";
  auto rows = txn.exec(query.str(), pqxx::params{start, end});
  return rows[0][0].as<bool>();
}

std::unordered_set<std::string> get_existing_partitions(pqxx::work& txn, std::string_view table_name) {
  std::unordered_set<std::string> result;
  auto rows = txn.exec(R"SQL(
    SELECT child.relname
    FROM pg_inherits
      JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
      JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
      JOIN pg_class child ON child.oid = pg_inherits.inhrelid
      JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
    WHERE parent_ns.nspname = 'public'
      AND child_ns.nspname = 'public'
      AND parent.relname = $1
  )SQL", pqxx::params{std::string{table_name}});
  for (const auto& row : rows) {
    result.insert(row[0].as<std::string>());
  }
  return result;
}

void acquire_partition_manager_lock(pqxx::work& txn) {
  auto result = txn.exec(
      "SELECT pg_advisory_xact_lock($1::integer, $2::integer)",
      pqxx::params{partition_manager_lock_namespace, partition_manager_lock_id});
  if (result.empty()) {
    throw std::runtime_error("Failed to acquire Postgres partition manager advisory lock");
  }
}

bool try_acquire_partition_manager_lock(pqxx::work& txn) {
  auto [locked] = txn.exec(
      "SELECT pg_try_advisory_xact_lock($1::integer, $2::integer)",
      pqxx::params{partition_manager_lock_namespace, partition_manager_lock_id}).one_row().as<bool>();
  return locked;
}

}  // namespace

void PartitionManagerPostgres::ensure_partitions(pqxx::connection& c, std::uint32_t min_seqno,
                                                 std::uint32_t max_seqno) const {
  if (!config_.enabled) {
    return;
  }
  if (config_.partition_size_mc_seqnos == 0) {
    throw std::runtime_error("Postgres partition size must be greater than zero");
  }

  const std::uint64_t partition_size = config_.partition_size_mc_seqnos;
  const std::uint64_t first_start = (static_cast<std::uint64_t>(min_seqno) / partition_size) * partition_size;
  const std::uint64_t last_batch_start = (static_cast<std::uint64_t>(max_seqno) / partition_size) * partition_size;
  const auto max_safe_start =
      ((std::numeric_limits<std::uint64_t>::max() - partition_size) / partition_size) * partition_size;
  const auto available_future_partitions = (max_safe_start - last_batch_start) / partition_size;
  const auto future_partitions = std::min<std::uint64_t>(config_.precreate_count, available_future_partitions);
  const std::uint64_t last_start = last_batch_start + partition_size * future_partitions;

  pqxx::work txn(c);
  acquire_partition_manager_lock(txn);

  std::stringstream query;
  std::size_t created_count = 0;
  std::size_t skipped_count = 0;
  for (const auto& table : create_order) {
    auto existing = get_existing_partitions(txn, table.name);
    auto existing_ranges = get_existing_partition_ranges(existing, table.name);
    auto start = skip_existing_covered_ranges(existing_ranges, first_start);
    const auto target_end = last_start + partition_size;
    if (start > first_start) {
      skipped_count += (std::min(start, target_end) - first_start + partition_size - 1) / partition_size;
    }
    while (start < target_end) {
      if (std::numeric_limits<std::uint64_t>::max() - start < partition_size) {
        break;
      }
      const auto end = start + partition_size;
      const auto name = partition_name(table.name, start, end);
      if (existing.count(name) != 0) {
        start = end;
        continue;
      }
      auto overlapping_range = find_overlapping_range(existing_ranges, start, end);
      if (overlapping_range.has_value()) {
        start = overlapping_range->end;
        ++skipped_count;
        continue;
      }
      if (default_partition_has_rows_in_range(txn, table, existing, start, end)) {
        std::stringstream error;
        error << "Cannot create Postgres hot partition " << name << " for " << table.name
              << " seqno range [" << start << ", " << end << "): " << default_partition_name(table.name)
              << " contains rows in that range. Migrate or detach the default partition before enabling "
                 "--pg-manage-partitions.";
        throw std::runtime_error(error.str());
      }
      query << "CREATE TABLE IF NOT EXISTS " << qualified_name(name)
            << " PARTITION OF " << qualified_name(table.name)
            << " FOR VALUES FROM (" << start << ") TO (" << end << ");\n";
      ++created_count;
      start = end;
    }
  }

  if (created_count > 0) {
    const auto last_end = last_start + partition_size;
    LOG(INFO) << "Creating " << created_count << " Postgres hot partitions for seqno range ["
              << first_start << ", " << last_end << ")";
    txn.exec(query.str()).no_rows();
  }
  if (skipped_count > 0) {
    LOG(INFO) << "Skipped " << skipped_count << " already covered Postgres hot partition ranges";
  }
  txn.commit();
}

void PartitionManagerPostgres::drop_old_partitions(pqxx::connection& c) const {
  if (!config_.enabled || config_.retention_mc_seqnos == 0) {
    return;
  }

  pqxx::work txn(c);
  if (!try_acquire_partition_manager_lock(txn)) {
    LOG(INFO) << "Skipping old Postgres hot partition drop because another partition manager is active";
    txn.commit();
    return;
  }
  txn.exec("SET LOCAL lock_timeout = '250ms'").no_rows();

  auto progress = txn.exec("SELECT finalized_mc_seqno FROM ton_indexer_progress WHERE id = 1");
  if (progress.empty()) {
    txn.commit();
    return;
  }
  const auto finalized_seqno = progress[0][0].as<std::int32_t>();
  if (finalized_seqno <= 0 || static_cast<std::uint64_t>(finalized_seqno) <= config_.retention_mc_seqnos) {
    txn.commit();
    return;
  }

  const auto drop_before = static_cast<std::uint64_t>(finalized_seqno) - config_.retention_mc_seqnos;
  std::stringstream query;
  std::size_t dropped_count = 0;
  for (const auto& table : drop_order) {
    auto existing = get_existing_partitions(txn, table.name);
    std::vector<PartitionToDrop> partitions_to_drop;
    for (const auto& name : existing) {
      auto range = parse_partition_name(table.name, name);
      if (!range.has_value()) {
        continue;
      }
      if (range->second > drop_before) {
        continue;
      }
      partitions_to_drop.push_back({
          .name = name,
          .range = {.start = range->first, .end = range->second},
      });
    }
    std::sort(partitions_to_drop.begin(), partitions_to_drop.end(), [](const auto& lhs, const auto& rhs) {
      return std::tie(lhs.range.start, lhs.range.end, lhs.name) < std::tie(rhs.range.start, rhs.range.end, rhs.name);
    });
    for (const auto& partition : partitions_to_drop) {
      query << "DROP TABLE IF EXISTS " << qualified_name(partition.name) << ";\n";
      ++dropped_count;
    }
  }

  if (dropped_count > 0) {
    LOG(WARNING) << "Dropping " << dropped_count << " old Postgres hot partitions before seqno " << drop_before;
    txn.exec(query.str()).no_rows();
  }
  txn.commit();
}
