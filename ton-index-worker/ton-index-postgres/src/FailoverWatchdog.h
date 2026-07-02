#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include <sw/redis++/redis++.h>

#include "td/actor/actor.h"

#include "FailoverBaseline.h"

// Process exit code used when a Postgres or Kvrocks failover with data loss is
// detected. The supervisor (systemd Restart=always / docker restart policy)
// restarts the worker, which then recomputes the resume seqno from
// _ton_indexer_progress and the Kvrocks progress watermark and re-indexes the
// lost tail.
inline constexpr int FAILOVER_EXIT_CODE = 86;

// Watches direct facts about acknowledged data, not timing heuristics:
// - _ton_indexer_progress.finalized_mc_seqno went backwards: Postgres lost a
//   committed tail.
// - The masterchain block above finalized_mc_seqno is absent while higher
//   seqnos were acknowledged: Postgres lost committed blocks.
// - The Kvrocks watermark went backwards, disappeared, or fell below the
//   acknowledged head: Kvrocks lost written data.
// On detection the watchdog exits with FAILOVER_EXIT_CODE; supervisor restart
// then re-enters the resume logic and re-indexes the lost range.
class FailoverWatchdog : public td::actor::Actor {
public:
  struct Options {
    double check_interval{10.0};
    // Consecutive checks a probed inconsistency must persist before restart.
    std::uint32_t strikes{3};
  };
  using ContiguousIndexedSeqnoGetter = std::function<void(td::Promise<std::int32_t>)>;

  FailoverWatchdog(std::string pg_connection_string, std::shared_ptr<sw::redis::Redis> kvrocks,
                   ContiguousIndexedSeqnoGetter contiguous_indexed_seqno_getter,
                   std::shared_ptr<FailoverBaseline> baseline, Options options)
      : pg_connection_string_(std::move(pg_connection_string)),
        kvrocks_(std::move(kvrocks)),
        contiguous_indexed_seqno_getter_(std::move(contiguous_indexed_seqno_getter)),
        baseline_(std::move(baseline)),
        options_(options) {
  }

  void start_up() override;
  void alarm() override;

private:
  void check(std::int32_t contiguous_indexed_seqno);
  void adopt_baseline();
  void check_postgres(std::int32_t contiguous_indexed_seqno);
  void check_kvrocks(std::int32_t contiguous_indexed_seqno);
  void trip(const std::string& reason);

  std::string pg_connection_string_;
  std::shared_ptr<sw::redis::Redis> kvrocks_;
  ContiguousIndexedSeqnoGetter contiguous_indexed_seqno_getter_;
  std::shared_ptr<FailoverBaseline> baseline_;
  Options options_;

  std::optional<std::int64_t> shadow_finalized_seqno_;
  std::optional<std::int64_t> shadow_kvrocks_watermark_;
  std::uint32_t missing_block_strikes_{0};
  std::uint32_t watermark_lag_strikes_{0};
};
