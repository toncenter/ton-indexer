#include "FailoverWatchdog.h"

#include <cstdlib>

#include <pqxx/pqxx>

#include "td/utils/logging.h"

#include "KvrocksProgress.h"

void FailoverWatchdog::start_up() {
  LOG(INFO) << "Failover watchdog started: check_interval=" << options_.check_interval
            << "s, strikes=" << options_.strikes << ", kvrocks_watermark_check=" << (kvrocks_ ? "on" : "off");
  alarm_timestamp() = td::Timestamp::in(options_.check_interval);
}

void FailoverWatchdog::alarm() {
  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<std::int32_t> R) {
    if (R.is_error()) {
      LOG(WARNING) << "Failover watchdog failed to read indexed seqno: " << R.move_as_error();
      return;
    }
    td::actor::send_closure(SelfId, &FailoverWatchdog::check, R.move_as_ok());
  });
  contiguous_indexed_seqno_getter_(std::move(P));
  alarm_timestamp() = td::Timestamp::in(options_.check_interval);
}

void FailoverWatchdog::check(std::int32_t contiguous_indexed_seqno) {
  adopt_baseline();
  try {
    check_postgres(contiguous_indexed_seqno);
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failover watchdog Postgres check failed (skipping this round): " << e.what();
  }
  if (kvrocks_) {
    try {
      check_kvrocks(contiguous_indexed_seqno);
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failover watchdog Kvrocks check failed (skipping this round): " << e.what();
    }
  }
}

// Seeds the regression shadows from the values get_resume_seqno acted upon,
// so a failover happening between the resume read and the first watchdog
// sample is still detected as a regression instead of silently becoming the
// baseline.
void FailoverWatchdog::adopt_baseline() {
  if (!baseline_) {
    return;
  }
  if (!shadow_finalized_seqno_.has_value()) {
    const auto finalized = baseline_->finalized_mc_seqno.load(std::memory_order_relaxed);
    if (finalized != FailoverBaseline::kUnset) {
      shadow_finalized_seqno_ = finalized;
    }
  }
  if (kvrocks_ && !shadow_kvrocks_watermark_.has_value()) {
    const auto watermark = baseline_->kvrocks_watermark.load(std::memory_order_relaxed);
    if (watermark != FailoverBaseline::kUnset) {
      shadow_kvrocks_watermark_ = watermark;
    }
  }
}

void FailoverWatchdog::check_postgres(std::int32_t contiguous_indexed_seqno) {
  std::optional<std::int64_t> finalized_seqno;
  bool next_block_missing = false;

  {
    pqxx::connection c(pg_connection_string_);
    pqxx::work txn(c);
    auto rows = txn.exec("SELECT finalized_mc_seqno FROM _ton_indexer_progress WHERE id = 1");
    if (!rows.empty()) {
      finalized_seqno = rows[0][0].as<std::int64_t>();
    }
    // Probe within the same snapshot as the finalized read: if seqnos beyond
    // finalized were acknowledged, the block right above it must be visible.
    if (finalized_seqno.has_value() && static_cast<std::int64_t>(contiguous_indexed_seqno) > *finalized_seqno) {
      auto probe = txn.exec(
          "SELECT EXISTS(SELECT 1 FROM blocks WHERE workchain = -1 AND seqno = $1)",
          pqxx::params{*finalized_seqno + 1});
      next_block_missing = !probe[0][0].as<bool>();
    }
  }

  if (!finalized_seqno.has_value()) {
    if (shadow_finalized_seqno_.has_value()) {
      trip(PSTRING() << "Postgres indexer progress row disappeared (last seen finalized_mc_seqno "
                     << *shadow_finalized_seqno_ << ")");
    }
    return;
  }

  if (shadow_finalized_seqno_.has_value() && *finalized_seqno < *shadow_finalized_seqno_) {
    trip(PSTRING() << "Postgres indexer progress regressed from " << *shadow_finalized_seqno_ << " to "
                   << *finalized_seqno << ": Postgres failover lost a tail of committed data");
  }
  if (!shadow_finalized_seqno_.has_value() || *finalized_seqno > *shadow_finalized_seqno_) {
    shadow_finalized_seqno_ = *finalized_seqno;
  }

  if (next_block_missing) {
    ++missing_block_strikes_;
    if (missing_block_strikes_ >= options_.strikes) {
      trip(PSTRING() << "Masterchain block " << (*finalized_seqno + 1)
                     << " is missing from Postgres while seqnos up to " << contiguous_indexed_seqno
                     << " were acknowledged: Postgres failover lost committed blocks");
    }
    LOG(WARNING) << "Masterchain block " << (*finalized_seqno + 1)
                 << " is missing from Postgres while acknowledged head is " << contiguous_indexed_seqno
                 << " (strike " << missing_block_strikes_ << "/" << options_.strikes << ")";
  } else {
    missing_block_strikes_ = 0;
  }
}

void FailoverWatchdog::check_kvrocks(std::int32_t contiguous_indexed_seqno) {
  const auto watermark = kvrocks_progress::get_watermark(*kvrocks_);

  if (!watermark.has_value()) {
    if (shadow_kvrocks_watermark_.has_value()) {
      trip(PSTRING() << "Kvrocks progress watermark disappeared (last seen " << *shadow_kvrocks_watermark_
                     << "): Kvrocks failover lost its data");
    }
    // The watermark is not born yet (no batch committed since the feature was
    // introduced); the resume check has already reported this loudly.
    return;
  }

  if (shadow_kvrocks_watermark_.has_value() && *watermark < *shadow_kvrocks_watermark_) {
    trip(PSTRING() << "Kvrocks progress watermark regressed from " << *shadow_kvrocks_watermark_ << " to "
                   << *watermark << ": Kvrocks failover lost a tail of written data");
  }
  if (!shadow_kvrocks_watermark_.has_value() || *watermark > *shadow_kvrocks_watermark_) {
    shadow_kvrocks_watermark_ = *watermark;
  }

  // Scheduler acks happen after the Kvrocks range claim, so a healthy
  // watermark cannot lag the acknowledged head. The getter snapshots the ack
  // before this GET; reversing that order would race with new acks.
  if (static_cast<std::int64_t>(contiguous_indexed_seqno) > *watermark) {
    ++watermark_lag_strikes_;
    if (watermark_lag_strikes_ >= options_.strikes) {
      trip(PSTRING() << "Kvrocks progress watermark " << *watermark << " is below the acknowledged head "
                     << contiguous_indexed_seqno
                     << " whose Kvrocks writes were claimed before acknowledgement: Kvrocks lost written data");
    }
    LOG(WARNING) << "Kvrocks progress watermark " << *watermark << " is below the acknowledged head "
                 << contiguous_indexed_seqno << " (strike " << watermark_lag_strikes_ << "/" << options_.strikes
                 << ")";
  } else {
    watermark_lag_strikes_ = 0;
  }
}

void FailoverWatchdog::trip(const std::string& reason) {
  LOG(ERROR) << "Failover watchdog: " << reason << ". Exiting with code " << FAILOVER_EXIT_CODE
             << " so the supervisor restarts the worker; on startup it will rewind to the regressed progress "
             << "and re-index the lost range.";
  std::_Exit(FAILOVER_EXIT_CODE);
}
