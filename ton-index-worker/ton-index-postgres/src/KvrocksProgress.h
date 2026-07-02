#pragma once

#include <cstdint>
#include <optional>

#include <sw/redis++/redis++.h>

namespace kvrocks_progress {

// Monotonic watermark of the Kvrocks-side indexing progress: the highest
// masterchain seqno such that the Kvrocks writes of all seqnos up to it are
// contiguously present in the current Kvrocks history.
//
// It is advanced only through ranges claimed by claim_written_range(), which
// the leader calls right after the Kvrocks writes of each batch — so the
// claim enters the Kvrocks replication stream after the data it describes,
// and on any replica watermark W implies all Kvrocks data for seqnos <= W is
// present. A failover to a lagging replica rolls the watermark back together
// with the data: it can never overstate what survived. In particular it does
// NOT mirror the Postgres progress: during post-failover re-indexing Postgres
// finalized_mc_seqno runs ahead, and the watermark catches up only as the
// batches are actually re-written to Kvrocks.
inline constexpr const char kWatermarkKey[] = "ton-index:v1:progress:finalized_mc_seqno";

// Sorted set of claimed-but-not-yet-swept seqno ranges (score = range start,
// member = "start:end"). Batches complete out of order, so a claim above the
// contiguous frontier waits here until the gap below it is claimed too.
inline constexpr const char kPendingRangesKey[] = "ton-index:v1:progress:pending_mc_seqno_ranges";

// Current watermark, or nullopt when the key is missing. Throws on a
// non-numeric value.
std::optional<std::int64_t> get_watermark(sw::redis::Redis& redis);

// Creates the watermark key when it does not exist yet (first run of the
// feature, or explicit acceptance of a Kvrocks rebuild). Claims never create
// the watermark on their own: otherwise an in-flight batch racing with a
// just-lost Kvrocks state would silently re-create it at the head, masking
// the loss from the resume check.
void initialize_watermark_if_missing(sw::redis::Redis& redis, std::int64_t watermark);

// Atomically records that the Kvrocks writes of the inclusive seqno range
// [first, last] are fully applied, and advances the watermark through the
// contiguously claimed ranges. Ranges entirely below the watermark are
// discarded; a range above the contiguous frontier stays pending until the
// gap below it is claimed. No-op while the watermark key does not exist.
void claim_written_range(sw::redis::Redis& redis, std::int64_t first, std::int64_t last);

}  // namespace kvrocks_progress
