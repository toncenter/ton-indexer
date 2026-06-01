#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "IndexData.h"

class ExternalMessageAdmission {
public:
    enum class RejectReason {
        None,
        Duplicate,
        DestinationRateLimit,
    };

    struct Result {
        bool accepted{false};
        RejectReason reject_reason{RejectReason::None};
        std::size_t accepted_for_destination{0};
    };

    static constexpr std::size_t kDefaultMaxEmulationsPerDestination = 3;
    static constexpr std::int64_t kDefaultWindowSeconds = 10;

    ExternalMessageAdmission(
        std::size_t max_emulations_per_destination = kDefaultMaxEmulationsPerDestination,
        std::chrono::steady_clock::duration window = std::chrono::seconds(kDefaultWindowSeconds));

    Result try_acquire(const td::Bits256& msg_hash_norm, const block::StdAddress& destination);
    void mark_emulated(const td::Bits256& msg_hash_norm);
    void release_message(const td::Bits256& msg_hash_norm);

private:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    struct KnownMessage {
        TimePoint timestamp;
        bool in_progress{true};
    };

    void prune_locked(TimePoint now);

    std::size_t max_emulations_per_destination_;
    std::chrono::steady_clock::duration window_;

    std::mutex mutex_;
    std::unordered_map<td::Bits256, KnownMessage> known_messages_;
    std::deque<std::pair<TimePoint, td::Bits256>> known_message_order_;
    std::unordered_map<block::StdAddress, std::deque<TimePoint>> destination_windows_;
    std::deque<std::pair<TimePoint, block::StdAddress>> destination_order_;
};
