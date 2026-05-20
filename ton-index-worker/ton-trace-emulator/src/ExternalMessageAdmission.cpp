#include "ExternalMessageAdmission.h"

ExternalMessageAdmission::ExternalMessageAdmission(
    std::size_t max_emulations_per_destination,
    std::chrono::steady_clock::duration window)
    : max_emulations_per_destination_(max_emulations_per_destination), window_(window) {
}

ExternalMessageAdmission::Result ExternalMessageAdmission::try_acquire(
    const td::Bits256& msg_hash_norm, const block::StdAddress& destination) {
    std::lock_guard<std::mutex> lock(mutex_);

    const auto now = Clock::now();
    prune_locked(now);

    if (known_messages_.find(msg_hash_norm) != known_messages_.end()) {
        return {.accepted = false, .reject_reason = RejectReason::Duplicate};
    }

    auto& destination_window = destination_windows_[destination];
    if (destination_window.size() >= max_emulations_per_destination_) {
        return {
            .accepted = false,
            .reject_reason = RejectReason::DestinationRateLimit,
            .accepted_for_destination = destination_window.size(),
        };
    }

    known_messages_.emplace(msg_hash_norm, KnownMessage{now, true});
    known_message_order_.emplace_back(now, msg_hash_norm);
    destination_window.push_back(now);
    destination_order_.emplace_back(now, destination);

    return {
        .accepted = true,
        .reject_reason = RejectReason::None,
        .accepted_for_destination = destination_window.size(),
    };
}

void ExternalMessageAdmission::mark_emulated(const td::Bits256& msg_hash_norm) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = known_messages_.find(msg_hash_norm);
    if (it == known_messages_.end()) {
        return;
    }

    const auto now = Clock::now();
    it->second = KnownMessage{now, false};
    known_message_order_.emplace_back(now, msg_hash_norm);
}

void ExternalMessageAdmission::release_message(const td::Bits256& msg_hash_norm) {
    std::lock_guard<std::mutex> lock(mutex_);
    known_messages_.erase(msg_hash_norm);
}

void ExternalMessageAdmission::prune_locked(TimePoint now) {
    while (!known_message_order_.empty()) {
        const auto& [timestamp, hash] = known_message_order_.front();
        if (now - timestamp < window_) {
            break;
        }

        auto it = known_messages_.find(hash);
        if (it != known_messages_.end() && !it->second.in_progress && it->second.timestamp == timestamp) {
            known_messages_.erase(it);
        }
        known_message_order_.pop_front();
    }

    while (!destination_order_.empty()) {
        const auto& [timestamp, destination] = destination_order_.front();
        if (now - timestamp < window_) {
            break;
        }

        auto it = destination_windows_.find(destination);
        if (it != destination_windows_.end()) {
            auto& timestamps = it->second;
            if (!timestamps.empty() && timestamps.front() == timestamp) {
                timestamps.pop_front();
            } else {
                while (!timestamps.empty() && now - timestamps.front() >= window_) {
                    timestamps.pop_front();
                }
            }
            if (timestamps.empty()) {
                destination_windows_.erase(it);
            }
        }

        destination_order_.pop_front();
    }
}
