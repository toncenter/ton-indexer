#pragma once

#include <cstdint>

namespace gramstat {

// Fixed MC grid: independent of the run's start, retries and batch boundaries.
inline constexpr std::uint32_t TOTAL_SUPPLY_INTERVAL = 150;

constexpr bool should_calculate_total_supply(std::uint32_t mc_seqno) {
    return mc_seqno % TOTAL_SUPPLY_INTERVAL == 0;
}

}  // namespace gramstat
