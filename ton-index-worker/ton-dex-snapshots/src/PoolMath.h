#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include <common/refint.h>

namespace dex_snapshots {

// pool_snapshots.math_version of the curves with spot math here; 0 means no spot.
constexpr std::uint16_t kMathVersion = 1;

// depth0 / depth1 are the inputs that move the input asset's spot rate by this fraction.
constexpr double kDepthImpact = 0.02;

// Reserves use raw on-chain units; STON.fi v2 curve parameters are raw get_pool_data values.
struct PoolInputs {
  std::string dex;        // stonfi | dedust
  int dex_version{0};     // 1 | 2
  std::string pool_type;  // stonfi: constant_product, stableswap, ...; dedust: volatile, stable
  td::RefInt256 reserve0;
  td::RefInt256 reserve1;
  td::RefInt256 amp;   // stableswap: A as is; weighted_stableswap: A × 1e18
  td::RefInt256 rate;  // weighted_stableswap, DeDust stable: asset1's rate relative to asset0 × 1e18
  td::RefInt256 w0;    // weighted pools: asset0's weight × 1e18
};

struct SpotDepth {
  std::optional<double> spot;     // raw asset1 per raw asset0 at the mid, no fees
  std::optional<double> depth0;   // raw asset0 in that lowers spot by kDepthImpact (no fees)
  std::optional<double> depth1;   // raw asset1 in that lowers 1/spot by kDepthImpact (no fees)
  std::uint16_t math_version{0};  // kMathVersion with a spot, otherwise 0 and no values
};

// Computes spot and depth for supported curves; returns math_version = 0 if unsupported or invalid.
// The caller must skip removed pools before calling this.
SpotDepth spot_depth(const PoolInputs& pool);

// Nearest double to x (round half to even); NaN for a null or invalid x.
double to_double(const td::RefInt256& x);

}
