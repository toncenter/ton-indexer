#include "PoolMath.h"

#include <cmath>
#include <functional>

namespace dex_snapshots {

namespace {

bool positive(const td::RefInt256& x) {
  return x.not_null() && x->is_valid() && td::sgn(x) > 0;
}

bool valid(const td::RefInt256& x) {
  return x.not_null() && x->is_valid();
}

bool constant_product(const PoolInputs& pool) {
  return (pool.dex == "stonfi" && (pool.dex_version == 1 || pool.dex_version == 2) &&
          pool.pool_type == "constant_product") ||
         (pool.dex == "dedust" && (pool.dex_version == 1 || pool.dex_version == 2) && pool.pool_type == "volatile");
}

// An invariant k(x, y) that grows with y, and its partial derivatives. The spot rate of asset0 in asset1 is
// -dy/dx = kx / ky. Values are doubles: raw reserves stay below 2^120 and every term below 1e300.
struct Curve {
  std::function<double(double, double)> k;
  std::function<double(double, double)> kx;
  std::function<double(double, double)> ky;
};

constexpr double kFixedPoint = 1e18;

// STON.fi v2 stableswap (contracts/pool/pools/stableswap/math.fc): Curve's two-coin invariant with A used as is,
// A(x + y) + D = A·D + D³/(4xy). D is solved like the contract; along a trade D stays fixed.
std::optional<Curve> stonfi_stableswap(double a, double x, double y) {
  if (!(a >= 1)) {
    return std::nullopt;
  }
  const double s = x + y;
  double d = s;
  bool converged = false;
  for (int i = 0; i < 255 && !converged; i++) {
    const double dp = d * d / (2 * x) * d / (2 * y);
    const double next = (a * s + 2 * dp) * d / ((a - 1) * d + 3 * dp);
    converged = std::fabs(next - d) <= 1e-15 * d;
    d = next;
  }
  if (!converged || !std::isfinite(d)) {
    return std::nullopt;
  }
  const double d3 = d * d * d / 4;
  return Curve{[a, d3](double x, double y) { return a * (x + y) - d3 / (x * y); },
               [a, d3](double x, double y) { return a + d3 / (x * x * y); },
               [a, d3](double x, double y) { return a + d3 / (x * y * y); }};
}

// STON.fi v2 weighted stableswap (weighted_stableswap/math.fc): k = A(x + r·y) + x^p·(r·y)^q with q = 1 - p.
std::optional<Curve> stonfi_weighted_stableswap(double a, double r, double p) {
  if (!(a >= 0) || !(r > 0) || !(p > 0 && p < 1)) {
    return std::nullopt;
  }
  const double q = 1 - p;
  return Curve{[=](double x, double y) { return a * (x + r * y) + std::pow(x, p) * std::pow(r * y, q); },
               [=](double x, double y) { return a + p * std::pow(x, p - 1) * std::pow(r, q) * std::pow(y, q); },
               [=](double x, double y) { return a * r + q * std::pow(x, p) * std::pow(r, q) * std::pow(y, q - 1); }};
}

// STON.fi v2 weighted constant product (weighted_const_product/math.fc): k = x^w·y^(1-w), here as its logarithm.
std::optional<Curve> stonfi_weighted_const_product(double w) {
  if (!(w > 0 && w < 1)) {
    return std::nullopt;
  }
  return Curve{[w](double x, double y) { return w * std::log(x) + (1 - w) * std::log(y); },
               [w](double x, double) { return w / x; },
               [w](double, double y) { return (1 - w) / y; }};
}

// DeDust v1 stable (docs.dedust.io/docs/concepts): k = X³Y + XY³ on reserves times their
// rates. Only the ratio matters (k is homogeneous): X = x, Y = r·y with r = rate1 / rate0.
std::optional<Curve> dedust_stable(double r) {
  if (!(r > 0) || !std::isfinite(r)) {
    return std::nullopt;
  }
  return Curve{[r](double x, double y) { y *= r; return x * x * x * y + x * y * y * y; },
               [r](double x, double y) { y *= r; return 3 * x * x * y + y * y * y; },
               [r](double x, double y) { y *= r; return r * (x * x * x + 3 * x * y * y); }};
}

std::optional<Curve> curve_of(const PoolInputs& pool, double x, double y) {
  if (pool.dex == "dedust" && pool.dex_version == 1 && pool.pool_type == "stable") {
    return positive(pool.rate) ? dedust_stable(to_double(pool.rate) / kFixedPoint) : std::nullopt;
  }
  if (pool.dex != "stonfi" || pool.dex_version != 2) {
    return std::nullopt;
  }
  if (pool.pool_type == "stableswap" && positive(pool.amp)) {
    return stonfi_stableswap(to_double(pool.amp), x, y);
  }
  if (pool.pool_type == "weighted_stableswap" && valid(pool.amp) && positive(pool.rate) && positive(pool.w0)) {
    return stonfi_weighted_stableswap(to_double(pool.amp) / kFixedPoint, to_double(pool.rate) / kFixedPoint,
                                      to_double(pool.w0) / kFixedPoint);
  }
  if (pool.pool_type == "weighted_const_product" && positive(pool.w0)) {
    return stonfi_weighted_const_product(to_double(pool.w0) / kFixedPoint);
  }
  return std::nullopt;
}

// The other reserve after `in` of the first one comes in: k(x + in, y') = k(x, y), 0 < y' <= y, by bisection.
double other_after(const std::function<double(double, double)>& k, double x, double y, double in) {
  const double target = k(x, y);
  double lo = 0, hi = y;
  for (int i = 0; i < 200 && hi - lo > hi * 1e-15; i++) {
    const double mid = (lo + hi) / 2;
    (k(x + in, mid) < target ? lo : hi) = mid;
  }
  return hi;
}

// The input of the first asset that lowers its spot rate kx / ky by kDepthImpact; NaN if none is found.
double depth(const std::function<double(double, double)>& k, const std::function<double(double, double)>& kx,
             const std::function<double(double, double)>& ky, double x, double y) {
  const double target = kx(x, y) / ky(x, y) * (1 - kDepthImpact);
  auto spot_after = [&](double in) {
    const double y2 = other_after(k, x, y, in);
    return kx(x + in, y2) / ky(x + in, y2);
  };
  double lo = 0, hi = x * 1e-3;
  for (int i = 0; i < 400 && !(spot_after(hi) <= target); i++) {
    lo = hi;
    hi *= 2;
  }
  if (!(spot_after(hi) <= target)) {
    return NAN;
  }
  for (int i = 0; i < 200 && hi - lo > hi * 1e-12; i++) {
    const double mid = (lo + hi) / 2;
    (spot_after(mid) > target ? lo : hi) = mid;
  }
  return (lo + hi) / 2;
}

SpotDepth curve_spot_depth(const Curve& c, double x, double y) {
  SpotDepth res;
  const double spot = c.kx(x, y) / c.ky(x, y);
  // Asset1 coming in: the same search on the mirrored curve.
  auto k1 = [&](double b, double a) { return c.k(a, b); };
  auto kx1 = [&](double b, double a) { return c.ky(a, b); };
  auto ky1 = [&](double b, double a) { return c.kx(a, b); };
  const double d0 = depth(c.k, c.kx, c.ky, x, y);
  const double d1 = depth(k1, kx1, ky1, y, x);
  if (!(spot > 0) || !std::isfinite(spot) || !(d0 > 0) || !(d1 > 0)) {
    return res;
  }
  res.spot = spot;
  res.depth0 = d0;
  res.depth1 = d1;
  res.math_version = kMathVersion;
  return res;
}

}

double to_double(const td::RefInt256& x) {
  if (!valid(x)) {
    return NAN;
  }
  if (td::sgn(x) < 0) {
    return -to_double(-x);
  }
  // A sticky bit preserves round-to-even when discarded bits would break a tie.
  int shift = x->bit_size(false) - 63;
  if (shift <= 0) {
    return static_cast<double>(x->to_long());
  }
  auto top = x >> shift;
  long long bits = top->to_long();
  if (td::cmp(top << shift, x) != 0) {
    bits |= 1;
  }
  return std::ldexp(static_cast<double>(bits), shift);
}

SpotDepth spot_depth(const PoolInputs& pool) {
  SpotDepth res;
  if (!positive(pool.reserve0) || !positive(pool.reserve1)) {
    return res;
  }
  double x = to_double(pool.reserve0);
  double y = to_double(pool.reserve1);
  if (!constant_product(pool)) {
    auto curve = curve_of(pool, x, y);
    return curve ? curve_spot_depth(*curve, x, y) : res;
  }
  // For x*y = k, adding d of asset0 scales spot by (x/(x+d))^2; asset1 is symmetric.
  double depth_factor = 1.0 / std::sqrt(1.0 - kDepthImpact) - 1.0;
  res.spot = y / x;
  res.depth0 = x * depth_factor;
  res.depth1 = y * depth_factor;
  res.math_version = kMathVersion;
  return res;
}

}
