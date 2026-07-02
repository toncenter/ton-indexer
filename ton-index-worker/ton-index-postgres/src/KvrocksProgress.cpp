#include "KvrocksProgress.h"

#include <chrono>
#include <stdexcept>
#include <string>

namespace kvrocks_progress {

namespace {

// KEYS[1] = watermark, KEYS[2] = pending ranges zset,
// ARGV[1] = range start, ARGV[2] = range end.
// Registers the claimed range and sweeps the pending set: drops ranges fully
// below the watermark, extends the watermark through ranges adjacent to or
// overlapping the contiguous frontier, stops at the first gap.
const char kClaimRangeScript[] = R"(local w = redis.call('GET', KEYS[1])
if not w then
  return -1
end
w = tonumber(w)
redis.call('ZADD', KEYS[2], ARGV[1], ARGV[1] .. ':' .. ARGV[2])
local advanced = false
while true do
  local items = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
  if #items == 0 then
    break
  end
  local first = tonumber(items[2])
  local last = tonumber(string.match(items[1], ':(%d+)$'))
  if last <= w then
    redis.call('ZREM', KEYS[2], items[1])
  elseif first <= w + 1 then
    w = last
    advanced = true
    redis.call('ZREM', KEYS[2], items[1])
  else
    break
  end
end
if advanced then
  redis.call('SET', KEYS[1], w)
end
return w)";

}  // namespace

std::optional<std::int64_t> get_watermark(sw::redis::Redis& redis) {
  auto value = redis.get(kWatermarkKey);
  if (!value.has_value()) {
    return std::nullopt;
  }
  std::size_t parsed = 0;
  std::int64_t watermark = 0;
  try {
    watermark = std::stoll(*value, &parsed);
  } catch (...) {
    parsed = 0;
  }
  if (parsed == 0 || parsed != value->size()) {
    throw std::runtime_error("Kvrocks progress watermark holds a non-numeric value: " + *value);
  }
  return watermark;
}

void initialize_watermark_if_missing(sw::redis::Redis& redis, std::int64_t watermark) {
  redis.set(kWatermarkKey, std::to_string(watermark), std::chrono::milliseconds(0), sw::redis::UpdateType::NOT_EXIST);
}

void claim_written_range(sw::redis::Redis& redis, std::int64_t first, std::int64_t last) {
  const auto first_str = std::to_string(first);
  const auto last_str = std::to_string(last);
  redis.eval<long long>(kClaimRangeScript, {kWatermarkKey, kPendingRangesKey}, {first_str, last_str});
}

}  // namespace kvrocks_progress
