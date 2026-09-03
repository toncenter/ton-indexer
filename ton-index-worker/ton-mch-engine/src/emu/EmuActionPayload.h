// Serialized inputs for the emulator's Redis transaction. The standard-library-
// only representation keeps matcher value and arena types out of the insert path.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace mch {

struct EmuActionRoute {
  std::string type;
  std::vector<std::string> accounts;
};

struct EmuActionPayload {
  // Bare msgpack. Empty means no payload and suppresses the `actions` field.
  // Payload is plain MsgPack (consumer detects gzip by magic bytes if compressed).
  std::string actions_blob;
  // (account, "<trace_key>:<action_id>") for every (action, account) pair.
  std::vector<std::pair<std::string, std::string>> aai;
  // Small routing summary used by streaming before it decides to load the trace.
  std::vector<EmuActionRoute> routes;
  std::int64_t aai_score{0};    // Python's trace.start_lt == root tx lt
  std::size_t action_count{0};  // telemetry only
  std::uint64_t update_seq{0};
  // Written as `mch_classify_state`. Null suppresses the field entirely.
  const char *state{nullptr};
  // Minimum node finality, shared by the wire payload and write guard.
  std::uint8_t finality{0};
};

}  // namespace mch
