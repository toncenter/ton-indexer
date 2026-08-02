// Serialized inputs for the emulator's Redis transaction. The standard-library-
// only representation keeps matcher value and arena types out of the insert path.
#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mch {

struct EmuActionPayload {
  // Bare msgpack. Empty means no payload and suppresses the `actions` field.
  // Payload is plain MsgPack (consumer detects gzip by magic bytes if compressed).
  std::string actions_blob;
  // (account, "<trace_key>:<action_id>") for every (action, account) pair.
  std::vector<std::pair<std::string, std::string>> aai;
  // Accounts that appear in an action but in no transaction of the trace.
  std::vector<std::string> referenced_accounts;
  std::int64_t aai_score{0};    // Python's trace.start_lt == root tx lt
  std::size_t action_count{0};  // telemetry only
  // Written as `mch_classify_state`. Null suppresses the field entirely.
  const char *state{nullptr};
  // Minimum node finality, shared by the wire payload and write guard.
  std::uint8_t finality{0};
};

// Reports whether `stored` is more advanced than the emission. Missing or
// malformed values permit a repair write rather than blocking updates.
inline bool actions_write_is_stale(const std::optional<std::string> &stored,
                                   std::uint8_t emission_finality) {
  if (!stored) {
    return false;  // nothing stored: nothing to downgrade
  }
  int stored_finality = -1;
  try {
    stored_finality = std::stoi(*stored);
  } catch (const std::exception &) {
    return false;  // unreadable: treat as absent rather than block writes forever
  }
  return stored_finality > static_cast<int>(emission_finality);
}

}  // namespace mch
