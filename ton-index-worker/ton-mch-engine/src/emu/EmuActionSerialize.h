// Serializes mch::Action rows for the trace hash's `actions` field. The caller
// runs it while the view's cell anchor remains alive.
#pragma once

#include "ActionBuild.h"
#include "EmuTypes.h"

#include <cstddef>
#include <string>
#include <vector>

namespace mch {

// Counts unexpected Value shapes serialized with their defined fallback encoding.
struct ActionSerializeStats {
  std::size_t float_values{0};  // VType::Float is packed as a msgpack double.
  std::size_t cell_values{0};   // VType::Cell is base64(BOC); Action fields should be Bytes.
  std::size_t unrenderable{0};  // null RefInt256 / null cell / BOC failure / out-of-int64 -> nil
};

// Returns an uncompressed msgpack array of action maps. An empty action list is
// a one-byte array; an empty string means no payload. The view supplies the
// trace key and minimum node finality.
std::string serialize_actions(const std::vector<Action> &actions, const EmuTraceView &view,
                              ActionSerializeStats *stats = nullptr);

}  // namespace mch
