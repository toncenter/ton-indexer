// Test-only LZ4/MsgPack fixture loader. It decodes the trace fields required by
// matching plus the per-account interface map used by fixture lookups. Unknown
// MsgPack keys are ignored; production uses SchemaTraceLoader.
#pragma once

#include "BlockTree.h"  // TraceContext
#include "TraceLoader.h"

#include "td/utils/Status.h"

#include <string>

namespace mch {

td::Result<Trace> load_trace(const std::string &path);

// Fixture path -> Trace -> EventTree -> BlockArena. Empty traces (null root)
// are errors. A product TU must not reference this reader, or every
// mch-classify link pulls in lz4/msgpack.
td::Result<TraceContext> load_trace_context(const std::string &path);

}  // namespace mch
