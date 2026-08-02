// Test-only LZ4/MsgPack fixture loader. It decodes the trace fields required by
// matching plus the per-account interface map used by fixture lookups. Unknown
// MsgPack keys are ignored; production uses SchemaTraceLoader.
#pragma once

#include "BlockTree.h"  // TraceContext
#include "TraceLoader.h"

#include "td/utils/Status.h"

#include <string>

namespace mch {

// Reads, decompresses and decodes one fixture file.
td::Result<Trace> load_trace(const std::string &path);

// Fixture path -> the full block substrate (Trace -> EventTree -> BlockArena).
// tree.root == nullptr (empty trace) is an error. Lifted out of BlockTree.cpp:
// a product TU must not reference the fixture reader, or every target that
// links mch-classify drags lz4/msgpack in with it.
td::Result<TraceContext> load_trace_context(const std::string &path);

}  // namespace mch
