// The embedding entry point: prepare the trace-independent classify state once,
// shared (const) by every classifier spawn.
//
// There is nothing to load. The matcher table is compiled into the binary
// (GenMatchers.h), so this is the whole startup cost of the engine: an advisory
// registry check plus prepare_classify().
//
// The emulator's EmuClassifierActor and ton-trace-emulator are the live
// consumers.
#pragma once

#include "ClassifyCore.h"
#include "IrTables.h"

#include "td/utils/Status.h"

#include <memory>
#include <vector>

namespace mch {

// Trace-independent engine state: the compiled matcher table + its setup (skip
// table, generated build-fn map, referenced lookup kinds). `matchers` points at
// the process-lifetime generated table, and `setup.included` indexes into it.
struct MchEnginePrep {
  const std::vector<CompiledMatcher> *matchers = nullptr;
  ClassifySetup setup;
};

// Prepare the classify setup over the compiled-in table. Fails (Result error) on
// a registry conflict or table/function mismatch. Both indicate build bugs; the
// caller decides whether the error is fatal. Performs no I/O.
td::Result<std::shared_ptr<const MchEnginePrep>> make_engine_prep();

}  // namespace mch
