// Stable interface for generated build-program functions. The matcher, build,
// and where tables share one compile-time artifact and are cross-checked by
// source hash.
#pragma once

#include "BuildRuntime.h"

#include <vector>

namespace mch {

struct GenBuild {
  int id;            // matcher index (artifact) / vector index (build_vectors)
  const char *name;  // matcher / vector name (diagnostics)
  BuildOutcome (*fn)(BuildEnv &);
};

// Table from MCH_CLASSIFY_ARTIFACT, ascending matcher index.
const std::vector<GenBuild> &gen_builds_ir();

// SHA-256 (hex) of the document this table was generated from. This is the value
// prepare_classify compares against the matcher and where tables'.
const char *gen_builds_ir_source_sha();

}  // namespace mch
