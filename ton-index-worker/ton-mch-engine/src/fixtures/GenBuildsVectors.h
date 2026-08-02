// TEST-ONLY. Part of mch-fixtures; never linked into mch-classify or any
// product binary. See ton-mch-engine/CMakeLists.txt MCH_FIXTURE_SOURCES.
//
// The build-program conformance-vector table, generated from ir/build_vectors.json
// by `mch-codegen --builds --suffix vectors`. Consumed only by the
// --build-vectors runner; the product tables are declared in GenBuilds.h.
#pragma once

#include "GenBuilds.h"  // GenBuild

#include <vector>

namespace mch {

// Table from ir/build_vectors.json, ascending vector index.
const std::vector<GenBuild> &gen_builds_vectors();
const char *gen_builds_vectors_source_sha();

}  // namespace mch
