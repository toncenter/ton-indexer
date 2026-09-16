// Stable interface for generated where-expression functions, keyed by the
// artifact-global node index. The walker has no alternate runtime path.
#pragma once

#include "ExprRuntime.h"

#include <vector>

namespace mch {

struct GenWhere {
  int node_id;  // artifact-global node index
  EvalResult (*fn)(const WhereEnv &);
};

// Table from MCH_CLASSIFY_ARTIFACT, ascending node_id (artifact node order).
const std::vector<GenWhere> &gen_wheres_ir();

// SHA-256 (hex) of the document this table was generated from. This is the value
// prepare_classify compares against the matcher and build tables'.
const char *gen_wheres_ir_source_sha();

}  // namespace mch
