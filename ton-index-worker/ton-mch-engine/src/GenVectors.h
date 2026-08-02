// Test-only interface for generated expression-vector functions. Comparing this
// table with the interpreter verifies expression emission against ExprEval.
#pragma once

#include "ExprRuntime.h"

#include <vector>

namespace mch {

struct GenVec {
  const char *name;
  EvalResult (*fn)(const Env &, const Lookups &);
};

// The generated table, in expr_vectors.json order (index-aligned with the JSON).
const std::vector<GenVec> &gen_vectors();

}  // namespace mch
