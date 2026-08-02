// Deterministic MCH IR expression-to-C++ generator.
#pragma once

#include "td/utils/JsonBuilder.h"

#include <string>

namespace mch_codegen {

// C++ string-literal body (escaped): \\ \" \n \t \r, plus every other control
// byte (< 0x20 and 0x7F, NUL included) as a 3-digit octal escape. An adjacent
// digit cannot extend it (the \x greedy-hex hazard). Verified by `--selftest`.
std::string cstr(const std::string &s);

std::string generate_function(const std::string &fn_name, const td::JsonValue &expr);

// Emit one `EvalResult <fn_name>(const mch::WhereEnv &w)` for a node-level
// inline `where (expr)`: `dotfield` reads the candidate block via rt_dotfield;
// `name` / `lookup` fault (a where_expr is builtin-only and unbound, mirroring
// the Python sync evaluator).
std::string generate_where_function(const std::string &fn_name, const td::JsonValue &expr);

std::string generate_vectors_file(const td::JsonValue &root, const std::string &header);

// Emit the whole wheres_<suffix>_generated.cpp from a decoded IR artifact root:
// one where-function per `nodes[i]` carrying a `where_expr`, keyed by the global
// node index i (== CompiledNode::global_id), plus the `gen_wheres_<suffix>()` /
// `gen_wheres_<suffix>_source_sha()` accessors. `suffix` pairs the table with its
// document exactly like generate_builds_file (ir artifact / testbed artifact), so
// both tables link into one binary and are selected at run time by source sha.
std::string generate_wheres_file(const td::JsonValue &root, const std::string &header,
                                 const std::string &source_sha, const std::string &suffix);

// Accepts either an IR artifact or the fixture harness's build-vectors document;
// `suffix` keeps their generated symbols distinct when linked together.
std::string generate_builds_file(const td::JsonValue &root, const std::string &header,
                                 const std::string &source_sha, const std::string &suffix);

}  // namespace mch_codegen
