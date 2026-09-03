// Deterministic MCH IR expression-to-C++ generator.
#pragma once

#include "td/utils/JsonBuilder.h"

#include <string>
#include <vector>

namespace mch_codegen {

// C++ string-literal body (escaped): \\ \" \n \t \r, plus every other control
// byte (< 0x20 and 0x7F, NUL included) as a 3-digit octal escape. An adjacent
// digit cannot extend it (the \x greedy-hex hazard). Verified by `--selftest`.
std::string cstr(const std::string &s);

inline std::string join(const std::vector<std::string> &parts, const std::string &sep) {
  std::string out;
  for (std::size_t i = 0; i < parts.size(); i++) {
    if (i) out += sep;
    out += parts[i];
  }
  return out;
}

std::string generate_function(const std::string &fn_name, const td::JsonValue &expr);

// Emit one `EvalResult <fn_name>(const mch::WhereEnv &w)` for a node-level
// inline `where (expr)`: `dotfield` reads the candidate block via rt_dotfield;
// the named entry capture reads its fixed slot, and any other name faults.
std::string generate_where_function(const std::string &fn_name, const td::JsonValue &expr,
                                    const std::string &entry_name, int entry_slot);

std::string generate_vectors_file(const td::JsonValue &root, const std::string &header);

// One where-function per `nodes[i]` with a `where_expr`, keyed by global node
// index i. `suffix` pairs the table with its document like generate_builds_file
// so both tables can link into one binary and be selected by source sha.
std::string generate_wheres_file(const td::JsonValue &root, const std::string &header,
                                 const std::string &source_sha, const std::string &suffix);

// Accepts either an IR artifact or the fixture harness's build-vectors document;
// `suffix` keeps their generated symbols distinct when linked together.
std::string generate_builds_file(const td::JsonValue &root, const std::string &header,
                                 const std::string &source_sha, const std::string &suffix);

}  // namespace mch_codegen
