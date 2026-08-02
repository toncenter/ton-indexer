// Pure classification pipeline from a TraceContext and compiled tables to a
// buffered per-trace result. It performs no I/O or rendering.
#pragma once

#include "ActionBuild.h"  // ActionRow
#include "BlockTree.h"
#include "BuildRuntime.h"
#include "IrTables.h"
#include "TraceLoader.h"
#include "Walker.h"  // WhereTable

#include <cstdint>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace mch {

// Trace-terminal failure categories used by production-classifier telemetry:
//   - lookup_infra_fail:  two-phase lookup did not converge (infra error).
//   - engine_fault:       a mid-trace exception (Python's per-trace fallback).
//   - malformed_trace:    the trace could not be loaded into an event tree
//                         (schema map / empty tree), set caller/actor-side.
// Matcher skips are setup-level facts, while clean rejection, parse-null, and
// per-lookup misses are per-attempt events. They cannot be the single terminal
// state held here and are deliberately not enumerators.
enum class FailureCategory {
  none,
  lookup_infra_fail,
  engine_fault,
  malformed_trace,
};

// One produced action with raw block pointers and fire-time auxiliary blocks.
// The adapter renders block_key(anchor)/data/consumed/aux or builds an Action
// row from `produced`. Pointers reference the caller's arena (TraceContext),
// valid for as long as the caller keeps it alive.
struct CoreAction {
  std::string matcher_name;
  Block *anchor;
  Block *produced;
  std::vector<Block *> aux;    // fire-time excess/bounce auto-append (anchor)
};

// Buffered atomic per-trace result. On a mid-trace exception the pipeline sets
// `failure` and returns what it collected; production commits only on success.
struct ClassifyResult {
  std::vector<CoreAction> actions;  // surviving post-pass, in fire order
  // Post-pass spine and recursive child rows for production serialization.
  // This differs from the matcher-fire `actions` view: this list is the
  // post-processing spine used for serialization; `actions` records fires in order.
  std::vector<ActionRow> action_rows;
  bool failure = false;
  std::string failure_reason;
  // Trace-terminal failure category (additive telemetry; none unless `failure`).
  FailureCategory failure_category = FailureCategory::none;
  // Basic-action fallback, populated only on failure. A fresh leaf-only classify
  // with no matchers runs the
  // post-processor chain, serialized the same way as `action_rows`, so every row
  // is a basic btype (call_contract / contract_deploy / ton_transfer).
  // Production commits these when classification fails. Ungated (corpus has no
  // failing traces).
  std::vector<ActionRow> fallback_rows;
  // Set when classification succeeds but neither normal nor ghost serialization
  // produces a row for a nonempty trace. The adapter emits an unknown action so
  // "classified with no recognized action" remains distinct from "not classified".
  bool unknown_trace = false;
};

// Trace-independent run prep computed once from the matcher table: the runnable
// matcher set, the SKIP table (name -> reason, sorted), the generated build-fn
// map, the generated where_expr table the walker evaluates, and the union of
// referenced lookup kinds.
struct ClassifySetup {
  std::vector<int> included;  // indices into the matcher table, priority order
  std::vector<std::pair<std::string, std::string>> skips;
  std::unordered_map<int, BuildOutcome (*)(BuildEnv &)> build_fns;
  WhereTable where_fns;  // node global_id -> generated where fn (GenWheres.h)
  std::set<std::string> kinds;
  bool table_missing = false;  // the three generated tables disagree on their source
  bool fn_missing = false;     // an included matcher has no generated fn
  std::string error;           // populated when table_missing/fn_missing
};

// Build the run prep from the compiled-in matcher table. No I/O. Pass the SAME
// table classify_trace will get. `included` indexes into it. On a table/fn
// mismatch, sets the *_missing flags + error and returns what it has (adapter
// reports + exits; production is startup-fatal).
ClassifySetup prepare_classify(const std::vector<CompiledMatcher> &matchers);

// Classify one trace with the matcher loop and post-processing passes. The
// pipeline allocates produced and proxy blocks from the caller-owned arena.
ClassifyResult classify_trace(TraceContext &ctx, const std::vector<CompiledMatcher> &matchers,
                              const ClassifySetup &setup, const LookupSource &src);

}  // namespace mch
