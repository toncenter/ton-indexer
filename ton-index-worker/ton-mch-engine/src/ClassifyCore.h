// Pure classification pipelines from schema transactions or a TraceContext to
// buffered per-trace results. They perform no I/O or rendering.
#pragma once

#include "ActionBuild.h"  // ActionRow
#include "BlockTree.h"
#include "BuildRuntime.h"
#include "IndexData.h"  // schema::Transaction
#include "IrTables.h"
#include "TraceLoader.h"
#include "Walker.h"  // WhereTable

#include <cstddef>
#include <cstdint>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace mch {

struct MchEnginePrep;
class ParsedBlockLookupSource;

// Trace-terminal failure categories used by production-classifier telemetry:
//   - lookup_infra_fail:  two-phase lookup did not converge (infra error).
//   - engine_fault:       a mid-trace exception (per-trace fallback).
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

struct SchemaClassifyResult {
  bool failure{false};
  std::string failure_reason;
  FailureCategory failure_category{FailureCategory::none};
  bool used_fallback{false};
  std::size_t unported_btypes{0};
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

// Canonical BFS over the reachable graph, de-duplicated by block identity.
std::vector<Block *> gather_blocks(Block *root);

// Build the run prep from the compiled-in matcher table. No I/O. Pass the SAME
// table classify_trace will get. `included` indexes into it. On a table/fn
// mismatch, sets the *_missing flags + error and returns what it has (adapter
// reports + exits; production is startup-fatal).
ClassifySetup prepare_classify(const std::vector<CompiledMatcher> &matchers);

// Classify one trace with the matcher loop and post-processing passes. The
// pipeline allocates produced and proxy blocks from the caller-owned arena.
ClassifyResult classify_trace(TraceContext &ctx, const std::vector<CompiledMatcher> &matchers,
                              const ClassifySetup &setup, const LookupSource &src);

// `prep` is process-lifetime const and shareable across threads.
// The interface map behind `src` is shareable iff immutable.
// `src` and its Tier2Hook capture are for one classify call on one thread only.
SchemaClassifyResult classify_schema_trace(
    const MchEnginePrep &prep, const std::string &trace_id,
    const std::vector<schema::Transaction> &txs, const ParsedBlockLookupSource &src,
    std::vector<Action> &rows, std::vector<std::string> &matcher_names,
    std::size_t &scrubbed, bool &unknown_row);

}  // namespace mch
