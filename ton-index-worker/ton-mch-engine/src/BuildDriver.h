// Shared build-phase driver pieces: the artifact-derived build skip table,
// capture->slot conversion, and the two-phase lookup execution.
#pragma once

#include "BuildRuntime.h"
#include "IrTables.h"
#include "Walker.h"

#include <set>
#include <string>
#include <vector>

namespace mch {

// MATCH-inclusion filter: empty == the linked host supplies every predicate
// the matcher's match phase consults; else the SKIP reason. Computed at
// setup time rather than compiled into the table.
std::string match_skip_reason(const CompiledMatcher &m);

// Build-inclusion filter: empty == runnable end-to-end on the current C++
// surface; else the SKIP reason (builder-directive, missing parsers / lookup
// kinds / host fns / shapers). Match-phase preds are match_skip_reason's half.
std::string build_skip_reason(const CompiledMatcher &m);

// MatchResult captures -> slot values in slot-declaration order
// (scalar: Block/Null; many: List with Null gaps).
std::vector<Value> slots_from_captures(const std::vector<Capture> &captures);

// Two-phase lookup execution: collect keys to fixpoint, fetch through `src`,
// resume against the immutable filled table. Each pass runs with a fresh
// BuildEnv (bodies are per-run state).
//
// `needs_lookups` = the matcher references a `lookup` node or a host `fn` (which
// fetches via the table). When false (12/36 matchers), the collect pass can only
// ever produce zero misses, so it is skipped: a single final pass against an
// empty filled table is byte-identical to collect→converge→final. (A2)
BuildOutcome run_two_phase(BuildOutcome (*fn)(BuildEnv &), const Block *anchor,
                           const std::vector<Value> &slots, const Value &consumed,
                           const std::set<std::string> &kinds, const LookupSource &src,
                           bool needs_lookups);

}  // namespace mch
