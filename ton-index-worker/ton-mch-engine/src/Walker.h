// Read-only MCH IR matcher walker. Candidate choices use stable min_lt ordering,
// and inline where expressions run through the generated table after head tests.
#pragma once

#include "BlockTree.h"
#include "ExprRuntime.h"
#include "IrTables.h"

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace mch {

// The two service opcodes the walker treats as aux and classify appends under include_excess / include_bounces.
inline constexpr std::uint32_t kExcessOpcode = 0xD53276DB;
inline constexpr std::uint32_t kBounceOpcode = 0xFFFFFFFF;

// Generated where_expr functions keyed by artifact-global node id. Built once
// per run by prepare_classify (from gen_wheres_ir) and carried in
// ClassifySetup; matcher_match takes it by reference, so a walk without a table
// is not representable.
using WhereFn = EvalResult (*)(const WhereEnv &);
using WhereTable = std::unordered_map<int, WhereFn>;

// Stack guard for pathological block-tree recursion beyond kMaxMatchDepth.
// The classify boundary catches it and marks the trace failed.
struct MatchDepthExceeded : std::runtime_error {
  MatchDepthExceeded() : std::runtime_error("match recursion depth exceeded") {}
};

// One assembled capture. A scalar (`one`/`opt`) has is_list=false and exactly
// one entry in `vals` (nullptr == unbound/None). A `many` capture has
// is_list=true and `vals` in assembly order (nullptr == gap-aligned None).
struct Capture {
  std::string name;
  bool is_list{false};
  std::vector<Block *> vals;
};

struct MatchResult {
  std::vector<Block *> consumed;  // traversal order, identity-deduped
  std::vector<Capture> captures;  // slot declaration order
};

// Anchor test. Opcode anchors require btype == call_contract.
bool matcher_test_self(const CompiledMatcher &m, const Block *b);

// Full match attempt (test_self + walk, no build). nullopt == structural
// mismatch. Leaves the tree untouched. `gen` must cover every where_expr node id
// of `m` (guaranteed by prepare_classify's coverage check); an uncovered node
// fails its head test rather than aborting the walk.
std::optional<MatchResult> matcher_match(const CompiledMatcher &m, Block *anchor,
                                         const WhereTable &gen);

}  // namespace mch
