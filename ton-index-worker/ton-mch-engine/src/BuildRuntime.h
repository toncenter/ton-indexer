// Build faults and explicit rejections are clean matcher rejections. Captures are
// slot-indexed; capture names do not exist at runtime.
#pragma once

#include "ExprRuntime.h"
#include "Value.h"

#include <map>
#include <set>
#include <string>
#include <vector>

namespace mch {

struct Block;  // BlockTree.h

// program.py BuildOutcome | None. rejected==true corresponds to Python's None
// (a `reject when` fired OR an EvalError surfaced); the other fields are then
// meaningless. btype empty == Python btype None (non-switch matcher: caller
// uses produces[0]).
struct BuildOutcome {
  bool is_rejected{false};
  std::string reject_reason;  // diagnostics only
  std::string btype;
  Value data;  // Dict; defaults to the empty dict (Python: data None -> {})
  bool failed{false};
  bool broken{false};

  static BuildOutcome rejected(std::string reason = {}) {
    BuildOutcome o;
    o.is_rejected = true;
    o.reject_reason = std::move(reason);
    return o;
  }
  static BuildOutcome accepted() {
    BuildOutcome o;
    o.data = Value::make_dict({});
    return o;
  }
};

// Lookup interface. `has` mirrors the evaluator's registration
// check (unregistered kind => EvalError => rejection); `get` returns the
// fetched value (Null == unknown key, Python lookup returning None).
// Null-strict argument handling lives in rt_lookup_build, not here.
class LookupTable {
 public:
  virtual ~LookupTable() = default;
  virtual bool has(const std::string &kind) const = 0;
  virtual Value get(const std::string &kind, const std::vector<Value> &args) const = 0;
};

// Harness table with every kind registered and every key unknown.
class NullLookupTable : public LookupTable {
 public:
  bool has(const std::string &) const override { return true; }
  Value get(const std::string &, const std::vector<Value> &) const override {
    return Value::null();
  }
};

// Two-phase lookup runtime

std::string lookup_key(const std::string &kind, const std::vector<Value> &args);

// Collect-pass table: `has` covers every kind the artifact references.
// Registration is checked first; an uncovered kind faults the collect pass
// before recording anything past it); `get` serves already-fetched values and
// RECORDS misses (returning null). Re-running the build fn against this table
// until no new misses appear reaches the key fixpoint even for lookup-arg-of-
// lookup chains (a single dry pass cannot see those: the inner null
// short-circuits the outer call before its key can form).
class CollectingLookupTable : public LookupTable {
 public:
  CollectingLookupTable(const std::set<std::string> &kinds,
                        const std::map<std::string, Value> &fetched)
      : kinds_(kinds), fetched_(fetched) {
  }
  bool has(const std::string &kind) const override { return kinds_.count(kind) != 0; }
  Value get(const std::string &kind, const std::vector<Value> &args) const override;

  mutable std::map<std::string, std::pair<std::string, std::vector<Value>>> misses;

 private:
  const std::set<std::string> &kinds_;
  const std::map<std::string, Value> &fetched_;
};

// Resume-pass table: immutable fetched values; missing key -> null (nullable-
// lookup semantics), `has` still covers the artifact's kind set.
class FilledLookupTable : public LookupTable {
 public:
  FilledLookupTable(std::set<std::string> kinds, std::map<std::string, Value> values)
      : kinds_(std::move(kinds)), values_(std::move(values)) {
  }
  bool has(const std::string &kind) const override { return kinds_.count(kind) != 0; }
  Value get(const std::string &kind, const std::vector<Value> &args) const override {
    auto it = values_.find(lookup_key(kind, args));
    return it != values_.end() ? it->second : Value::null();
  }

 private:
  std::set<std::string> kinds_;
  std::map<std::string, Value> values_;
};

// Fetch adapter interface. Production uses an actor-request round per trace;
// offline: the fixture-backed source below.
class LookupSource {
 public:
  virtual ~LookupSource() = default;
  virtual Value fetch(const std::string &kind, const std::vector<Value> &args) const = 0;
};

// Every lookup kind the artifact may reference. Both the runnability gating in
// BuildDriver and the production ParsedBlockLookupSource key off this set; the
// fixture-backed source that serves it offline lives in mch-fixtures
// (fixtures/FixtureLookupSource.h).
const std::set<std::string> &lookup_kinds();

struct BuildEnv {
  const Block *anchor{nullptr};
  const Value *slots{nullptr};  // capture values, slot-indexed (Block/Null/List)
  std::size_t n_slots{0};
  Value consumed;               // well-known `consumed`: List of Block values
  const LookupTable *lookups{nullptr};
  // program.py Evaluator.bodies: block identity -> parsed body (Null == every
  // soft-parse alternative failed). Filled by rt_parse, read by `.body`.
  std::map<const Block *, Value> bodies;
};

// `parse TARGET as T1|T2` (program.py _do_parse/_soft_parse): target null ->
// no-op; list -> parse each non-null Block element; Block -> parse. Per
// element: first type whose parser succeeds wins; parser failure tries the
// next; all fail -> body Null. An UNREGISTERED type name faults (rejection).
EvalResult rt_parse(BuildEnv &env, const Value &target, const std::vector<std::string> &types);

// `parse TARGET as T1|T2` as a build expression. Unlike the
// statement form, this RETURNS the parsed message Obj directly and FAULTS on
// total failure instead of returning a soft null. A null or non-block
// target -> fault; every type alternative failing to parse -> fault; an
// unregistered type name -> fault. Stateless (no env.bodies side table); env is
// accepted only for signature symmetry with the other build-mode rt_* leaves.
EvalResult rt_parse_expr(BuildEnv &env, const Value &target,
                         const std::vector<std::string> &types);

// Attribute access in build context: `.body` on a Block/List resolves through
// env.bodies (missing entry == "was not parse'd" fault, exactly expr_eval's
// _access with a bodies dict); every other field delegates to rt_access.
EvalResult rt_access_build(const BuildEnv &env, const Value &obj, const std::string &field);

// `lookup kind(args)` (expr.py _eval_lookup, in ITS order): unregistered kind
// -> fault FIRST; then null-strict (any null arg -> null, table untouched);
// else the table value.
EvalResult rt_lookup_build(const BuildEnv &env, const std::string &kind,
                           const std::vector<Value> &args);

// Host `fn` call (build mode only): dispatches to HostRegistry::host_fns().
// UNLIKE rt_call_builtin, arguments are NOT null-strict — they pass through to
// the fn verbatim. Unknown name -> fault; the fn's own fault propagates. The
// generated build code emits this for every `call` whose fn is not a builtin.
EvalResult rt_call_hostfn(BuildEnv &env, const std::string &name,
                          const std::vector<Value> &args);

// Host-rejection observability
// Host faults and null-gated results are clean match rejections. Emit the
// `[mch-reject]` diagnostic only for the final lookup pass; collecting passes
// intentionally operate with missing values.
struct RejectCtx {
  const std::string *trace_id{nullptr};
  const std::string *matcher{nullptr};
  const std::string *fn{nullptr};  // set by rt_call_hostfn, read by both paths
  const Block *anchor{nullptr};
  bool final_pass{false};
};

// Thread-local (a classify runs on one thread); mutate only through RejectScope.
RejectCtx &reject_ctx();

// Saves the whole context and restores it on exit, so a mid-build throw can
// never leave `final_pass` armed over a later dry pass.
class RejectScope {
 public:
  explicit RejectScope(const RejectCtx &c) : saved_(reject_ctx()) { reject_ctx() = c; }
  ~RejectScope() { reject_ctx() = saved_; }
  RejectScope(const RejectScope &) = delete;
  RejectScope &operator=(const RejectScope &) = delete;

 private:
  RejectCtx saved_;
};

void reject_log(const std::string &reason);

// For a host fn whose Python twin is `try: ... except: return None`: the Null
// return IS the rejection. rt_call_hostfn cannot log those centrally — it
// cannot tell them from a legitimately null RESULT (jvault_stake_period,
// tonstakers_minted_nft) — so such a fn rejects through here, naming its reason.
EvalResult host_reject(const std::string &reason);

}  // namespace mch
