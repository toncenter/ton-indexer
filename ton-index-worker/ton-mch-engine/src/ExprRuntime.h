// Shared leaf-operation semantics for generated expressions and the interpreter.
// Faults are explicit EvalResult values, and each operation defines its own null
// behavior.
#pragma once

#include "Value.h"

#include <functional>
#include <map>
#include <string>
#include <vector>

namespace mch {

struct EvalResult {
  bool faulted{false};
  Value value;
  std::string message;  // fault reason (diagnostics only)
};

using Env = std::map<std::string, Value>;
using Lookups = std::map<std::string, Value>;

// Evaluation context for a node-level inline `where (expr)`: the candidate block
// plus slot-indexed
// captures. Both the interpreter (ExprEval with a where context) and the
// generated where-functions take this, one signature, so codegen never has to
// change when slots land: `slots[i]` will hold the capture bound to slot i
// (Null until bound), n_slots = the matcher's slot count.
struct Block;  // BlockTree.h
struct WhereEnv {
  const Block *block{nullptr};
  const Value *slots{nullptr};
  std::size_t n_slots{0};
};

// --- result helpers ---
inline EvalResult rt_ok(Value v) { return EvalResult{false, std::move(v), {}}; }
EvalResult rt_fault(const std::string &msg);
inline bool rt_is_null(const Value &v) { return v.t == VType::Null; }

// --- name / access / lookup ---
EvalResult rt_name(const Env &env, const std::string &id);
EvalResult rt_access(const Value &obj, const std::string &field);
EvalResult rt_lookup(const Lookups &lk, const std::string &name, const std::vector<Value> &args);

// Leading-dot field access inside a `where (expr)`: reads the candidate
// block's data (mirrors expr_eval._sync_eval "dotfield": null data -> null,
// dict data -> key lookup with missing-key fault, other data -> rt_access).
EvalResult rt_dotfield(const WhereEnv &w, const std::string &name);

// --- unary ---
EvalResult rt_neg(const Value &x);
EvalResult rt_not(const Value &x);
EvalResult rt_require_bool(const Value &x);

// --- binary strict operators (lazy and/or/??/ternary live at the call site) ---
EvalResult rt_eq(const Value &l, const Value &r);
EvalResult rt_ne(const Value &l, const Value &r);
EvalResult rt_lt(const Value &l, const Value &r);
EvalResult rt_le(const Value &l, const Value &r);
EvalResult rt_gt(const Value &l, const Value &r);
EvalResult rt_ge(const Value &l, const Value &r);
EvalResult rt_add(const Value &l, const Value &r);
EvalResult rt_sub(const Value &l, const Value &r);
EvalResult rt_mul(const Value &l, const Value &r);

// --- builtins (null-strict: any null argument short-circuits to null) ---
// Per-builtin cores. The interpreter dispatches through rt_call_builtin; the
// generated code calls the typed rt_builtin_<name> directly, without a name
// string or per-call argument vector. Both paths run the same core.
EvalResult rt_call_builtin(const std::string &name, const std::vector<Value> &args);
EvalResult rt_builtin_account(const Value &x);
// Comprehensions
// Shared cores for `map(xs as e => body)` / `any|all(xs as e => body)`. `body`
// is invoked once per element with that element bound; the core owns the
// null, non-list, empty, and short-circuit semantics for both the interpreter
// and generated code. A null xs produces null;
// non-list xs -> fault; empty -> [] (map) / false (any) / true (all). Quantifiers
// short-circuit like std::any_of/std::all_of and require a bool body.
using ElementFn = std::function<EvalResult(const Value &)>;
EvalResult rt_mapc(const Value &xs, const ElementFn &body);
EvalResult rt_quant(bool is_all, const Value &xs, const ElementFn &body);
EvalResult rt_builtin_amount(const Value &x);
EvalResult rt_builtin_asset(const Value &x);
EvalResult rt_builtin_ton_asset();
EvalResult rt_builtin_addr_none();
EvalResult rt_builtin_b64(const Value &x);
EvalResult rt_builtin_asset_of(const Value &x);
EvalResult rt_builtin_tail_unwrap(const Value &x);
EvalResult rt_builtin_bytes_of(const Value &x);
EvalResult rt_builtin_first(const Value &x);
EvalResult rt_builtin_last(const Value &x);
EvalResult rt_builtin_len(const Value &x);
EvalResult rt_builtin_sum(const Value &x);
EvalResult rt_builtin_zip(const Value &a, const Value &b);
EvalResult rt_builtin_map(const Value &a, const Value &b);
EvalResult rt_builtin_concat(const Value &a, const Value &b);
EvalResult rt_builtin_contains(const Value &a, const Value &b);

}  // namespace mch
