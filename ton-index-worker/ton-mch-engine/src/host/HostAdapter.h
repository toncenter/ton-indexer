// Typed host-authoring seam. Keeps the runtime
// host ABI (EvalResult(BuildEnv&, args)) but lets a protocol core be written as
//   HostResult<SwapRecord> core(HostContext&, const ConsumedBlocks&);
// The adapter checks arity, decodes the `Value` args into typed inputs, applies
// the null/reject policy, encodes the typed result back to a `Value`, and tags
// faults with the binding name. See BuildRuntime.h for BuildEnv / EvalResult.
#pragma once

#include "ExprRuntime.h"
#include "Value.h"
#include "host/DexRecords.h"

#include <string>
#include <vector>

namespace mch {

struct Block;     // BlockTree.h
struct BuildEnv;  // BuildRuntime.h

// The consumed match set handed to a swap-style host fn: the single List arg
// decoded into its Block elements (non-Block elements dropped, matching the old
// hand loops). The anchor is element 0; `others` is the tail.
struct ConsumedBlocks {
  std::vector<const Block *> blocks;

  const Block *anchor() const { return blocks.front(); }
  std::vector<const Block *> others() const {
    return blocks.empty() ? std::vector<const Block *>{}
                          : std::vector<const Block *>(blocks.begin() + 1, blocks.end());
  }
};

// Everything a typed core needs beyond its decoded args: the build env (for
// env.lookups two-phase access) and the binding name for diagnostics.
struct HostContext {
  BuildEnv &env;
  const char *binding;
};

// A typed host result. `Value` -> encode + rt_ok; `Reject` -> rt_ok(Null) (the
// spec `reject when`s the null); `Fault` -> rt_fault (clean match rejection).
template <class T>
struct HostResult {
  enum class Kind { Value, Reject, Fault };
  Kind kind{Kind::Reject};
  T value{};
  std::string message;

  static HostResult ok(T v) { return HostResult{Kind::Value, std::move(v), {}}; }
  static HostResult reject() { return HostResult{Kind::Reject, {}, {}}; }
  static HostResult fault(std::string m) { return HostResult{Kind::Fault, {}, std::move(m)}; }
};

// Decode the single-List-arg calling convention (Coffee / Stonfi v2 / DeDust
// shape): args must be exactly one non-empty List, and it must contain at least
// one Block. Faults (binding-tagged) otherwise.
EvalResult decode_consumed(const std::vector<Value> &args, const char *binding,
                           ConsumedBlocks &out);

// A swap-style typed core: single-List consumed set -> a SwapRecord result.
using SwapCore = HostResult<SwapRecord> (*)(HostContext &, const ConsumedBlocks &);

// Register a swap core through the runtime ABI: decode the consumed set, run the
// core, then map its HostResult to an EvalResult (encode the record / Null-
// reject / fault). This is the whole body of a registered *_swap_data thunk.
EvalResult run_swap_host(BuildEnv &env, const std::vector<Value> &args, const char *binding,
                         SwapCore core);

}  // namespace mch
