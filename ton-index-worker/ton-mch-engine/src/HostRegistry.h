// Host predicate and fn bindings. The IR loader gates matcher runnability on
// this registry: a matcher is runnable iff all its pred names are registered;
// missing names become its skip reason.
#pragma once

#include "ExprRuntime.h"
#include "Value.h"

#include <map>
#include <string>
#include <vector>

namespace mch {

struct Block;   // BlockTree.h
struct BlockArena;  // BlockTree.h
struct BuildEnv;  // BuildRuntime.h

using HostPredFn = bool (*)(const Block *);

// Registered predicate implementations, name -> fn.
const std::map<std::string, HostPredFn> &host_predicates();

// Host `fn` implementations: the build-time escape hatch. After fixed arity
// is validated, arguments pass through unchanged and a null arg is not
// short-circuited. Signature takes the BuildEnv so a fn can reach env.lookups
// / env.consumed. A thrown/faulted result is a clean match rejection.
// Returning Null likewise rejects when the spec `reject when`s the null.
using HostFn = EvalResult (*)(BuildEnv &, const std::vector<Value> &);

struct HostFnEntry {
  HostFn fn;
  int arity;  // -1 leaves arity validation to the host function
};

// Registered host fns, name -> fn. The IR loader / build_skip_reason gate a
// matcher's inclusion on this registry.
const std::map<std::string, HostFnEntry> &host_fns();

// Host `shape` implementations: a post-build topology pass. Captured blocks
// are reached by name; an arena is needed to mint proxy blocks (C++ blocks
// are arena-owned). Shapers run from ClassifyCore::try_build after merge as
// the last build step. Registration keeps build_skip_reason from reporting a
// registered shaper as missing.
struct ShaperMatch {
  std::map<std::string, Block *> captures;  // capture name -> block (or absent)
  std::vector<Block *> consumed;            // match consumed set (`match.consumed`)
  BlockArena *arena{nullptr};               // proxy-block allocation

  Block *capture(const std::string &name) const {
    auto it = captures.find(name);
    return it == captures.end() ? nullptr : it->second;
  }
};

using HostShaperFn = void (*)(Block *produced, const ShaperMatch &m);

// Registered host shapers, name -> fn.
const std::map<std::string, HostShaperFn> &host_shapers();

}  // namespace mch
