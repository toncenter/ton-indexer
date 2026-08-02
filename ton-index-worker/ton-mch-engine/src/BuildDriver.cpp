#include "BuildDriver.h"

#include "HostRegistry.h"
#include "MsgParse.h"

#include <map>
#include <stdexcept>

namespace mch {

std::string match_skip_reason(const CompiledMatcher &m) {
  // The skip reason lists only the MISSING names, sorted (ref_preds is ordered).
  std::string missing;
  for (const std::string &p : m.ref_preds) {
    if (host_predicates().find(p) == host_predicates().end()) {
      if (!missing.empty()) missing += ",";
      missing += p;
    }
  }
  if (!missing.empty()) {
    return "pred:" + missing;
  }
  return "";
}

std::string build_skip_reason(const CompiledMatcher &m) {
  if (!m.has_build_program) {
    return m.ref_builders.empty() ? "no_build_program" : "builder";
  }
  std::vector<std::string> missing_types;
  for (const std::string &t : m.ref_msgtypes) {
    if (message_parsers().find(t) == message_parsers().end()) {
      missing_types.push_back(t);
    }
  }
  std::vector<std::string> missing_lookups;
  for (const std::string &l : m.ref_lookups) {
    if (lookup_kinds().count(l) == 0) {
      missing_lookups.push_back(l);
    }
  }
  std::vector<std::string> missing_fns;
  for (const std::string &fn : m.ref_fns) {
    if (host_fns().find(fn) == host_fns().end()) {
      missing_fns.push_back(fn);
    }
  }
  std::vector<std::string> missing_shapers;
  for (const std::string &s : m.ref_shapers) {
    if (host_shapers().find(s) == host_shapers().end()) {
      missing_shapers.push_back(s);
    }
  }
  std::string reason;
  auto add = [&reason](const std::string &tag, const std::vector<std::string> &names) {
    if (names.empty()) {
      return;
    }
    if (!reason.empty()) reason += ";";
    reason += tag + ":";
    for (std::size_t i = 0; i < names.size(); i++) {
      if (i) reason += ",";
      reason += names[i];
    }
  };
  add("types", missing_types);
  add("lookups", missing_lookups);
  add("fns", missing_fns);
  add("shaper", missing_shapers);
  return reason;  // empty == included
}

std::vector<Value> slots_from_captures(const std::vector<Capture> &captures) {
  std::vector<Value> slots;
  for (const Capture &c : captures) {
    if (c.is_list) {
      std::vector<Value> xs;
      for (Block *b : c.vals) {
        xs.push_back(b != nullptr ? Value::make_block(b) : Value::null());
      }
      slots.push_back(Value::make_list(std::move(xs)));
    } else {
      Block *b = c.vals.empty() ? nullptr : c.vals[0];
      slots.push_back(b != nullptr ? Value::make_block(b) : Value::null());
    }
  }
  return slots;
}

BuildOutcome run_two_phase(BuildOutcome (*fn)(BuildEnv &), const Block *anchor,
                           const std::vector<Value> &slots, const Value &consumed,
                           const std::set<std::string> &kinds, const LookupSource &src,
                           bool needs_lookups) {
  auto fresh_env = [&](const LookupTable *table) {
    BuildEnv env;
    env.anchor = anchor;
    env.slots = slots.data();
    env.n_slots = slots.size();
    env.consumed = consumed;
    env.lookups = table;
    return env;
  };

  // A2: a matcher with no lookup nodes and no host fns can never miss in the dry
  // pass, so run the final pass directly against an empty filled table. Same
  // result as the loop below (which would converge on round 0 with no fetches).
  // The final pass is the only one whose rejects are about the DATA (the dry
  // passes below see an empty table on purpose), so it is the only one that arms
  // the [mch-reject] log.
  RejectCtx armed = reject_ctx();
  armed.final_pass = true;

  if (!needs_lookups) {
    FilledLookupTable filled(kinds, {});
    BuildEnv env = fresh_env(&filled);
    RejectScope scope(armed);
    return fn(env);
  }

  std::map<std::string, Value> fetched;
  bool converged = false;
  for (int round = 0; round < 8; round++) {
    CollectingLookupTable coll(kinds, fetched);
    BuildEnv env = fresh_env(&coll);
    fn(env);  // dry pass, outcome discarded
    if (coll.misses.empty()) {
      converged = true;
      break;
    }
    for (const auto &[key, ka] : coll.misses) {
      fetched.emplace(key, src.fetch(ka.first, ka.second));
    }
  }
  // Python resolves any lookup-arg-of-lookup depth inline, so an unconverged
  // fixpoint here is a genuine divergence, not a benign truncation — do not
  // proceed with a partial table (silent wrongness). Throw to the classify
  // trace boundary (Python's per-trace fallback). Today's artifacts have max
  // lookup depth 2 and converge in <=2 rounds, so this never fires.
  if (!converged) {
    throw std::runtime_error("two-phase lookup did not converge after 8 rounds");
  }

  FilledLookupTable filled(kinds, std::move(fetched));
  BuildEnv env = fresh_env(&filled);
  RejectScope scope(armed);
  return fn(env);
}

}  // namespace mch
