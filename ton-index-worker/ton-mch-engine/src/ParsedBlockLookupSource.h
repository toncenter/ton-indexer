// Resolves lookups from interfaces already computed for the block, with an
// injectable fallback for persisted data. Malformed addresses and missing
// accounts are clean misses.
#pragma once

#include "BuildRuntime.h"  // LookupSource, Value

#include "IndexData.h"  // schema::BlockchainInterfaceV2, ParsedBlock (tondb-scanner, PUBLIC)

#include <functional>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

namespace mch {

class ParsedBlockLookupSource : public LookupSource {
 public:
  // Tier-2 resolver: same signature as fetch. Empty == no fallback (production
  // stub until the DB/kvrocks round is wired; the offline gate injects a
  // fixture-backed fallback for the tier-2-only kinds).
  using Tier2Hook = std::function<Value(const std::string &kind, const std::vector<Value> &args)>;

  using InterfaceMap =
      std::unordered_map<block::StdAddress, std::vector<schema::BlockchainInterfaceV2>>;

  explicit ParsedBlockLookupSource(const InterfaceMap *account_interfaces, Tier2Hook tier2 = {})
      : account_interfaces_(account_interfaces), tier2_(std::move(tier2)) {
  }

  // The set of kinds this source recognises (== lookup_kinds(), so
  // prepare_classify's skip table is identical on the production path).
  static const std::set<std::string> &kinds();

  Value fetch(const std::string &kind, const std::vector<Value> &args) const override;

  // Tier-attribution telemetry: every resolved lookup
  // is charged to the tier that served it; unresolved calls are misses. Gives
  // the tier-2 hit rate in a production classifier run
  // (tier2_hits / (tier1_hits + tier2_hits)). Does not affect fetch() results.
  struct LookupStats {
    std::size_t tier1_hits{0};  // served from account_interfaces_ (in-block)
    std::size_t tier2_hits{0};  // served from the tier-2 fallback (DB/kvrocks / fixture)
    std::size_t misses{0};      // unresolved after both tiers (null result)
    // Miss attribution per lookup kind, so a production run can tell an expected
    // gap (a kind with no tier-1 source) from a real resolution failure. Only
    // missed kinds get an entry, and the values sum to `misses` exactly.
    std::map<std::string, std::size_t> misses_by_kind;
  };
  const LookupStats &stats() const { return stats_; }

  // Shared V2-to-Value shape map for tier 1 and tier 2.
  // Null == this variant does not answer this kind. A tier-2 hook that produces
  // a schema::*V2 renders it through HERE rather than building a Value of its
  // own. This makes the two tiers shape-identical by construction
  // instead of by review.
  static Value iface_value(const std::string &kind, const schema::BlockchainInterfaceV2 &iface);

 private:
  // Tier-1 resolve from account_interfaces_ (null == miss). Only the tier-1
  // kinds are handled; everything else returns null so fetch() routes to tier 2.
  Value tier1(const std::string &kind, const std::string &addr) const;

  const InterfaceMap *account_interfaces_;
  Tier2Hook tier2_;
  mutable LookupStats stats_;  // fetch() is const; counters are telemetry
};

}  // namespace mch
