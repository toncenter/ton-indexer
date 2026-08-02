// Test-only LookupSource over a fixture's interface map. It mirrors registered
// lookup shapes for jetton wallets, NFT items, and nominator pools. Non-string
// keys produce a clean miss; current matchers supply address strings. The shared
// supported-kind set lives in BuildRuntime.
#pragma once

#include "BuildRuntime.h"  // LookupSource, lookup_kinds
#include "Value.h"

#include <map>
#include <string>
#include <vector>

namespace mch {

class FixtureLookupSource : public LookupSource {
 public:
  explicit FixtureLookupSource(const std::map<std::string, Value> *interfaces)
      : interfaces_(interfaces) {
  }
  Value fetch(const std::string &kind, const std::vector<Value> &args) const override;

 private:
  const std::map<std::string, Value> *interfaces_;
};

}  // namespace mch
