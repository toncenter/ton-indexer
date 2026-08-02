// Shared pTON master list + wallet->asset conversion (see host/DexPton.h).
#include "host/DexPton.h"

#include <algorithm>

namespace mch {

const std::vector<std::string> &pton_masters() {
  static const std::vector<std::string> m = {
      "0:8CDC1D7640AD5EE326527FC1AD0514F468B30DC84B0173F0E155F451B4E11F7C",
      "0:671963027F7F85659AB55B821671688601CDCF1EE674FC7FBBB1A776A18D34A3",
      "0:949C4C66760C002800E2FA3D8A3CA4E1C90A9373B53AE7472033483BF14CD95E"};
  return m;
}

bool is_pton_master(const std::string &s) {
  const auto &m = pton_masters();
  return std::find(m.begin(), m.end(), s) != m.end();
}

Value wallet_jetton_asset(const Value &wallet, bool pton_conversion) {
  const Value *jf = wallet.is_null() ? nullptr : wallet.field("jetton");
  if (jf == nullptr || jf->t != VType::Str) {
    // v1 (no conversion): AccountId(None) -> Asset(is_ton=True).
    // v2/tonco (conversion): wallet.jetton absent -> caller-visible Null.
    return pton_conversion ? Value::null() : Value::make_asset_ton();
  }
  if (pton_conversion && is_pton_master(jf->str)) {
    return Value::make_asset_ton();
  }
  auto norm = normalize_raw_address(jf->str);
  return Value::make_asset_jetton(norm ? *norm : jf->str);
}

}  // namespace mch
