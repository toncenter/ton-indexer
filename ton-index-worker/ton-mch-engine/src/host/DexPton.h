// Shared pTON master list + wallet->asset conversion for the DEX host TUs.
//
// The pTON master address list and the jetton-wallet -> Asset conversion were
// duplicated VERBATIM across HostStonfi.cpp and HostTonco.cpp (three call-site
// variants). This is the single home; see BuildRuntime.h for the Value model.
#pragma once

#include "Value.h"

#include <string>
#include <vector>

namespace mch {

// The three canonical pTON master addresses (raw "wc:HEX" upper). Both Stonfi v2
// and Tonco treat a jetton wallet on one of these masters as a native TON asset.
const std::vector<std::string> &pton_masters();
bool is_pton_master(const std::string &s);

// A jetton_wallet lookup record's `jetton` master -> Asset, with the two
// correlated DEX conventions selected by `pton_conversion`:
//  - true  (Stonfi v2 / Tonco): a pTON-master wallet becomes Asset(TON); an
//    absent jetton master yields Null (caller treats it as "no asset").
//  - false (Stonfi v1): NO pTON conversion (a real pTON wallet stays a jetton
//    asset), and an absent jetton master yields Asset(is_ton=True), Python
//    builds `Asset(is_ton = jetton is None)` directly.
// `wallet` may be Null (a lookup miss): jetton is then absent.
Value wallet_jetton_asset(const Value &wallet, bool pton_conversion);

}  // namespace mch
