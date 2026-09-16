// Shared pTON master list and wallet-to-asset conversion for DEX hosts.
#pragma once

#include "Value.h"

#include <optional>
#include <string>
#include <vector>

namespace mch {

// The three canonical pTON master addresses (raw "wc:HEX" upper). Both Stonfi v2
// and Tonco treat a jetton wallet on one of these masters as a native TON asset.
const std::vector<std::string> &pton_masters();
bool is_pton_master(const std::string &s);

// The normalized wallet.jetton master, or nullopt for a lookup miss or a
// non-string jetton field. pTON conversion and Asset wrapping stay with callers.
std::optional<std::string> wallet_jetton_master_str(const Value &wallet);

// A jetton_wallet lookup's `jetton` master -> Asset, selected by
// `pton_conversion`:
//  - true  (Stonfi v2 / Tonco): a pTON-master wallet becomes Asset(TON); an
//    absent jetton master yields Null (caller treats it as "no asset").
//  - false (Stonfi v1): no pTON conversion (a real pTON wallet stays a jetton
//    asset); an absent jetton master yields Asset(is_ton=True).
// `wallet` may be Null (a lookup miss): jetton is then absent.
Value wallet_jetton_asset(const Value &wallet, bool pton_conversion);

}  // namespace mch
