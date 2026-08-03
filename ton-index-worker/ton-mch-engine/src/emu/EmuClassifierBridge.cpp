#include "EmuClassifierBridge.h"

#include "EmuInterfaces.h"

#include "TraceEmulator.h"

#include "td/utils/overloaded.h"

#include <utility>
#include <variant>
#include <vector>

namespace mch {

ParsedBlockLookupSource::InterfaceMap make_interface_map(const ::Trace &trace) {
  ParsedBlockLookupSource::InterfaceMap result;

  // Derive NFT code hashes from the detector's account state, preferring the
  // latest emulated state over committed state. Zero means unknown.
  auto account_code_hash = [&trace](const block::StdAddress &addr) {
    td::Bits256 h;
    h.set_zero();
    auto range = trace.emulated_accounts.equal_range(addr);
    if (range.first != range.second) {
      const block::Account &acc = std::prev(range.second)->second;
      if (acc.code.not_null()) {
        return td::Bits256{acc.code->get_hash().bits()};
      }
    }
    auto it = trace.committed_accounts.find(addr);
    if (it != trace.committed_accounts.end() && it->second.code.not_null()) {
      return td::Bits256{it->second.code->get_hash().bits()};
    }
    return h;
  };

  // Use trace-final interfaces so emulated accounts resolve in tier 1. Variants
  // without a tier-1 lookup kind are ignored.
  for (const auto &[addr, ifaces] : trace.interfaces) {
    std::vector<schema::BlockchainInterfaceV2> adapted;
    const td::Bits256 code_hash = account_code_hash(addr);
    for (const auto &iface : ifaces) {
      std::visit(
          td::overloaded(
              [&](const JettonWalletDetectorR::Result &r) { adapted.push_back(to_v2(r)); },
              [&](const NftItemDetectorR::Result &r) {
                auto v = to_v2(r);
                v.code_hash = code_hash;
                adapted.push_back(std::move(v));
              },
              [&](const GetGemsNftFixPriceSale::Result &r) {
                auto v = to_v2(r);
                v.code_hash = code_hash;
                adapted.push_back(std::move(v));
              },
              [&](const GetGemsNftAuction::Result &r) {
                auto v = to_v2(r);
                v.code_hash = code_hash;
                adapted.push_back(std::move(v));
              },
              // DeDust and nominator pools resolve in tier 1. nft_sale accepts
              // either fixed-price-sale variant.
              [&](const GetGemsNftFixPriceSaleV4::Result &r) {
                auto v = to_v2(r);
                v.code_hash = code_hash;
                adapted.push_back(std::move(v));
              },
              [&](const DedustPoolDetector::Result &r) { adapted.push_back(to_v2(r)); },
              [&](const MultisigOrder::Result &r) { adapted.push_back(to_v2(r)); },
              [&](const NominatorPoolContract::Result &r) { adapted.push_back(to_v2(r)); },
              [](const JettonMasterDetectorR::Result &) {},
              [](const NftCollectionDetectorR::Result &) {}),
          iface);
    }
    if (!adapted.empty()) {
      result.emplace(addr, std::move(adapted));
    }
  }
  return result;
}

}  // namespace mch
