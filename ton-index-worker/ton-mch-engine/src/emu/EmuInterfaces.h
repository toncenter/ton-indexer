// Adapts emulator detector results to the pipeline's schema V2 structs for
// tier-1 lookup. Fields unavailable from detectors remain value-initialized.
#pragma once

#include "IndexData.h"  // schema::*V2 + the detector Result types

namespace mch {

schema::JettonWalletDataV2 to_v2(const JettonWalletDetectorR::Result &r);
schema::NFTItemDataV2 to_v2(const NftItemDetectorR::Result &r);  // content carried
schema::GetGemsNftFixPriceSaleData to_v2(const GetGemsNftFixPriceSale::Result &r);
schema::GetGemsNftAuctionData to_v2(const GetGemsNftAuction::Result &r);
// The cell-db lookup uses the same adapters as tier 1.
schema::DedustPoolData to_v2(const DedustPoolDetector::Result &r);
schema::NominatorPoolData to_v2(const NominatorPoolContract::Result &r);
schema::GetGemsNftFixPriceSaleV4Data to_v2(const GetGemsNftFixPriceSaleV4::Result &r);
schema::MultisigOrderData to_v2(const MultisigOrder::Result &r);

}  // namespace mch
