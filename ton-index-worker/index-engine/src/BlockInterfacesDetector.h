#include <td/actor/actor.h>
#include <block/block.h>
#include "IndexData.h"
#include "parse_contract_methods.h"
#include "smc-interfaces/InterfacesDetector.h"
#include "smc-interfaces/ParentAccountCache.h"

using AllShardStates = std::vector<td::Ref<vm::Cell>>;
using FullDetector = InterfacesDetector<JettonWalletDetectorR, JettonMasterDetectorR,
                                     NftItemDetectorR, NftCollectionDetectorR,
                                     GetGemsNftAuction, GetGemsNftFixPriceSale,
                                     GetGemsNftFixPriceSaleV4,
                                     MultisigContract, MultisigOrder,
                                     VestingContract, NominatorPoolContract,
                                     TelemintContract, DedustPoolDetector>;

class BlockInterfaceProcessor: public td::actor::Actor {
private:
    ParsedBlockPtr block_;
    td::Promise<ParsedBlockPtr> promise_;
    std::unordered_map<block::StdAddress, std::vector<schema::BlockchainInterfaceV2>> interfaces_{};
    std::unordered_multimap<td::Bits256, uint64_t> contract_methods_{};
    td::Timer timer_{true};
    std::shared_ptr<ParentAccountCache> parents_;
    std::shared_ptr<const ParentAccountView> parent_view_;
public:
    BlockInterfaceProcessor(ParsedBlockPtr block, td::Promise<ParsedBlockPtr> promise,
                            std::shared_ptr<ParentAccountCache> parents = {},
                            std::shared_ptr<const ParentAccountView> parent_view = {}) :
        block_(std::move(block)), promise_(std::move(promise)),
        parents_(std::move(parents)), parent_view_(std::move(parent_view)) {}

    void start_up() override;
    void process_address_interfaces(block::StdAddress address, std::vector<typename FullDetector::DetectedInterface> interfaces,
                                     td::Bits256 code_hash, td::Bits256 data_hash, uint64_t last_trans_lt, uint32_t last_trans_now,
                                     TokenDetectionStatus token_detection, td::Promise<td::Unit> promise);
    void finish(td::Result<td::Unit> status);
};
