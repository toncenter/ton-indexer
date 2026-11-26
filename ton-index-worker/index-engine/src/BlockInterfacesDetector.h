#include <td/actor/actor.h>
#include <block/block.h>
#include <unordered_set>
#include "IndexData.h"
#include "parse_contract_methods.h"
#include "smc-interfaces/InterfacesDetector.h"
#include "smc-interfaces/DexPools.h"

using AllShardStates = std::vector<td::Ref<vm::Cell>>;
using FullDetector = InterfacesDetector<JettonWalletDetectorR, JettonMasterDetectorR,
                                     NftItemDetectorR, NftCollectionDetectorR,
                                     GetGemsNftAuction, GetGemsNftFixPriceSale,
                                     GetGemsNftFixPriceSaleV4,
                                     MultisigContract, MultisigOrder,
                                     VestingContract, NominatorPoolContract,
                                     TelemintContract, DedustPoolDetector, StonfiPoolDetector>;

class BlockInterfaceProcessor: public td::actor::Actor {
private:
    ParsedBlockPtr block_;
    td::Promise<ParsedBlockPtr> promise_;
    std::unordered_map<block::StdAddress, std::vector<schema::BlockchainInterfaceV2>> interfaces_{};
    std::unordered_multimap<td::Bits256, uint64_t> contract_methods_{};
    std::vector<schema::DexPoolSnapshot> dex_pool_snapshots_{};
    std::unordered_set<block::StdAddress> dex_removed_{};  // pools whose newest state in this block removes them
    td::Timer timer_{true};
public:
    BlockInterfaceProcessor(ParsedBlockPtr block, td::Promise<ParsedBlockPtr> promise) :
        block_(std::move(block)), promise_(std::move(promise)) {}

    void start_up() override;
    void process_address_interfaces(block::StdAddress address, std::vector<typename FullDetector::DetectedInterface> interfaces,
                                    td::Bits256 code_hash, td::Bits256 data_hash, uint64_t last_trans_lt, uint32_t last_trans_now, td::Promise<td::Unit> promise);
    void finish(td::Result<td::Unit> status);
};
