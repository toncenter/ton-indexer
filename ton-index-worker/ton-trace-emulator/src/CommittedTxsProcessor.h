#pragma once

#include <td/actor/actor.h>
#include <vector>
#include <unordered_map>
#include <vm/cells/Cell.h>
#include <block/block.h>
#include <block/transaction.h>
#include "smc-interfaces/InterfacesDetector.h"
#include "IndexData.h"

// Forward declarations
struct TransactionInfo;

struct CommittedAccountTxs {
    block::Account account;
    std::vector<TransactionInfo> txs;
    using Detector = InterfacesDetector<JettonWalletDetectorR, JettonMasterDetectorR, 
                                        NftItemDetectorR, NftCollectionDetectorR,
                                        GetGemsNftFixPriceSale, GetGemsNftAuction>;
    std::vector<typename Detector::DetectedInterface> interfaces;
};

class CommittedTxsProcessor : public td::actor::Actor {
private:
    std::function<void(CommittedAccountTxs, td::Promise<td::Unit>)> insert_committed_;
    std::vector<td::Ref<vm::Cell>> shard_states_;
    std::shared_ptr<block::ConfigInfo> config_;
    std::unordered_map<block::StdAddress, std::vector<TransactionInfo>> grouped_txs_;
    std::unordered_map<block::StdAddress, block::Account> fetched_accounts_;
    std::unordered_map<block::StdAddress, std::vector<typename CommittedAccountTxs::Detector::DetectedInterface>> account_interfaces_;
    size_t pending_interface_detections_;

    void got_interfaces(block::StdAddress address, std::vector<typename CommittedAccountTxs::Detector::DetectedInterface> interfaces, td::Promise<td::Unit> promise);
    void finish_processing();

public:
    CommittedTxsProcessor(std::function<void(CommittedAccountTxs, td::Promise<td::Unit>)> insert_committed): insert_committed_(std::move(insert_committed)), pending_interface_detections_(0) {}

    void process_transactions(std::vector<TransactionInfo> transactions, std::vector<td::Ref<vm::Cell>> shard_states, std::shared_ptr<block::ConfigInfo> config);
};
