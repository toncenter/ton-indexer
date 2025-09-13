#include "CommittedTxsProcessor.h"
#include "BlockEmulator.h"
#include "smc-interfaces/FetchAccountFromShard.h"
#include <td/utils/logging.h>
#include <algorithm>

void CommittedTxsProcessor::process_transactions(std::vector<TransactionInfo> transactions, std::vector<td::Ref<vm::Cell>> shard_states, std::shared_ptr<block::ConfigInfo> config) {
    shard_states_ = std::move(shard_states);
    config_ = std::move(config);
    grouped_txs_.clear();
    fetched_accounts_.clear();
    account_interfaces_.clear();
    pending_interface_detections_ = 0;

    // Step 1: Group transactions by account
    for (auto& tx : transactions) {
        grouped_txs_[tx.account].push_back(std::move(tx));
    }

    // Step 2: Sort transactions within each group by logical time (ascending)
    for (auto& [account, txs] : grouped_txs_) {
        std::sort(txs.begin(), txs.end(), [](const TransactionInfo& a, const TransactionInfo& b) {
            return a.lt < b.lt;
        });
    }

    // Step 3: Start interface detection for each account
    if (grouped_txs_.empty()) {
        // No transactions to process
        stop();
        return;
    }

    td::MultiPromise mp;
    auto ig = mp.init_guard();
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> res) mutable {
        if (res.is_error()) {
            LOG(ERROR) << "Failed to detect interfaces: " << res.error();
        }
        td::actor::send_closure(SelfId, &CommittedTxsProcessor::finish_processing);
    });
    ig.add_promise(std::move(P));

    for (const auto& [account_addr, txs] : grouped_txs_) {
        account_interfaces_[account_addr] = {};
        pending_interface_detections_++;

        // Fetch account from shard states (similar to EmulationContext::search_in_shard_states)
        bool account_found = false;
        
        for (const auto& shard_state : shard_states_) {
            block::gen::ShardStateUnsplit::Record sstate;
            if (!tlb::unpack_cell(shard_state, sstate)) {
                continue;
            }

            vm::AugmentedDictionary accounts_dict(vm::load_cell_slice_ref(sstate.accounts), 256, block::tlb::aug_ShardAccounts);
            auto account_state = block::Account(account_addr.workchain, account_addr.addr.cbits());
            auto account_cell = accounts_dict.lookup(account_addr.addr);

            if (account_cell.is_null()) {
                if (!account_state.init_new(sstate.gen_utime)) {
                    LOG(ERROR) << "Failed to initialize new account for " << std::to_string(account_addr.workchain) << ":" << account_addr.addr.to_hex();
                    continue;
                }
            } else {
                if (!account_state.unpack(std::move(account_cell), sstate.gen_utime, 
                            account_addr.workchain == ton::masterchainId && config_->is_special_smartcontract(account_addr.addr))) {
                    LOG(ERROR) << "Failed to unpack account for " << std::to_string(account_addr.workchain) << ":" << account_addr.addr.to_hex();
                    continue;
                }
            }

            fetched_accounts_[account_addr] = std::move(account_state);
            account_found = true;
            break;
        }

        if (!account_found) {
            LOG(FATAL) << "Account " << std::to_string(account_addr.workchain) << ":" << account_addr.addr.to_hex() << " not found in shard states, creating uninitialized account";
        }

        // Run interface detection with the fetched account
        auto& account_obj = fetched_accounts_[account_addr];
        if (account_obj.status == block::Account::acc_active && account_obj.code.not_null() && account_obj.data.not_null()) {
            td::actor::create_actor<CommittedAccountTxs::Detector>
                ("InterfacesDetector", account_addr, account_obj.code, account_obj.data, shard_states_, config_,
                td::PromiseCreator::lambda([SelfId = actor_id(this), account_addr, promise = ig.get_promise()](std::vector<typename CommittedAccountTxs::Detector::DetectedInterface> interfaces) mutable {
                    td::actor::send_closure(SelfId, &CommittedTxsProcessor::got_interfaces, account_addr, std::move(interfaces), std::move(promise));
            })).release();
        } else {
            // Account is not active, skip interface detection
            LOG(DEBUG) << "Account " << std::to_string(account_addr.workchain) << ":" << account_addr.addr.to_hex() << " is not active, skipping interface detection";
            std::vector<typename CommittedAccountTxs::Detector::DetectedInterface> empty_interfaces;
            td::actor::send_closure(actor_id(this), &CommittedTxsProcessor::got_interfaces, account_addr, std::move(empty_interfaces), ig.get_promise());
        }
    }
}

void CommittedTxsProcessor::got_interfaces(block::StdAddress address, std::vector<typename CommittedAccountTxs::Detector::DetectedInterface> interfaces, td::Promise<td::Unit> promise) {
    account_interfaces_[address] = std::move(interfaces);
    pending_interface_detections_--;
    promise.set_value(td::Unit());
}

void CommittedTxsProcessor::finish_processing() {
    // Step 4: Create CommittedAccountTxs structs and call insert_committed_ for each
    for (const auto& [account_addr, txs] : grouped_txs_) {
        CommittedAccountTxs committed_account_txs;
        committed_account_txs.account = fetched_accounts_[account_addr];
        committed_account_txs.txs = txs;
        committed_account_txs.interfaces = account_interfaces_[account_addr];

        // Call insert_committed_ with the CommittedAccountTxs
        auto insert_promise = td::PromiseCreator::lambda([account_addr](td::Result<td::Unit> res) {
            if (res.is_error()) {
                LOG(ERROR) << "Failed to insert committed transactions for account " << std::to_string(account_addr.workchain) << ":" << account_addr.addr.to_hex() << ": " << res.error();
            } else {
                LOG(DEBUG) << "Successfully inserted committed transactions for account " << std::to_string(account_addr.workchain) << ":" << account_addr.addr.to_hex();
            }
        });

        insert_committed_(std::move(committed_account_txs), std::move(insert_promise));
    }

    stop();
}
