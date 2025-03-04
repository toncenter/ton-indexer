#include "EventProcessor.h"
#include "td/actor/actor.h"
#include "vm/cells/Cell.h"
#include "vm/stack.hpp"
#include "common/refcnt.hpp"
#include "smc-envelope/SmartContract.h"
#include "crypto/block/block-auto.h"
#include "td/utils/base64.h"
#include "IndexData.h"
#include "td/actor/MultiPromise.h"
#include "convert-utils.h"
#include "tokens-tlb.h"


// process ParsedBlock and try detect master and wallet interfaces
void EventProcessor::process(ParsedBlockPtr block, td::Promise<> &&promise) {
  auto P = td::PromiseCreator::lambda([SelfId=actor_id(this), block, promise = std::move(promise)](td::Result<std::vector<BlockchainInterface>> res) mutable {
    if (res.is_error()) {
      promise.set_error(res.move_as_error_prefix("Failed to process account states for mc block " + std::to_string(block->blocks_[0].seqno) + ": "));
      return;
    }
    block->interfaces_ = res.move_as_ok();

    std::vector<schema::Transaction> transactions;
    for (const auto& block : block->blocks_) {
      for (const auto& transaction : block.transactions) {
        transactions.push_back(transaction);
      }
    }
    td::actor::send_closure(SelfId, &EventProcessor::process_transactions, std::move(transactions), promise.wrap([block](std::vector<BlockchainEvent> events) {
      block->events_ = std::move(events);
      return td::Unit();
    }));
  });
  process_states(block->account_states_, block->mc_block_, std::move(P));
}

// void EventProcessor::process(ParsedBlockPtr block, td::Promise<ParsedBlockPtr> promise) {
//   auto P = td::PromiseCreator::lambda([SelfId=actor_id(this), block, promise = std::move(promise)](td::Result<std::vector<BlockchainInterface>> res) mutable {
//     if (res.is_error()) {
//       promise.set_error(res.move_as_error_prefix("Failed to process account states for mc block " + std::to_string(block->blocks_[0].seqno) + ": "));
//       return;
//     }
//     block->interfaces_ = res.move_as_ok();

//     std::vector<schema::Transaction> transactions;
//     for (const auto& block : block->blocks_) {
//       for (const auto& transaction : block.transactions) {
//         transactions.push_back(transaction);
//       }
//     }
//     td::actor::send_closure(SelfId, &EventProcessor::process_transactions, std::move(transactions), promise.wrap([block](std::vector<BlockchainEvent> events) {
//       block->events_ = std::move(events);
//       return td::Unit();
//     }));
//   });
//   process_states(block->account_states_, block->mc_block_, std::move(P));
// }

std::mutex interfaces_mutex;
void EventProcessor::process_states(const std::vector<schema::AccountState>& account_states, const MasterchainBlockDataState& blocks_ds, td::Promise<std::vector<BlockchainInterface>> &&promise) {
  auto found_interfaces = std::make_shared<std::vector<BlockchainInterface>>();

  auto P = td::PromiseCreator::lambda([SelfId=actor_id(this), found_interfaces, promise = std::move(promise)](td::Result<td::Unit> res) mutable {
    if (res.is_error()) {
      promise.set_error(res.move_as_error_prefix("Failed to process events: "));
      return;
    }
    promise.set_value(std::move(*found_interfaces));
  });

  td::MultiPromise mp;
  auto ig = mp.init_guard();
  ig.add_promise(std::move(P));
  for (const auto& account_state : account_states) {
    auto raw_address = convert::to_raw_address(account_state.account);
    if (raw_address == "-1:5555555555555555555555555555555555555555555555555555555555555555" || 
        raw_address == "-1:3333333333333333333333333333333333333333333333333333333333333333") {
      continue;
    }
    auto& address = account_state.account;
    
    auto code_cell = account_state.code;
    auto data_cell = account_state.data; 
    if (code_cell.is_null() || data_cell.is_null()) {
      continue;
    }
    auto last_tx_lt = account_state.last_trans_lt;
    auto last_tx_now = account_state.timestamp;
    auto P1 = td::PromiseCreator::lambda([this, code_cell, address, found_interfaces, promise = ig.get_promise()](td::Result<JettonMasterData> master_data) mutable {
      if (master_data.is_ok()) {
        LOG(DEBUG) << "Detected interface JETTON_MASTER for " << convert::to_raw_address(address);
        std::lock_guard<std::mutex> guard(interfaces_mutex);
        found_interfaces->push_back(master_data.move_as_ok());
      }
      promise.set_value(td::Unit());
    });
    td::actor::send_closure(jetton_master_detector_, &JettonMasterDetector::detect, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(P1));

    auto P2 = td::PromiseCreator::lambda([this, code_cell, address, found_interfaces, promise = ig.get_promise()](td::Result<JettonWalletData> wallet_data) mutable {
      if (wallet_data.is_ok()) {
        LOG(DEBUG) << "Detected interface JETTON_WALLET for " << convert::to_raw_address(address);
        std::lock_guard<std::mutex> guard(interfaces_mutex);
        found_interfaces->push_back(wallet_data.move_as_ok());
      }
      promise.set_value(td::Unit());
    });
    td::actor::send_closure(jetton_wallet_detector_, &JettonWalletDetector::detect, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(P2));

    auto P3 = td::PromiseCreator::lambda([this, code_cell, address, found_interfaces, promise = ig.get_promise()](td::Result<NFTCollectionData> nft_collection_data) mutable {
      if (nft_collection_data.is_ok()) {
        LOG(DEBUG) << "Detected interface NFT_COLLECTION for " << convert::to_raw_address(address);
        std::lock_guard<std::mutex> guard(interfaces_mutex);
        found_interfaces->push_back(nft_collection_data.move_as_ok());
      }
      promise.set_value(td::Unit());
    });
    td::actor::send_closure(nft_collection_detector_, &NFTCollectionDetector::detect, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(P3));

    auto P4 = td::PromiseCreator::lambda([this, code_cell, address, found_interfaces, promise = ig.get_promise()](td::Result<NFTItemData> nft_item_data) mutable {
      if (nft_item_data.is_ok()) {
        LOG(DEBUG) << "Detected interface NFT_ITEM for " << convert::to_raw_address(address);
        std::lock_guard<std::mutex> guard(interfaces_mutex);
        found_interfaces->push_back(nft_item_data.move_as_ok());
      }
      promise.set_value(td::Unit());
    });
    td::actor::send_closure(nft_item_detector_, &NFTItemDetector::detect, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(P4));
  }
}

std::mutex events_mutex;
void EventProcessor::process_transactions(const std::vector<schema::Transaction>& transactions, td::Promise<std::vector<BlockchainEvent>> &&promise) {
  LOG(DEBUG) << "Detecting tokens transactions " << transactions.size();

  auto events = std::make_shared<std::vector<BlockchainEvent>>();

  auto P = td::PromiseCreator::lambda([SelfId=actor_id(this), events, promise = std::move(promise)](td::Result<td::Unit> res) mutable {
    if (res.is_error()) {
      promise.set_error(res.move_as_error_prefix("Failed to process events: "));
      return;
    }
    promise.set_value(std::move(*events));
  });

  td::MultiPromise mp;
  auto ig = mp.init_guard();
  ig.add_promise(std::move(P));

  for (auto& tx : transactions) {
    if (!tx.in_msg || tx.in_msg.value().body.is_null()) {
      // tx doesn't have in_msg, skipping
      continue;
    }

    // template lambda to process td::Result<T> event
    auto process = [events, &tx, &ig](auto&& event, td::Promise<> promise) mutable {
      if (event.is_error()) {
        LOG(DEBUG) << "Failed to parse event (tx hash " << tx.hash << "): " << event.move_as_error();
      } else {
        LOG(DEBUG) << "Event: " << event.ok().transaction_hash;
        std::lock_guard<std::mutex> guard(events_mutex);
        events->push_back(event.move_as_ok());
      }
      promise.set_value(td::Unit());
    };

    auto cs = vm::load_cell_slice_ref(tx.in_msg.value().body);
    switch (tokens::gen::t_InternalMsgBody.check_tag(*cs)) {
      case tokens::gen::InternalMsgBody::transfer_jetton: 
        td::actor::send_closure(jetton_wallet_detector_, &JettonWalletDetector::parse_transfer, tx, cs, 
          td::PromiseCreator::lambda([process, promise = ig.get_promise()](td::Result<JettonTransfer> transfer) mutable { 
            process(std::move(transfer), std::move(promise));
          })
        );
        break;
      case tokens::gen::InternalMsgBody::burn: 
        td::actor::send_closure(jetton_wallet_detector_, &JettonWalletDetector::parse_burn, tx, cs, 
          td::PromiseCreator::lambda([process, promise = ig.get_promise()](td::Result<JettonBurn> burn) mutable { 
            process(std::move(burn), std::move(promise));
          })
        );
        break;
      case tokens::gen::InternalMsgBody::transfer_nft: 
        td::actor::send_closure(nft_item_detector_, &NFTItemDetector::parse_transfer, tx, cs, 
          td::PromiseCreator::lambda([process, promise = ig.get_promise()](td::Result<NFTTransfer> transfer) mutable { 
            process(std::move(transfer), std::move(promise));
          })
        );
        break;
      default:
        continue;
    }
  }
}