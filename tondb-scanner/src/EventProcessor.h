#pragma once
#include "InterfaceDetectors.hpp"


class EventProcessor: public td::actor::Actor {
private:
  td::actor::ActorOwn<InterfaceManager> interface_manager_;
  td::actor::ActorOwn<JettonMasterDetector> jetton_master_detector_;
  td::actor::ActorOwn<JettonWalletDetector> jetton_wallet_detector_;
  td::actor::ActorOwn<NFTCollectionDetector> nft_collection_detector_;
  td::actor::ActorOwn<NFTItemDetector> nft_item_detector_;
public:
  EventProcessor(td::actor::ActorId<InsertManagerInterface> insert_manager): 
    interface_manager_(td::actor::create_actor<InterfaceManager>("interface_manager", insert_manager)),
    jetton_master_detector_(td::actor::create_actor<JettonMasterDetector>("jetton_master_detector", interface_manager_.get(), insert_manager)), 
    jetton_wallet_detector_(td::actor::create_actor<JettonWalletDetector>("jetton_wallet_detector", jetton_master_detector_.get(), interface_manager_.get(), insert_manager)),
    nft_collection_detector_(td::actor::create_actor<NFTCollectionDetector>("nft_collection_detector", interface_manager_.get(), insert_manager)),
    nft_item_detector_(td::actor::create_actor<NFTItemDetector>("nft_item_detector", interface_manager_.get(), insert_manager, nft_collection_detector_.get())) {
  }

  void process(ParsedBlockPtr block, td::Promise<> &&promise);

private:
  void process_states(const std::vector<schema::AccountState>& account_states, td::Promise<td::Unit> &&promise);
  void process_transactions(const std::vector<schema::Transaction>& transactions, td::Promise<std::vector<BlockchainEvent>> &&promise);
};