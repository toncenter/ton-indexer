#include "InsertManager.h"



template<>
void InsertManagerInterface::upsert_entity(JettonWalletData entity, td::Promise<td::Unit> promise) {
  upsert_jetton_wallet(entity, std::move(promise));
}

template<>
void InsertManagerInterface::upsert_entity(JettonMasterData entity, td::Promise<td::Unit> promise) {
  upsert_jetton_master(entity, std::move(promise));
}

template<>
void InsertManagerInterface::upsert_entity(NFTCollectionData entity, td::Promise<td::Unit> promise) {
  upsert_nft_collection(entity, std::move(promise));
}

template<>
void InsertManagerInterface::upsert_entity(NFTItemData entity, td::Promise<td::Unit> promise) {
  upsert_nft_item(entity, std::move(promise));
}
