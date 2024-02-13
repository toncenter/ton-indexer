#include "InsertManager.h"


QueueStatus operator+(QueueStatus l, const QueueStatus &r) {
  return QueueStatus{l.mc_blocks_ + r.mc_blocks_, l.blocks_ + r.blocks_, l.txs_ + r.txs_, l.msgs_ + r.msgs_};
}

QueueStatus operator-(QueueStatus l, const QueueStatus &r) {
  return QueueStatus{l.mc_blocks_ - r.mc_blocks_, l.blocks_ - r.blocks_, l.txs_ - r.txs_, l.msgs_ - r.msgs_};
}

QueueStatus& QueueStatus::operator+=(const QueueStatus& r) {
  mc_blocks_ += r.mc_blocks_;
  blocks_ += r.blocks_;
  txs_ += r.txs_;
  msgs_ += r.msgs_;
  return *this;
}

QueueStatus& QueueStatus::operator-=(const QueueStatus& r) {
  mc_blocks_ -= r.mc_blocks_;
  blocks_ -= r.blocks_;
  txs_ -= r.txs_;
  msgs_ -= r.msgs_;
  return *this;
}


template<>
void InsertManagerInterface::get_entity(std::string address, td::Promise<JettonWalletData> promise) {
  get_jetton_wallet(address, std::move(promise));
}

template<>
void InsertManagerInterface::get_entity(std::string address, td::Promise<JettonMasterData> promise) {
  get_jetton_master(address, std::move(promise));
}

template<>
void InsertManagerInterface::get_entity(std::string address, td::Promise<NFTCollectionData> promise) {
  get_nft_collection(address, std::move(promise));
}

template<>
void InsertManagerInterface::get_entity(std::string address, td::Promise<NFTItemData> promise) {
  get_nft_item(address, std::move(promise));
}

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
