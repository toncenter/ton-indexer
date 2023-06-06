#pragma once
#include "td/actor/actor.h"
#include "IndexData.h"

enum ErrorCode {
  DB_ERROR = 500,
  EVENT_PARSE_ERROR = 501,
  NOT_FOUND_ERROR = 502,
  SMC_INTERFACE_PARSE_ERROR = 503,
};

class InsertManagerInterface: public td::actor::Actor {
public:
  virtual void insert(ParsedBlockPtr block_ds, td::Promise<td::Unit> promise) = 0;

  virtual void upsert_jetton_wallet(JettonWalletData jetton_wallet, td::Promise<td::Unit> promise) = 0;
  virtual void get_jetton_wallet(std::string address, td::Promise<JettonWalletData> promise) = 0;

  virtual void upsert_jetton_master(JettonMasterData jetton_master, td::Promise<td::Unit> promise) = 0;
  virtual void get_jetton_master(std::string address, td::Promise<JettonMasterData> promise) = 0;

  virtual void upsert_nft_collection(NFTCollectionData nft_collection, td::Promise<td::Unit> promise) = 0;
  virtual void get_nft_collection(std::string address, td::Promise<NFTCollectionData> promise) = 0;

  virtual void upsert_nft_item(NFTItemData nft_item, td::Promise<td::Unit> promise) = 0;
  virtual void get_nft_item(std::string address, td::Promise<NFTItemData> promise) = 0;

  // helper template functions
  template <class T>
  void upsert_entity(T entity, td::Promise<td::Unit> promise);

  template <class T>
  void get_entity(std::string address, td::Promise<T> promise);
};
