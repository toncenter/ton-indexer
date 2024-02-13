#pragma once
#include "td/actor/actor.h"
#include "IndexData.h"

enum ErrorCode {
  DB_ERROR = 500,
  CACHED_CODE_HASH_NO_ENTITY = 501,
  DATA_PARSING_ERROR = 502,
  GET_METHOD_WRONG_RESULT = 503,
  ADDITIONAL_CHECKS_FAILED = 504,
  EVENT_PARSING_ERROR = 505,

  CODE_HASH_NOT_FOUND = 600,
  ENTITY_NOT_FOUND = 601
};

struct QueueStatus {
  unsigned long mc_blocks_;
  unsigned long blocks_;
  unsigned long txs_;
  unsigned long msgs_;

  friend QueueStatus operator+(QueueStatus l, const QueueStatus& r);
  friend QueueStatus operator-(QueueStatus l, const QueueStatus& r);
  QueueStatus& operator+=(const QueueStatus& r);
  QueueStatus& operator-=(const QueueStatus& r);
};


class InsertManagerInterface: public td::actor::Actor {
public:
  virtual void insert(std::uint32_t mc_seqno, ParsedBlockPtr block_ds, td::Promise<QueueStatus> queued_promise, td::Promise<td::Unit> inserted_promise) = 0;
  virtual void get_insert_queue_status(td::Promise<QueueStatus> promise) = 0;

  virtual void get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise) = 0;

  virtual void upsert_jetton_wallet(JettonWalletData jetton_wallet, td::Promise<td::Unit> promise) = 0;

  virtual void upsert_jetton_master(JettonMasterData jetton_master, td::Promise<td::Unit> promise) = 0;

  virtual void upsert_nft_collection(NFTCollectionData nft_collection, td::Promise<td::Unit> promise) = 0;

  virtual void upsert_nft_item(NFTItemData nft_item, td::Promise<td::Unit> promise) = 0;

  // helper template functions
  template <class T>
  void upsert_entity(T entity, td::Promise<td::Unit> promise);
};
