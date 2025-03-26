#pragma once
#include "td/actor/actor.h"
#include "queue_state.h"
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


class InsertManagerInterface: public td::actor::Actor {
public:
  virtual void insert(std::uint32_t mc_seqno, ParsedBlockPtr block_ds, bool force, td::Promise<QueueState> queued_promise, td::Promise<td::Unit> inserted_promise) = 0;
  virtual void get_insert_queue_state(td::Promise<QueueState> promise) = 0;
  virtual void get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise, std::int32_t from_seqno = 0, std::int32_t to_seqno = 0) = 0;
};
