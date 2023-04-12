#pragma once
#include "td/actor/actor.h"
#include "crypto/common/refcnt.hpp"
#include "validator/interfaces/block.h"
#include "validator/interfaces/shard.h"

struct BlockDataState {
  td::Ref<ton::validator::BlockData> block_data;
  td::Ref<ton::validator::ShardState> block_state;
};

class InsertManagerInterface: public td::actor::Actor {
public:
  virtual void insert(std::vector<BlockDataState> block_ds, td::Promise<td::Unit> promise) = 0;
};
