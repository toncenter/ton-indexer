#pragma once
#include <vector>
#include "crypto/common/refcnt.hpp"
#include "validator/interfaces/block.h"
#include "validator/interfaces/shard.h"
#include "SchemaPostgres.h"


struct BlockDataState {
  td::Ref<ton::validator::BlockData> block_data;
  td::Ref<ton::validator::ShardState> block_state;
};

using MasterchainBlockDataState = std::vector<BlockDataState>;

struct ParsedBlock {
  MasterchainBlockDataState mc_block_;

  std::vector<schema::Block> blocks_;
  std::vector<schema::Transaction> transactions_;
  std::vector<schema::Message> messages_;
  std::vector<schema::TransactionMessage> transaction_messages_;
  std::vector<schema::AccountState> account_states_;
};
