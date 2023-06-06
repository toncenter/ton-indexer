#pragma once
#include <vector>
#include <variant>
#include "crypto/common/refcnt.hpp"
#include "validator/interfaces/block.h"
#include "validator/interfaces/shard.h"
#include "SchemaPostgres.h"


struct JettonMasterData {
  std::string address;
  uint64_t total_supply;
  bool mintable;
  td::optional<std::string> admin_address;
  td::optional<std::map<std::string, std::string>> jetton_content;
  vm::CellHash jetton_wallet_code_hash;
  vm::CellHash data_hash;
  vm::CellHash code_hash;
  uint64_t last_transaction_lt;
  std::string code_boc;
  std::string data_boc;
};

struct JettonWalletData {
  uint64_t balance;
  std::string address;
  std::string owner;
  std::string jetton;
  uint64_t last_transaction_lt;
  vm::CellHash code_hash;
  vm::CellHash data_hash;
};

struct JettonTransfer {
  std::string transaction_hash;
  uint64_t query_id;
  td::RefInt256 amount;
  std::string destination;
  std::string response_destination;
  td::Ref<vm::Cell> custom_payload;
  td::RefInt256 forward_ton_amount;
  td::Ref<vm::Cell> forward_payload;
};

struct JettonBurn {
  std::string transaction_hash;
  uint64_t query_id;
  td::RefInt256 amount;
  std::string response_destination;
  td::Ref<vm::Cell> custom_payload;
};

struct NFTCollectionData {
  std::string address;
  td::RefInt256 next_item_index;
  td::optional<std::string> owner_address;
  td::optional<std::map<std::string, std::string>> collection_content;
  vm::CellHash data_hash;
  vm::CellHash code_hash;
  uint64_t last_transaction_lt;
  std::string code_boc;
  std::string data_boc;
};

struct NFTItemData {
  std::string address;
  bool init;
  td::RefInt256 index;
  std::string collection_address;
  std::string owner_address;
  td::optional<std::map<std::string, std::string>> content;
  uint64_t last_transaction_lt;
  vm::CellHash code_hash;
  vm::CellHash data_hash;
};

struct NFTTransfer {
  std::string transaction_hash;
  uint64_t query_id;
  std::string nft_item;
  std::string old_owner;
  std::string new_owner;
  std::string response_destination;
  td::Ref<vm::Cell> custom_payload;
  td::RefInt256 forward_amount;
  td::Ref<vm::Cell> forward_payload;
};

struct BlockDataState {
  td::Ref<ton::validator::BlockData> block_data;
  td::Ref<ton::validator::ShardState> block_state;
};

using MasterchainBlockDataState = std::vector<BlockDataState>;
using BlockchainEvent = std::variant<JettonTransfer, 
                                     JettonBurn,
                                     NFTTransfer>;

struct ParsedBlock {
  MasterchainBlockDataState mc_block_;

  std::vector<schema::Block> blocks_;
  std::vector<schema::Transaction> transactions_;
  std::vector<schema::Message> messages_;
  std::vector<schema::TransactionMessage> transaction_messages_;
  std::vector<schema::AccountState> account_states_;

  std::vector<BlockchainEvent> events_;
  
  template <class T>
  std::vector<T> get_events() {
    std::vector<T> result;
    for (auto& event: events_) {
      if (std::holds_alternative<T>(event)) {
        result.push_back(std::get<T>(event));
      }
    }
    return result;
  }
};

using ParsedBlockPtr = std::shared_ptr<ParsedBlock>;