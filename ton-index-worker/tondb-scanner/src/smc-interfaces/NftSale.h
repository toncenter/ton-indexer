#pragma once
#include <block/block.h>
#include <td/actor/actor.h>
#include <mc-config.h>

using AllShardStates = std::vector<td::Ref<vm::Cell>>;

class GetGemsNftFixPriceSale: public td::actor::Actor {
public:
  struct Result {
    block::StdAddress address;
    bool is_complete;
    uint32_t created_at;
    block::StdAddress marketplace_address;
    block::StdAddress nft_address;
    std::optional<block::StdAddress> nft_owner_address;
    td::RefInt256 full_price;
    block::StdAddress marketplace_fee_address;
    td::RefInt256 marketplace_fee;
    block::StdAddress royalty_address;
    td::RefInt256 royalty_amount;
  };

  GetGemsNftFixPriceSale(block::StdAddress address, 
                       td::Ref<vm::Cell> code_cell,
                       td::Ref<vm::Cell> data_cell, 
                       AllShardStates shard_states,
                       std::shared_ptr<block::ConfigInfo> config,
                       td::Promise<Result> promise);

  void start_up() override;

private:
  block::StdAddress address_;
  td::Ref<vm::Cell> code_cell_;
  td::Ref<vm::Cell> data_cell_;
  AllShardStates shard_states_;
  std::shared_ptr<block::ConfigInfo> config_;
  td::Promise<Result> promise_;
};

class GetGemsNftAuction: public td::actor::Actor {
public:
  struct Result {
    block::StdAddress address;
    bool end;
    uint32_t end_time;
    block::StdAddress mp_addr;
    block::StdAddress nft_addr;
    std::optional<block::StdAddress> nft_owner;
    td::RefInt256 last_bid;
    std::optional<block::StdAddress> last_member;
    uint32_t min_step;
    block::StdAddress mp_fee_addr;
    uint32_t mp_fee_factor, mp_fee_base;
    block::StdAddress royalty_fee_addr;
    uint32_t royalty_fee_factor, royalty_fee_base;
    td::RefInt256 max_bid;
    td::RefInt256 min_bid;
    uint32_t created_at;
    uint32_t last_bid_at;
    bool is_canceled;
  };

  GetGemsNftAuction(block::StdAddress address, 
                       td::Ref<vm::Cell> code_cell,
                       td::Ref<vm::Cell> data_cell, 
                       AllShardStates shard_states,
                       std::shared_ptr<block::ConfigInfo> config,
                       td::Promise<Result> promise);

  void start_up() override;

private:
  block::StdAddress address_;
  td::Ref<vm::Cell> code_cell_;
  td::Ref<vm::Cell> data_cell_;
  AllShardStates shard_states_;
  std::shared_ptr<block::ConfigInfo> config_;
  td::Promise<Result> promise_;
};
