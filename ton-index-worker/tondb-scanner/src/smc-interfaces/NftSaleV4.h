#pragma once
#include <block/block.h>
#include <td/actor/actor.h>
#include <mc-config.h>

using AllShardStates = std::vector<td::Ref<vm::Cell>>;

class GetGemsNftFixPriceSaleV4: public td::actor::Actor {
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
    uint32_t sold_at;
    uint64_t sold_query_id;
    std::map<std::string, std::string> jetton_price_dict;
  };

  GetGemsNftFixPriceSaleV4(block::StdAddress address,
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
