#pragma once

#include <memory>
#include <string>
#include <vector>

#include <block/block.h>
#include <mc-config.h>

#include <td/actor/actor.h>

using AllShardStates = std::vector<td::Ref<vm::Cell>>;

class StonfiPoolDetector : public td::actor::Actor {
 public:
  struct Result {
    block::StdAddress address;
    int version{0};         // 1 or 2
    std::string pool_type;  // constant_product, constant_sum, stableswap, weighted_const_product, weighted_stableswap
    block::StdAddress router;
    td::Ref<vm::CellSlice> wallet_0;  // router-owned jetton wallets, in pool order
    td::Ref<vm::CellSlice> wallet_1;
    block::StdAddress asset_0;  // jetton masters of wallet_0 / wallet_1 (a pTON master for TON)
    block::StdAddress asset_1;
    td::RefInt256 reserve_0;
    td::RefInt256 reserve_1;
    td::RefInt256 lp_fee;
    td::RefInt256 protocol_fee;
    td::RefInt256 ref_fee;          // v1 only
    td::RefInt256 total_supply_lp;  // v2 only
    bool is_locked{false};          // v2 only
    td::RefInt256 amp;              // stableswap, weighted_stableswap
    td::RefInt256 rate;             // weighted_stableswap
    td::RefInt256 w0;               // weighted_const_product, weighted_stableswap
  };

  StonfiPoolDetector(block::StdAddress address, td::Ref<vm::Cell> code_cell, td::Ref<vm::Cell> data_cell,
                     AllShardStates shard_states, std::shared_ptr<block::ConfigInfo> config,
                     td::Promise<Result> promise);

  void start_up() override;

  static td::Result<Result> detect(const block::StdAddress& address, const td::Ref<vm::Cell>& code_cell,
                                   const td::Ref<vm::Cell>& data_cell, const AllShardStates& shard_states,
                                   const std::shared_ptr<block::ConfigInfo>& config);

 private:
  static td::Status read_pool(const block::StdAddress& address, const td::Ref<vm::Cell>& code_cell,
                              const td::Ref<vm::Cell>& data_cell, const std::shared_ptr<block::ConfigInfo>& config,
                              Result& data);

  static td::Status verify_identity(const block::StdAddress& address, const AllShardStates& shard_states,
                                    const std::shared_ptr<block::ConfigInfo>& config, Result& data);

  block::StdAddress address_;
  td::Ref<vm::Cell> code_cell_;
  td::Ref<vm::Cell> data_cell_;
  AllShardStates shard_states_;
  std::shared_ptr<block::ConfigInfo> config_;
  td::Promise<Result> promise_;
};
