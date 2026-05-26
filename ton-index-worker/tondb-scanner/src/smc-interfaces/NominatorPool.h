#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <block/block.h>
#include <mc-config.h>
#include <td/actor/actor.h>
#include <td/utils/Status.h>

#include "tokens-tlb.h"

namespace nominator_pool {

const std::string CODE_HASH = "9A3EC14BC098F6B44064C305222CAEA2800F17DDA85EE6A8198A7095EDE10DCF";
const std::uint32_t RECOVER_STAKE_OK_OPCODE = 0xf96f7324;
const std::string ELECTOR_ADDRESS = "-1:3333333333333333333333333333333333333333333333333333333333333333";

struct NominatorInfo {
  block::StdAddress address;
  td::RefInt256 amount;
  td::RefInt256 pending_deposit_amount;
};

struct ParsedStorage {
  int state;
  int nominators_count;
  td::RefInt256 stake_amount_sent;
  td::RefInt256 validator_amount;
  block::StdAddress validator_address;
  int validator_reward_share;
  int max_nominators_count;
  td::RefInt256 min_validator_stake;
  td::RefInt256 min_nominator_stake;
  std::vector<NominatorInfo> nominators;
  std::vector<block::StdAddress> withdraw_requests;
};

td::Result<ParsedStorage> parse_storage(td::Ref<vm::Cell> data_cell);

}  // namespace nominator_pool

class NominatorPoolContract: public td::actor::Actor {
public:
  struct Result {
    block::StdAddress address;
    int state;
    int nominators_count;
    td::RefInt256 stake_amount_sent;
    td::RefInt256 validator_amount;
    block::StdAddress validator_address;
    int validator_reward_share;
    int max_nominators_count;
    td::RefInt256 min_validator_stake;
    td::RefInt256 min_nominator_stake;
    std::vector<nominator_pool::NominatorInfo> nominators;
  };

  NominatorPoolContract(block::StdAddress address,
                        td::Ref<vm::Cell> code_cell,
                        td::Ref<vm::Cell> data_cell,
                        std::vector<td::Ref<vm::Cell>> shard_states,
                        std::shared_ptr<block::ConfigInfo> config,
                        td::Promise<Result> promise);

  void start_up() override;

private:
  block::StdAddress address_;
  td::Ref<vm::Cell> code_cell_;
  td::Ref<vm::Cell> data_cell_;
  std::vector<td::Ref<vm::Cell>> shard_states_;
  std::shared_ptr<block::ConfigInfo> config_;
  td::Promise<Result> promise_;
};
