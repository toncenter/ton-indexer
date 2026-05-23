#include "NominatorPool.h"

#include "crypto/block/block-auto.h"
#include "td/utils/logging.h"
#include "vm/dict.h"

namespace {

td::Result<td::RefInt256> parse_grams(td::Ref<vm::CellSlice> value, td::Slice field_name) {
  auto amount = validators::gen::t_Grams.as_integer(value);
  if (amount.is_null()) {
    return td::Status::Error(PSLICE() << "Failed to parse " << field_name);
  }
  return amount;
}

block::StdAddress nominator_address_from_hash(const td::BitArray<256>& addr_hash) {
  block::StdAddress address;
  address.workchain = 0;
  address.addr = addr_hash;
  return address;
}

td::Result<block::StdAddress> validator_address_from_hash(td::RefInt256 addr_hash) {
  if (addr_hash.is_null()) {
    return td::Status::Error("Validator address is null");
  }
  td::BitArray<256> bits;
  if (!addr_hash->export_bytes(bits.data(), 32, false)) {
    return td::Status::Error("Failed to parse validator address");
  }
  block::StdAddress address;
  address.workchain = ton::masterchainId;
  address.addr = bits;
  return address;
}

}  // namespace

namespace nominator_pool {

td::Result<ParsedStorage> parse_storage(td::Ref<vm::Cell> data_cell) {
  if (data_cell.is_null()) {
    return td::Status::Error("Pool state data is null");
  }

  validators::gen::NominatorPoolStorage::Record pool_storage;
  if (!validators::gen::t_NominatorPoolStorage.cell_unpack(data_cell, pool_storage)) {
    return td::Status::Error("Failed to unpack NominatorPoolStorage");
  }

  TRY_RESULT(stake_amount_sent, parse_grams(pool_storage.stake_amount_sent, "stake_amount_sent"));
  TRY_RESULT(validator_amount, parse_grams(pool_storage.validator_amount, "validator_amount"));

  validators::gen::NominatorPoolConfig::Record pool_config;
  if (!validators::gen::t_NominatorPoolConfig.cell_unpack(pool_storage.config, pool_config)) {
    return td::Status::Error("Failed to unpack NominatorPoolConfig");
  }
  TRY_RESULT(min_validator_stake, parse_grams(pool_config.min_validator_stake, "min_validator_stake"));
  TRY_RESULT(min_nominator_stake, parse_grams(pool_config.min_nominator_stake, "min_nominator_stake"));
  TRY_RESULT(validator_address, validator_address_from_hash(pool_config.validator_address));

  ParsedStorage result{
      .state = pool_storage.state,
      .nominators_count = pool_storage.nominators_count,
      .stake_amount_sent = stake_amount_sent,
      .validator_amount = validator_amount,
      .validator_address = validator_address,
      .validator_reward_share = pool_config.validator_reward_share,
      .max_nominators_count = pool_config.max_nominators_count,
      .min_validator_stake = min_validator_stake,
      .min_nominator_stake = min_nominator_stake,
      .nominators = {},
      .withdraw_requests = {},
  };

  if (!pool_storage.nominators.is_null() && pool_storage.nominators->size() != 0) {
    td::Ref<vm::Cell> nominators_cell;
    auto nominators_slice = *pool_storage.nominators;
    if (nominators_slice.fetch_maybe_ref(nominators_cell) && nominators_cell.not_null()) {
      vm::Dictionary nominators_dict{nominators_cell, 256};
      for (auto it = nominators_dict.begin(); !it.eof(); ++it) {
        validators::gen::Nominator::Record nominator_data;
        auto nominator_cs = *it.cur_value();
        if (!validators::gen::t_Nominator.unpack(nominator_cs, nominator_data)) {
          LOG(DEBUG) << "Failed to unpack nominator pool entry";
          continue;
        }

        auto amount = validators::gen::t_Grams.as_integer(nominator_data.amount);
        auto pending_deposit_amount = validators::gen::t_Grams.as_integer(nominator_data.pending_deposit_amount);
        if (amount.is_null() || pending_deposit_amount.is_null()) {
          LOG(DEBUG) << "Failed to parse nominator pool entry balances";
          continue;
        }

        result.nominators.push_back({
            .address = nominator_address_from_hash(td::BitArray<256>(it.cur_pos())),
            .amount = amount,
            .pending_deposit_amount = pending_deposit_amount,
        });
      }
    }
  }

  if (!pool_storage.withdraw_requests.is_null() && pool_storage.withdraw_requests->size() != 0) {
    td::Ref<vm::Cell> withdraw_requests_cell;
    auto withdraw_requests_slice = *pool_storage.withdraw_requests;
    if (withdraw_requests_slice.fetch_maybe_ref(withdraw_requests_cell) && withdraw_requests_cell.not_null()) {
      vm::Dictionary withdraw_requests_dict{withdraw_requests_cell, 256};
      for (auto it = withdraw_requests_dict.begin(); !it.eof(); ++it) {
        result.withdraw_requests.push_back(nominator_address_from_hash(td::BitArray<256>(it.cur_pos())));
      }
    }
  }

  return result;
}

}  // namespace nominator_pool

NominatorPoolContract::NominatorPoolContract(block::StdAddress address,
                                             td::Ref<vm::Cell> code_cell,
                                             td::Ref<vm::Cell> data_cell,
                                             std::vector<td::Ref<vm::Cell>> shard_states,
                                             std::shared_ptr<block::ConfigInfo> config,
                                             td::Promise<Result> promise)
    : address_(std::move(address)),
      code_cell_(std::move(code_cell)),
      data_cell_(std::move(data_cell)),
      shard_states_(std::move(shard_states)),
      config_(std::move(config)),
      promise_(std::move(promise)) {
}

void NominatorPoolContract::start_up() {
  if (code_cell_.is_null() || data_cell_.is_null()) {
    promise_.set_error(td::Status::Error("Code or data null"));
    stop();
    return;
  }
  if (code_cell_->get_hash().to_hex() != nominator_pool::CODE_HASH) {
    promise_.set_error(td::Status::Error("Code hash mismatch"));
    stop();
    return;
  }

  auto storage = nominator_pool::parse_storage(data_cell_);
  if (storage.is_error()) {
    promise_.set_error(storage.move_as_error());
    stop();
    return;
  }

  auto parsed = storage.move_as_ok();
  promise_.set_value({
      .address = address_,
      .state = parsed.state,
      .nominators_count = parsed.nominators_count,
      .stake_amount_sent = parsed.stake_amount_sent,
      .validator_amount = parsed.validator_amount,
      .validator_address = parsed.validator_address,
      .validator_reward_share = parsed.validator_reward_share,
      .max_nominators_count = parsed.max_nominators_count,
      .min_validator_stake = parsed.min_validator_stake,
      .min_nominator_stake = parsed.min_nominator_stake,
      .nominators = std::move(parsed.nominators),
  });
  stop();
}
