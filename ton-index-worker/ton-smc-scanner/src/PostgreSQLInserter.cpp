#include "PostgreSQLInserter.h"

#include <algorithm>
#include <cstdlib>

#include <pqxx/pqxx>

#include "postgresql_tools.h"

namespace {

bool has_nft_real_owner_data(const kvrocks_state::StateBatch& batch) {
  return !batch.nft_items.empty() || !batch.getgems_nft_sales.empty() || !batch.getgems_nft_auctions.empty();
}

bool is_retriable_insert_error(const std::exception& e) {
  if (dynamic_cast<const sw::redis::IoError*>(&e) != nullptr ||
      dynamic_cast<const sw::redis::ClosedError*>(&e) != nullptr ||
      dynamic_cast<const pqxx::broken_connection*>(&e) != nullptr) {
    return true;
  }

  const auto* redis_error = dynamic_cast<const sw::redis::Error*>(&e);
  if (redis_error == nullptr) {
    return false;
  }
  const std::string message = redis_error->what();
  return message.find("Failed to fetch a connection") != std::string::npos;
}

std::chrono::milliseconds insert_retry_delay(const ScannerInsertConfig& config, std::uint32_t retry_index) {
  auto delay_ms = std::max<std::int64_t>(0, config.insert_retry_initial_delay.count());
  const auto max_delay_ms = std::max<std::int64_t>(delay_ms, config.insert_retry_max_delay.count());
  for (std::uint32_t i = 1; i < retry_index && delay_ms < max_delay_ms; ++i) {
    delay_ms = std::min(delay_ms * 2, max_delay_ms);
  }
  return std::chrono::milliseconds(delay_ms);
}

}  // namespace

void PostgreSQLInserter::start_up() {
  try_insert();
}

void PostgreSQLInserter::alarm() {
  try_insert();
}

void PostgreSQLInserter::try_insert() {
  ++attempt_;
  try {
    auto batch = kvrocks_state::prepare_point_state_batch(data_, config_.source_mc_seqno, config_.max_data_depth);
    insert_kvrocks(batch);
    insert_postgres(batch);
    promise_.set_value(td::Unit());
  } catch (const std::exception& e) {
    if (attempt_ <= config_.max_insert_retries && is_retriable_insert_error(e)) {
      auto delay = insert_retry_delay(config_, attempt_);
      LOG(WARNING) << "Failed to insert scanner data on attempt " << attempt_ << "/"
                   << (config_.max_insert_retries + 1) << ", retrying in " << delay.count()
                   << " ms: " << e.what();
      alarm_timestamp() = td::Timestamp::in(delay.count() / 1000.0);
      return;
    }
    promise_.set_error(td::Status::Error("Error inserting scanner data: " + std::string(e.what())));
  }

  stop();
}

void PostgreSQLInserter::insert_kvrocks(const kvrocks_state::StateBatch& batch) {
  if (!kvrocks_) {
    return;
  }
  kvrocks_state::write_batch_with_script_reload(*kvrocks_, batch);
  kvrocks_state::repair_nft_real_owners_with_script_reload(*kvrocks_, batch);
}

void PostgreSQLInserter::insert_postgres(const kvrocks_state::StateBatch& batch) {
  if (!config_.postgres_enabled) {
    return;
  }

  pqxx::connection c(config_.postgres_connection_string);
  if (!c.is_open()) {
    throw std::runtime_error("failed to open PostgreSQL connection");
  }

  pqxx::work txn(c);
  lock_nft_real_owner_reconciliation(txn, batch);
  insert_latest_account_states(txn, batch);
  insert_jetton_masters(txn, batch);
  insert_jetton_wallets(txn, batch);
  insert_nft_items(txn, batch);
  insert_nft_collections(txn, batch);
  insert_getgems_nft_auctions(txn, batch);
  insert_getgems_nft_sales(txn, batch);
  reconcile_nft_real_owners(txn, batch);
  insert_multisig_contracts(txn, batch);
  insert_multisig_orders(txn, batch);
  insert_vesting_contracts(txn, batch);
  insert_nominator_pools(txn, batch);
  insert_telemint_contracts(txn, batch);
  insert_dedust_pools(txn, batch);
  txn.commit();
}

void PostgreSQLInserter::insert_latest_account_states(pqxx::work& txn, const kvrocks_state::StateBatch& batch) {
  std::initializer_list<std::string_view> columns = {
    "account", "hash", "balance", "balance_extra_currencies", "account_status", "timestamp",
    "last_trans_hash", "last_trans_lt", "frozen_hash", "data_hash", "code_hash", "data_boc", "code_boc"
  };
  PopulateTableStream stream(txn, "latest_account_states", columns, 1000, false);
  stream.setConflictDoUpdate({"account"}, "latest_account_states.last_trans_lt < EXCLUDED.last_trans_lt");
  for (const auto& row : batch.latest_account_states) {
    stream.insert_row(row);
  }
  stream.finish();
}

void PostgreSQLInserter::insert_jetton_masters(pqxx::work& txn, const kvrocks_state::StateBatch& batch) {
  std::initializer_list<std::string_view> columns = {
    "address", "total_supply", "mintable", "admin_address", "jetton_content",
    "jetton_wallet_code_hash", "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };
  PopulateTableStream stream(txn, "jetton_masters", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "jetton_masters.last_transaction_lt < EXCLUDED.last_transaction_lt");
  for (const auto& row : batch.jetton_masters) {
    stream.insert_row(row);
  }
  stream.finish();
}

void PostgreSQLInserter::insert_jetton_wallets(pqxx::work& txn, const kvrocks_state::StateBatch& batch) {
  std::initializer_list<std::string_view> columns = {
    "balance", "address", "owner", "jetton", "last_transaction_lt", "code_hash", "data_hash", "mintless_is_claimed", "destroyed"
  };
  PopulateTableStream stream(txn, "jetton_wallets", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "jetton_wallets.last_transaction_lt < EXCLUDED.last_transaction_lt");
  for (const auto& row : batch.jetton_wallets) {
    stream.insert_row(row);
  }
  stream.finish();

  PopulateTableStream mintless_stream(txn, "mintless_jetton_masters", {"address", "is_indexed"}, 1000, false);
  mintless_stream.setConflictDoNothing();
  for (const auto& row : batch.mintless_jetton_masters) {
    mintless_stream.insert_row(row);
  }
  mintless_stream.finish();
}

void PostgreSQLInserter::insert_nft_collections(pqxx::work& txn, const kvrocks_state::StateBatch& batch) {
  std::initializer_list<std::string_view> columns = {
    "address", "next_item_index", "owner_address", "collection_content", "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };
  PopulateTableStream stream(txn, "nft_collections", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "nft_collections.last_transaction_lt < EXCLUDED.last_transaction_lt");
  for (const auto& row : batch.nft_collections) {
    stream.insert_row(row);
  }
  stream.finish();
}

void PostgreSQLInserter::insert_nft_items(pqxx::work& txn, const kvrocks_state::StateBatch& batch) {
  std::initializer_list<std::string_view> columns = {
    "address", "init", "index", "collection_address", "owner_address", "content", "last_transaction_lt",
    "code_hash", "data_hash", "real_owner", "destroyed"
  };
  PopulateTableStream stream(txn, "nft_items", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "nft_items.last_transaction_lt < EXCLUDED.last_transaction_lt");
  for (const auto& row : batch.nft_items) {
    stream.insert_row(row);
  }
  stream.finish();

  std::initializer_list<std::string_view> dns_columns = {
    "nft_item_address", "nft_item_owner", "domain", "dns_next_resolver", "dns_wallet", "dns_site_adnl",
    "dns_storage_bag_id", "last_transaction_lt", "destroyed"
  };
  PopulateTableStream dns_stream(txn, "dns_entries", dns_columns, 1000, false);
  dns_stream.setConflictDoUpdate({"nft_item_address"}, "dns_entries.last_transaction_lt < EXCLUDED.last_transaction_lt");
  for (const auto& row : batch.dns_entries) {
    dns_stream.insert_row(row);
  }
  dns_stream.finish();
}

void PostgreSQLInserter::lock_nft_real_owner_reconciliation(pqxx::work& txn,
                                                            const kvrocks_state::StateBatch& batch) {
  if (!has_nft_real_owner_data(batch)) {
    return;
  }
  txn.exec("SELECT pg_advisory_xact_lock(7564925639186208761)");
}

void PostgreSQLInserter::reconcile_nft_real_owners(pqxx::work& txn, const kvrocks_state::StateBatch& batch) {
  if (!has_nft_real_owner_data(batch)) {
    return;
  }

  for (const auto& row : batch.getgems_nft_sales) {
    if (row.destroyed || !row.nft_owner_address) {
      continue;
    }
    txn.exec(R"(
      UPDATE nft_items
      SET real_owner = $1
      WHERE address = $2
        AND owner_address = $3
        AND real_owner IS DISTINCT FROM $1
    )", pqxx::params{*row.nft_owner_address, row.nft_address, row.address});
  }

  for (const auto& row : batch.getgems_nft_auctions) {
    if (row.destroyed || !row.nft_owner) {
      continue;
    }
    txn.exec(R"(
      UPDATE nft_items
      SET real_owner = $1
      WHERE address = $2
        AND owner_address = $3
        AND real_owner IS DISTINCT FROM $1
    )", pqxx::params{*row.nft_owner, row.nft_addr, row.address});
  }

  for (const auto& row : batch.nft_items) {
    if (row.destroyed || !row.owner_address) {
      continue;
    }
    txn.exec(R"(
      UPDATE nft_items AS n
      SET real_owner = s.nft_owner_address
      FROM getgems_nft_sales AS s
      WHERE n.address = $1
        AND n.owner_address = $2
        AND s.address = n.owner_address
        AND s.nft_address = n.address
        AND s.nft_owner_address IS NOT NULL
        AND NOT s.destroyed
        AND NOT n.destroyed
        AND n.real_owner IS DISTINCT FROM s.nft_owner_address
    )", pqxx::params{row.address, *row.owner_address});

    txn.exec(R"(
      UPDATE nft_items AS n
      SET real_owner = a.nft_owner
      FROM getgems_nft_auctions AS a
      WHERE n.address = $1
        AND n.owner_address = $2
        AND a.address = n.owner_address
        AND a.nft_addr = n.address
        AND a.nft_owner IS NOT NULL
        AND NOT a.destroyed
        AND NOT n.destroyed
        AND n.real_owner IS DISTINCT FROM a.nft_owner
    )", pqxx::params{row.address, *row.owner_address});
  }
}

void PostgreSQLInserter::insert_getgems_nft_sales(pqxx::work& txn, const kvrocks_state::StateBatch& batch) {
  std::initializer_list<std::string_view> columns = {
    "address", "is_complete", "created_at", "marketplace_address", "nft_address", "nft_owner_address", "full_price",
    "marketplace_fee_address", "marketplace_fee", "royalty_address", "royalty_amount",
    "sold_at", "sold_query_id", "jetton_price_dict",
    "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };
  PopulateTableStream stream(txn, "getgems_nft_sales", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "getgems_nft_sales.last_transaction_lt < EXCLUDED.last_transaction_lt");
  for (const auto& row : batch.getgems_nft_sales) {
    stream.insert_row(row);
  }
  stream.finish();
}

void PostgreSQLInserter::insert_getgems_nft_auctions(pqxx::work& txn, const kvrocks_state::StateBatch& batch) {
  std::initializer_list<std::string_view> columns = {
    "address", "end_flag", "end_time", "mp_addr", "nft_addr", "nft_owner", "last_bid", "last_member",
    "min_step", "mp_fee_addr", "mp_fee_factor", "mp_fee_base", "royalty_fee_addr", "royalty_fee_factor",
    "royalty_fee_base", "max_bid", "min_bid", "created_at", "last_bid_at", "is_canceled", "activated",
    "step_time", "last_query_id", "jetton_wallet", "jetton_master", "is_broken_state", "public_key",
    "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };
  PopulateTableStream stream(txn, "getgems_nft_auctions", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "getgems_nft_auctions.last_transaction_lt < EXCLUDED.last_transaction_lt");
  for (const auto& row : batch.getgems_nft_auctions) {
    stream.insert_row(row);
  }
  stream.finish();
}

void PostgreSQLInserter::insert_multisig_contracts(pqxx::work& txn, const kvrocks_state::StateBatch& batch) {
  std::initializer_list<std::string_view> columns = {
    "address", "next_order_seqno", "threshold", "signers", "proposers", "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };
  PopulateTableStream stream(txn, "multisig", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "multisig.last_transaction_lt < EXCLUDED.last_transaction_lt");
  for (const auto& row : batch.multisig_contracts) {
    stream.insert_row(row);
  }
  stream.finish();
}

void PostgreSQLInserter::insert_multisig_orders(pqxx::work& txn, const kvrocks_state::StateBatch& batch) {
  std::initializer_list<std::string_view> columns = {
    "address", "multisig_address", "order_seqno", "threshold", "sent_for_execution", "approvals_mask",
    "approvals_num", "expiration_date", "order_boc", "signers", "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };
  PopulateTableStream stream(txn, "multisig_orders", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "multisig_orders.last_transaction_lt < EXCLUDED.last_transaction_lt");
  for (const auto& row : batch.multisig_orders) {
    stream.insert_row(row);
  }
  stream.finish();
}

void PostgreSQLInserter::insert_vesting_contracts(pqxx::work& txn, const kvrocks_state::StateBatch& batch) {
  std::initializer_list<std::string_view> vesting_columns = {
    "address", "vesting_start_time", "vesting_total_duration", "unlock_period",
    "cliff_duration", "vesting_total_amount", "vesting_sender_address", "owner_address",
    "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };
  PopulateTableStream vesting_stream(txn, "vesting_contracts", vesting_columns, 1000, false);
  vesting_stream.setConflictDoUpdate({"address"}, "vesting_contracts.last_transaction_lt < EXCLUDED.last_transaction_lt");
  for (const auto& row : batch.vesting_contracts) {
    vesting_stream.insert_row(row);
  }
  vesting_stream.finish();

  PopulateTableStream whitelist_stream(txn, "vesting_whitelist", {"vesting_contract_address", "wallet_address"}, 1000, false);
  whitelist_stream.setConflictDoNothing();
  for (const auto& row : batch.vesting_whitelist) {
    whitelist_stream.insert_row(row);
  }
  whitelist_stream.finish();
}

void PostgreSQLInserter::insert_nominator_pools(pqxx::work& txn, const kvrocks_state::StateBatch& batch) {
  std::initializer_list<std::string_view> columns = {
    "address", "state", "nominators_count", "stake_amount_sent", "validator_amount",
    "validator_address", "validator_reward_share", "max_nominators_count", "min_validator_stake", "min_nominator_stake",
    "active_nominators", "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };
  PopulateTableStream stream(txn, "nominator_pools", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "nominator_pools.last_transaction_lt < EXCLUDED.last_transaction_lt");
  for (const auto& row : batch.nominator_pools) {
    stream.insert_row(row);
  }
  stream.finish();
}

void PostgreSQLInserter::insert_telemint_contracts(pqxx::work& txn, const kvrocks_state::StateBatch& batch) {
  std::initializer_list<std::string_view> columns = {
    "address", "token_name", "bidder_address", "bid", "bid_ts", "min_bid", "end_time",
    "beneficiary_address", "initial_min_bid", "max_bid", "min_bid_step", "min_extend_time", "duration",
    "royalty_numerator", "royalty_denominator", "royalty_destination", "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };
  PopulateTableStream stream(txn, "telemint_nft_items", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "telemint_nft_items.last_transaction_lt < EXCLUDED.last_transaction_lt");
  for (const auto& row : batch.telemint_nft_items) {
    stream.insert_row(row);
  }
  stream.finish();
}

void PostgreSQLInserter::insert_dedust_pools(pqxx::work& txn, const kvrocks_state::StateBatch& batch) {
  std::initializer_list<std::string_view> columns = {
    "address", "asset_1", "asset_2", "reserve_1", "reserve_2", "pool_type", "dex", "fee",
    "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };
  PopulateTableStream stream(txn, "dex_pools", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "dex_pools.last_transaction_lt < EXCLUDED.last_transaction_lt");
  for (const auto& row : batch.dedust_pools) {
    stream.insert_row(row);
  }
  stream.finish();
}

void PostgreSQLInsertManager::start_up() {
  if (!config_.kvrocks_config.enabled) {
    LOG(INFO) << "Kvrocks connection is not configured for ton-smc-scanner";
    return;
  }

  try {
    kvrocks_state::KvrocksClient kvrocks(config_.kvrocks_config);
    kvrocks_ = kvrocks.make_redis();
    auto ping_response = kvrocks_->ping();
    if (ping_response != "PONG") {
      throw std::runtime_error("unexpected Kvrocks PING response: " + ping_response);
    }
    kvrocks_state::load_kvrocks_scripts(*kvrocks_);
    LOG(INFO) << "Kvrocks connection ready: " << config_.kvrocks_config.describe();
  } catch (const std::exception& e) {
    LOG(ERROR) << "Error connecting to Kvrocks: " << e.what();
    std::_Exit(2);
  }
}

void PostgreSQLInsertManager::insert_done(size_t cnt) {
  --in_progress_;
  inserted_count_ += cnt;
}

void PostgreSQLInsertManager::insert_data(std::vector<InsertData> data, td::Promise<td::Unit> promise) {
  td::actor::create_actor<PostgreSQLInserter>(
      "ScannerInserter", config_, kvrocks_, std::move(data), std::move(promise)).release();
}
