#include "PostgreSQLInserter.h"
#include <pqxx/pqxx>

#include "convert-utils.h"
#include "postgresql_tools.h"

#include <td/utils/JsonBuilder.h>

#define TO_SQL_BOOL(x) ((x) ? "TRUE" : "FALSE")
#define TO_SQL_STRING(x, transaction) (transaction.quote(x))
#define TO_SQL_OPTIONAL(x) ((x) ? std::to_string(x.value()) : "NULL")
#define TO_SQL_OPTIONAL_STRING(x, transaction) ((x) ? transaction.quote(x.value()) : "NULL")
#define TO_B64_HASH(x) td::base64_encode((x).as_slice())

std::string content_to_json_string(const std::map<std::string, std::string> &content) {
  td::JsonBuilder jetton_content_json;
  auto obj = jetton_content_json.enter_object();
  for (auto &attr : content) {
    auto value = attr.second;
    // We erase all \0 bytes because Postgres can't contain such strings
    value.erase(std::remove(value.begin(), value.end(), '\0'), value.end());
    obj(attr.first, value);
  }
  obj.leave();

  return jetton_content_json.string_builder().as_cslice().str();
}

std::string extra_currencies_to_json_string(const std::map<uint32_t, td::RefInt256> &extra_currencies) {
  td::JsonBuilder extra_currencies_json;
  auto obj = extra_currencies_json.enter_object();
  for (auto &currency : extra_currencies) {
    obj(std::to_string(currency.first), currency.second->to_dec_string());
  }
  obj.leave();

  return extra_currencies_json.string_builder().as_cslice().str();
}

void PostgreSQLInserter::start_up() {
    try {
        pqxx::connection c(connection_string_);
        if (!c.is_open()) {
            promise_.set_error(td::Status::Error("Failed to open database"));
            stop();
            return;
        }

        {
            pqxx::work txn(c);
            insert_latest_account_states(txn);
            insert_jetton_masters(txn);
            insert_jetton_wallets(txn);
            insert_nft_items(txn);
            insert_nft_collections(txn);
            insert_multisig_contracts(txn);
            insert_multisig_orders(txn);
            insert_vesting_contracts(txn);
            txn.commit();
        }

        promise_.set_value(td::Unit());
    } catch (const std::exception &e) {
        promise_.set_error(td::Status::Error("Error inserting to PG: " + std::string(e.what())));
    }

    stop();
}

void PostgreSQLInserter::insert_latest_account_states(pqxx::work &transaction) {
  std::vector<schema::AccountState> latest_account_states;
  std::unordered_set<std::string> accounts_set;
  for (auto &data : data_) {
      if (std::holds_alternative<schema::AccountState>(data)) {
        auto account_state = std::get<schema::AccountState>(data);
        std::string account_addr = convert::to_raw_address(account_state.account);
        if (accounts_set.find(account_addr) == accounts_set.end()) {
          accounts_set.insert(account_addr);
          latest_account_states.push_back(account_state);
        }
      }
  }
  
  std::initializer_list<std::string_view> columns = {
    "account", "account_friendly", "hash", "balance", "balance_extra_currencies", "account_status", "timestamp",
    "last_trans_hash", "last_trans_lt", "frozen_hash", "data_hash", "code_hash", "data_boc", "code_boc"
  };
  PopulateTableStream stream(transaction, "latest_account_states", columns, 1000, false);
  stream.setConflictDoUpdate({"account"}, "latest_account_states.last_trans_lt < EXCLUDED.last_trans_lt");

  for (const auto& account_state : latest_account_states) {
    std::optional<std::string> code_str = std::nullopt;
    std::optional<std::string> data_str = std::nullopt;
    if (account_state.data.not_null()) {
      auto data_res = vm::std_boc_serialize(account_state.data);
      if (data_res.is_ok()){
        data_str = td::base64_encode(data_res.move_as_ok());
      } else {
        LOG(ERROR) << "Failed to serialize account data for " << account_state.account
                   << ": " << data_res.move_as_error().message();
        continue;  // Skip this account if serialization fails
      }
    }
    {
      auto code_res = vm::std_boc_serialize(account_state.code);
      if (code_res.is_ok()){
        code_str = td::base64_encode(code_res.move_as_ok());
      }
      if (code_str->length() > 128000) {
        LOG(WARNING) << "Large account code: " << account_state.account;
      }
    }
    auto tuple = std::make_tuple(
      account_state.account,
      std::nullopt,
      account_state.hash,
      account_state.balance.grams,
      extra_currencies_to_json_string(account_state.balance.extra_currencies),
      account_state.account_status,
      account_state.timestamp,
      account_state.last_trans_hash,
      account_state.last_trans_lt,
      account_state.frozen_hash,
      account_state.data_hash,
      account_state.code_hash,
      data_str,
      code_str
    );
    stream.insert_row(std::move(tuple));
  }
  stream.finish();
}

void PostgreSQLInserter::insert_jetton_masters(pqxx::work &transaction) {
  std::vector<schema::JettonMasterDataV2> jetton_masters;
  for (auto &data : data_) {
      if (std::holds_alternative<schema::JettonMasterDataV2>(data)) {
          jetton_masters.push_back(std::get<schema::JettonMasterDataV2>(data));
      }
  }

  std::initializer_list<std::string_view> columns = {
    "address", "total_supply", "mintable", "admin_address", "jetton_content", 
    "jetton_wallet_code_hash", "last_transaction_lt", "code_hash", "data_hash"
  };

  PopulateTableStream stream(transaction, "jetton_masters", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "jetton_masters.last_transaction_lt < EXCLUDED.last_transaction_lt");

  for (const auto& jetton_master : jetton_masters) {
    std::optional<std::string> jetton_content_str = std::nullopt;
    if (jetton_master.jetton_content) {
      jetton_content_str = content_to_json_string(jetton_master.jetton_content.value());
    }

    auto tuple = std::make_tuple(
      jetton_master.address,
      jetton_master.total_supply,
      jetton_master.mintable,
      jetton_master.admin_address,
      jetton_content_str,
      jetton_master.jetton_wallet_code_hash,
      jetton_master.last_transaction_lt,
      jetton_master.code_hash,
      jetton_master.data_hash
    );
    stream.insert_row(std::move(tuple));
  }
  stream.finish();
}

void PostgreSQLInserter::insert_jetton_wallets(pqxx::work &transaction) {
  std::vector<schema::JettonWalletDataV2> jetton_wallets;
  for (auto& data : data_) {
    if (std::holds_alternative<schema::JettonWalletDataV2>(data)) {
      jetton_wallets.push_back(std::get<schema::JettonWalletDataV2>(data));
    }
  }

  std::initializer_list<std::string_view> columns = {
    "balance", "address", "owner", "jetton", "last_transaction_lt", "code_hash", "data_hash", "mintless_is_claimed"
  };

  PopulateTableStream stream(transaction, "jetton_wallets", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "jetton_wallets.last_transaction_lt < EXCLUDED.last_transaction_lt");

  std::unordered_set<block::StdAddress> known_mintless_masters;
  for (const auto& jetton_wallet : jetton_wallets) {
    auto tuple = std::make_tuple(
      jetton_wallet.balance,
      jetton_wallet.address,
      jetton_wallet.owner,
      jetton_wallet.jetton,
      jetton_wallet.last_transaction_lt,
      jetton_wallet.code_hash,
      jetton_wallet.data_hash,
      jetton_wallet.mintless_is_claimed
    );
    stream.insert_row(std::move(tuple));
    if (jetton_wallet.mintless_is_claimed.has_value()) {
      known_mintless_masters.insert(jetton_wallet.jetton);
    }
  }
  stream.finish();
  
  if (!known_mintless_masters.empty()) {
    PopulateTableStream mintless_stream(transaction, "mintless_jetton_masters", {"address", "is_indexed"}, 1000, false);
    mintless_stream.setConflictDoNothing();
  
    for (const auto &addr : known_mintless_masters) {
      auto tuple = std::make_tuple(addr, false);
      mintless_stream.insert_row(std::move(tuple));
    }
    mintless_stream.finish();
  }
}

void PostgreSQLInserter::insert_nft_collections(pqxx::work &txn) {
  std::vector<schema::NFTCollectionDataV2> nft_collections;
  for (auto& data : data_) {
    if (std::holds_alternative<schema::NFTCollectionDataV2>(data)) {
      nft_collections.push_back(std::get<schema::NFTCollectionDataV2>(data));
    }
  }

  std::initializer_list<std::string_view> columns = {
    "address", "next_item_index", "owner_address", "collection_content", "last_transaction_lt", "code_hash", "data_hash"
  };

  PopulateTableStream stream(txn, "nft_collections", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "nft_collections.last_transaction_lt < EXCLUDED.last_transaction_lt");

  for (const auto& nft_collection : nft_collections) {
    std::optional<std::string> collection_content_str = std::nullopt;
    if (nft_collection.collection_content) {
      collection_content_str = content_to_json_string(nft_collection.collection_content.value());
    }
    auto tuple = std::make_tuple(
      nft_collection.address,
      nft_collection.next_item_index,
      nft_collection.owner_address,
      collection_content_str,
      nft_collection.last_transaction_lt,
      nft_collection.code_hash,
      nft_collection.data_hash
    );
    stream.insert_row(std::move(tuple));
  }
  stream.finish();
}

void PostgreSQLInserter::insert_nft_items(pqxx::work &txn) {
  std::vector<schema::NFTItemDataV2> nft_items;
  for (auto& data : data_) {
    if (std::holds_alternative<schema::NFTItemDataV2>(data)) {
      nft_items.push_back(std::get<schema::NFTItemDataV2>(data));
    }
  }
  std::initializer_list<std::string_view> columns = {
    "address", "init", "index", "collection_address", "owner_address", "content", "last_transaction_lt", "code_hash", "data_hash"
  };
  
  PopulateTableStream stream(txn, "nft_items", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "nft_items.last_transaction_lt < EXCLUDED.last_transaction_lt");

  for (const auto& nft_item : nft_items) {
    std::optional<std::string> content_str = std::nullopt;
    if (nft_item.content) {
      content_str = content_to_json_string(nft_item.content.value());
    }
    auto tuple = std::make_tuple(
      nft_item.address,
      nft_item.init,
      nft_item.index,
      nft_item.collection_address,
      nft_item.owner_address,
      content_str,
      nft_item.last_transaction_lt,
      nft_item.code_hash,
      nft_item.data_hash
    );
    stream.insert_row(std::move(tuple));
  }
  stream.finish();

  std::initializer_list<std::string_view> dns_columns = {
    "nft_item_address", "nft_item_owner", "domain", "dns_next_resolver", "dns_wallet", "dns_site_adnl", "dns_storage_bag_id", "last_transaction_lt"
  };
  PopulateTableStream dns_stream(txn, "dns_entries", dns_columns, 1000, false);
  dns_stream.setConflictDoUpdate({"nft_item_address"}, "dns_entries.last_transaction_lt < EXCLUDED.last_transaction_lt");

  for (const auto& nft_item : nft_items) {
    if (!nft_item.dns_entry) {
      continue;
    }
    
    auto tuple = std::make_tuple(
      nft_item.address,
      nft_item.owner_address,
      nft_item.dns_entry->domain,
      nft_item.dns_entry->next_resolver,
      nft_item.dns_entry->wallet,
      nft_item.dns_entry->site_adnl,
      nft_item.dns_entry->storage_bag_id,
      nft_item.last_transaction_lt
    );
    dns_stream.insert_row(std::move(tuple));
  };

  dns_stream.finish();
}

void PostgreSQLInserter::insert_multisig_contracts(pqxx::work &txn) {
  std::vector<schema::MultisigContractData> multisig_contracts;
  for (auto& data : data_) {
    if (std::holds_alternative<schema::MultisigContractData>(data)) {
      multisig_contracts.push_back(std::get<schema::MultisigContractData>(data));
    }
  }
  std::initializer_list<std::string_view> columns = {
    "address", "next_order_seqno", "threshold", "signers", "proposers", "last_transaction_lt", "code_hash", "data_hash"
  };

  PopulateTableStream stream(txn, "multisig", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "multisig.last_transaction_lt < EXCLUDED.last_transaction_lt");

  for (const auto& multisig_contract : multisig_contracts) {
    auto tuple = std::make_tuple(
      multisig_contract.address,
      multisig_contract.next_order_seqno,
      multisig_contract.threshold,
      multisig_contract.signers,
      multisig_contract.proposers,
      multisig_contract.last_transaction_lt,
      multisig_contract.code_hash,
      multisig_contract.data_hash
    );
    stream.insert_row(std::move(tuple));
  }
  stream.finish();
}

void PostgreSQLInserter::insert_multisig_orders(pqxx::work &txn) {
  std::vector<schema::MultisigOrderData> multisig_orders;
  for (auto& data : data_) {
    if (std::holds_alternative<schema::MultisigOrderData>(data)) {
      multisig_orders.push_back(std::get<schema::MultisigOrderData>(data));
    }
  }
  std::initializer_list<std::string_view> columns = {
    "address", "multisig_address", "order_seqno", "threshold", "sent_for_execution", "approvals_mask", "approvals_num",
    "expiration_date", "order_boc", "signers", "last_transaction_lt", "code_hash", "data_hash"
  };

  PopulateTableStream stream(txn, "multisig_orders", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "multisig_orders.last_transaction_lt < EXCLUDED.last_transaction_lt");

  for (const auto&multisig_order : multisig_orders) {
    std::optional<std::string> order_boc_str = std::nullopt;
    if (multisig_order.order.not_null()) {
      auto order_res = vm::std_boc_serialize(multisig_order.order);
      if (order_res.is_ok()) {
        order_boc_str = td::base64_encode(order_res.move_as_ok());
      }
    }

    auto tuple = std::make_tuple(
      multisig_order.address,
      multisig_order.multisig_address,
      multisig_order.order_seqno,
      multisig_order.threshold,
      multisig_order.sent_for_execution,
      multisig_order.approvals_mask,
      multisig_order.approvals_num,
      multisig_order.expiration_date,
      order_boc_str,
      multisig_order.signers,
      multisig_order.last_transaction_lt,
      multisig_order.code_hash,
      multisig_order.data_hash
    );
    stream.insert_row(std::move(tuple));
  }
  stream.finish();
}

void PostgreSQLInserter::insert_vesting_contracts(pqxx::work &txn) {
  std::vector<schema::VestingData> vesting_contracts;
  for (auto& data : data_) {
    if (std::holds_alternative<schema::VestingData>(data)) {
      vesting_contracts.push_back(std::get<schema::VestingData>(data));
    }
  }
  
  std::initializer_list<std::string_view> vesting_columns = {
      "address", "vesting_start_time", "vesting_total_duration", "unlock_period", 
      "cliff_duration", "vesting_total_amount", "vesting_sender_address", "owner_address",
      "last_transaction_lt", "code_hash", "data_hash"
  };
  PopulateTableStream vesting_stream(txn, "vesting_contracts", vesting_columns, 1000, false);
  vesting_stream.setConflictDoUpdate({"address"}, "vesting_contracts.last_transaction_lt < EXCLUDED.last_transaction_lt");

  for (const auto&vesting : vesting_contracts) {
      auto tuple = std::make_tuple(
        vesting.address,
        vesting.vesting_start_time,
        vesting.vesting_total_duration,
        vesting.unlock_period,
        vesting.cliff_duration,
        vesting.vesting_total_amount,
        vesting.vesting_sender_address,
        vesting.owner_address,
        vesting.last_transaction_lt,
        vesting.code_hash,
        vesting.data_hash
      );
      vesting_stream.insert_row(std::move(tuple));
  }

  vesting_stream.finish();

  // Insert whitelist entries
  std::initializer_list<std::string_view> whitelist_columns = {
      "vesting_contract_address", "wallet_address"
  };
  PopulateTableStream whitelist_stream(txn, "vesting_whitelist", whitelist_columns, 1000, false);
  whitelist_stream.setConflictDoNothing();

  for (const auto& vesting : vesting_contracts) {
      for (const auto& wallet_addr : vesting.whitelist) {
          auto tuple = std::make_tuple(
            vesting.address,
            wallet_addr
          );
          whitelist_stream.insert_row(std::move(tuple));
      }
  }
  whitelist_stream.finish();
}

void PostgreSQLInsertManager::insert_done(size_t cnt) {
  --in_progress_;
  inserted_count_ += cnt;
}

void PostgreSQLInsertManager::insert_data(std::vector<InsertData> data, td::Promise<td::Unit> promise) {
  td::actor::create_actor<PostgreSQLInserter>("PostgresInserter", connection_string_, data, std::move(promise)).release();
}
