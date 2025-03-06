#include "PostgreSQLInserter.h"
#include "convert-utils.h"
#include <pqxx/pqxx>

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
            return;
        }

        {
            pqxx::work txn(c);
            insert_latest_account_states(txn);
            insert_jetton_masters(txn);
            insert_jetton_wallets(txn);
            insert_nft_items(txn);
            insert_nft_collections(txn);
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
  std::ostringstream query;
  query << "INSERT INTO latest_account_states (account, account_friendly, hash, balance, balance_extra_currencies, "
                                              "account_status, timestamp, last_trans_hash, last_trans_lt, "
                                              "frozen_hash, data_hash, code_hash, "
                                              "data_boc, code_boc) VALUES ";
  bool is_first = true;
  for (auto &account_state : latest_account_states) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    std::string code_str = "NULL";
    std::string data_str = "NULL";

    auto data_res = vm::std_boc_serialize(account_state.data);
    if (data_res.is_ok()){
      data_str = transaction.quote(td::base64_encode(data_res.move_as_ok().as_slice().str()));
    }
    auto code_res = vm::std_boc_serialize(account_state.code);
    if (code_res.is_ok()){
      code_str = transaction.quote(td::base64_encode(code_res.move_as_ok().as_slice().str()));
    }
    std::optional<std::string> frozen_hash;
    if (account_state.frozen_hash) {
      frozen_hash = td::base64_encode(account_state.frozen_hash.value().as_slice());
    }
    std::optional<std::string> code_hash;
    if (account_state.code_hash) {
      code_hash = td::base64_encode(account_state.code_hash.value().as_slice());
    }
    std::optional<std::string> data_hash;
    if (account_state.data_hash) {
      data_hash = td::base64_encode(account_state.data_hash.value().as_slice());
    }
    // TODO: extracurrencies
    query << "("
          << transaction.quote(convert::to_raw_address(account_state.account)) << ","
          << "NULL,"
          << transaction.quote(td::base64_encode(account_state.hash.as_slice())) << ","
          << account_state.balance.grams << ","
          << transaction.quote(extra_currencies_to_json_string(account_state.balance.extra_currencies)) << ","
          << transaction.quote(account_state.account_status) << ","
          << account_state.timestamp << ","
          << transaction.quote(td::base64_encode(account_state.last_trans_hash.as_slice())) << ","
          << std::to_string(static_cast<std::int64_t>(account_state.last_trans_lt)) << ","
          << (frozen_hash.has_value()? transaction.quote(frozen_hash.value()) : "NULL") << ","
          << (data_hash.has_value()? transaction.quote(data_hash.value()) : "NULL") << ","
          << (code_hash.has_value()? transaction.quote(code_hash.value()) : "NULL") << ","
          << data_str << ","
          << code_str << ")";
  }
  if (is_first) {
    return;
  }
  query << " ON CONFLICT (account) DO UPDATE SET "
        << "account_friendly = EXCLUDED.account_friendly, "
        << "hash = EXCLUDED.hash, "
        << "balance = EXCLUDED.balance, "
        << "account_status = EXCLUDED.account_status, "
        << "timestamp = EXCLUDED.timestamp, "
        << "last_trans_hash = EXCLUDED.last_trans_hash, "
        << "last_trans_lt = EXCLUDED.last_trans_lt, "
        << "frozen_hash = EXCLUDED.frozen_hash, "
        << "data_hash = EXCLUDED.data_hash, "
        << "code_hash = EXCLUDED.code_hash, "
        << "data_boc = EXCLUDED.data_boc, "
        << "code_boc = EXCLUDED.code_boc "
        << "WHERE latest_account_states.last_trans_lt <= EXCLUDED.last_trans_lt;\n";
  transaction.exec0(query.str());
  return;
}

void PostgreSQLInserter::insert_jetton_masters(pqxx::work &transaction) {
    std::vector<JettonMasterDataV2> jetton_masters;
    for (auto &data : data_) {
        if (std::holds_alternative<JettonMasterDataV2>(data)) {
            jetton_masters.push_back(std::get<JettonMasterDataV2>(data));
        }
    }

    std::ostringstream query;
    query << "INSERT INTO jetton_masters (address, total_supply, mintable, admin_address, jetton_content, jetton_wallet_code_hash, data_hash, code_hash, last_transaction_lt) VALUES ";
    bool is_first = true;
    for (const auto &jetton_master : jetton_masters) {
        if (is_first) {
            is_first = false;
        } else {
            query << ", ";
        }
        std::optional<std::string> raw_admin_address;
        if (jetton_master.admin_address) {
          raw_admin_address = convert::to_raw_address(jetton_master.admin_address.value());
        }
        query << "("
              << TO_SQL_STRING(convert::to_raw_address(jetton_master.address), transaction) << ","
              << jetton_master.total_supply << ","
              << TO_SQL_BOOL(jetton_master.mintable) << ","
              << TO_SQL_OPTIONAL_STRING(raw_admin_address, transaction) << ","
              << (jetton_master.jetton_content ? TO_SQL_STRING(content_to_json_string(jetton_master.jetton_content.value()), transaction) : "NULL") << ","
              << TO_SQL_STRING(td::base64_encode(jetton_master.jetton_wallet_code_hash.as_slice()), transaction) << ","
              << TO_SQL_STRING(td::base64_encode(jetton_master.data_hash.as_slice()), transaction) << ","
              << TO_SQL_STRING(td::base64_encode(jetton_master.code_hash.as_slice()), transaction) << ","
              << jetton_master.last_transaction_lt 
              << ")";
    }
    if (is_first) {
        return;
    }
    query << " ON CONFLICT (address) DO UPDATE SET "
          << "total_supply = EXCLUDED.total_supply, "
          << "mintable = EXCLUDED.mintable, "
          << "admin_address = EXCLUDED.admin_address, "
          << "jetton_content = EXCLUDED.jetton_content, "
          << "jetton_wallet_code_hash = EXCLUDED.jetton_wallet_code_hash, "
          << "data_hash = EXCLUDED.data_hash, "
          << "code_hash = EXCLUDED.code_hash, "
          << "last_transaction_lt = EXCLUDED.last_transaction_lt WHERE jetton_masters.last_transaction_lt <= EXCLUDED.last_transaction_lt";

    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());
}

void PostgreSQLInserter::insert_jetton_wallets(pqxx::work &transaction) {
  std::vector<JettonWalletDataV2> jetton_wallets;
  for (auto& data : data_) {
    if (std::holds_alternative<JettonWalletDataV2>(data)) {
      jetton_wallets.push_back(std::get<JettonWalletDataV2>(data));
    }
  }

  std::ostringstream query;
  query << "INSERT INTO jetton_wallets (balance, address, owner, jetton, last_transaction_lt, code_hash, data_hash) VALUES ";
  bool is_first = true;
  for (const auto& jetton_wallet : jetton_wallets) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    
    query << "("
          << jetton_wallet.balance << ","
          << TO_SQL_STRING(convert::to_raw_address(jetton_wallet.address), transaction) << ","
          << TO_SQL_STRING(convert::to_raw_address(jetton_wallet.owner), transaction) << ","
          << TO_SQL_STRING(convert::to_raw_address(jetton_wallet.jetton), transaction) << ","
          << jetton_wallet.last_transaction_lt << ","
          << TO_SQL_STRING(TO_B64_HASH(jetton_wallet.code_hash), transaction) << ","
          << TO_SQL_STRING(TO_B64_HASH(jetton_wallet.data_hash), transaction)
          << ")";
  }
  if (is_first) {
    return;
  }
  query << " ON CONFLICT (address) DO UPDATE SET "
        << "balance = EXCLUDED.balance, "
        << "owner = EXCLUDED.owner, "
        << "jetton = EXCLUDED.jetton, "
        << "last_transaction_lt = EXCLUDED.last_transaction_lt, "
        << "code_hash = EXCLUDED.code_hash, "
        << "data_hash = EXCLUDED.data_hash WHERE jetton_wallets.last_transaction_lt <= EXCLUDED.last_transaction_lt";

  // LOG(DEBUG) << "Running SQL query: " << query.str();
  transaction.exec0(query.str());
}

void PostgreSQLInserter::insert_nft_collections(pqxx::work &txn) {
  std::vector<NFTCollectionDataV2> nft_collections;
  for (auto& data : data_) {
    if (std::holds_alternative<NFTCollectionDataV2>(data)) {
      nft_collections.push_back(std::get<NFTCollectionDataV2>(data));
    }
  }

  std::ostringstream query;
  query << "INSERT INTO nft_collections (address, next_item_index, owner_address, collection_content, last_transaction_lt, code_hash, data_hash) VALUES ";
  bool is_first = true;
  for (const auto& nft_collection : nft_collections) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    std::optional<std::string> raw_owner_address;
    if (nft_collection.owner_address) {
      raw_owner_address = convert::to_raw_address(nft_collection.owner_address.value());
    }
    query << "("
          << txn.quote(convert::to_raw_address(nft_collection.address)) << ","
          << nft_collection.next_item_index << ","
          << TO_SQL_OPTIONAL_STRING(raw_owner_address, txn) << ","
          << (nft_collection.collection_content ? txn.quote(content_to_json_string(nft_collection.collection_content.value())) : "NULL") << ","
          << nft_collection.last_transaction_lt << ","
          << txn.quote(td::base64_encode(nft_collection.code_hash.as_slice())) << ","
          << txn.quote(td::base64_encode(nft_collection.data_hash.as_slice()))
          << ")";
  }
  if (is_first) {
    return;
  }
  query << " ON CONFLICT (address) DO UPDATE SET "
        << "next_item_index = EXCLUDED.next_item_index, "
        << "owner_address = EXCLUDED.owner_address, "
        << "collection_content = EXCLUDED.collection_content, "
        << "last_transaction_lt = EXCLUDED.last_transaction_lt, "
        << "code_hash = EXCLUDED.code_hash, " 
        << "data_hash = EXCLUDED.data_hash WHERE nft_collections.last_transaction_lt < EXCLUDED.last_transaction_lt;\n";
  
  txn.exec0(query.str());
}

void PostgreSQLInserter::insert_nft_items(pqxx::work &txn) {
  std::vector<NFTItemDataV2> nft_items;
  for (auto& data : data_) {
    if (std::holds_alternative<NFTItemDataV2>(data)) {
      nft_items.push_back(std::get<NFTItemDataV2>(data));
    }
  }
  std::ostringstream query;
  bool is_first = true;
  for (const auto& nft_item : nft_items) {
    if (is_first) {
      query << "INSERT INTO nft_items (address, init, index, collection_address, owner_address, content, last_transaction_lt, code_hash, data_hash) VALUES ";
      is_first = false;
    } else {
      query << ", ";
    }
    std::optional<std::string> raw_collection_address;
    if (nft_item.collection_address) {
      raw_collection_address = convert::to_raw_address(nft_item.collection_address.value());
    }
    std::optional<std::string> raw_owner_address;
    if (nft_item.owner_address) {
      raw_owner_address = convert::to_raw_address(nft_item.owner_address.value());
    }
    query << "("
          << txn.quote(convert::to_raw_address(nft_item.address)) << ","
          << TO_SQL_BOOL(nft_item.init) << ","
          << nft_item.index << ","
          << TO_SQL_OPTIONAL_STRING(raw_collection_address, txn) << ","
          << TO_SQL_OPTIONAL_STRING(raw_owner_address, txn) << ","
          << (nft_item.content ? txn.quote(content_to_json_string(nft_item.content.value())) : "NULL") << ","
          << nft_item.last_transaction_lt << ","
          << txn.quote(td::base64_encode(nft_item.code_hash.as_slice())) << ","
          << txn.quote(td::base64_encode(nft_item.data_hash.as_slice()))
          << ")";
  }
  if (!is_first) {
    query << " ON CONFLICT (address) DO UPDATE SET "
          << "init = EXCLUDED.init, "
          << "index = EXCLUDED.index, "
          << "collection_address = EXCLUDED.collection_address, "
          << "owner_address = EXCLUDED.owner_address, "
          << "content = EXCLUDED.content, "
          << "last_transaction_lt = EXCLUDED.last_transaction_lt, "
          << "code_hash = EXCLUDED.code_hash, "
          << "data_hash = EXCLUDED.data_hash WHERE nft_items.last_transaction_lt < EXCLUDED.last_transaction_lt;\n";
  }

  is_first = true;
  for (const auto& nft_item : nft_items) {
    if (!nft_item.dns_entry) {
      continue;
    }
    if (is_first) {
      query << "INSERT INTO dns_entries (nft_item_address, nft_item_owner, domain, dns_next_resolver, dns_wallet, dns_site_adnl, dns_storage_bag_id, last_transaction_lt) VALUES ";
      is_first = false;
    } else {
      query << ", ";
    }

    std::optional<std::string> raw_owner_address;
    if (nft_item.owner_address) {
      raw_owner_address = convert::to_raw_address(nft_item.owner_address.value());
    }
    std::optional<std::string> raw_dns_next_resolver;
    if (nft_item.dns_entry->next_resolver) {
      raw_dns_next_resolver = convert::to_raw_address(nft_item.dns_entry->next_resolver.value());
    }
    std::optional<std::string> raw_dns_wallet;
    if (nft_item.dns_entry->wallet) {
      raw_dns_wallet = convert::to_raw_address(nft_item.dns_entry->wallet.value());
    }
    std::optional<std::string> raw_dns_site;
    if (nft_item.dns_entry->site_adnl) {
      raw_dns_site = nft_item.dns_entry->site_adnl->to_hex();
    }
    std::optional<std::string> raw_dns_storage_bag_id;
    if (nft_item.dns_entry->storage_bag_id) {
      raw_dns_storage_bag_id = nft_item.dns_entry->storage_bag_id->to_hex();
    }
    query << "("
          << txn.quote(convert::to_raw_address(nft_item.address)) << ","
          << TO_SQL_OPTIONAL_STRING(raw_owner_address, txn) << ","
          << txn.quote(nft_item.dns_entry->domain) << ","
          << TO_SQL_OPTIONAL_STRING(raw_dns_next_resolver, txn) << ","
          << TO_SQL_OPTIONAL_STRING(raw_dns_wallet, txn) << ","
          << TO_SQL_OPTIONAL_STRING(raw_dns_site, txn) << ","
          << TO_SQL_OPTIONAL_STRING(raw_dns_storage_bag_id, txn) << ","
          << nft_item.last_transaction_lt
          << ")";
  }
  if (!is_first) {
    query << " ON CONFLICT (nft_item_address) DO UPDATE SET "
          << "nft_item_owner = EXCLUDED.nft_item_owner, "
          << "domain = EXCLUDED.domain, "
          << "dns_next_resolver = EXCLUDED.dns_next_resolver, "
          << "dns_wallet = EXCLUDED.dns_wallet, "
          << "dns_site_adnl = EXCLUDED.dns_site_adnl, "
          << "dns_storage_bag_id = EXCLUDED.dns_storage_bag_id, "
          << "last_transaction_lt = EXCLUDED.last_transaction_lt "
          << "WHERE dns_entries.last_transaction_lt < EXCLUDED.last_transaction_lt;\n";
  }

  txn.exec0(query.str());
}

void PostgreSQLInsertManager::start_up() {
  queue_.reserve(batch_size_ * 2);
  alarm_timestamp() = td::Timestamp::in(10.0);
}

void PostgreSQLInsertManager::alarm() {
  alarm_timestamp() = td::Timestamp::in(10.0);
  LOG(INFO) << "Inserted: " << inserted_count_ << ". Batches in progress: " << in_progress_;
  check_queue(true);
}

void PostgreSQLInsertManager::check_queue(bool force) {
  if (force || (queue_.size() > batch_size_)) {
    std::vector<InsertData> to_insert;
    std::copy(queue_.begin(), queue_.end(), std::back_inserter(to_insert));
    queue_.clear();

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), len = to_insert.size()](td::Result<td::Unit> R) {
      if (R.is_error()) {
        LOG(ERROR) << "Failed to insert batch of " << len << " states: " << R.move_as_error();
      } else {
        // LOG(INFO) << "Inserted " << len << " states";
      }
      td::actor::send_closure(SelfId, &PostgreSQLInsertManager::insert_done, len);
    });

    ++in_progress_;
    td::actor::create_actor<PostgreSQLInserter>("PostgresInserter", connection_string_, to_insert, std::move(P)).release();
  }
}

void PostgreSQLInsertManager::insert_done(size_t cnt) {
  --in_progress_;
  inserted_count_ += cnt;
}

void PostgreSQLInsertManager::checkpoint(ton::ShardIdFull shard, td::Bits256 cur_addr_) {
  try {
      pqxx::connection c(connection_string_);
      if (!c.is_open()) { return; }
      pqxx::work txn(c);    
      td::StringBuilder sb;
      sb << "create unlogged table if not exists _ton_smc_scanner(id int, workchain bigint, shard bigint, cur_addr varchar, primary key (id, workchain, shard));\n";
      sb << "insert into _ton_smc_scanner(id, workchain, shard, cur_addr) values (1, " 
         << shard.workchain << "," << static_cast<std::int64_t>(shard.shard) << ","
         << txn.quote(cur_addr_.to_hex()) 
         << ") on conflict(id, workchain, shard) do update set cur_addr = excluded.cur_addr;\n";
      txn.exec0(sb.as_cslice().str());
      txn.commit();
  } catch (const std::exception &e) {
      LOG(ERROR) << "Error inserting to PG: " +std::string(e.what());
  }
}

void PostgreSQLInsertManager::checkpoint_read(ton::ShardIdFull shard, td::Promise<td::Bits256> promise) {
  try {
      pqxx::connection c(connection_string_);
      if (!c.is_open()) { return; }
      pqxx::work txn(c);    
      
      td::StringBuilder sb;
      sb << "select cur_addr from _ton_smc_scanner where id = 1 and workchain = "
         << shard.workchain << " and shard = " << static_cast<std::int64_t>(shard.shard) << ";";
      auto row = txn.exec1(sb.as_cslice().str());
      td::Bits256 cur_addr;
      cur_addr.from_hex(row[0].as<std::string>());
      promise.set_value(std::move(cur_addr));
  } catch (const std::exception &e) {
      promise.set_error(td::Status::Error("Error reading checkpoint from PG: " + std::string(e.what())));
  }
}

void PostgreSQLInsertManager::checkpoint_reset(ton::ShardIdFull shard) {
  try {
      pqxx::connection c(connection_string_);
      if (!c.is_open()) { return; }
      pqxx::work txn(c);    
      
      td::StringBuilder sb;
      sb << "create unlogged table if not exists _ton_smc_scanner(id int, workchain bigint, shard bigint, cur_addr varchar, primary key (id, workchain, shard));\n";
      sb << "delete from _ton_smc_scanner where id = 1 and workchain = " << shard.workchain << " and shard = " << static_cast<std::int64_t>(shard.shard) << ";\n";
      txn.exec0(sb.as_cslice().str());
  } catch (const std::exception &e) {
      LOG(ERROR) << "Error reseting checkpoint from PG: " + std::string(e.what());
  }
}

void PostgreSQLInsertManager::insert_data(std::vector<InsertData> data) {
  for(auto &&row : data) {
    queue_.push_back(std::move(row));
  }

  check_queue();
}
