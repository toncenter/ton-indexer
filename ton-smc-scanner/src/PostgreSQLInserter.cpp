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
  query << "INSERT INTO latest_account_states (account, account_friendly, hash, balance, "
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
    query << "("
          << transaction.quote(convert::to_raw_address(account_state.account)) << ","
          << "NULL,"
          << transaction.quote(td::base64_encode(account_state.hash.as_slice())) << ","
          << account_state.balance << ","
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
    std::vector<JettonMasterData> jetton_masters;
    for (auto &data : data_) {
        if (std::holds_alternative<JettonMasterData>(data)) {
            jetton_masters.push_back(std::get<JettonMasterData>(data));
        }
    }

    std::ostringstream query;
    query << "INSERT INTO jetton_masters (address, total_supply, mintable, admin_address, jetton_content, jetton_wallet_code_hash, data_hash, code_hash, last_transaction_lt, code_boc, data_boc) VALUES ";
    bool is_first = true;
    for (const auto &jetton_master : jetton_masters) {
        if (is_first) {
            is_first = false;
        } else {
            query << ", ";
        }
        query << "("
              << TO_SQL_STRING(jetton_master.address, transaction) << ","
              << jetton_master.total_supply << ","
              << TO_SQL_BOOL(jetton_master.mintable) << ","
              << TO_SQL_OPTIONAL_STRING(jetton_master.admin_address, transaction) << ","
              << (jetton_master.jetton_content ? TO_SQL_STRING(content_to_json_string(jetton_master.jetton_content.value()), transaction) : "NULL") << ","
              << TO_SQL_STRING(td::base64_encode(jetton_master.jetton_wallet_code_hash.as_slice()), transaction) << ","
              << TO_SQL_STRING(td::base64_encode(jetton_master.data_hash.as_slice()), transaction) << ","
              << TO_SQL_STRING(td::base64_encode(jetton_master.code_hash.as_slice()), transaction) << ","
              << jetton_master.last_transaction_lt << ","
              << TO_SQL_STRING(jetton_master.code_boc, transaction) << ","
              << TO_SQL_STRING(jetton_master.data_boc, transaction)
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
          << "last_transaction_lt = EXCLUDED.last_transaction_lt, "
          << "code_boc = EXCLUDED.code_boc, "
          << "data_boc = EXCLUDED.data_boc WHERE jetton_masters.last_transaction_lt <= EXCLUDED.last_transaction_lt";

    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());
}

void PostgreSQLInserter::insert_jetton_wallets(pqxx::work &transaction) {
  std::vector<JettonWalletData> jetton_wallets;
  for (auto& data : data_) {
    if (std::holds_alternative<JettonWalletData>(data)) {
      jetton_wallets.push_back(std::get<JettonWalletData>(data));
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
          << TO_SQL_STRING(jetton_wallet.address, transaction) << ","
          << TO_SQL_STRING(jetton_wallet.owner, transaction) << ","
          << TO_SQL_STRING(jetton_wallet.jetton, transaction) << ","
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

void PostgreSQLInsertManager::start_up() {
  queue_.reserve(batch_size_ * 2);
  alarm_timestamp() = td::Timestamp::in(10.0);
}

void PostgreSQLInsertManager::alarm() {
  alarm_timestamp() = td::Timestamp::in(10.0);
  LOG(INFO) << "Inserted: " << inserted_count_ << " In progress: " << in_progress_;
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
      td::actor::send_closure(SelfId, &PostgreSQLInsertManager::insert_done);
    });

    ++in_progress_;
    inserted_count_ += to_insert.size();
    td::actor::create_actor<PostgreSQLInserter>("PostgresInserter", connection_string_, to_insert, std::move(P)).release();
  }
}

void PostgreSQLInsertManager::insert_done() {
  --in_progress_;
}

void PostgreSQLInsertManager::checkpoint(ton::ShardIdFull shard, td::Bits256 cur_addr_) {
  try {
      pqxx::connection c(connection_string_);
      if (!c.is_open()) { return; }
      pqxx::work txn(c);    
      td::StringBuilder sb;
      sb << "create table if not exists _ton_smc_scanner(id int, workchain bigint, shard bigint, cur_addr varchar, primary key (id, workchain, shard));\n";
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
      sb << "create table if not exists _ton_smc_scanner(id int, workchain bigint, shard bigint, cur_addr varchar, primary key (id, workchain, shard));\n";
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
