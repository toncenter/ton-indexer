#include "PostgresInserter.h"
#include <pqxx/pqxx>

#define TO_SQL_BOOL(x) ((x) ? "TRUE" : "FALSE")
#define TO_SQL_STRING(x) (transaction.quote(x))
#define TO_SQL_OPTIONAL(x) ((x) ? std::to_string(x.value()) : "NULL")
#define TO_SQL_OPTIONAL_STRING(x) ((x) ? transaction.quote(x.value()) : "NULL")



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

PostgresInserter::PostgresInserter(PgCredential credential, std::vector<PgInsertData> data, td::Promise<td::Unit> promise)
    : credential_(credential), data_(std::move(data)), promise_(std::move(promise)) {}

void PostgresInserter::start_up() {
    auto connection_string = credential_.get_connection_string();
    try {
        pqxx::connection c(connection_string);
        if (!c.is_open()) {
            promise_.set_error(td::Status::Error("Failed to open database"));
            return;
        }

        {
            pqxx::work txn(c);
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

void PostgresInserter::insert_jetton_masters(pqxx::work &transaction) {
  std::vector<PgJettonMasterData> jetton_masters;
  for (auto& data : data_) {
    if (std::holds_alternative<PgJettonMasterData>(data)) {
      jetton_masters.push_back(std::get<PgJettonMasterData>(data));
    }
  }

  std::ostringstream query;
  query << "INSERT INTO jetton_masters (address, total_supply, mintable, admin_address, jetton_content, jetton_wallet_code_hash, data_hash, code_hash, last_transaction_lt, code_boc, data_boc) VALUES ";
  bool is_first = true;
  for (const auto& jetton_master : jetton_masters) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    query << "("
          << TO_SQL_STRING(jetton_master.address) << ","
          << jetton_master.total_supply << ","
          << TO_SQL_BOOL(jetton_master.mintable) << ","
          << TO_SQL_OPTIONAL_STRING(jetton_master.admin_address) << ","
          << (jetton_master.jetton_content ? TO_SQL_STRING(content_to_json_string(jetton_master.jetton_content.value())) : "NULL") << ","
          << TO_SQL_STRING(td::base64_encode(jetton_master.jetton_wallet_code_hash.as_slice())) << ","
          << TO_SQL_STRING(td::base64_encode(jetton_master.data_hash.as_slice())) << ","
          << TO_SQL_STRING(td::base64_encode(jetton_master.code_hash.as_slice())) << ","
          << jetton_master.last_transaction_lt << ","
          << TO_SQL_STRING(jetton_master.code_boc) << ","
          << TO_SQL_STRING(jetton_master.data_boc)
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

void PostgresInserter::insert_jetton_wallets(pqxx::work &transaction) {
  std::vector<PgJettonWalletData> jetton_wallets;
  for (auto& data : data_) {
    if (std::holds_alternative<PgJettonWalletData>(data)) {
      jetton_wallets.push_back(std::get<PgJettonWalletData>(data));
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
          << TO_SQL_STRING(jetton_wallet.address) << ","
          << TO_SQL_STRING(jetton_wallet.owner) << ","
          << TO_SQL_STRING(jetton_wallet.jetton) << ","
          << jetton_wallet.last_transaction_lt << ","
          << TO_SQL_STRING(jetton_wallet.code_hash) << ","
          << TO_SQL_STRING(jetton_wallet.data_hash)
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


// void PostgresSmcInsertManager::insert(std::vector<PgInsertData> data, td::Promise<td::Unit> promise) {
//   td::actor::create_actor<PostgresInserter>("PostgresInserter", cred_, std::move(data), std::move(promise)).release();
// }


std::string PgCredential::get_connection_string()  {
  return (
    "hostaddr=" + host +
    " port=" + std::to_string(port) + 
    (user.length() ? " user=" + user : "") +
    (password.length() ? " password=" + password : "") +
    (dbname.length() ? " dbname=" + dbname : "")
  );
}