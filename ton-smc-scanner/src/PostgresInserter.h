#pragma once
#include <td/actor/actor.h>
#include <td/utils/base64.h>
#include <td/utils/JsonBuilder.h>
#include <crypto/vm/cells/CellHash.h>
#include <pqxx/pqxx>





struct PgJettonMasterData {
  std::string address;
  std::string total_supply;
  bool mintable;
  td::optional<std::string> admin_address;
  td::optional<std::map<std::string, std::string>> jetton_content;
  vm::CellHash jetton_wallet_code_hash;
  vm::CellHash data_hash;
  vm::CellHash code_hash;
  uint64_t last_transaction_lt;
  std::string code_boc;
  std::string data_boc;
};

struct PgJettonWalletData {
  std::string balance;
  std::string address;
  std::string owner;
  std::string jetton;
  uint64_t last_transaction_lt;
  std::string code_hash;
  std::string data_hash;
};

struct PgCredential {
  std::string host = "127.0.0.1";
  int port = 5432;
  std::string user = "postgres";
  std::string password = "postgr3s_PwD";
  std::string dbname = "ton_index";

  std::string get_connection_string();
};

using PgInsertData = std::variant<PgJettonMasterData, PgJettonWalletData>;

class PostgresInserter : public td::actor::Actor {
public:
    PostgresInserter(PgCredential credential, std::vector<PgInsertData> data, td::Promise<td::Unit> promise);

    void start_up() override;

private:
    void insert_jetton_masters(pqxx::work &transaction);
    void insert_jetton_wallets(pqxx::work &transaction);

    PgCredential credential_;
    std::vector<PgInsertData> data_;
    td::Promise<td::Unit> promise_;
};

// class PostgresSmcInsertManager: public td::actor::Actor {
// public:

//   PostgresSmcInsertManager(PgCredential cred)
//     : cred_(cred) {}

//   void insert(std::vector<PgInsertData> data, td::Promise<td::Unit> promise);

// private:
//   PgCredential cred_;
//   td::Promise<td::Unit> promise_;
// };