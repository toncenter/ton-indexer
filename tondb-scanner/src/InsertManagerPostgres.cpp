#include <pqxx/pqxx>
#include <chrono>
#include "td/utils/JsonBuilder.h"
#include "InsertManagerPostgres.h"
#include "convert-utils.h"

#define TO_SQL_BOOL(x) ((x) ? "TRUE" : "FALSE")
#define TO_SQL_STRING(x) ("'" + (x) + "'")
#define TO_SQL_OPTIONAL(x) ((x) ? std::to_string(x.value()) : "NULL")
#define TO_SQL_OPTIONAL_STRING(x) ((x) ? ("'" + x.value() + "'") : "NULL")

std::string content_to_json_string(const std::map<std::string, std::string> &content) {
  td::JsonBuilder jetton_content_json;
  auto obj = jetton_content_json.enter_object();
  for (auto &attr : content) {
    obj(attr.first, attr.second);
  }
  obj.leave();

  return jetton_content_json.string_builder().as_cslice().str();
}

class InsertBatchMcSeqnos: public td::actor::Actor {
private:
  std::string connection_string_;
  std::vector<ParsedBlockPtr> mc_blocks_;
  td::Promise<td::Unit> promise_;
public:
  InsertBatchMcSeqnos(std::string connection_string, std::vector<ParsedBlockPtr> mc_blocks, td::Promise<td::Unit> promise): 
    connection_string_(std::move(connection_string)), 
    mc_blocks_(std::move(mc_blocks)), 
    promise_(std::move(promise))
  {
    LOG(DEBUG) << "Created InsertBatchMcSeqnos with " << mc_blocks_.size() << "blocks";
  }

  void insert_blocks(pqxx::work &transaction) {
    std::ostringstream query;
    query << "INSERT INTO blocks (workchain, shard, seqno, root_hash, file_hash, mc_block_workchain, "
                                  "mc_block_shard, mc_block_seqno, global_id, version, after_merge, before_split, "
                                  "after_split, want_split, key_block, vert_seqno_incr, flags, gen_utime, start_lt, "
                                  "end_lt, validator_list_hash_short, gen_catchain_seqno, min_ref_mc_seqno, "
                                  "prev_key_block_seqno, vert_seqno, master_ref_seqno, rand_seed, created_by) VALUES ";

    bool is_first = true;
    for (const auto& mc_block : mc_blocks_) {
      for (const auto& block : mc_block->blocks_) {
        if (is_first) {
          is_first = false;
        } else {
          query << ", ";
        }
        query << "("
              << block.workchain << ","
              << block.shard << ","
              << block.seqno << ","
              << TO_SQL_STRING(block.root_hash) << ","
              << TO_SQL_STRING(block.file_hash) << ","
              << TO_SQL_OPTIONAL(block.mc_block_workchain) << ","
              << TO_SQL_OPTIONAL(block.mc_block_shard) << ","
              << TO_SQL_OPTIONAL(block.mc_block_seqno) << ","
              << block.global_id << ","
              << block.version << ","
              << TO_SQL_BOOL(block.after_merge) << ","
              << TO_SQL_BOOL(block.before_split) << ","
              << TO_SQL_BOOL(block.after_split) << ","
              << TO_SQL_BOOL(block.want_split) << ","
              << TO_SQL_BOOL(block.key_block) << ","
              << TO_SQL_BOOL(block.vert_seqno_incr) << ","
              << block.flags << ","
              << block.gen_utime << ","
              << block.start_lt << ","
              << block.end_lt << ","
              << block.validator_list_hash_short << ","
              << block.gen_catchain_seqno << ","
              << block.min_ref_mc_seqno << ","
              << block.prev_key_block_seqno << ","
              << block.vert_seqno << ","
              << TO_SQL_OPTIONAL(block.master_ref_seqno) << ","
              << TO_SQL_STRING(block.rand_seed) << ","
              << TO_SQL_STRING(block.created_by)
              << ")";
      }
    }
    if (is_first) {
      return;
    }
    query << " ON CONFLICT DO NOTHING";

    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());
  }

  void insert_transactions(pqxx::work &transaction) {
    std::ostringstream query;
    query << "INSERT INTO transactions (block_workchain, block_shard, block_seqno, account, hash, lt, utime, transaction_type, "
                                       "account_state_hash_before, account_state_hash_after, fees, storage_fees, in_fwd_fees, computation_fees, "
                                       "action_fees, compute_exit_code, compute_gas_used, compute_gas_limit, compute_gas_credit, "
                                       "compute_gas_fees, compute_vm_steps, compute_skip_reason, action_result_code, action_total_fwd_fees, "
                                       "action_total_action_fees) VALUES ";
    bool is_first = true;
    for (const auto& mc_block : mc_blocks_) {
      for (const auto& transaction : mc_block->transactions_) {
        if (is_first) {
          is_first = false;
        } else {
          query << ", ";
        }
        query << "("
              << transaction.block_workchain << ","
              << transaction.block_shard << ","
              << transaction.block_seqno << ","
              << "'" << transaction.account << "',"
              << "'" << transaction.hash << "',"
              << transaction.lt << ","
              << transaction.utime << ","
              << "'" << transaction.transaction_type << "',"
              << "'" << transaction.account_state_hash_before << "',"
              << "'" << transaction.account_state_hash_after << "',"
              << transaction.fees << ","
              << transaction.storage_fees << ","
              << transaction.in_fwd_fees << ","
              << transaction.computation_fees << ","
              << transaction.action_fees << ","
              << TO_SQL_OPTIONAL(transaction.compute_exit_code) << ","
              << TO_SQL_OPTIONAL(transaction.compute_gas_used) << ","
              << TO_SQL_OPTIONAL(transaction.compute_gas_limit) << ","
              << TO_SQL_OPTIONAL(transaction.compute_gas_credit) << ","
              << TO_SQL_OPTIONAL(transaction.compute_gas_fees) << ","
              << TO_SQL_OPTIONAL(transaction.compute_vm_steps) << ","
              << TO_SQL_OPTIONAL_STRING(transaction.compute_skip_reason) << ","
              << TO_SQL_OPTIONAL(transaction.action_result_code) << ","
              << TO_SQL_OPTIONAL(transaction.action_total_fwd_fees) << ","
              << TO_SQL_OPTIONAL(transaction.action_total_action_fees)
              << ")";
      }
    }
    if (is_first) {
      return;
    }
    query << " ON CONFLICT DO NOTHING";

    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());
  }
  
  void insert_messsages(pqxx::work &transaction) {
    std::ostringstream query;
    query << "INSERT INTO messages (hash, source, destination, value, fwd_fee, ihr_fee, created_lt, created_at, opcode, "
                                 "ihr_disabled, bounce, bounced, import_fee, body_hash, init_state_hash) VALUES ";
    bool is_first = true;
    for (const auto& mc_block : mc_blocks_) {
      for (const auto& message : mc_block->messages_) {
        if (is_first) {
          is_first = false;
        } else {
          query << ", ";
        }
        query << "("
              << "'" << message.hash << "',"
              << (message.source ? "'" + message.source.value() + "'" : "NULL") << ","
              << (message.destination ? "'" + message.destination.value() + "'" : "NULL") << ","
              << (message.value ? std::to_string(message.value.value()) : "NULL") << ","
              << (message.fwd_fee ? std::to_string(message.fwd_fee.value()) : "NULL") << ","
              << (message.ihr_fee ? std::to_string(message.ihr_fee.value()) : "NULL") << ","
              << (message.created_lt ? std::to_string(message.created_lt.value()) : "NULL") << ","
              << (message.created_at ? std::to_string(message.created_at.value()) : "NULL") << ","
              << (message.opcode ? std::to_string(message.opcode.value()) : "NULL") << ","
              << (message.ihr_disabled ? TO_SQL_BOOL(message.ihr_disabled.value()) : "NULL") << ","
              << (message.bounce ? TO_SQL_BOOL(message.bounce.value()) : "NULL") << ","
              << (message.bounced ? TO_SQL_BOOL(message.bounced.value()) : "NULL") << ","
              << (message.import_fee ? std::to_string(message.import_fee.value()) : "NULL") << ","
              << "'" << message.body_hash << "',"
              << (message.init_state_hash ? "'" + message.init_state_hash.value() + "'" : "NULL")
              << ")";
      }
    }
    if (is_first) {
      return;
    }
    query << " ON CONFLICT DO NOTHING";

    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());

    query.str("");
    is_first = true;
    query << "INSERT INTO message_contents (hash, body) VALUES ";
    for (const auto& mc_block : mc_blocks_) {
      for (const auto& message : mc_block->messages_) {
        if (is_first) {
          is_first = false;
        } else {
          query << ", ";
        }
        query << "("
              << "'" << message.body_hash << "',"
              << "'" << message.body << "'"
              << ")";
        if (message.init_state_hash) {
          query << ", ("
                << "'" << message.init_state_hash.value() << "',"
                << "'" << message.init_state.value() << "'"
                << ")";
        }
      }
    }
    if (is_first) {
      return;
    }
    query << " ON CONFLICT DO NOTHING";

    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());
  }

  void insert_tx_msgs(pqxx::work &transaction) {
    std::ostringstream query;
    query << "INSERT INTO transaction_messages (transaction_hash, message_hash, direction) VALUES ";
    bool is_first = true;
    for (const auto& mc_block : mc_blocks_) {
      for (const auto& tx_msg : mc_block->transaction_messages_) {
        if (is_first) {
          is_first = false;
        } else {
          query << ", ";
        }
        query << "("
              << TO_SQL_STRING(tx_msg.transaction_hash) << ","
              << TO_SQL_STRING(tx_msg.message_hash) << ","
              << TO_SQL_STRING(tx_msg.direction)
              << ")";
      }
    }
    if (is_first) {
      return;
    }
    query << " ON CONFLICT DO NOTHING";

    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());
  }

  void insert_account_states(pqxx::work &transaction) {
    std::ostringstream query;
    query << "INSERT INTO account_states (hash, account, balance, account_status, frozen_hash, code_hash, data_hash) VALUES ";
    bool is_first = true;
    for (const auto& mc_block : mc_blocks_) {
      for (const auto& account_state : mc_block->account_states_) {
        if (is_first) {
          is_first = false;
        } else {
          query << ", ";
        }
        query << "("
              << TO_SQL_STRING(account_state.hash) << ","
              << TO_SQL_STRING(account_state.account) << ","
              << account_state.balance << ","
              << TO_SQL_STRING(account_state.account_status) << ","
              << TO_SQL_OPTIONAL_STRING(account_state.frozen_hash) << ","
              << TO_SQL_OPTIONAL_STRING(account_state.code_hash) << ","
              << TO_SQL_OPTIONAL_STRING(account_state.data_hash)
              << ")";
      }
    }
    if (is_first) {
      return;
    }
    query << " ON CONFLICT DO NOTHING";
    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());
  }

  void insert_jetton_transfers(pqxx::work &transaction) {
    std::ostringstream query;
    query << "INSERT INTO jetton_transfers (transaction_hash, query_id, amount, destination, response_destination, custom_payload, forward_ton_amount, forward_payload) VALUES ";
    bool is_first = true;
    for (const auto& mc_block : mc_blocks_) {
      for (const auto& transfer : mc_block->get_events<JettonTransfer>()) {
        if (is_first) {
          is_first = false;
        } else {
          query << ", ";
        }
        auto custom_payload_boc_r = convert::to_bytes(transfer.custom_payload);
        auto custom_payload_boc = custom_payload_boc_r.is_ok() ? custom_payload_boc_r.move_as_ok() : td::optional<std::string>{};

        auto forward_payload_boc_r = convert::to_bytes(transfer.forward_payload);
        auto forward_payload_boc = forward_payload_boc_r.is_ok() ? forward_payload_boc_r.move_as_ok() : td::optional<std::string>{};

        query << "("
              << "'" << transfer.transaction_hash << "',"
              << transfer.query_id << ","
              << (transfer.amount.not_null() ? transfer.amount->to_dec_string() : "NULL") << ","
              << "'" << transfer.destination << "',"
              << "'" << transfer.response_destination << "',"
              << TO_SQL_OPTIONAL_STRING(custom_payload_boc) << ","
              << (transfer.forward_ton_amount.not_null() ? transfer.forward_ton_amount->to_dec_string() : "NULL") << ","
              << TO_SQL_OPTIONAL_STRING(forward_payload_boc)
              << ")";
      }
    }
    if (is_first) {
      return;
    }
    query << " ON CONFLICT DO NOTHING";

    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());
  }

  void insert_jetton_burns(pqxx::work &transaction) {
    std::ostringstream query;
    query << "INSERT INTO jetton_burns (transaction_hash, query_id, amount, response_destination, custom_payload) VALUES ";
    bool is_first = true;
    for (const auto& mc_block : mc_blocks_) {
      for (const auto& burn : mc_block->get_events<JettonBurn>()) {
        if (is_first) {
          is_first = false;
        } else {
          query << ", ";
        }

        auto custom_payload_boc_r = convert::to_bytes(burn.custom_payload);
        auto custom_payload_boc = custom_payload_boc_r.is_ok() ? custom_payload_boc_r.move_as_ok() : td::optional<std::string>{};

        query << "("
              << "'" << burn.transaction_hash << "',"
              << burn.query_id << ","
              << (burn.amount.not_null() ? burn.amount->to_dec_string() : "NULL") << ","
              << "'" << burn.response_destination << "',"
              << TO_SQL_OPTIONAL_STRING(custom_payload_boc)
              << ")";
      }
    }
    if (is_first) {
      return;
    }
    query << " ON CONFLICT DO NOTHING";

    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());
  }

  void insert_nft_transfers(pqxx::work &transaction) {
    std::ostringstream query;
    query << "INSERT INTO nft_transfers (transaction_hash, query_id, nft_item, old_owner, new_owner, response_destination, custom_payload, forward_amount, forward_payload) VALUES ";
    bool is_first = true;
    for (const auto& mc_block : mc_blocks_) {
      for (const auto& transfer : mc_block->get_events<NFTTransfer>()) {
        if (is_first) {
          is_first = false;
        } else {
          query << ", ";
        }
        auto custom_payload_boc_r = convert::to_bytes(transfer.custom_payload);
        auto custom_payload_boc = custom_payload_boc_r.is_ok() ? custom_payload_boc_r.move_as_ok() : td::optional<std::string>{};

        auto forward_payload_boc_r = convert::to_bytes(transfer.forward_payload);
        auto forward_payload_boc = forward_payload_boc_r.is_ok() ? forward_payload_boc_r.move_as_ok() : td::optional<std::string>{};

        query << "("
              << "'" << transfer.transaction_hash << "',"
              << transfer.query_id << ","
              << "'" << transfer.nft_item << "',"
              << "'" << transfer.old_owner << "',"
              << "'" << transfer.new_owner << "',"
              << "'" << transfer.response_destination << "',"
              << TO_SQL_OPTIONAL_STRING(custom_payload_boc) << ","
              << (transfer.forward_amount.not_null() ? transfer.forward_amount->to_dec_string() : "NULL") << ","
              << TO_SQL_OPTIONAL_STRING(forward_payload_boc)
              << ")";
      }
    }
    if (is_first) {
      return;
    }
    query << " ON CONFLICT DO NOTHING";

    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());
  }


  void start_up() {
    try {
      pqxx::connection c(connection_string_);
      if (!c.is_open()) {
        promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, "Failed to open database"));
        stop();
        return;
      }
      pqxx::work txn(c);
      insert_blocks(txn);
      insert_transactions(txn);
      insert_messsages(txn);
      insert_tx_msgs(txn);
      insert_account_states(txn);
      insert_jetton_transfers(txn);
      insert_jetton_burns(txn);
      insert_nft_transfers(txn);
      txn.commit();
      promise_.set_value(td::Unit());
    } catch (const std::exception &e) {
      promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error inserting to PG: " << e.what()));
    }
    stop();
  }
};


class UpsertJettonWallet: public td::actor::Actor {
private:
  std::string connection_string_;
  JettonWalletData wallet_;
  td::Promise<td::Unit> promise_;
public:
  UpsertJettonWallet(std::string connection_string, JettonWalletData wallet, td::Promise<td::Unit> promise): 
    connection_string_(std::move(connection_string)), 
    wallet_(std::move(wallet)), 
    promise_(std::move(promise))
  {
    LOG(DEBUG) << "Created UpsertJettonWallet";
  }

  void start_up() {
    try {
      pqxx::connection c(connection_string_);
      if (!c.is_open()) {
        promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, "Failed to open database"));
        stop();
        return;
      }
      pqxx::work txn(c);
      
      std::string query = "INSERT INTO jetton_wallets (balance, address, owner, jetton, last_transaction_lt, code_hash, data_hash) "
                            "VALUES ($1, $2, $3, $4, $5, $6, $7) "
                            "ON CONFLICT (address) "
                            "DO UPDATE SET "
                            "balance = EXCLUDED.balance, "
                            "owner = EXCLUDED.owner, "
                            "jetton = EXCLUDED.jetton, "
                            "last_transaction_lt = EXCLUDED.last_transaction_lt, "
                            "code_hash = EXCLUDED.code_hash, "
                            "data_hash = EXCLUDED.data_hash "
                            "WHERE jetton_wallets.last_transaction_lt < EXCLUDED.last_transaction_lt;";

      txn.exec_params(query,
                      wallet_.balance,
                      wallet_.address,
                      wallet_.owner,
                      wallet_.jetton,
                      wallet_.last_transaction_lt,
                      td::base64_encode(wallet_.code_hash.as_slice()),
                      td::base64_encode(wallet_.data_hash.as_slice()));


      txn.commit();
      promise_.set_value(td::Unit());
    } catch (const std::exception &e) {
      promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error inserting to PG: " << e.what()));
    }
    stop();
  }
};


class GetJettonWallet : public td::actor::Actor {
private:
  std::string connection_string_;
  std::string address_;
  td::Promise<JettonWalletData> promise_;
public:
  GetJettonWallet(std::string connection_string, std::string address, td::Promise<JettonWalletData> promise)
    : connection_string_(std::move(connection_string))
    , address_(std::move(address))
    , promise_(std::move(promise))
  {
    LOG(DEBUG) << "Created GetJettonWallet";
  }

  void start_up() override {
    try {
      pqxx::connection c(connection_string_);
      if (!c.is_open()) {
        promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, "Failed to open database"));
        stop();
        return;
      }
      pqxx::work txn(c);

      std::string query = "SELECT balance, address, owner, jetton, last_transaction_lt, code_hash, data_hash "
                          "FROM jetton_wallets "
                          "WHERE address = $1;";

      pqxx::result result = txn.exec_params(query, address_);

      if (result.size() != 1) {
        if (result.size() == 0) {
          promise_.set_error(td::Status::Error(ErrorCode::NOT_FOUND_ERROR, PSLICE() << "Jetton Wallet for address " << address_ << " not found"));
        } else {
          promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Jetton Wallet for address " << address_ << " is not unique (found " << result.size() << " wallets)"));
        }
        stop();
        return;
      }

      const auto& row = result[0];
      
      JettonWalletData wallet;
      wallet.balance = row[0].as<uint64_t>();
      wallet.address = row[1].as<std::string>();
      wallet.owner = row[2].as<std::string>();
      wallet.jetton = row[3].as<std::string>();
      wallet.last_transaction_lt = row[4].as<uint64_t>();
      wallet.code_hash = vm::CellHash::from_slice(td::base64_decode(row[5].as<std::string>()).move_as_ok());
      wallet.data_hash = vm::CellHash::from_slice(td::base64_decode(row[6].as<std::string>()).move_as_ok());

      txn.commit();
      promise_.set_value(std::move(wallet));
    } catch (const std::exception &e) {
      promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error retrieving wallet from PG: " << e.what()));
    }
    stop();
  }
};

class UpsertJettonMaster : public td::actor::Actor {
private:
  std::string connection_string_;
  JettonMasterData master_data_;
  td::Promise<td::Unit> promise_;
public:
  UpsertJettonMaster(std::string connection_string, JettonMasterData master_data, td::Promise<td::Unit> promise)
    : connection_string_(std::move(connection_string))
    , master_data_(std::move(master_data))
    , promise_(std::move(promise))
  {
    LOG(DEBUG) << "Created UpsertJettonMaster";
  }

  td::optional<std::string> prepare_jetton_content(JettonMasterData &master_data) {
    if (!master_data.jetton_content) {
      return {};
    }
    td::JsonBuilder jetton_content_json;
    auto obj = jetton_content_json.enter_object();
    for (auto &attr : master_data.jetton_content.value()) {
      obj(attr.first, attr.second);
    }
    obj.leave();

    return jetton_content_json.string_builder().as_cslice().str();
  }

  void start_up() override {
    try {
      pqxx::connection c(connection_string_);
      if (!c.is_open()) {
        promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, "Failed to open database"));
        stop();
        return;
      }
      pqxx::work txn(c);

      std::string query = "INSERT INTO jetton_masters "
                          "(address, total_supply, mintable, admin_address, jetton_content, jetton_wallet_code_hash, data_hash, code_hash, last_transaction_lt, code_boc, data_boc) "
                          "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) "
                          "ON CONFLICT (address) "
                          "DO UPDATE SET "
                          "total_supply = EXCLUDED.total_supply, "
                          "mintable = EXCLUDED.mintable, "
                          "admin_address = EXCLUDED.admin_address, "
                          "jetton_content = EXCLUDED.jetton_content,"
                          "jetton_wallet_code_hash = EXCLUDED.jetton_wallet_code_hash, "
                          "data_hash = EXCLUDED.data_hash, "
                          "code_hash = EXCLUDED.code_hash, "
                          "last_transaction_lt = EXCLUDED.last_transaction_lt, "
                          "code_boc = EXCLUDED.code_boc, "
                          "data_boc = EXCLUDED.data_boc "
                          "WHERE jetton_masters.last_transaction_lt < EXCLUDED.last_transaction_lt;";

      auto jetton_content = prepare_jetton_content(master_data_);

      txn.exec_params(query,
                      master_data_.address,
                      master_data_.total_supply,
                      master_data_.mintable,
                      master_data_.admin_address ? master_data_.admin_address.value().c_str() : nullptr,
                      jetton_content ? jetton_content.value().c_str() : nullptr,
                      td::base64_encode(master_data_.jetton_wallet_code_hash.as_slice()),
                      td::base64_encode(master_data_.data_hash.as_slice()),
                      td::base64_encode(master_data_.code_hash.as_slice()),
                      master_data_.last_transaction_lt,
                      master_data_.code_boc,
                      master_data_.data_boc);

      txn.commit();
      promise_.set_value(td::Unit());
    } catch (const std::exception &e) {
      promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error inserting to PG: " << e.what()));
    }
    stop();
  }
};

class GetJettonMaster : public td::actor::Actor {
private:
  std::string connection_string_;
  std::string address_;
  td::Promise<JettonMasterData> promise_;
public:
  GetJettonMaster(std::string connection_string, std::string address, td::Promise<JettonMasterData> promise)
    : connection_string_(std::move(connection_string))
    , address_(std::move(address))
    , promise_(std::move(promise))
  {
    LOG(DEBUG) << "Created GetJettonMaster";
  }

  void start_up() override {
    try {
      pqxx::connection c(connection_string_);
      if (!c.is_open()) {
        promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, "Failed to open database"));
        stop();
        return;
      }
      pqxx::work txn(c);

      std::string query = "SELECT address, total_supply, mintable, admin_address, jetton_wallet_code_hash, data_hash, code_hash, last_transaction_lt, code_boc, data_boc "
                          "FROM jetton_masters "
                          "WHERE address = $1;";

      pqxx::result result = txn.exec_params(query, address_);

      if (result.size() != 1) {
        if (result.size() == 0) {
          promise_.set_error(td::Status::Error(ErrorCode::NOT_FOUND_ERROR, PSLICE() << "Jetton master not found: " << address_));
        } else {
          promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Jetton master not unique: " << address_ << ", found " << result.size() << " records"));
        }
        
        stop();
        return;
      }

      pqxx::row row = result[0];

      JettonMasterData master_data;
      master_data.address = row[0].as<std::string>();
      master_data.total_supply = row[1].as<uint64_t>();
      master_data.mintable = row[2].as<bool>();
      if (!row[3].is_null()) {
        master_data.admin_address = row[3].as<std::string>();
      }
      master_data.jetton_wallet_code_hash = vm::CellHash::from_slice(td::base64_decode(row[4].as<std::string>()).move_as_ok());
      master_data.data_hash = vm::CellHash::from_slice(td::base64_decode(row[5].as<std::string>()).move_as_ok());
      master_data.code_hash = vm::CellHash::from_slice(td::base64_decode(row[6].as<std::string>()).move_as_ok());
      master_data.last_transaction_lt = row[7].as<uint64_t>();
      master_data.code_boc = row[8].as<std::string>();
      master_data.data_boc = row[9].as<std::string>();

      txn.commit();
      promise_.set_value(std::move(master_data));
    } catch (const std::exception &e) {
      promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error retrieving master from PG: " << e.what()));
    }
    stop();
  }
};

class UpsertNFTCollection: public td::actor::Actor {
private:
  std::string connection_string_;
  NFTCollectionData collection_;
  td::Promise<td::Unit> promise_;

public:
  UpsertNFTCollection(std::string connection_string, NFTCollectionData collection, td::Promise<td::Unit> promise)
    : connection_string_(std::move(connection_string))
    , collection_(std::move(collection))
    , promise_(std::move(promise))
  {
    LOG(DEBUG) << "Created UpsertNFTCollection";
  }

  td::optional<std::string> prepare_collection_content(NFTCollectionData &collection) {
    if (!collection.collection_content) {
      return {};
    }
    td::JsonBuilder collection_content_json;
    auto obj = collection_content_json.enter_object();
    for (auto &attr : collection.collection_content.value()) {
      obj(attr.first, attr.second);
    }
    obj.leave();

    return collection_content_json.string_builder().as_cslice().str();
  }

  void start_up() override {
    try {
      pqxx::connection c(connection_string_);
      if (!c.is_open()) {
        promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, "Failed to open database"));
        stop();
        return;
      }
      pqxx::work txn(c);

      std::string query = "INSERT INTO nft_collections "
                          "(address, next_item_index, owner_address, collection_content, data_hash, code_hash, last_transaction_lt, code_boc, data_boc) "
                          "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) "
                          "ON CONFLICT (address) "
                          "DO UPDATE SET "
                          "next_item_index = EXCLUDED.next_item_index, "
                          "owner_address = EXCLUDED.owner_address, "
                          "collection_content = EXCLUDED.collection_content, "
                          "data_hash = EXCLUDED.data_hash, "
                          "code_hash = EXCLUDED.code_hash, "
                          "last_transaction_lt = EXCLUDED.last_transaction_lt, "
                          "code_boc = EXCLUDED.code_boc, "
                          "data_boc = EXCLUDED.data_boc "
                          "WHERE nft_collections.last_transaction_lt < EXCLUDED.last_transaction_lt;";

      auto collection_content = prepare_collection_content(collection_);

      txn.exec_params(query,
                      collection_.address,
                      collection_.next_item_index->to_dec_string(),
                      collection_.owner_address ? collection_.owner_address.value().c_str() : nullptr,
                      collection_content ? collection_content.value().c_str() : nullptr,
                      td::base64_encode(collection_.data_hash.as_slice()),
                      td::base64_encode(collection_.code_hash.as_slice()),
                      collection_.last_transaction_lt,
                      collection_.code_boc,
                      collection_.data_boc);

      txn.commit();
      promise_.set_value(td::Unit());
    } catch (const std::exception &e) {
      promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error inserting to PG: " << e.what()));
    }
    stop();
  }
};

class GetNFTCollection : public td::actor::Actor {
private:
  std::string connection_string_;
  std::string address_;
  td::Promise<NFTCollectionData> promise_;
public:
  GetNFTCollection(std::string connection_string, std::string address, td::Promise<NFTCollectionData> promise)
    : connection_string_(std::move(connection_string))
    , address_(std::move(address))
    , promise_(std::move(promise))
  {
    LOG(DEBUG) << "Created GetNFTCollection";
  }

  void start_up() override {
    try {
      pqxx::connection c(connection_string_);
      if (!c.is_open()) {
        promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, "Failed to open database"));
        stop();
        return;
      }
      pqxx::work txn(c);

      std::string query = "SELECT address, next_item_index, owner_address, collection_content, data_hash, code_hash, last_transaction_lt, code_boc, data_boc "
                          "FROM nft_collections "
                          "WHERE address = $1;";

      pqxx::result result = txn.exec_params(query, address_);

      if (result.size() != 1) {
        if (result.size() == 0) {
          promise_.set_error(td::Status::Error(ErrorCode::NOT_FOUND_ERROR, PSLICE() << "NFT Collection not found: " << address_));
        } else {
          promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "NFT Collection not unique: " << address_ << ", found " << result.size() << " records"));
        }
        
        stop();
        return;
      }

      pqxx::row row = result[0];

      NFTCollectionData collection_data;
      collection_data.address = row[0].as<std::string>();
      collection_data.next_item_index = td::dec_string_to_int256(row[1].as<std::string>());
      if (!row[2].is_null()) {
        collection_data.owner_address = row[2].as<std::string>();
      }
      if (!row[3].is_null()) {
        // TODO: Parse the JSON string into a map
      }
      collection_data.data_hash = vm::CellHash::from_slice(td::base64_decode(row[4].as<std::string>()).move_as_ok());
      collection_data.code_hash = vm::CellHash::from_slice(td::base64_decode(row[5].as<std::string>()).move_as_ok());
      collection_data.last_transaction_lt = row[6].as<uint64_t>();
      collection_data.code_boc = row[7].as<std::string>();
      collection_data.data_boc = row[8].as<std::string>();

      txn.commit();
      promise_.set_value(std::move(collection_data));
    } catch (const std::exception &e) {
      promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error retrieving collection from PG: " << e.what()));
    }
    stop();
  }
};

class UpsertNFTItem : public td::actor::Actor {
private:
  std::string connection_string_;
  NFTItemData item_data_;
  td::Promise<td::Unit> promise_;

public:
  UpsertNFTItem(std::string connection_string, NFTItemData item_data, td::Promise<td::Unit> promise)
    : connection_string_(std::move(connection_string))
    , item_data_(std::move(item_data))
    , promise_(std::move(promise))
  {
    LOG(DEBUG) << "Created UpsertNFTItem";
  }

  void start_up() override {
    try {
      pqxx::connection c(connection_string_);
      if (!c.is_open()) {
        promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, "Failed to open database"));
        stop();
        return;
      }
      pqxx::work txn(c);

      std::string query = "INSERT INTO nft_items (address, init, index, collection_address, owner_address, content, last_transaction_lt, code_hash, data_hash) "
                          "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) "
                          "ON CONFLICT (address) DO UPDATE "
                          "SET init = $2, index = $3, collection_address = $4, owner_address = $5, content = $6, last_transaction_lt = $7, code_hash = $8, data_hash = $9;";

      txn.exec_params(query,
                      item_data_.address,
                      item_data_.init,
                      item_data_.index->to_dec_string(),
                      item_data_.collection_address,
                      item_data_.owner_address,
                      item_data_.content ? content_to_json_string(item_data_.content.value()).c_str() : nullptr,
                      item_data_.last_transaction_lt,
                      td::base64_encode(item_data_.code_hash.as_slice().str()),
                      td::base64_encode(item_data_.data_hash.as_slice().str()));
      txn.commit();
      promise_.set_value(td::Unit());
    } catch (const std::exception &e) {
      promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error inserting/updating NFT item in PG: " << e.what()));
    }
    stop();
  }
};

class GetNFTItem : public td::actor::Actor {
private:
  std::string connection_string_;
  std::string address_;
  td::Promise<NFTItemData> promise_;

public:
  GetNFTItem(std::string connection_string, std::string address, td::Promise<NFTItemData> promise)
    : connection_string_(std::move(connection_string))
    , address_(std::move(address))
    , promise_(std::move(promise))
  {
    LOG(DEBUG) << "Created GetNFTItem";
  }

  void start_up() override {
    try {
      pqxx::connection c(connection_string_);
      if (!c.is_open()) {
        promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, "Failed to open database"));
        stop();
        return;
      }
      pqxx::work txn(c);

      std::string query = "SELECT address, init, index, collection_address, owner_address, content, last_transaction_lt, code_hash, data_hash "
                          "FROM nft_items "
                          "WHERE address = $1;";

      pqxx::result result = txn.exec_params(query, address_);

      if (result.size() != 1) {
        if (result.empty()) {
          promise_.set_error(td::Status::Error(ErrorCode::NOT_FOUND_ERROR, "NFT item not found"));
        } else {
          promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, "Multiple NFT items found with same address"));
        }
        stop();
        return;
      }

      NFTItemData item_data;
      const pqxx::row &row = result[0];
      item_data.address = row[0].as<std::string>();
      item_data.init = row[1].as<bool>();
      item_data.index = td::dec_string_to_int256(row[2].as<std::string>());
      if (!row[3].is_null()) {
        item_data.collection_address = row[3].as<std::string>();
      }
      item_data.owner_address = row[4].as<std::string>();
      // item_data.content = row[5].as<std::string>(); TODO: parse JSON string to std::map<std::string, std::string>
      item_data.last_transaction_lt = row[6].as<uint64_t>();
      item_data.code_hash = vm::CellHash::from_slice(td::base64_decode(row[7].as<std::string>()).move_as_ok());
      item_data.data_hash = vm::CellHash::from_slice(td::base64_decode(row[8].as<std::string>()).move_as_ok());

      promise_.set_value(std::move(item_data));
    } catch (const std::exception &e) {
      promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error retrieving item from PG: " << e.what()));
    }
    stop();
  }
};

InsertManagerPostgres::InsertManagerPostgres(): 
    inserted_count_(0),
    start_time_(std::chrono::high_resolution_clock::now()) 
{}

void InsertManagerPostgres::start_up() {
  alarm_timestamp() = td::Timestamp::in(1.0);
}

void InsertManagerPostgres::report_statistics() {
  auto now_time_ = std::chrono::high_resolution_clock::now();
  auto last_report_seconds_ = std::chrono::duration_cast<std::chrono::seconds>(now_time_ - last_verbose_time_);
  if (last_report_seconds_.count() > 10) {
    last_verbose_time_ = now_time_;

    auto total_seconds_ = std::chrono::duration_cast<std::chrono::seconds>(now_time_ - start_time_);
    auto tasks_per_second = double(inserted_count_) / total_seconds_.count();

    LOG(INFO) << "Total: " << inserted_count_ 
              << " Time: " << total_seconds_.count() 
              << " (TPS: " << tasks_per_second << ")"
              << " Queued: " << insert_queue_.size();
  }
}

void InsertManagerPostgres::alarm() {
  report_statistics();

  LOG(DEBUG) << "insert queue size: " << insert_queue_.size();

  std::vector<td::Promise<td::Unit>> promises;
  std::vector<ParsedBlockPtr> schema_blocks;
  while (!insert_queue_.empty() && schema_blocks.size() < batch_size) {
    auto schema_block = std::move(insert_queue_.front());
    insert_queue_.pop();

    auto promise = std::move(promise_queue_.front());
    promise_queue_.pop();

    promises.push_back(std::move(promise));
    schema_blocks.push_back(std::move(schema_block));
    if(inserted_count_ == 0) {
      start_time_ = std::chrono::high_resolution_clock::now();
    }
    ++inserted_count_;  // FIXME: increasing before insertion. Move this line in promise P
  }
  if (!schema_blocks.empty()) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), promises = std::move(promises)](td::Result<td::Unit> R) mutable {
      if (R.is_error()) {
        LOG(ERROR) << "Error inserting to PG: " << R.move_as_error();
        for (auto& p : promises) {
          p.set_error(R.move_as_error());
        }
        return;
      }
      
      for (auto& p : promises) {
        p.set_result(td::Unit());
      }
    });
    td::actor::create_actor<InsertBatchMcSeqnos>("insertbatchmcseqnos", credential.getConnectionString(), std::move(schema_blocks), std::move(P)).release();
  }

  if (!insert_queue_.empty()) {
    alarm_timestamp() = td::Timestamp::in(0.001);
  } else {
    alarm_timestamp() = td::Timestamp::in(1.0);
  }
}

void InsertManagerPostgres::insert(ParsedBlockPtr block_ds, td::Promise<td::Unit> promise) {
  insert_queue_.push(std::move(block_ds));
  promise_queue_.push(std::move(promise));
}

void InsertManagerPostgres::upsert_jetton_wallet(JettonWalletData jetton_wallet, td::Promise<td::Unit> promise) {
  td::actor::create_actor<UpsertJettonWallet>("upsertjettonwallet", credential.getConnectionString(), std::move(jetton_wallet), std::move(promise)).release();
}

void InsertManagerPostgres::get_jetton_wallet(std::string address, td::Promise<JettonWalletData> promise) {
  td::actor::create_actor<GetJettonWallet>("getjettonwallet", credential.getConnectionString(), std::move(address), std::move(promise)).release();
}

void InsertManagerPostgres::upsert_jetton_master(JettonMasterData jetton_wallet, td::Promise<td::Unit> promise) {
  td::actor::create_actor<UpsertJettonMaster>("upsertjettonmaster", credential.getConnectionString(), std::move(jetton_wallet), std::move(promise)).release();
}

void InsertManagerPostgres::get_jetton_master(std::string address, td::Promise<JettonMasterData> promise) {
  td::actor::create_actor<GetJettonMaster>("getjettonmaster", credential.getConnectionString(), std::move(address), std::move(promise)).release();
}

void InsertManagerPostgres::upsert_nft_collection(NFTCollectionData nft_collection, td::Promise<td::Unit> promise) {
  td::actor::create_actor<UpsertNFTCollection>("upsertnftcollection", credential.getConnectionString(), std::move(nft_collection), std::move(promise)).release();
}

void InsertManagerPostgres::get_nft_collection(std::string address, td::Promise<NFTCollectionData> promise) {
  td::actor::create_actor<GetNFTCollection>("getnftcollection", credential.getConnectionString(), std::move(address), std::move(promise)).release();
}

void InsertManagerPostgres::upsert_nft_item(NFTItemData nft_item, td::Promise<td::Unit> promise) {
  td::actor::create_actor<UpsertNFTItem>("upsertnftitem", credential.getConnectionString(), std::move(nft_item), std::move(promise)).release();
}

void InsertManagerPostgres::get_nft_item(std::string address, td::Promise<NFTItemData> promise) {
  td::actor::create_actor<GetNFTItem>("getnftitem", credential.getConnectionString(), std::move(address), std::move(promise)).release();
}

std::string InsertManagerPostgres::PostgresCredential::getConnectionString()  {
  return (
    "hostaddr=" + host +
    " port=" + std::to_string(port) + 
    (user.length() ? " user=" + user : "") +
    (password.length() ? " password=" + password : "") +
    (dbname.length() ? " dbname=" + dbname : "")
  );
}