#include "InsertManagerPostgres.h"
#include "BlockToSchema.hpp"
#include <pqxx/pqxx>
#include <chrono>

#define TO_SQL_BOOL(x) ((x) ? "TRUE" : "FALSE")
#define TO_SQL_STRING(x) ("'" + (x) + "'")
#define TO_SQL_OPTIONAL(x) ((x) ? std::to_string(x.value()) : "NULL")
#define TO_SQL_OPTIONAL_STRING(x) ((x) ? ("'" + x.value() + "'") : "NULL")


class InsertBatchMcSeqnos: public td::actor::Actor {
private:
  std::string connection_string_;
  std::vector<schema::BlockToSchema> mc_blocks_;
  td::Promise<td::Unit> promise_;
public:
  InsertBatchMcSeqnos(std::string connection_string, std::vector<schema::BlockToSchema> mc_blocks, td::Promise<td::Unit> promise): 
    connection_string_(std::move(connection_string)), 
    mc_blocks_(std::move(mc_blocks)), 
    promise_(std::move(promise))
  {}

  void insert_blocks(pqxx::work &transaction) {
    std::ostringstream query;
    query << "INSERT INTO blocks (workchain, shard, seqno, root_hash, file_hash, mc_block_workchain, "
                                  "mc_block_shard, mc_block_seqno, global_id, version, after_merge, before_split, "
                                  "after_split, want_split, key_block, vert_seqno_incr, flags, gen_utime, start_lt, "
                                  "end_lt, validator_list_hash_short, gen_catchain_seqno, min_ref_mc_seqno, "
                                  "prev_key_block_seqno, vert_seqno, master_ref_seqno, rand_seed, created_by) VALUES ";

    bool is_first = true;
    for (const auto& mc_block : mc_blocks_) {
      for (const auto& block : mc_block.get_blocks()) {
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
      for (const auto& transaction : mc_block.get_transactions()) {
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
      for (const auto& message : mc_block.get_messages()) {
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
    query << " ON CONFLICT DO NOTHING";

    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());

    query.str("");
    is_first = true;
    query << "INSERT INTO message_contents (hash, body) VALUES ";
    for (const auto& mc_block : mc_blocks_) {
      for (const auto& message : mc_block.get_messages()) {
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
    query << " ON CONFLICT DO NOTHING";

    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());
  }

  void insert_tx_msgs(pqxx::work &transaction) {
    std::ostringstream query;
    query << "INSERT INTO transaction_messages (transaction_hash, message_hash, direction) VALUES ";
    bool is_first = true;
    for (const auto& mc_block : mc_blocks_) {
      for (const auto& tx_msg : mc_block.get_transaction_messages()) {
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
    query << " ON CONFLICT DO NOTHING";

    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());
  }

  void insert_account_states(pqxx::work &transaction) {
    std::ostringstream query;
    query << "INSERT INTO account_states (hash, account, balance, account_status, frozen_hash, code_hash, data_hash) VALUES ";
    bool is_first = true;
    for (const auto& mc_block : mc_blocks_) {
      for (const auto& account_state : mc_block.get_account_states()) {
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
    query << " ON CONFLICT DO NOTHING";
    // LOG(DEBUG) << "Running SQL query: " << query.str();
    transaction.exec0(query.str());
  }

  void start_up() {
    try {
      pqxx::connection c(connection_string_);
      if (!c.is_open()) {
        promise_.set_error(td::Status::Error("Failed to open database"));
        stop();
        return;
      }
      pqxx::work txn(c);
      insert_blocks(txn);
      insert_transactions(txn);
      insert_messsages(txn);
      insert_tx_msgs(txn);
      insert_account_states(txn);
      txn.commit();
      promise_.set_value(td::Unit());
      stop();
    } catch (const std::exception &e) {
      promise_.set_error(td::Status::Error(PSLICE() << "Error inserting to PG: " << e.what()));
      stop();
    }
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
  if (last_report_seconds_.count() > 3) {
    last_verbose_time_ = now_time_;

    auto total_seconds_ = std::chrono::duration_cast<std::chrono::seconds>(now_time_ - start_time_);
    auto tasks_per_second = double(inserted_count_) / total_seconds_.count();

    LOG(INFO) << "Queue size: " << insert_queue_.size() << " Tasks per second: " << tasks_per_second;
  }
}

void InsertManagerPostgres::alarm() {
  report_statistics();

  LOG(DEBUG) << "insert queue size: " << insert_queue_.size();

  std::vector<td::Promise<td::Unit>> promises;
  std::vector<schema::BlockToSchema> schema_blocks;
  while (!insert_queue_.empty() && schema_blocks.size() < 2048) {
    auto block_ds = insert_queue_.front();
    insert_queue_.pop();

    auto promise = std::move(promise_queue_.front());
    promise_queue_.pop();

    auto schema_block = schema::BlockToSchema(std::move(block_ds));
    auto parse_res = schema_block.parse();

    if (parse_res.is_error()) {
      LOG(ERROR) << "Error parsing block: " << parse_res.error();
      promise.set_error(parse_res.move_as_error_prefix("Error parsing block: "));
      continue;
    }
    promises.push_back(std::move(promise));
    schema_blocks.push_back(std::move(schema_block));

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
    LOG(ERROR) << credential.getConnectionString();
    td::actor::create_actor<InsertBatchMcSeqnos>("insertbatchmcseqnos", credential.getConnectionString(), std::move(schema_blocks), std::move(P)).release();
  }

  if (!insert_queue_.empty()) {
    alarm_timestamp() = td::Timestamp::in(0.1);
  } else {
    alarm_timestamp() = td::Timestamp::in(1.0);
  }
}

void InsertManagerPostgres::insert(std::vector<BlockDataState> block_ds, td::Promise<td::Unit> promise) {
  insert_queue_.push(std::move(block_ds));
  promise_queue_.push(std::move(promise));
}