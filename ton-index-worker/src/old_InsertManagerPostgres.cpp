#include <chrono>
#include <mutex>
#include "td/utils/JsonBuilder.h"
#include "InsertManagerPostgres.h"
#include "convert-utils.h"

#define TO_SQL_BOOL(x) ((x) ? "TRUE" : "FALSE")
#define TO_SQL_STRING(x) ("'" + (x) + "'")
#define TO_SQL_OPTIONAL(x) ((x) ? std::to_string(x.value()) : "NULL")
#define TO_SQL_OPTIONAL_STRING(x) ((x) ? ("'" + x.value() + "'") : "NULL")


//
// utils
//
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


// BitHasher
struct BitArrayHasher {
  std::size_t operator()(const td::Bits256& k) const {
    std::size_t seed = 0;
    for(const auto& el : k.as_array()) {
        seed ^= std::hash<td::uint8>{}(el) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }
};

// This set is used as a synchronization mechanism to prevent multiple queries for the same message
// Otherwise Posgres will throw an error deadlock_detected
std::unordered_set<td::Bits256, BitArrayHasher> messages_in_progress;
std::unordered_set<td::Bits256, BitArrayHasher> msg_bodies_in_progress;
std::mutex messages_in_progress_mutex;


//
// InsertBatchPostgres
//
void InsertBatchPostgres::start_up() {
  std::vector<schema::Message> messages;
  std::vector<TxMsg> tx_msgs;
  std::vector<MsgBody> msg_bodies;
  {
    std::lock_guard<std::mutex> guard(messages_in_progress_mutex);
    for (const auto& mc_block : mc_blocks_) {
      for (const auto& blk : mc_block->blocks_) {
        for (const auto& transaction : blk.transactions) {
          if (transaction.in_msg.has_value()) {
            auto &msg = transaction.in_msg.value();
            if (messages_in_progress.find(msg.hash) == messages_in_progress.end()) {
              messages.push_back(msg);
              messages_in_progress.insert(msg.hash);
            }
            td::Bits256 body_hash = msg.body->get_hash().bits();
            if (msg_bodies_in_progress.find(body_hash) == msg_bodies_in_progress.end()) {
              msg_bodies.push_back({transaction.tenant_id, td::base64_encode(body_hash.as_slice()), msg.body_boc});
              msg_bodies_in_progress.insert(body_hash);
            }
            if (msg.init_state_boc) {
              td::Bits256 init_state_hash = msg.init_state->get_hash().bits();
              if (msg_bodies_in_progress.find(init_state_hash) == msg_bodies_in_progress.end()) {
                msg_bodies.push_back({transaction.tenant_id, td::base64_encode(init_state_hash.as_slice()), msg.init_state_boc.value()});
                msg_bodies_in_progress.insert(init_state_hash);
              }
            }
            tx_msgs.push_back({transaction.tenant_id, td::base64_encode(transaction.hash.as_slice()), td::base64_encode(transaction.in_msg.value().hash.as_slice()), "in"});
          }
          for (const auto& msg : transaction.out_msgs) {
            if (messages_in_progress.find(msg.hash) == messages_in_progress.end()) {
              messages.push_back(msg);
              messages_in_progress.insert(msg.hash);
            }
            td::Bits256 body_hash = msg.body->get_hash().bits();
            if (msg_bodies_in_progress.find(body_hash) == msg_bodies_in_progress.end()) {
              msg_bodies.push_back({transaction.tenant_id, td::base64_encode(body_hash.as_slice()), msg.body_boc});
              msg_bodies_in_progress.insert(body_hash);
            }
            if (msg.init_state_boc) {
              td::Bits256 init_state_hash = msg.init_state->get_hash().bits();
              if (msg_bodies_in_progress.find(init_state_hash) == msg_bodies_in_progress.end()) {
                msg_bodies.push_back({transaction.tenant_id, td::base64_encode(init_state_hash.as_slice()), msg.init_state_boc.value()});
                msg_bodies_in_progress.insert(init_state_hash);
              }
            }
            tx_msgs.push_back({transaction.tenant_id, td::base64_encode(transaction.hash.as_slice()), td::base64_encode(msg.hash.as_slice()), "out"});
          }
        }
      }
    }
  }

  try {
    pqxx::connection c(connection_string_);
    if (!c.is_open()) {
      promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, "Failed to open database"));
      return;
    }

    pqxx::work txn(c);
    insert_blocks(txn, mc_blocks_);
    insert_transactions(txn, mc_blocks_);
    insert_messsages(txn, messages, msg_bodies, tx_msgs);
    insert_account_states(txn, mc_blocks_);
    insert_jetton_transfers(txn, mc_blocks_);
    insert_jetton_burns(txn, mc_blocks_);
    insert_nft_transfers(txn, mc_blocks_);
    txn.commit();

    LOG(WARNING) << "Inserted " 
          << mc_blocks_.size() << " mc blocks, "
          << blocks_count_ << " blocks, " 
          << transactions_count_ << " txs, " 
          << messages_count_ << " msgs";

    promise_.set_value(td::Unit());
  } catch (const std::exception &e) {
    promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error inserting to PG: " << e.what()));
  }

  {
    std::lock_guard<std::mutex> guard(messages_in_progress_mutex);
    for (const auto& msg : messages) {
      messages_in_progress.erase(msg.hash);
    }
  }
  stop();
}



std::string InsertBatchPostgres::stringify(schema::ComputeSkipReason compute_skip_reason) {
  switch (compute_skip_reason) {
      case schema::ComputeSkipReason::cskip_no_state: return "no_state";
      case schema::ComputeSkipReason::cskip_bad_state: return "bad_state";
      case schema::ComputeSkipReason::cskip_no_gas: return "no_gas";
      case schema::ComputeSkipReason::cskip_suspended: return "suspended";
  };
  UNREACHABLE();
}


std::string InsertBatchPostgres::stringify(schema::AccStatusChange acc_status_change) {
  switch (acc_status_change) {
      case schema::AccStatusChange::acst_unchanged: return "unchanged";
      case schema::AccStatusChange::acst_frozen: return "frozen";
      case schema::AccStatusChange::acst_deleted: return "deleted";
  };
  UNREACHABLE();
}


std::string InsertBatchPostgres::stringify(schema::AccountStatus account_status)
{
  switch (account_status) {
      case schema::AccountStatus::frozen: return "frozen";
      case schema::AccountStatus::uninit: return "uninit";
      case schema::AccountStatus::active: return "active";
      case schema::AccountStatus::nonexist: return "nonexist";
  };
  UNREACHABLE();
}


std::string InsertBatchPostgres::jsonify(const schema::SplitMergeInfo& info) {
  auto jb = td::JsonBuilder();
  auto c = jb.enter_object();
  c("cur_shard_pfx_len", static_cast<int>(info.cur_shard_pfx_len));
  c("acc_split_depth", static_cast<int>(info.acc_split_depth));
  c("this_addr", info.this_addr.to_hex());
  c("sibling_addr", info.sibling_addr.to_hex());
  c.leave();
  return jb.string_builder().as_cslice().str();
}


std::string InsertBatchPostgres::jsonify(const schema::StorageUsedShort& s) {
  auto jb = td::JsonBuilder();
  auto c = jb.enter_object();
  c("cells", std::to_string(s.cells));
  c("bits", std::to_string(s.bits));
  c.leave();
  return jb.string_builder().as_cslice().str();
}


std::string InsertBatchPostgres::jsonify(const schema::TrStoragePhase& s) {
  auto jb = td::JsonBuilder();
  auto c = jb.enter_object();
  c("storage_fees_collected", std::to_string(s.storage_fees_collected));
  if (s.storage_fees_due) {
    c("storage_fees_due", std::to_string(*(s.storage_fees_due)));
  }
  c("status_change", stringify(s.status_change));
  c.leave();
  return jb.string_builder().as_cslice().str();
}


std::string InsertBatchPostgres::jsonify(const schema::TrCreditPhase& c) {
  auto jb = td::JsonBuilder();
  auto cc = jb.enter_object();
  cc("due_fees_collected", std::to_string(c.due_fees_collected));
  cc("credit", std::to_string(c.credit));
  cc.leave();
  return jb.string_builder().as_cslice().str();
}


std::string InsertBatchPostgres::jsonify(const schema::TrActionPhase& action) {
  auto jb = td::JsonBuilder();
  auto c = jb.enter_object();
  c("success", td::JsonBool(action.success));
  c("valid", td::JsonBool(action.valid));
  c("no_funds", td::JsonBool(action.no_funds));
  c("status_change", stringify(action.status_change));
  if (action.total_fwd_fees) {
    c("total_fwd_fees", std::to_string(*(action.total_fwd_fees)));
  }
  if (action.total_action_fees) {
    c("total_action_fees", std::to_string(*(action.total_action_fees)));
  }
  c("result_code", action.result_code);
  if (action.result_arg) {
    c("result_arg", *(action.result_arg));
  }
  c("tot_actions", action.tot_actions);
  c("spec_actions", action.spec_actions);
  c("skipped_actions", action.skipped_actions);
  c("msgs_created", action.msgs_created);
  c("action_list_hash", td::base64_encode(action.action_list_hash.as_slice()));
  c("tot_msg_size", td::JsonRaw(jsonify(action.tot_msg_size)));
  c.leave();
  return jb.string_builder().as_cslice().str();
}


std::string InsertBatchPostgres::jsonify(const schema::TrBouncePhase& bounce) {
  auto jb = td::JsonBuilder();
  auto c = jb.enter_object();
  if (std::holds_alternative<schema::TrBouncePhase_negfunds>(bounce)) {
    c("type", "negfunds");
  } else if (std::holds_alternative<schema::TrBouncePhase_nofunds>(bounce)) {
    const auto& nofunds = std::get<schema::TrBouncePhase_nofunds>(bounce);
    c("type", "nofunds");
    c("msg_size", td::JsonRaw(jsonify(nofunds.msg_size)));
    c("req_fwd_fees", std::to_string(nofunds.req_fwd_fees));
  } else if (std::holds_alternative<schema::TrBouncePhase_ok>(bounce)) {
    const auto& ok = std::get<schema::TrBouncePhase_ok>(bounce);
    c("type", "ok");
    c("msg_size", td::JsonRaw(jsonify(ok.msg_size)));
    c("msg_fees", std::to_string(ok.msg_fees));
    c("fwd_fees", std::to_string(ok.fwd_fees));
  }
  c.leave();
  return jb.string_builder().as_cslice().str();
}


std::string InsertBatchPostgres::jsonify(const schema::TrComputePhase& compute) {
  auto jb = td::JsonBuilder();
  auto c = jb.enter_object();
  if (std::holds_alternative<schema::TrComputePhase_skipped>(compute)) {
    c("type", "skipped");
    c("skip_reason", stringify(std::get<schema::TrComputePhase_skipped>(compute).reason));
  } else if (std::holds_alternative<schema::TrComputePhase_vm>(compute)) {
    c("type", "vm");
    auto& computed = std::get<schema::TrComputePhase_vm>(compute);
    c("success", td::JsonBool(computed.success));
    c("msg_state_used", td::JsonBool(computed.msg_state_used));
    c("account_activated", td::JsonBool(computed.account_activated));
    c("gas_fees", std::to_string(computed.gas_fees));
    c("gas_used",std::to_string(computed.gas_used));
    c("gas_limit", std::to_string(computed.gas_limit));
    if (computed.gas_credit) {
      c("gas_credit", std::to_string(*(computed.gas_credit)));
    }
    c("mode", computed.mode);
    c("exit_code", computed.exit_code);
    if (computed.exit_arg) {
      c("exit_arg", *(computed.exit_arg));
    }
    c("vm_steps", static_cast<int64_t>(computed.vm_steps));
    c("vm_init_state_hash", td::base64_encode(computed.vm_init_state_hash.as_slice()));
    c("vm_final_state_hash", td::base64_encode(computed.vm_final_state_hash.as_slice()));
  }
  c.leave();
  return jb.string_builder().as_cslice().str();
}


std::string InsertBatchPostgres::jsonify(schema::TransactionDescr descr) {
  char tmp[10000]; // Adjust the size if needed
  td::StringBuilder sb(td::MutableSlice{tmp, sizeof(tmp)});
  td::JsonBuilder jb(std::move(sb));

  auto obj = jb.enter_object();
  if (std::holds_alternative<schema::TransactionDescr_ord>(descr)) {
    const auto& ord = std::get<schema::TransactionDescr_ord>(descr);
    obj("type", "ord");
    obj("credit_first", td::JsonBool(ord.credit_first));
    obj("storage_ph", td::JsonRaw(jsonify(ord.storage_ph)));
    obj("credit_ph", td::JsonRaw(jsonify(ord.credit_ph)));
    obj("compute_ph", td::JsonRaw(jsonify(ord.compute_ph)));
    if (ord.action.has_value()) {
      obj("action", td::JsonRaw(jsonify(ord.action.value())));
    }
    obj("aborted", td::JsonBool(ord.aborted));
    obj("bounce", td::JsonRaw(jsonify(ord.bounce)));
    obj("destroyed", td::JsonBool(ord.destroyed));
    obj.leave();
  }
  else if (std::holds_alternative<schema::TransactionDescr_storage>(descr)) {
    const auto& storage = std::get<schema::TransactionDescr_storage>(descr);
    obj("type", "storage");
    obj("storage_ph", td::JsonRaw(jsonify(storage.storage_ph)));
    obj.leave();
  }
  else if (std::holds_alternative<schema::TransactionDescr_tick_tock>(descr)) {
    const auto& tt = std::get<schema::TransactionDescr_tick_tock>(descr);
    obj("type", "tick_tock");
    obj("is_tock", td::JsonBool(tt.is_tock));
    obj("storage_ph", td::JsonRaw(jsonify(tt.storage_ph)));
    obj("compute_ph", td::JsonRaw(jsonify(tt.compute_ph)));
    if (tt.action.has_value()) {
      obj("action", td::JsonRaw(jsonify(tt.action.value())));
    }
    obj("aborted", td::JsonBool(tt.aborted));
    obj("destroyed", td::JsonBool(tt.destroyed));
    obj.leave();
  }
  else if (std::holds_alternative<schema::TransactionDescr_split_prepare>(descr)) {
    const auto& split = std::get<schema::TransactionDescr_split_prepare>(descr);
    obj("type", "split_prepare");
    obj("split_info", td::JsonRaw(jsonify(split.split_info)));
    if (split.storage_ph.has_value()) {
      obj("storage_ph", td::JsonRaw(jsonify(split.storage_ph.value())));
    }
    obj("compute_ph", td::JsonRaw(jsonify(split.compute_ph)));
    if (split.action.has_value()) {
      obj("action", td::JsonRaw(jsonify(split.action.value())));
    }
    obj("aborted", td::JsonBool(split.aborted));
    obj("destroyed", td::JsonBool(split.destroyed));
    obj.leave();
  }
  else if (std::holds_alternative<schema::TransactionDescr_split_install>(descr)) {
    const auto& split = std::get<schema::TransactionDescr_split_install>(descr);
    obj("type", "split_install");
    obj("split_info", td::JsonRaw(jsonify(split.split_info)));
    obj("installed", td::JsonBool(split.installed));
    obj.leave();
  }
  else if (std::holds_alternative<schema::TransactionDescr_merge_prepare>(descr)) {
    const auto& merge = std::get<schema::TransactionDescr_merge_prepare>(descr);
    obj("type", "merge_prepare");
    obj("split_info", td::JsonRaw(jsonify(merge.split_info)));
    obj("storage_ph", td::JsonRaw(jsonify(merge.storage_ph)));
    obj("aborted", td::JsonBool(merge.aborted));
    obj.leave();
  }
  else if (std::holds_alternative<schema::TransactionDescr_merge_install>(descr)) {
    const auto& merge = std::get<schema::TransactionDescr_merge_install>(descr);
    obj("type", "merge_install");
    obj("split_info", td::JsonRaw(jsonify(merge.split_info)));
    if (merge.storage_ph.has_value()) {
      obj("storage_ph", td::JsonRaw(jsonify(merge.storage_ph.value())));
    }
    if (merge.credit_ph.has_value()) {
      obj("credit_ph", td::JsonRaw(jsonify(merge.credit_ph.value())));
    }
    obj("compute_ph", td::JsonRaw(jsonify(merge.compute_ph)));
    if (merge.action.has_value()) {
      obj("action", td::JsonRaw(jsonify(merge.action.value())));
    }
    obj("aborted", td::JsonBool(merge.aborted));
    obj("destroyed", td::JsonBool(merge.destroyed));
    obj.leave();
  }

  return jb.string_builder().as_cslice().str();
}


void InsertBatchPostgres::insert_blocks(pqxx::work &transaction, const std::vector<ParsedBlockPtr>& mc_blocks) {
  std::ostringstream query;
  query << "INSERT INTO blocks (workchain, shard, seqno, root_hash, file_hash, mc_block_workchain, "
                                "mc_block_shard, mc_block_seqno, global_id, version, after_merge, before_split, "
                                "after_split, want_split, key_block, vert_seqno_incr, flags, gen_utime, start_lt, "
                                "end_lt, validator_list_hash_short, gen_catchain_seqno, min_ref_mc_seqno, "
                                "prev_key_block_seqno, vert_seqno, master_ref_seqno, rand_seed, created_by) VALUES ";

  bool is_first = true;
  for (const auto& mc_block : mc_blocks) {
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
      ++blocks_count_;
    }
  }
  if (is_first) {
    return;
  }
  query << " ON CONFLICT DO NOTHING";

  // LOG(DEBUG) << "Running SQL query: " << query.str();
  transaction.exec0(query.str());
}


void InsertBatchPostgres::insert_transactions(pqxx::work &transaction, const std::vector<ParsedBlockPtr>& mc_blocks) {
  std::ostringstream query;
  query << "INSERT INTO transactions (tenant_id, block_workchain, block_shard, block_seqno, account, hash, lt, prev_trans_hash, prev_trans_lt, now, orig_status, end_status, "
                                      "total_fees, account_state_hash_before, account_state_hash_after, description) VALUES ";
  bool is_first = true;
  for (const auto& mc_block : mc_blocks) {
    for (const auto &blk : mc_block->blocks_) {
      for (const auto& transaction : blk.transactions) {
        if (is_first) {
          is_first = false;
        } else {
          query << ", ";
        }
        query << "("
              << transaction.tenant_id << ","
              << blk.workchain << ","
              << blk.shard << ","
              << blk.seqno << ","
              << TO_SQL_STRING(convert::to_raw_address(transaction.account)) << ","
              << TO_SQL_STRING(td::base64_encode(transaction.hash.as_slice())) << ","
              << transaction.lt << ","
              << TO_SQL_STRING(td::base64_encode(transaction.prev_trans_hash.as_slice())) << ","
              << transaction.prev_trans_lt << ","
              << transaction.now << ","
              << TO_SQL_STRING(stringify(transaction.orig_status)) << ","
              << TO_SQL_STRING(stringify(transaction.end_status)) << ","
              << transaction.total_fees << ","
              << TO_SQL_STRING(td::base64_encode(transaction.account_state_hash_before.as_slice())) << ","
              << TO_SQL_STRING(td::base64_encode(transaction.account_state_hash_after.as_slice())) << ","
              << "'" << jsonify(transaction.description) << "'"  // FIXME: remove for production
              << ")";
        ++transactions_count_;
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


void InsertBatchPostgres::insert_messsages(pqxx::work &transaction, const std::vector<schema::Message> &messages, const std::vector<MsgBody>& message_bodies, const std::vector<TxMsg> &tx_msgs) {
  messages_count_ = messages.size();
  if (!messages_count_) {
    return;
  }
  
  insert_messages_contents(message_bodies, transaction);
  insert_messages_impl(messages, transaction);
  insert_messages_txs(tx_msgs, transaction);
}

const int max_chunk_size = 1000000;

void InsertBatchPostgres::insert_messages_contents(const std::vector<MsgBody>& message_bodies, pqxx::work& transaction) {
  std::ostringstream query;
  bool is_first = true;
  auto exec_query = [&]() {
    query << " ON CONFLICT DO NOTHING";
    transaction.exec0(query.str());
    query.str("");
    query.clear();
    query << "INSERT INTO message_contents (tenant_id, hash, body) VALUES ";
    is_first = true;
  };

  query << "INSERT INTO message_contents (tenant_id, hash, body) VALUES ";
  for (const auto& msg_body : message_bodies) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }

    query << "("
          << msg_body.tenant_id << ","
          << "'" << msg_body.hash << "',"
          << TO_SQL_STRING(msg_body.body)
          << ")";

    if (query.str().length() >= max_chunk_size) {
      exec_query();
    }
  }

  if (!is_first) {
    exec_query();
  }
}

void InsertBatchPostgres::insert_messages_impl(const std::vector<schema::Message>& messages, pqxx::work& transaction) {
  std::ostringstream query;
  bool is_first = true;
  auto exec_query = [&]() {
    query << " ON CONFLICT DO NOTHING";
    transaction.exec0(query.str());
    query.str("");
    query.clear();
    query << "INSERT INTO messages (tenant_id, hash, source, destination, value, fwd_fee, ihr_fee, created_lt, created_at, opcode, "
                                  "ihr_disabled, bounce, bounced, import_fee, body_hash, init_state_hash) VALUES ";
    is_first = true;
  };

  query << "INSERT INTO messages (tenant_id, hash, source, destination, value, fwd_fee, ihr_fee, created_lt, created_at, opcode, "
                                "ihr_disabled, bounce, bounced, import_fee, body_hash, init_state_hash) VALUES ";
  for (const auto& message : messages) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    query << "("
          << message.tenant_id << ","
          << "'" << td::base64_encode(message.hash.as_slice()) << "',"
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
          << "'" << td::base64_encode(message.body->get_hash().as_slice()) << "',"
          << (message.init_state.not_null() ? TO_SQL_STRING(td::base64_encode(message.init_state->get_hash().as_slice())) : "NULL")
          << ")";


    if (query.str().length() >= max_chunk_size) {
      exec_query();
    }
  }

  if (!is_first) {
    exec_query();
  }
}

void InsertBatchPostgres::insert_messages_txs(const std::vector<TxMsg>& tx_msgs, pqxx::work& transaction) {
  std::ostringstream query;
  bool is_first = true;
  auto exec_query = [&]() {
    query << " ON CONFLICT DO NOTHING";
    transaction.exec0(query.str());
    query.str("");
    query.clear();
    query << "INSERT INTO transaction_messages (tenant_id, transaction_hash, message_hash, direction) VALUES ";
    is_first = true;
  };

  query << "INSERT INTO transaction_messages (tenant_id, transaction_hash, message_hash, direction) VALUES ";
  for (const auto& tx_msg : tx_msgs) {
   if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    query << "("
          << tx_msg.tenant_id << ","
          << TO_SQL_STRING(tx_msg.tx_hash) << ","
          << TO_SQL_STRING(tx_msg.msg_hash) << ","
          << TO_SQL_STRING(tx_msg.direction)
          << ")";

    if (query.str().length() >= max_chunk_size) {
      exec_query();
    }
  }

  if (!is_first) {
    exec_query();
  }
}


void InsertBatchPostgres::insert_account_states(pqxx::work &transaction, const std::vector<ParsedBlockPtr>& mc_blocks) {
  std::ostringstream query;
  query << "INSERT INTO account_states (tenant_id, hash, account, balance, account_status, frozen_hash, code_hash, data_hash) VALUES ";
  bool is_first = true;
  for (const auto& mc_block : mc_blocks) {
    for (const auto& account_state : mc_block->account_states_) {
      if (is_first) {
        is_first = false;
      } else {
        query << ", ";
      }
      query << "("
            << account_state.tenant_id << ","
            << TO_SQL_STRING(td::base64_encode(account_state.hash.as_slice())) << ","
            << TO_SQL_STRING(convert::to_raw_address(account_state.account)) << ","
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

void InsertBatchPostgres::insert_jetton_transfers(pqxx::work &transaction, const std::vector<ParsedBlockPtr>& mc_blocks) {
  std::ostringstream query;
  query << "INSERT INTO jetton_transfers (tenant_id, transaction_hash, query_id, amount, source, destination, jetton_wallet_address, response_destination, custom_payload, forward_ton_amount, forward_payload) VALUES ";
  bool is_first = true;
  for (const auto& mc_block : mc_blocks) {
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
            << transfer.tenant_id << ","
            << TO_SQL_STRING(td::base64_encode(transfer.transaction_hash.as_slice())) << ","
            << transfer.query_id << ","
            << (transfer.amount.not_null() ? transfer.amount->to_dec_string() : "NULL") << ","
            << TO_SQL_STRING(transfer.source) << ","
            << TO_SQL_STRING(transfer.destination) << ","
            << TO_SQL_STRING(transfer.jetton_wallet) << ","
            << TO_SQL_STRING(transfer.response_destination) << ","
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

void InsertBatchPostgres::insert_jetton_burns(pqxx::work &transaction, const std::vector<ParsedBlockPtr>& mc_blocks) {
  std::ostringstream query;
  query << "INSERT INTO jetton_burns (tenant_id, transaction_hash, query_id, owner, jetton_wallet_address, amount, response_destination, custom_payload) VALUES ";
  bool is_first = true;
  for (const auto& mc_block : mc_blocks) {
    for (const auto& burn : mc_block->get_events<JettonBurn>()) {
      if (is_first) {
        is_first = false;
      } else {
        query << ", ";
      }

      auto custom_payload_boc_r = convert::to_bytes(burn.custom_payload);
      auto custom_payload_boc = custom_payload_boc_r.is_ok() ? custom_payload_boc_r.move_as_ok() : td::optional<std::string>{};

      query << "("
            << burn.tenant_id << ","
            << TO_SQL_STRING(td::base64_encode(burn.transaction_hash.as_slice())) << ","
            << burn.query_id << ","
            << TO_SQL_STRING(burn.owner) << ","
            << TO_SQL_STRING(burn.jetton_wallet) << ","
            << (burn.amount.not_null() ? burn.amount->to_dec_string() : "NULL") << ","
            << TO_SQL_STRING(burn.response_destination) << ","
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

void InsertBatchPostgres::insert_nft_transfers(pqxx::work &transaction, const std::vector<ParsedBlockPtr>& mc_blocks) {
  std::ostringstream query;
  query << "INSERT INTO nft_transfers (tenant_id, transaction_hash, query_id, nft_item_address, old_owner, new_owner, response_destination, custom_payload, forward_amount, forward_payload) VALUES ";
  bool is_first = true;
  for (const auto& mc_block : mc_blocks) {
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
            << transfer.tenant_id << ","
            << TO_SQL_STRING(td::base64_encode(transfer.transaction_hash.as_slice())) << ","
            << transfer.query_id << ","
            << TO_SQL_STRING(convert::to_raw_address(transfer.nft_item)) << ","
            << TO_SQL_STRING(transfer.old_owner) << ","
            << TO_SQL_STRING(transfer.new_owner) << ","
            << TO_SQL_STRING(transfer.response_destination) << ","
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
          promise_.set_error(td::Status::Error(ErrorCode::ENTITY_NOT_FOUND, PSLICE() << "Jetton Wallet for address " << address_ << " not found"));
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

      txn.exec_params(query,
                      master_data_.address,
                      master_data_.total_supply,
                      master_data_.mintable,
                      master_data_.admin_address ? master_data_.admin_address.value().c_str() : nullptr,
                      master_data_.jetton_content ? content_to_json_string(master_data_.jetton_content.value()).c_str() : nullptr,
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
          promise_.set_error(td::Status::Error(ErrorCode::ENTITY_NOT_FOUND, PSLICE() << "Jetton master not found: " << address_));
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
      
      txn.exec_params(query,
                      collection_.address,
                      collection_.next_item_index->to_dec_string(),
                      collection_.owner_address ? collection_.owner_address.value().c_str() : nullptr,
                      collection_.collection_content ? content_to_json_string(collection_.collection_content.value()).c_str() : nullptr,
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
          promise_.set_error(td::Status::Error(ErrorCode::ENTITY_NOT_FOUND, PSLICE() << "NFT Collection not found: " << address_));
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
          promise_.set_error(td::Status::Error(ErrorCode::ENTITY_NOT_FOUND, "NFT item not found"));
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

InsertManagerPostgres::InsertManagerPostgres(InsertManagerPostgres::Credential credential) : 
    credential_(credential),
    inserted_count_(0),
    start_time_(std::chrono::high_resolution_clock::now())
{
}

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
  int tx_count = 0;
  int msg_count = 0;
  while (!insert_queue_.empty() && tx_count < batch_tx_count_ && msg_count < batch_msg_count_ &&
         schema_blocks.size() < batch_blocks_count_ && parallel_insert_actors_ < max_parallel_insert_actors_) {
    auto schema_block = std::move(insert_queue_.front());
    insert_queue_.pop();

    auto promise = std::move(promise_queue_.front());
    promise_queue_.pop();

    for (const auto& bl : schema_block->blocks_) {
      tx_count += bl.transactions.size();
      for(const auto& tx: bl.transactions) {
        msg_count += tx.out_msgs.size() + (tx.in_msg ? 1 : 0);
      }
    }

    promises.push_back(std::move(promise));
    schema_blocks.push_back(std::move(schema_block));
    if(inserted_count_ == 0) {
      start_time_ = std::chrono::high_resolution_clock::now();
    }
  }
  bool scheduled = false;
  if (!schema_blocks.empty()) {
    scheduled = true;
    auto P = td::PromiseCreator::lambda([this, promises = std::move(promises)](td::Result<td::Unit> R) mutable {
      parallel_insert_actors_--;
      if (R.is_error()) {
        LOG(ERROR) << "Error inserting to PG: " << R.error();
        for (auto& p : promises) {
          p.set_error(R.error().clone());
        }
        return;
      }
      
      for (auto& p : promises) {
        p.set_result(td::Unit());
      }
      
      inserted_count_ += promises.size();
    });
    parallel_insert_actors_++;
    td::actor::create_actor<InsertBatchPostgres>("insert_batch_postgres", credential_.getConnectionString(), std::move(schema_blocks), std::move(P)).release();
  }

  if (!insert_queue_.empty() && scheduled) {
    alarm_timestamp() = td::Timestamp::in(0.1);
  } else {
    alarm_timestamp() = td::Timestamp::in(1.0);
  }
}

void InsertManagerPostgres::upsert_jetton_wallet(JettonWalletData jetton_wallet, td::Promise<td::Unit> promise) {
  td::actor::create_actor<UpsertJettonWallet>("upsertjettonwallet", credential_.getConnectionString(), std::move(jetton_wallet), std::move(promise)).release();
}

void InsertManagerPostgres::get_jetton_wallet(std::string address, td::Promise<JettonWalletData> promise) {
  td::actor::create_actor<GetJettonWallet>("getjettonwallet", credential_.getConnectionString(), std::move(address), std::move(promise)).release();
}

void InsertManagerPostgres::upsert_jetton_master(JettonMasterData jetton_wallet, td::Promise<td::Unit> promise) {
  td::actor::create_actor<UpsertJettonMaster>("upsertjettonmaster", credential_.getConnectionString(), std::move(jetton_wallet), std::move(promise)).release();
}

void InsertManagerPostgres::get_jetton_master(std::string address, td::Promise<JettonMasterData> promise) {
  td::actor::create_actor<GetJettonMaster>("getjettonmaster", credential_.getConnectionString(), std::move(address), std::move(promise)).release();
}

void InsertManagerPostgres::upsert_nft_collection(NFTCollectionData nft_collection, td::Promise<td::Unit> promise) {
  td::actor::create_actor<UpsertNFTCollection>("upsertnftcollection", credential_.getConnectionString(), std::move(nft_collection), std::move(promise)).release();
}

void InsertManagerPostgres::get_nft_collection(std::string address, td::Promise<NFTCollectionData> promise) {
  td::actor::create_actor<GetNFTCollection>("getnftcollection", credential_.getConnectionString(), std::move(address), std::move(promise)).release();
}

void InsertManagerPostgres::upsert_nft_item(NFTItemData nft_item, td::Promise<td::Unit> promise) {
  td::actor::create_actor<UpsertNFTItem>("upsertnftitem", credential_.getConnectionString(), std::move(nft_item), std::move(promise)).release();
}

void InsertManagerPostgres::get_nft_item(std::string address, td::Promise<NFTItemData> promise) {
  td::actor::create_actor<GetNFTItem>("getnftitem", credential_.getConnectionString(), std::move(address), std::move(promise)).release();
}

std::string InsertManagerPostgres::Credential::get_connection_string()  {
  return (
    "hostaddr=" + host +
    " port=" + std::to_string(port) + 
    (user.length() ? " user=" + user : "") +
    (password.length() ? " password=" + password : "") +
    (dbname.length() ? " dbname=" + dbname : "")
  );
}


void InsertManagerPostgres::get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise) {
  LOG(INFO) << "Reading existing seqnos";
  std::vector<std::uint32_t> existing_mc_seqnos;
  try {
    pqxx::connection c(credential_.getConnectionString());
    pqxx::work txn(c);
    for (auto [seqno]: txn.query<std::uint32_t>("select seqno from blocks where workchain = -1")) {
      existing_mc_seqnos.push_back(seqno);
    }
    promise.set_result(std::move(existing_mc_seqnos));
  } catch (const std::exception &e) {
    promise.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error selecting from PG: " << e.what()));
  }
}
