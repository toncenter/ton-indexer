#include <mutex>
#include "td/utils/JsonBuilder.h"
#include "InsertManagerPostgres.h"
#include "convert-utils.h"

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


std::string InsertManagerPostgres::Credential::get_connection_string()  {
  return (
    "hostaddr=" + host +
    " port=" + std::to_string(port) + 
    (user.length() ? " user=" + user : "") +
    (password.length() ? " password=" + password : "") +
    (dbname.length() ? " dbname=" + dbname : "")
  );
}


// This set is used as a synchronization mechanism to prevent multiple queries for the same message
// Otherwise Posgres will throw an error deadlock_detected
std::unordered_set<td::Bits256, BitArrayHasher> messages_in_progress;
std::unordered_set<td::Bits256, BitArrayHasher> msg_bodies_in_progress;
std::mutex messages_in_progress_mutex;
std::mutex latest_account_states_update_mutex;


//
// InsertBatchPostgres
//
void InsertBatchPostgres::start_up() {
  connection_string_ = credential_.get_connection_string();

  std::vector<schema::Message> messages;
  std::vector<TxMsg> tx_msgs;
  std::vector<MsgBody> msg_bodies;
  {
    std::lock_guard<std::mutex> guard(messages_in_progress_mutex);
    for (const auto& task : insert_tasks_) {
      for (const auto& blk : task.parsed_block_->blocks_) {
        for (const auto& transaction : blk.transactions) {
          if (transaction.in_msg.has_value()) {
            auto &msg = transaction.in_msg.value();
            if (messages_in_progress.find(msg.hash) == messages_in_progress.end()) {
              messages.push_back(msg);
              messages_in_progress.insert(msg.hash);
            }
            td::Bits256 body_hash = msg.body->get_hash().bits();
            if (msg_bodies_in_progress.find(body_hash) == msg_bodies_in_progress.end()) {
              msg_bodies.push_back({body_hash, msg.body_boc});
              msg_bodies_in_progress.insert(body_hash);
            }
            if (msg.init_state_boc) {
              td::Bits256 init_state_hash = msg.init_state->get_hash().bits();
              if (msg_bodies_in_progress.find(init_state_hash) == msg_bodies_in_progress.end()) {
                msg_bodies.push_back({init_state_hash, msg.init_state_boc.value()});
                msg_bodies_in_progress.insert(init_state_hash);
              }
            }
            tx_msgs.push_back({td::base64_encode(transaction.hash.as_slice()), td::base64_encode(transaction.in_msg.value().hash.as_slice()), "in"});
          }
          for (const auto& msg : transaction.out_msgs) {
            if (messages_in_progress.find(msg.hash) == messages_in_progress.end()) {
              messages.push_back(msg);
              messages_in_progress.insert(msg.hash);
            }
            td::Bits256 body_hash = msg.body->get_hash().bits();
            if (msg_bodies_in_progress.find(body_hash) == msg_bodies_in_progress.end()) {
              msg_bodies.push_back({body_hash, msg.body_boc});
              msg_bodies_in_progress.insert(body_hash);
            }
            if (msg.init_state_boc) {
              td::Bits256 init_state_hash = msg.init_state->get_hash().bits();
              if (msg_bodies_in_progress.find(init_state_hash) == msg_bodies_in_progress.end()) {
                msg_bodies.push_back({init_state_hash, msg.init_state_boc.value()});
                msg_bodies_in_progress.insert(init_state_hash);
              }
            }
            tx_msgs.push_back({td::base64_encode(transaction.hash.as_slice()), td::base64_encode(msg.hash.as_slice()), "out"});
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

    // update account states
    {
      std::lock_guard<std::mutex> guard(latest_account_states_update_mutex);
      pqxx::work txn(c);
      insert_blocks(txn, insert_tasks_);
      insert_shard_state(txn, insert_tasks_);
      insert_transactions(txn, insert_tasks_);
      insert_messsages(txn, messages, msg_bodies, tx_msgs);
      insert_account_states(txn, insert_tasks_);
      insert_jetton_transfers(txn, insert_tasks_);
      insert_jetton_burns(txn, insert_tasks_);
      insert_nft_transfers(txn, insert_tasks_);
      insert_latest_account_states(txn, insert_tasks_);
      insert_jetton_masters(txn, insert_tasks_);
      insert_jetton_wallets(txn, insert_tasks_);
      insert_nft_collections(txn, insert_tasks_);
      insert_nft_items(txn, insert_tasks_);
      txn.commit();
    }
    
    for(auto& task : insert_tasks_) {
      task.promise_.set_value(td::Unit());
    }

    promise_.set_value(td::Unit());
  } catch (const std::exception &e) {
    for(auto& task : insert_tasks_) {
      task.promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error inserting to PG: " << e.what()));
    }

    promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error inserting to PG: " << e.what()));
  }

  {
    std::lock_guard<std::mutex> guard(messages_in_progress_mutex);
    for (const auto& msg : messages) {
      messages_in_progress.erase(msg.hash);
    }
    for (const auto& msg_body : msg_bodies) {
      msg_bodies_in_progress.erase(msg_body.hash);
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
  if (c.due_fees_collected) {
    cc("due_fees_collected", std::to_string(*(c.due_fees_collected)));
  }
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
    if (ord.bounce.has_value()) {
      obj("bounce", td::JsonRaw(jsonify(ord.bounce.value())));
    }
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


std::string InsertBatchPostgres::jsonify(const schema::BlockReference& block_ref) {
  td::JsonBuilder jb;
  auto obj = jb.enter_object();

  obj("workchain", td::JsonInt(block_ref.workchain));
  obj("shard", td::JsonLong(block_ref.shard));
  obj("seqno", td::JsonInt(block_ref.seqno));
  obj.leave();

  return jb.string_builder().as_cslice().str();
}



std::string InsertBatchPostgres::jsonify(const std::vector<schema::BlockReference>& prev_blocks) {
  td::JsonBuilder jb;
  auto obj = jb.enter_array();

  for (auto & p : prev_blocks) {
    obj.enter_value() << td::JsonRaw(jsonify(p));
  }
  obj.leave();
  return jb.string_builder().as_cslice().str();
}


void InsertBatchPostgres::insert_blocks(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks) {
  std::ostringstream query;
  query << "INSERT INTO blocks (workchain, shard, seqno, root_hash, file_hash, mc_block_workchain, "
                                "mc_block_shard, mc_block_seqno, global_id, version, after_merge, before_split, "
                                "after_split, want_merge, want_split, key_block, vert_seqno_incr, flags, gen_utime, start_lt, "
                                "end_lt, validator_list_hash_short, gen_catchain_seqno, min_ref_mc_seqno, "
                                "prev_key_block_seqno, vert_seqno, master_ref_seqno, rand_seed, created_by, tx_count, prev_blocks) VALUES ";

  bool is_first = true;
  for (const auto& task : insert_tasks) {
    for (const auto& block : task.parsed_block_->blocks_) {
      if (is_first) {
        is_first = false;
      } else {
        query << ", ";
      }
      query << "("
            << block.workchain << ","
            << block.shard << ","
            << block.seqno << ","
            << transaction.quote(block.root_hash) << ","
            << TO_SQL_STRING(block.file_hash) << ","
            << TO_SQL_OPTIONAL(block.mc_block_workchain) << ","
            << TO_SQL_OPTIONAL(block.mc_block_shard) << ","
            << TO_SQL_OPTIONAL(block.mc_block_seqno) << ","
            << block.global_id << ","
            << block.version << ","
            << TO_SQL_BOOL(block.after_merge) << ","
            << TO_SQL_BOOL(block.before_split) << ","
            << TO_SQL_BOOL(block.after_split) << ","
            << TO_SQL_BOOL(block.want_merge) << ","
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
            << TO_SQL_STRING(block.created_by) << ","
            << block.transactions.size() << ","
            << "'" << jsonify(block.prev_blocks) << "'"
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


void InsertBatchPostgres::insert_shard_state(pqxx::work &transaction, const std::vector<InsertTaskStruct> &insert_tasks_) {
  std::ostringstream query;
  query << "INSERT INTO shard_state (mc_seqno, workchain, shard, seqno) VALUES ";

  bool is_first = true;
  for (const auto& task : insert_tasks_) {
    for (const auto& shard : task.parsed_block_->shard_state_) {
      if (is_first) {
        is_first = false;
      } else {
        query << ", ";
      }
      query << "("
            << shard.mc_seqno << ","
            << shard.workchain << ","
            << shard.shard << ","
            << shard.seqno
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


void InsertBatchPostgres::insert_transactions(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks) {
  std::ostringstream query;
  query << "INSERT INTO transactions (block_workchain, block_shard, block_seqno, mc_block_seqno, account, hash, lt, prev_trans_hash, prev_trans_lt, now, orig_status, end_status, "
                                      "total_fees, account_state_hash_before, account_state_hash_after, description) VALUES ";
  bool is_first = true;
  for (const auto& task : insert_tasks) {
    for (const auto &blk : task.parsed_block_->blocks_) {
      for (const auto& tx : blk.transactions) {
        if (is_first) {
          is_first = false;
        } else {
          query << ", ";
        }
        query << "("
              << blk.workchain << ","
              << blk.shard << ","
              << blk.seqno << ","
              << TO_SQL_OPTIONAL(blk.mc_block_seqno) << ","
              << TO_SQL_STRING(convert::to_raw_address(tx.account)) << ","
              << TO_SQL_STRING(td::base64_encode(tx.hash.as_slice())) << ","
              << tx.lt << ","
              << TO_SQL_STRING(td::base64_encode(tx.prev_trans_hash.as_slice())) << ","
              << tx.prev_trans_lt << ","
              << tx.now << ","
              << TO_SQL_STRING(stringify(tx.orig_status)) << ","
              << TO_SQL_STRING(stringify(tx.end_status)) << ","
              << tx.total_fees << ","
              << TO_SQL_STRING(td::base64_encode(tx.account_state_hash_before.as_slice())) << ","
              << TO_SQL_STRING(td::base64_encode(tx.account_state_hash_after.as_slice())) << ","
              << "'" << jsonify(tx.description) << "'"  // FIXME: remove for production
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


void InsertBatchPostgres::insert_messsages(pqxx::work &transaction, const std::vector<schema::Message> &messages, const std::vector<MsgBody>& message_bodies, const std::vector<TxMsg> &tx_msgs) {
  if (!messages.size()) {
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
    query << "INSERT INTO message_contents (hash, body) VALUES ";
    is_first = true;
  };

  query << "INSERT INTO message_contents (hash, body) VALUES ";
  for (const auto& msg_body : message_bodies) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }

    query << "("
          << TO_SQL_STRING(td::base64_encode(msg_body.hash.as_slice())) << ","
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
    query << "INSERT INTO messages (hash, source, destination, value, fwd_fee, ihr_fee, created_lt, created_at, opcode, "
                                  "ihr_disabled, bounce, bounced, import_fee, body_hash, init_state_hash) VALUES ";
    is_first = true;
  };

  query << "INSERT INTO messages (hash, source, destination, value, fwd_fee, ihr_fee, created_lt, created_at, opcode, "
                                "ihr_disabled, bounce, bounced, import_fee, body_hash, init_state_hash) VALUES ";
  for (const auto& message : messages) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    query << "("
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
    query << "INSERT INTO transaction_messages (transaction_hash, message_hash, direction) VALUES ";
    is_first = true;
  };

  query << "INSERT INTO transaction_messages (transaction_hash, message_hash, direction) VALUES ";
  for (const auto& tx_msg : tx_msgs) {
   if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    query << "("
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

void InsertBatchPostgres::insert_account_states(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks) {
  std::ostringstream query;
  query << "INSERT INTO account_states (hash, account, balance, account_status, frozen_hash, code_hash, data_hash) VALUES ";
  bool is_first = true;
  for (const auto& task : insert_tasks) {
    for (const auto& account_state : task.parsed_block_->account_states_) {
      if (account_state.account_status == "nonexist") {
        // nonexist account state is inserted on DB initialization
        continue;
      }
      if (is_first) {
        is_first = false;
      } else {
        query << ", ";
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
            << TO_SQL_STRING(td::base64_encode(account_state.hash.as_slice())) << ","
            << TO_SQL_STRING(convert::to_raw_address(account_state.account)) << ","
            << account_state.balance << ","
            << TO_SQL_STRING(account_state.account_status) << ","
            << TO_SQL_OPTIONAL_STRING(frozen_hash) << ","
            << TO_SQL_OPTIONAL_STRING(code_hash) << ","
            << TO_SQL_OPTIONAL_STRING(data_hash)
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

void InsertBatchPostgres::insert_latest_account_states(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks) {
  std::unordered_map<std::string, schema::AccountState> latest_account_states;
  for (const auto& task : insert_tasks) {
    for (const auto& account_state : task.parsed_block_->account_states_) {
      auto account_addr = convert::to_raw_address(account_state.account);
      if (latest_account_states.find(account_addr) == latest_account_states.end()) {
        latest_account_states[account_addr] = account_state;
      } else {
        if (latest_account_states[account_addr].last_trans_lt < account_state.last_trans_lt) {
          latest_account_states[account_addr] = account_state;
        }
      }
    }
  }

  std::ostringstream query;
  query << "INSERT INTO latest_account_states (account, hash, balance, last_trans_lt, timestamp, account_status, frozen_hash, code_hash, data_hash) VALUES ";
  bool is_first = true;
  for (auto i = latest_account_states.begin(); i != latest_account_states.end(); ++i) {
    auto& account_state = i->second;
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
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
          << TO_SQL_STRING(convert::to_raw_address(account_state.account)) << ","
          << TO_SQL_STRING(td::base64_encode(account_state.hash.as_slice())) << ","
          << account_state.balance << ","
          << account_state.last_trans_lt << ","
          << account_state.timestamp << ","
          << TO_SQL_STRING(account_state.account_status) << ","
          << TO_SQL_OPTIONAL_STRING(frozen_hash) << ","
          << TO_SQL_OPTIONAL_STRING(code_hash) << ","
          << TO_SQL_OPTIONAL_STRING(data_hash)
          << ")";
  }
  if (is_first) {
    return;
  }
  query << " ON CONFLICT (account) DO UPDATE SET "
        << "hash = EXCLUDED.hash,"
        << "balance = EXCLUDED.balance, "
        << "last_trans_lt = EXCLUDED.last_trans_lt, "
        << "timestamp = EXCLUDED.timestamp, "
        << "account_status = EXCLUDED.account_status, "
        << "frozen_hash = EXCLUDED.frozen_hash, "
        << "code_hash = EXCLUDED.code_hash, "
        << "data_hash = EXCLUDED.data_hash WHERE latest_account_states.last_trans_lt < EXCLUDED.last_trans_lt";
  // LOG(DEBUG) << "Running SQL query: " << query.str();
  transaction.exec0(query.str());
}

void InsertBatchPostgres::insert_jetton_masters(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks) {
  std::map<std::string, JettonMasterData> jetton_masters;
  for (const auto& task : insert_tasks) {
    for (const auto& jetton_master : task.parsed_block_->get_accounts<JettonMasterData>()) {
      auto existing = jetton_masters.find(jetton_master.address);
      if (existing == jetton_masters.end()) {
        jetton_masters[jetton_master.address] = jetton_master;
      } else {
        if (existing->second.last_transaction_lt < jetton_master.last_transaction_lt) {
          jetton_masters[jetton_master.address] = jetton_master;
        }
      }
    }
  }

  std::ostringstream query;
  query << "INSERT INTO jetton_masters (address, total_supply, mintable, admin_address, jetton_content, jetton_wallet_code_hash, data_hash, code_hash, last_transaction_lt, code_boc, data_boc) VALUES ";
  bool is_first = true;
  for (const auto& [addr, jetton_master] : jetton_masters) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    query << "("
          << TO_SQL_STRING(jetton_master.address) << ","
          << (jetton_master.total_supply.not_null() ? jetton_master.total_supply->to_dec_string() : "NULL") << ","
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
        << "data_boc = EXCLUDED.data_boc WHERE jetton_masters.last_transaction_lt < EXCLUDED.last_transaction_lt";

  // LOG(DEBUG) << "Running SQL query: " << query.str();
  transaction.exec0(query.str());
}

void InsertBatchPostgres::insert_jetton_wallets(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks) {
  std::map<std::string, JettonWalletData> jetton_wallets;
  for (const auto& task : insert_tasks) {
    for (const auto& jetton_wallet : task.parsed_block_->get_accounts<JettonWalletData>()) {
      auto existing = jetton_wallets.find(jetton_wallet.address);
      if (existing == jetton_wallets.end()) {
        jetton_wallets[jetton_wallet.address] = jetton_wallet;
      } else {
        if (existing->second.last_transaction_lt < jetton_wallet.last_transaction_lt) {
          jetton_wallets[jetton_wallet.address] = jetton_wallet;
        }
      }
    }
  }

  std::ostringstream query;
  query << "INSERT INTO jetton_wallets (balance, address, owner, jetton, last_transaction_lt, code_hash, data_hash) VALUES ";
  bool is_first = true;
  for (const auto& [addr, jetton_wallet] : jetton_wallets) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    query << "("
          << (jetton_wallet.balance.not_null() ? jetton_wallet.balance->to_dec_string() : "NULL") << ","
          << TO_SQL_STRING(jetton_wallet.address) << ","
          << TO_SQL_STRING(jetton_wallet.owner) << ","
          << TO_SQL_STRING(jetton_wallet.jetton) << ","
          << jetton_wallet.last_transaction_lt << ","
          << TO_SQL_STRING(td::base64_encode(jetton_wallet.code_hash.as_slice())) << ","
          << TO_SQL_STRING(td::base64_encode(jetton_wallet.data_hash.as_slice()))
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
        << "data_hash = EXCLUDED.data_hash WHERE jetton_wallets.last_transaction_lt < EXCLUDED.last_transaction_lt";

  // LOG(DEBUG) << "Running SQL query: " << query.str();
  transaction.exec0(query.str());
}

void InsertBatchPostgres::insert_nft_collections(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks) {
  std::map<std::string, NFTCollectionData> nft_collections;
  for (const auto& task : insert_tasks) {
    for (const auto& nft_collection : task.parsed_block_->get_accounts<NFTCollectionData>()) {
      auto existing = nft_collections.find(nft_collection.address);
      if (existing == nft_collections.end()) {
        nft_collections[nft_collection.address] = nft_collection;
      } else {
        if (existing->second.last_transaction_lt < nft_collection.last_transaction_lt) {
          nft_collections[nft_collection.address] = nft_collection;
        }
      }
    }
  }
  std::ostringstream query;
  query << "INSERT INTO  nft_collections (address, next_item_index, owner_address, collection_content, data_hash, code_hash, last_transaction_lt, code_boc, data_boc) VALUES ";
  bool is_first = true;
  for (const auto& [addr, nft_collection] : nft_collections) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    query << "("
          << TO_SQL_STRING(nft_collection.address) << ","
          << nft_collection.next_item_index << ","
          << TO_SQL_OPTIONAL_STRING(nft_collection.owner_address) << ","
          << (nft_collection.collection_content ? TO_SQL_STRING(content_to_json_string(nft_collection.collection_content.value())) : "NULL") << ","
          << TO_SQL_STRING(td::base64_encode(nft_collection.data_hash.as_slice())) << ","
          << TO_SQL_STRING(td::base64_encode(nft_collection.code_hash.as_slice())) << ","
          << nft_collection.last_transaction_lt << ","
          << TO_SQL_STRING(nft_collection.code_boc) << ","
          << TO_SQL_STRING(nft_collection.data_boc)
          << ")";
  }
  if (is_first) {
    return;
  }
  query << " ON CONFLICT (address) DO UPDATE SET "
        << "next_item_index = EXCLUDED.next_item_index, "
        << "owner_address = EXCLUDED.owner_address, "
        << "collection_content = EXCLUDED.collection_content, "
        << "data_hash = EXCLUDED.data_hash, "
        << "code_hash = EXCLUDED.code_hash, "
        << "last_transaction_lt = EXCLUDED.last_transaction_lt, "
        << "code_boc = EXCLUDED.code_boc, "
        << "data_boc = EXCLUDED.data_boc WHERE nft_collections.last_transaction_lt < EXCLUDED.last_transaction_lt";
  
  // LOG(DEBUG) << "Running SQL query: " << query.str();
  transaction.exec0(query.str());
}

void InsertBatchPostgres::insert_nft_items(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks) {
  std::map<std::string, NFTItemData> nft_items;
  for (const auto& task : insert_tasks) {
    for (const auto& nft_item : task.parsed_block_->get_accounts<NFTItemData>()) {
      auto existing = nft_items.find(nft_item.address);
      if (existing == nft_items.end()) {
        nft_items[nft_item.address] = nft_item;
      } else {
        if (existing->second.last_transaction_lt < nft_item.last_transaction_lt) {
          nft_items[nft_item.address] = nft_item;
        }
      }
    }
  }
  std::ostringstream query;
  query << "INSERT INTO nft_items (address, init, index, collection_address, owner_address, content, last_transaction_lt, code_hash, data_hash) VALUES ";
  bool is_first = true;
  for (const auto& [addr, nft_item] : nft_items) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    query << "("
          << TO_SQL_STRING(nft_item.address) << ","
          << TO_SQL_BOOL(nft_item.init) << ","
          << nft_item.index << ","
          << TO_SQL_STRING(nft_item.collection_address) << ","
          << TO_SQL_STRING(nft_item.owner_address) << ","
          << (nft_item.content ? TO_SQL_STRING(content_to_json_string(nft_item.content.value())) : "NULL") << ","
          << nft_item.last_transaction_lt << ","
          << TO_SQL_STRING(td::base64_encode(nft_item.code_hash.as_slice())) << ","
          << TO_SQL_STRING(td::base64_encode(nft_item.data_hash.as_slice()))
          << ")";
  }
  if (is_first) {
    return;
  }
  query << " ON CONFLICT (address) DO UPDATE SET "
        << "init = EXCLUDED.init, "
        << "index = EXCLUDED.index, "
        << "collection_address = EXCLUDED.collection_address, "
        << "owner_address = EXCLUDED.owner_address, "
        << "content = EXCLUDED.content, "
        << "last_transaction_lt = EXCLUDED.last_transaction_lt, "
        << "code_hash = EXCLUDED.code_hash, "
        << "data_hash = EXCLUDED.data_hash WHERE nft_items.last_transaction_lt < EXCLUDED.last_transaction_lt";
  
  // LOG(DEBUG) << "Running SQL query: " << query.str();
  transaction.exec0(query.str());
}

void InsertBatchPostgres::insert_jetton_transfers(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks) {
  std::ostringstream query;
  query << "INSERT INTO jetton_transfers (transaction_hash, query_id, amount, source, destination, jetton_wallet_address, response_destination, custom_payload, forward_ton_amount, forward_payload) VALUES ";
  bool is_first = true;
  for (const auto& task : insert_tasks) {
    for (const auto& transfer : task.parsed_block_->get_events<JettonTransfer>()) {
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

void InsertBatchPostgres::insert_jetton_burns(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks) {
  std::ostringstream query;
  query << "INSERT INTO jetton_burns (transaction_hash, query_id, owner, jetton_wallet_address, amount, response_destination, custom_payload) VALUES ";
  bool is_first = true;
  for (const auto& task : insert_tasks) {
    for (const auto& burn : task.parsed_block_->get_events<JettonBurn>()) {
      if (is_first) {
        is_first = false;
      } else {
        query << ", ";
      }

      auto custom_payload_boc_r = convert::to_bytes(burn.custom_payload);
      auto custom_payload_boc = custom_payload_boc_r.is_ok() ? custom_payload_boc_r.move_as_ok() : td::optional<std::string>{};

      query << "("
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

void InsertBatchPostgres::insert_nft_transfers(pqxx::work &transaction, const std::vector<InsertTaskStruct>& insert_tasks) {
  std::ostringstream query;
  query << "INSERT INTO nft_transfers (transaction_hash, query_id, nft_item_address, old_owner, new_owner, response_destination, custom_payload, forward_amount, forward_payload) VALUES ";
  bool is_first = true;
  for (const auto& task : insert_tasks) {
    for (const auto& transfer : task.parsed_block_->get_events<NFTTransfer>()) {
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

void InsertManagerPostgres::create_insert_actor(std::vector<InsertTaskStruct> insert_tasks, td::Promise<td::Unit> promise) {
  td::actor::create_actor<InsertBatchPostgres>("insert_batch_postgres", credential_, std::move(insert_tasks), std::move(promise)).release();
}

void InsertManagerPostgres::get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise, std::int32_t from_seqno, std::int32_t to_seqno) {
  LOG(INFO) << "Reading existing seqnos";
  std::vector<std::uint32_t> existing_mc_seqnos;
  try {
    pqxx::connection c(credential_.get_connection_string());
    pqxx::work txn(c);
    td::StringBuilder sb;
    sb << "select seqno from blocks where workchain = -1";
    if (from_seqno > 0) {
      sb << " and seqno >= " << from_seqno;
    }
    if (to_seqno > 0) {
      sb << " and seqno <= " << to_seqno;
    }
    for (auto [seqno]: txn.query<std::uint32_t>(sb.as_cslice().str())) {
      existing_mc_seqnos.push_back(seqno);
    }

    promise.set_result(std::move(existing_mc_seqnos));
  } catch (const std::exception &e) {
    promise.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error selecting from PG: " << e.what()));
  }
}

void InsertManagerPostgres::get_trace_assembler_state(td::Promise<schema::TraceAssemblerState> promise) {
    UNREACHABLE();
}
