#include <mutex>
#include "td/utils/JsonBuilder.h"
#include "InsertManagerPostgres.h"
#include "convert-utils.h"

#define TO_SQL_BOOL(x) ((x) ? "TRUE" : "FALSE")
#define TO_SQL_OPTIONAL(x) ((x) ? std::to_string(x.value()) : "NULL")
#define TO_SQL_OPTIONAL_BOOL(x) ((x) ? ((x.value()) ? "TRUE" : "FALSE") : "NULL")
#define TO_SQL_OPTIONAL_STRING(x, txn) ((x) ? (txn.quote(x.value())) : "NULL")

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


std::string InsertManagerPostgres::Credential::get_connection_string(std::string dbname) const {
  if ((dbname.length() == 0) && this->dbname.length()) {
    dbname = this->dbname;
  }
  std::string result = (
    "hostaddr=" + host +
    " port=" + std::to_string(port) + 
    (user.length() ? " user=" + user : "") +
    (password.length() ? " password=" + password : "") +
    (dbname.length() ? " dbname=" + dbname : "")
  );
  // LOG(INFO) << "connection string: " << result;
  return result;
}


template <>
struct std::hash<td::Bits256>
{
  std::size_t operator()(const td::Bits256& k) const {
    std::size_t seed = 0;
    for(const auto& el : k.as_array()) {
        seed ^= std::hash<td::uint8>{}(el) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }
};

template <>
struct std::hash<std::pair<td::Bits256, td::Bits256>>
{
  std::size_t operator()(const std::pair<td::Bits256, td::Bits256>& k) const {
    std::size_t seed = 0;
    for(const auto& el : k.first.as_array()) {
        seed ^= std::hash<td::uint8>{}(el) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    for(const auto& el : k.second.as_array()) {
        seed ^= std::hash<td::uint8>{}(el) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }
};

// This set is used as a synchronization mechanism to prevent multiple queries for the same message
// Otherwise Posgres will throw an error deadlock_detected
std::unordered_set<td::Bits256> msg_bodies_in_progress;
std::mutex messages_in_progress_mutex;
std::mutex latest_account_states_update_mutex;


//
// InsertBatchPostgres
//
void InsertBatchPostgres::start_up() {
  connection_string_ = credential_.get_connection_string();
  alarm();
}


void InsertBatchPostgres::alarm() {
  try {
    pqxx::connection c(connection_string_);
    if (!c.is_open()) {
      promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, "Failed to open database"));
      return;
    }

    // update account states
    pqxx::work txn(c);

    // prepare queries
    std::string insert_under_mutex_query;
    insert_blocks(txn);
    insert_shard_state(txn);
    insert_transactions(txn);
    insert_messages(txn);
    insert_account_states(txn);
    insert_jetton_transfers(txn);
    insert_jetton_burns(txn);
    insert_nft_transfers(txn);
    insert_under_mutex_query += insert_jetton_masters(txn);
    insert_under_mutex_query += insert_jetton_wallets(txn);
    insert_under_mutex_query += insert_nft_collections(txn);
    insert_under_mutex_query += insert_nft_items(txn);
    insert_under_mutex_query += insert_getgems_nft_auctions(txn);
    insert_under_mutex_query += insert_getgems_nft_sales(txn);
    insert_under_mutex_query += insert_latest_account_states(txn);
    insert_under_mutex_query += insert_traces(txn);

    // execute queries
    {
      std::lock_guard<std::mutex> guard(latest_account_states_update_mutex);
      txn.exec0(insert_under_mutex_query);
      txn.commit();
    }

    for(auto& task : insert_tasks_) {
      task.promise_.set_value(td::Unit());
    }
    promise_.set_value(td::Unit());
    stop();
    return;
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error inserting to PG: " << e.what();
    ++retry_count_;
  }
  alarm_timestamp() = td::Timestamp::in(10.0);
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

std::string InsertBatchPostgres::stringify(schema::Trace::State state) {
  switch (state) {
    case schema::Trace::State::complete: return "complete";
    case schema::Trace::State::pending: return "pending";
    case schema::Trace::State::broken: return "broken";
  };
  UNREACHABLE();
}


// std::string InsertBatchPostgres::jsonify(const schema::SplitMergeInfo& info) {
//   auto jb = td::JsonBuilder();
//   auto c = jb.enter_object();
//   c("cur_shard_pfx_len", static_cast<int>(info.cur_shard_pfx_len));
//   c("acc_split_depth", static_cast<int>(info.acc_split_depth));
//   c("this_addr", info.this_addr.to_hex());
//   c("sibling_addr", info.sibling_addr.to_hex());
//   c.leave();
//   return jb.string_builder().as_cslice().str();
// }


// std::string InsertBatchPostgres::jsonify(const schema::StorageUsedShort& s) {
//   auto jb = td::JsonBuilder();
//   auto c = jb.enter_object();
//   c("cells", std::to_string(s.cells));
//   c("bits", std::to_string(s.bits));
//   c.leave();
//   return jb.string_builder().as_cslice().str();
// }


// std::string InsertBatchPostgres::jsonify(const schema::TrStoragePhase& s) {
//   auto jb = td::JsonBuilder();
//   auto c = jb.enter_object();
//   c("storage_fees_collected", std::to_string(s.storage_fees_collected));
//   if (s.storage_fees_due) {
//     c("storage_fees_due", std::to_string(*(s.storage_fees_due)));
//   }
//   c("status_change", stringify(s.status_change));
//   c.leave();
//   return jb.string_builder().as_cslice().str();
// }


// std::string InsertBatchPostgres::jsonify(const schema::TrCreditPhase& c) {
//   auto jb = td::JsonBuilder();
//   auto cc = jb.enter_object();
//   if (c.due_fees_collected) {
//     cc("due_fees_collected", std::to_string(*(c.due_fees_collected)));
//   }
//   cc("credit", std::to_string(c.credit));
//   cc.leave();
//   return jb.string_builder().as_cslice().str();
// }


// std::string InsertBatchPostgres::jsonify(const schema::TrActionPhase& action) {
//   auto jb = td::JsonBuilder();
//   auto c = jb.enter_object();
//   c("success", td::JsonBool(action.success));
//   c("valid", td::JsonBool(action.valid));
//   c("no_funds", td::JsonBool(action.no_funds));
//   c("status_change", stringify(action.status_change));
//   if (action.total_fwd_fees) {
//     c("total_fwd_fees", std::to_string(*(action.total_fwd_fees)));
//   }
//   if (action.total_action_fees) {
//     c("total_action_fees", std::to_string(*(action.total_action_fees)));
//   }
//   c("result_code", action.result_code);
//   if (action.result_arg) {
//     c("result_arg", *(action.result_arg));
//   }
//   c("tot_actions", action.tot_actions);
//   c("spec_actions", action.spec_actions);
//   c("skipped_actions", action.skipped_actions);
//   c("msgs_created", action.msgs_created);
//   c("action_list_hash", td::base64_encode(action.action_list_hash.as_slice()));
//   c("tot_msg_size", td::JsonRaw(jsonify(action.tot_msg_size)));
//   c.leave();
//   return jb.string_builder().as_cslice().str();
// }


// std::string InsertBatchPostgres::jsonify(const schema::TrBouncePhase& bounce) {
//   auto jb = td::JsonBuilder();
//   auto c = jb.enter_object();
//   if (std::holds_alternative<schema::TrBouncePhase_negfunds>(bounce)) {
//     c("type", "negfunds");
//   } else if (std::holds_alternative<schema::TrBouncePhase_nofunds>(bounce)) {
//     const auto& nofunds = std::get<schema::TrBouncePhase_nofunds>(bounce);
//     c("type", "nofunds");
//     c("msg_size", td::JsonRaw(jsonify(nofunds.msg_size)));
//     c("req_fwd_fees", std::to_string(nofunds.req_fwd_fees));
//   } else if (std::holds_alternative<schema::TrBouncePhase_ok>(bounce)) {
//     const auto& ok = std::get<schema::TrBouncePhase_ok>(bounce);
//     c("type", "ok");
//     c("msg_size", td::JsonRaw(jsonify(ok.msg_size)));
//     c("msg_fees", std::to_string(ok.msg_fees));
//     c("fwd_fees", std::to_string(ok.fwd_fees));
//   }
//   c.leave();
//   return jb.string_builder().as_cslice().str();
// }


// std::string InsertBatchPostgres::jsonify(const schema::TrComputePhase& compute) {
//   auto jb = td::JsonBuilder();
//   auto c = jb.enter_object();
//   if (std::holds_alternative<schema::TrComputePhase_skipped>(compute)) {
//     c("type", "skipped");
//     c("skip_reason", stringify(std::get<schema::TrComputePhase_skipped>(compute).reason));
//   } else if (std::holds_alternative<schema::TrComputePhase_vm>(compute)) {
//     c("type", "vm");
//     auto& computed = std::get<schema::TrComputePhase_vm>(compute);
//     c("success", td::JsonBool(computed.success));
//     c("msg_state_used", td::JsonBool(computed.msg_state_used));
//     c("account_activated", td::JsonBool(computed.account_activated));
//     c("gas_fees", std::to_string(computed.gas_fees));
//     c("gas_used",std::to_string(computed.gas_used));
//     c("gas_limit", std::to_string(computed.gas_limit));
//     if (computed.gas_credit) {
//       c("gas_credit", std::to_string(*(computed.gas_credit)));
//     }
//     c("mode", computed.mode);
//     c("exit_code", computed.exit_code);
//     if (computed.exit_arg) {
//       c("exit_arg", *(computed.exit_arg));
//     }
//     c("vm_steps", static_cast<int64_t>(computed.vm_steps));
//     c("vm_init_state_hash", td::base64_encode(computed.vm_init_state_hash.as_slice()));
//     c("vm_final_state_hash", td::base64_encode(computed.vm_final_state_hash.as_slice()));
//   }
//   c.leave();
//   return jb.string_builder().as_cslice().str();
// }


// std::string InsertBatchPostgres::jsonify(schema::TransactionDescr descr) {
//   char tmp[10000]; // Adjust the size if needed
//   td::StringBuilder sb(td::MutableSlice{tmp, sizeof(tmp)});
//   td::JsonBuilder jb(std::move(sb));

//   auto obj = jb.enter_object();
//   if (std::holds_alternative<schema::TransactionDescr_ord>(descr)) {
//     const auto& ord = std::get<schema::TransactionDescr_ord>(descr);
//     obj("type", "ord");
//     obj("credit_first", td::JsonBool(ord.credit_first));
//     obj("storage_ph", td::JsonRaw(jsonify(ord.storage_ph)));
//     obj("credit_ph", td::JsonRaw(jsonify(ord.credit_ph)));
//     obj("compute_ph", td::JsonRaw(jsonify(ord.compute_ph)));
//     if (ord.action.has_value()) {
//       obj("action", td::JsonRaw(jsonify(ord.action.value())));
//     }
//     obj("aborted", td::JsonBool(ord.aborted));
//     if (ord.bounce.has_value()) {
//       obj("bounce", td::JsonRaw(jsonify(ord.bounce.value())));
//     }
//     obj("destroyed", td::JsonBool(ord.destroyed));
//     obj.leave();
//   }
//   else if (std::holds_alternative<schema::TransactionDescr_storage>(descr)) {
//     const auto& storage = std::get<schema::TransactionDescr_storage>(descr);
//     obj("type", "storage");
//     obj("storage_ph", td::JsonRaw(jsonify(storage.storage_ph)));
//     obj.leave();
//   }
//   else if (std::holds_alternative<schema::TransactionDescr_tick_tock>(descr)) {
//     const auto& tt = std::get<schema::TransactionDescr_tick_tock>(descr);
//     obj("type", "tick_tock");
//     obj("is_tock", td::JsonBool(tt.is_tock));
//     obj("storage_ph", td::JsonRaw(jsonify(tt.storage_ph)));
//     obj("compute_ph", td::JsonRaw(jsonify(tt.compute_ph)));
//     if (tt.action.has_value()) {
//       obj("action", td::JsonRaw(jsonify(tt.action.value())));
//     }
//     obj("aborted", td::JsonBool(tt.aborted));
//     obj("destroyed", td::JsonBool(tt.destroyed));
//     obj.leave();
//   }
//   else if (std::holds_alternative<schema::TransactionDescr_split_prepare>(descr)) {
//     const auto& split = std::get<schema::TransactionDescr_split_prepare>(descr);
//     obj("type", "split_prepare");
//     obj("split_info", td::JsonRaw(jsonify(split.split_info)));
//     if (split.storage_ph.has_value()) {
//       obj("storage_ph", td::JsonRaw(jsonify(split.storage_ph.value())));
//     }
//     obj("compute_ph", td::JsonRaw(jsonify(split.compute_ph)));
//     if (split.action.has_value()) {
//       obj("action", td::JsonRaw(jsonify(split.action.value())));
//     }
//     obj("aborted", td::JsonBool(split.aborted));
//     obj("destroyed", td::JsonBool(split.destroyed));
//     obj.leave();
//   }
//   else if (std::holds_alternative<schema::TransactionDescr_split_install>(descr)) {
//     const auto& split = std::get<schema::TransactionDescr_split_install>(descr);
//     obj("type", "split_install");
//     obj("split_info", td::JsonRaw(jsonify(split.split_info)));
//     obj("installed", td::JsonBool(split.installed));
//     obj.leave();
//   }
//   else if (std::holds_alternative<schema::TransactionDescr_merge_prepare>(descr)) {
//     const auto& merge = std::get<schema::TransactionDescr_merge_prepare>(descr);
//     obj("type", "merge_prepare");
//     obj("split_info", td::JsonRaw(jsonify(merge.split_info)));
//     obj("storage_ph", td::JsonRaw(jsonify(merge.storage_ph)));
//     obj("aborted", td::JsonBool(merge.aborted));
//     obj.leave();
//   }
//   else if (std::holds_alternative<schema::TransactionDescr_merge_install>(descr)) {
//     const auto& merge = std::get<schema::TransactionDescr_merge_install>(descr);
//     obj("type", "merge_install");
//     obj("split_info", td::JsonRaw(jsonify(merge.split_info)));
//     if (merge.storage_ph.has_value()) {
//       obj("storage_ph", td::JsonRaw(jsonify(merge.storage_ph.value())));
//     }
//     if (merge.credit_ph.has_value()) {
//       obj("credit_ph", td::JsonRaw(jsonify(merge.credit_ph.value())));
//     }
//     obj("compute_ph", td::JsonRaw(jsonify(merge.compute_ph)));
//     if (merge.action.has_value()) {
//       obj("action", td::JsonRaw(jsonify(merge.action.value())));
//     }
//     obj("aborted", td::JsonBool(merge.aborted));
//     obj("destroyed", td::JsonBool(merge.destroyed));
//     obj.leave();
//   }

//   return jb.string_builder().as_cslice().str();
// }


// std::string InsertBatchPostgres::jsonify(const schema::BlockReference& block_ref) {
//   td::JsonBuilder jb;
//   auto obj = jb.enter_object();

//   obj("workchain", td::JsonInt(block_ref.workchain));
//   obj("shard", td::JsonLong(block_ref.shard));
//   obj("seqno", td::JsonInt(block_ref.seqno));
//   obj.leave();

//   return jb.string_builder().as_cslice().str();
// }



// std::string InsertBatchPostgres::jsonify(const std::vector<schema::BlockReference>& prev_blocks) {
//   td::JsonBuilder jb;
//   auto obj = jb.enter_array();

//   for (auto & p : prev_blocks) {
//     obj.enter_value() << td::JsonRaw(jsonify(p));
//   }
//   obj.leave();
//   return jb.string_builder().as_cslice().str();
// }


std::string InsertBatchPostgres::insert_blocks(pqxx::work &txn) {
  std::ostringstream query;
  query << "INSERT INTO blocks (workchain, shard, seqno, root_hash, file_hash, mc_block_workchain, "
                                "mc_block_shard, mc_block_seqno, global_id, version, after_merge, before_split, "
                                "after_split, want_merge, want_split, key_block, vert_seqno_incr, flags, gen_utime, start_lt, "
                                "end_lt, validator_list_hash_short, gen_catchain_seqno, min_ref_mc_seqno, "
                                "prev_key_block_seqno, vert_seqno, master_ref_seqno, rand_seed, created_by, tx_count, prev_blocks) VALUES ";

  bool is_first = true;

  int count = 0;
  for (const auto& task : insert_tasks_) {
    for (const auto& block : task.parsed_block_->blocks_) {
      td::StringBuilder prev_blocks_str;
      prev_blocks_str << "{";
      bool first_prev_block = true;
      for(const auto &prev : block.prev_blocks) {
        if (first_prev_block) {
          first_prev_block = false;
        } else {
          prev_blocks_str << ", ";
        }
        prev_blocks_str << "\"(" << prev.workchain << ", " << prev.shard << ", " << prev.seqno << ")\""; 
      }
      prev_blocks_str << "}";

      if (is_first) {
        is_first = false;
      } else {
        query << ", ";
      }
      ++count;
      query << "("
            << block.workchain << ","
            << block.shard << ","
            << block.seqno << ","
            << txn.quote(block.root_hash) << ","
            << txn.quote(block.file_hash) << ","
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
            << txn.quote(block.rand_seed) << ","
            << txn.quote(block.created_by) << ","
            << block.transactions.size() << ","
            << txn.quote(prev_blocks_str.as_cslice().str())
            << ")";
    }
  }
  if (is_first) {
    return "";
  }
  query << " ON CONFLICT DO NOTHING;\n";
  txn.exec0(query.str());

  // LOG(DEBUG) << "Running SQL query: " << query.str();
  // LOG(INFO) << "Blocks query size: " << double(query.str().length()) / 1024 / 1024;
  return "";
}


std::string InsertBatchPostgres::insert_shard_state(pqxx::work &txn) {
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
    return "";
  }
  query << " ON CONFLICT DO NOTHING;\n";
  txn.exec0(query.str());

  // LOG(DEBUG) << "Running SQL query: " << query.str();
  return "";
}

template<typename T>
std::string to_int64(std::optional<T> value) {
  return ((value) ? std::to_string(static_cast<std::int64_t>(value.value())) : "NULL");
}

template<typename T>
std::string to_int64(td::optional<T> value) {
  return ((value) ? std::to_string(static_cast<std::int64_t>(value.value())) : "NULL");
}

template<typename T>
std::string to_int64(T value) {
  return std::to_string(static_cast<std::int64_t>(value));
}


std::string InsertBatchPostgres::insert_transactions(pqxx::work &txn) {
  std::ostringstream query;
  query << "INSERT INTO transactions (account, hash, lt, block_workchain, block_shard, block_seqno, "
                                     "mc_block_seqno, trace_id, prev_trans_hash, prev_trans_lt, now, "
                                     "orig_status, end_status, total_fees, account_state_hash_before, "
                                     "account_state_hash_after, descr, aborted, destroyed, "
                                     "credit_first, is_tock, installed, storage_fees_collected, "
                                     "storage_fees_due, storage_status_change, credit_due_fees_collected, "
                                     "credit, compute_skipped, skipped_reason, compute_success, "
                                     "compute_msg_state_used, compute_account_activated, compute_gas_fees, "
                                     "compute_gas_used, compute_gas_limit, compute_gas_credit, compute_mode, "
                                     "compute_exit_code, compute_exit_arg, compute_vm_steps, "
                                     "compute_vm_init_state_hash, compute_vm_final_state_hash, action_success, "
                                     "action_valid, action_no_funds, action_status_change, action_total_fwd_fees, "
                                     "action_total_action_fees, action_result_code, action_result_arg, "
                                     "action_tot_actions, action_spec_actions, action_skipped_actions, "
                                     "action_msgs_created, action_action_list_hash, action_tot_msg_size_cells, "
                                     "action_tot_msg_size_bits, bounce, bounce_msg_size_cells, bounce_msg_size_bits, "
                                     "bounce_req_fwd_fees, bounce_msg_fees, bounce_fwd_fees, split_info_cur_shard_pfx_len, "
                                     "split_info_acc_split_depth, split_info_this_addr, split_info_sibling_addr) VALUES ";

  auto store_storage_ph = [&](const schema::TrStoragePhase& storage_ph) {
    query << to_int64(storage_ph.storage_fees_collected) << ","
          << to_int64(storage_ph.storage_fees_due) << ","
          << txn.quote(stringify(storage_ph.status_change)) << ",";
  };
  auto store_empty_storage_ph = [&]() {
    query << "NULL, NULL, NULL,";
  };
  auto store_credit_ph = [&](const schema::TrCreditPhase& credit_ph) {
    query << to_int64(credit_ph.due_fees_collected) << ","
          << to_int64(credit_ph.credit) << ",";
  };
  auto store_empty_credit_ph = [&]() {
    query << "NULL,NULL,";
  };
  auto store_compute_ph = [&](const schema::TrComputePhase& compute_ph) {
      if (auto* v = std::get_if<schema::TrComputePhase_skipped>(&compute_ph)) {
        query << "TRUE,"
              << txn.quote(stringify(v->reason)) << ","
              << "NULL,NULL,NULL,NULL,NULL,NULL,NULL,"
              << "NULL,NULL,NULL,NULL,NULL,NULL,";
      }
      else if (auto* v = std::get_if<schema::TrComputePhase_vm>(&compute_ph)) {
        query << "FALSE,"
              << "NULL,"
              << TO_SQL_BOOL(v->success) << ","
              << TO_SQL_BOOL(v->msg_state_used) << ","
              << TO_SQL_BOOL(v->account_activated) << ","
              << to_int64(v->gas_fees) << ","
              << to_int64(v->gas_used) << ","
              << to_int64(v->gas_limit) << ","
              << to_int64(v->gas_credit) << ","
              << std::to_string(v->mode) << ","
              << v->exit_code << ","
              << TO_SQL_OPTIONAL(v->exit_arg) << ","
              << v->vm_steps << ","
              << txn.quote(td::base64_encode(v->vm_init_state_hash.as_slice())) << ","
              << txn.quote(td::base64_encode(v->vm_final_state_hash.as_slice())) << ",";
      }
  };
  auto store_empty_compute_ph = [&]() {
    query << "NULL,"
          << "NULL,"
          << "NULL,"
          << "NULL,"
          << "NULL,"
          << "NULL,"
          << "NULL,"
          << "NULL,"
          << "NULL,"
          << "NULL,"
          << "NULL,"
          << "NULL,"
          << "NULL,"
          << "NULL,"
          << "NULL,";
  };
  auto store_action_ph = [&](const schema::TrActionPhase& action) {
      query << TO_SQL_BOOL(action.success) << ","
            << TO_SQL_BOOL(action.valid) << ","
            << TO_SQL_BOOL(action.no_funds) << ","
            << txn.quote(stringify(action.status_change)) << ","
            << to_int64(action.total_fwd_fees) << ","
            << to_int64(action.total_action_fees) << ","
            << action.result_code << ","
            << TO_SQL_OPTIONAL(action.result_arg) << ","
            << action.tot_actions << ","
            << action.spec_actions << ","
            << action.skipped_actions << ","
            << action.msgs_created << ","
            << txn.quote(td::base64_encode(action.action_list_hash.as_slice())) << ","
            << to_int64(action.tot_msg_size.cells) << ","
            << to_int64(action.tot_msg_size.bits) << ",";
  };
  auto store_empty_action_ph = [&]() {
      query << "NULL,"
            << "NULL,"
            << "NULL,"
            << "NULL,"
            << "NULL,"
            << "NULL,"
            << "NULL,"
            << "NULL,"
            << "NULL,"
            << "NULL,"
            << "NULL,"
            << "NULL,"
            << "NULL,"
            << "NULL,"
            << "NULL,";
  };
  auto store_bounce_ph = [&](const schema::TrBouncePhase& bounce) {
      if(auto* v = std::get_if<schema::TrBouncePhase_negfunds>(&bounce)) {
        query << "'negfunds',"
              << "NULL,NULL,NULL,NULL,NULL,";
      } else if (auto* v = std::get_if<schema::TrBouncePhase_nofunds>(&bounce)) {
        query << "'nofunds',"
              << to_int64(v->msg_size.cells) << ","
              << to_int64(v->msg_size.bits) << ","
              << to_int64(v->req_fwd_fees) << ","
              << "NULL,NULL,";
      } else if (auto* v = std::get_if<schema::TrBouncePhase_ok>(&bounce)) {
          query << "'ok',"
              << to_int64(v->msg_size.cells) << ","
              << to_int64(v->msg_size.bits) << ","
              << "NULL,"
              << to_int64(v->msg_fees) << ","
              << to_int64(v->fwd_fees) << ",";
      }
  };
  auto store_empty_bounce_ph = [&]() {
    query << "NULL,NULL,NULL,NULL,NULL,NULL,";
  };
  auto store_split_info = [&](const schema::SplitMergeInfo& split_info) {
    query << split_info.cur_shard_pfx_len << ","
          << split_info.acc_split_depth << ","
          << txn.quote(td::base64_encode(split_info.this_addr.as_slice())) << ","
          << txn.quote(td::base64_encode(split_info.sibling_addr.as_slice())) << "";
  };
  auto store_empty_split_info = [&]() {
    query << "NULL,NULL,NULL,NULL";
  };

  bool is_first = true;
  for (const auto& task : insert_tasks_) {
    for (const auto &blk : task.parsed_block_->blocks_) {
      for (const auto& transaction : blk.transactions) {
        if (is_first) {
          is_first = false;
        } else {
          query << ", ";
        }
        query << "("
              << txn.quote(convert::to_raw_address(transaction.account)) << ","
              << txn.quote(td::base64_encode(transaction.hash.as_slice())) << ","
              << transaction.lt << ","
              << blk.workchain << ","
              << blk.shard << ","
              << blk.seqno << ","
              << TO_SQL_OPTIONAL(blk.mc_block_seqno) << ","
              << txn.quote(td::base64_encode(transaction.trace_id.as_slice())) << ","
              << txn.quote(td::base64_encode(transaction.prev_trans_hash.as_slice())) << ","
              << transaction.prev_trans_lt << ","
              << transaction.now << ","
              << txn.quote(stringify(transaction.orig_status)) << ","
              << txn.quote(stringify(transaction.end_status)) << ","
              << transaction.total_fees << ","
              << txn.quote(td::base64_encode(transaction.account_state_hash_before.as_slice())) << ","
              << txn.quote(td::base64_encode(transaction.account_state_hash_after.as_slice())) << ",";
        // insert description
        if (auto* v = std::get_if<schema::TransactionDescr_ord>(&transaction.description)) {
          query << "'ord',"
                << TO_SQL_BOOL(v->aborted) << ","
                << TO_SQL_BOOL(v->destroyed) << ","
                << TO_SQL_BOOL(v->credit_first) << ","
                << "NULL,"
                << "NULL,";
          store_storage_ph(v->storage_ph);
          store_credit_ph(v->credit_ph);
          store_compute_ph(v->compute_ph);
          if (v->action) {
            store_action_ph(v->action.value());
          } else {
            store_empty_action_ph();
          }
          if (v->bounce) {
            store_bounce_ph(v->bounce.value());
          } else {
            store_empty_bounce_ph();
          }
          store_empty_split_info();
        } 
        else if (auto* v = std::get_if<schema::TransactionDescr_storage>(&transaction.description)) {
          query << "'storage',"
                << "NULL,"
                << "NULL,"
                << "NULL,"
                << "NULL,"
                << "NULL,";
          store_storage_ph(v->storage_ph);
          store_empty_credit_ph();
          store_empty_compute_ph();
          store_empty_action_ph();
          store_empty_bounce_ph();
          store_empty_split_info();
        } 
        else if (auto* v = std::get_if<schema::TransactionDescr_tick_tock>(&transaction.description)) {
          query << "'tick_tock',"
                << TO_SQL_BOOL(v->aborted) << ","
                << TO_SQL_BOOL(v->destroyed) << ","
                << "NULL,"
                << TO_SQL_BOOL(v->is_tock) << ","
                << "NULL,";
          store_storage_ph(v->storage_ph);
          store_empty_credit_ph();
          store_compute_ph(v->compute_ph);
          if (v->action) {
              store_action_ph(v->action.value());
          } else {
              store_empty_action_ph();
          }
          store_empty_bounce_ph();
          store_empty_split_info();
        } 
        else if (auto* v = std::get_if<schema::TransactionDescr_split_prepare>(&transaction.description)) {
          query << "'split_prepare',"
                << TO_SQL_BOOL(v->aborted) << ","
                << TO_SQL_BOOL(v->destroyed) << ","
                << "NULL,"
                << "NULL,"
                << "NULL,";
          if (v->storage_ph) {
              store_storage_ph(v->storage_ph.value());
          } else {
              store_empty_storage_ph();
          }
          store_empty_credit_ph();
          store_compute_ph(v->compute_ph);
          if (v->action) {
              store_action_ph(v->action.value());
          } else {
              store_empty_action_ph();
          }
          store_empty_bounce_ph();
          store_split_info(v->split_info);
        } 
        else if (auto* v = std::get_if<schema::TransactionDescr_split_install>(&transaction.description)) {
          query << "'split_install',"
                << "NULL,"
                << "NULL,"
                << "NULL,"
                << "NULL,"
                << TO_SQL_BOOL(v->installed) << ",";
          store_empty_storage_ph();
          store_empty_credit_ph();
          store_empty_compute_ph();
          store_empty_action_ph();
          store_empty_bounce_ph();
          store_split_info(v->split_info);
        } 
        else if (auto* v = std::get_if<schema::TransactionDescr_merge_prepare>(&transaction.description)) {
          query << "'merge_prepare',"
                << TO_SQL_BOOL(v->aborted) << ","
                << "NULL,"
                << "NULL,"
                << "NULL,"
                << "NULL,";
          store_storage_ph(v->storage_ph);
          store_empty_credit_ph();
          store_empty_compute_ph();
          store_empty_action_ph();
          store_empty_bounce_ph();
          store_split_info(v->split_info);
        } 
        else if (auto* v = std::get_if<schema::TransactionDescr_merge_install>(&transaction.description)) {
          query << "'merge_install',"
                << TO_SQL_BOOL(v->aborted) << ","
                << TO_SQL_BOOL(v->destroyed) << ","
                << "NULL,"
                << "NULL,"
                << "NULL,";
          if (v->storage_ph) {
              store_storage_ph(v->storage_ph.value());
          } else {
              store_empty_storage_ph();
          }
          if (v->credit_ph) {
              store_credit_ph(v->credit_ph.value());
          } else {
              store_empty_credit_ph();
          }
          store_compute_ph(v->compute_ph);
          if (v->action) {
              store_action_ph(v->action.value());
          } else {
              store_empty_action_ph();
          }
          store_empty_bounce_ph();
          store_split_info(v->split_info);
        }
        query << ")";
      }
    }
  }
  if (is_first) {
    return "";
  }
  query << " ON CONFLICT DO NOTHING;\n";
  txn.exec0(query.str());
  // LOG(INFO) << "Transactions query size: " << double(query.str().length()) / 1024 / 1024;
  return "";
}


std::string InsertBatchPostgres::insert_messages(pqxx::work &txn) {
  std::vector<std::tuple<td::Bits256, std::string>> msg_bodies;
  {
    std::ostringstream query;
    query << "INSERT INTO messages (tx_hash, tx_lt, msg_hash, direction, trace_id, source, "
                                  "destination, value, fwd_fee, ihr_fee, created_lt, "
                                  "created_at, opcode, ihr_disabled, bounce, bounced, "
                                  "import_fee, body_hash, init_state_hash) VALUES ";
    bool is_first = true;
    auto store_message = [&](const schema::Transaction& tx, const schema::Message& msg, std::string direction) {
      if (is_first) {
        is_first = false;
      } else {
        query << ", ";
      }
      query << "("
            << txn.quote(td::base64_encode(tx.hash.as_slice())) << ","
            << tx.lt << ","
            << txn.quote(td::base64_encode(msg.hash.as_slice())) << ","
            << txn.quote(direction) << ","
            << txn.quote(td::base64_encode(msg.trace_id.as_slice())) << ","
            << TO_SQL_OPTIONAL_STRING(msg.source, txn) << ","
            << TO_SQL_OPTIONAL_STRING(msg.destination, txn) << ","
            << to_int64(msg.value) << ","
            << to_int64(msg.fwd_fee) << ","
            << to_int64(msg.ihr_fee) << ","
            << to_int64(msg.created_lt) << ","
            << TO_SQL_OPTIONAL(msg.created_at) << ","
            << TO_SQL_OPTIONAL(msg.opcode) << ","
            << TO_SQL_OPTIONAL_BOOL(msg.ihr_disabled) << ","
            << TO_SQL_OPTIONAL_BOOL(msg.bounce) << ","
            << TO_SQL_OPTIONAL_BOOL(msg.bounced) << ","
            << to_int64(msg.import_fee) << ","
            << txn.quote(td::base64_encode(msg.body->get_hash().as_slice())) << ","
            << (msg.init_state.not_null() ? txn.quote(td::base64_encode(msg.init_state->get_hash().as_slice())) : "NULL")
            << ")";
      // collect unique message contents
      {
        std::lock_guard<std::mutex> guard(messages_in_progress_mutex);
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
      }
    };
    for (const auto& task : insert_tasks_) {
      for (const auto &blk : task.parsed_block_->blocks_) {
        for (const auto& transaction : blk.transactions) {
          if(transaction.in_msg.has_value()) {
            store_message(transaction, transaction.in_msg.value(), "in");
          }
          for (const auto& msg : transaction.out_msgs) {
            store_message(transaction, msg, "out");
          }
        }
      }
    }
    if (is_first) {
      LOG(INFO) << "WFT???";
      return "";
    }
    query << " ON CONFLICT DO NOTHING;\n";
    // LOG(INFO) << "Messages query size: " << double(query.str().length()) / 1024 / 1024;
    txn.exec0(query.str());
  }

  // insert message contents
  {
    // LOG(INFO) << "Insert " << msg_bodies.size() << " msg bodies. In progress: " << msg_bodies_in_progress.size();
    std::ostringstream query;
    query << "INSERT INTO message_contents (hash, body) VALUES ";
    bool is_first = true;
    for(const auto& [body_hash, body] : msg_bodies) {
      if (is_first) {
        is_first = false;
      } else {
        query << ", ";
      }
      query << "("
            << txn.quote(td::base64_encode(body_hash.as_slice())) << ","
            << txn.quote(body)
            << ")";
    }
    if (!is_first) {
      query << " ON CONFLICT DO NOTHING;\n";
      txn.exec0(query.str());
    } else {
      LOG(WARNING) << "No message bodies in batch!";
    }
  }

  // unlock messages
  {
    std::lock_guard<std::mutex> guard(messages_in_progress_mutex);
    for (const auto& [body_hash, body] : msg_bodies) {
      msg_bodies_in_progress.erase(body_hash);
    }
  }

  return "";
}

std::string InsertBatchPostgres::insert_account_states(pqxx::work &txn) {
  std::ostringstream query;
  query << "INSERT INTO account_states (hash, account, balance, account_status, frozen_hash, code_hash, data_hash) VALUES ";
  bool is_first = true;
  for (const auto& task : insert_tasks_) {
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
            << txn.quote(td::base64_encode(account_state.hash.as_slice())) << ","
            << txn.quote(convert::to_raw_address(account_state.account)) << ","
            << account_state.balance << ","
            << txn.quote(account_state.account_status) << ","
            << TO_SQL_OPTIONAL_STRING(frozen_hash, txn) << ","
            << TO_SQL_OPTIONAL_STRING(code_hash, txn) << ","
            << TO_SQL_OPTIONAL_STRING(data_hash, txn)
            << ")";
    }
  }
  if (is_first) {
    return "";
  }
  query << " ON CONFLICT DO NOTHING;\n";
  // LOG(DEBUG) << "Running SQL query: " << query.str();
  // LOG(INFO) << "Account states query size: " << double(query.str().length()) / 1024 / 1024;
  txn.exec0(query.str());

  return "";
}

std::string InsertBatchPostgres::insert_latest_account_states(pqxx::work &txn) {
  std::unordered_map<std::string, schema::AccountState> latest_account_states;
  for (const auto& task : insert_tasks_) {
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
  query << "INSERT INTO latest_account_states (account, account_friendly, hash, balance, "
                                              "account_status, timestamp, last_trans_hash, last_trans_lt, "
                                              "frozen_hash, data_hash, code_hash, "
                                              "data_boc, code_boc) VALUES ";
  bool is_first = true;
  for (auto i = latest_account_states.begin(); i != latest_account_states.end(); ++i) {
    auto& account_state = i->second;
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    std::string code_str = "NULL";
    std::string data_str = "NULL";

    if (max_data_depth_ >= 0 && account_state.data.not_null() && (max_data_depth_ == 0 || account_state.data->get_depth() <= max_data_depth_)){
      auto data_res = vm::std_boc_serialize(account_state.data);
      if (data_res.is_ok()){
        data_str = txn.quote(td::base64_encode(data_res.move_as_ok().as_slice().str()));
      }
    } else {
      if (account_state.data.not_null()) {
        LOG(DEBUG) << "Large account data: " << account_state.account 
                  << " Depth: " << account_state.data->get_depth();
      }
    }
    {
      auto code_res = vm::std_boc_serialize(account_state.code);
      if (code_res.is_ok()){
        code_str = txn.quote(td::base64_encode(code_res.move_as_ok().as_slice().str()));
      }
      if (code_str.length() > 128000) {
        LOG(ERROR) << "Large account code:" << account_state.account;
      }
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
          << txn.quote(convert::to_raw_address(account_state.account)) << ","
          << "NULL,"
          << txn.quote(td::base64_encode(account_state.hash.as_slice())) << ","
          << account_state.balance << ","
          << txn.quote(account_state.account_status) << ","
          << account_state.timestamp << ","
          << txn.quote(td::base64_encode(account_state.last_trans_hash.as_slice())) << ","
          << to_int64(account_state.last_trans_lt) << ","
          << TO_SQL_OPTIONAL_STRING(frozen_hash, txn) << ","
          << TO_SQL_OPTIONAL_STRING(data_hash, txn) << ","
          << TO_SQL_OPTIONAL_STRING(code_hash, txn) << ","
          << data_str << ","
          << code_str << ")";
  }
  if (is_first) {
    return "";
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
        << "WHERE latest_account_states.last_trans_lt < EXCLUDED.last_trans_lt;\n";
  // LOG(INFO) << "Latest account states query size: " << double(query.str().length()) / 1024 / 1024;
  return query.str();
}

std::string InsertBatchPostgres::insert_jetton_masters(pqxx::work &txn) {
  std::unordered_map<block::StdAddress, JettonMasterDataV2, AddressHasher> jetton_masters;

  for (auto i = insert_tasks_.rbegin(); i != insert_tasks_.rend(); ++i) {
    const auto& task = *i;
    for (const auto& jetton_master : task.parsed_block_->get_accounts_v2<JettonMasterDataV2>()) {
      if (jetton_masters.find(jetton_master.address) == jetton_masters.end()) {
        jetton_masters[jetton_master.address] = jetton_master;
      } else {
        if (jetton_masters[jetton_master.address].last_transaction_lt < jetton_master.last_transaction_lt) {
          jetton_masters[jetton_master.address] = jetton_master;
        }
      }
    }
  }

  std::ostringstream query;
  query << "INSERT INTO jetton_masters (address, total_supply, mintable, admin_address, jetton_content, jetton_wallet_code_hash, last_transaction_lt, code_hash, data_hash) VALUES ";
  bool is_first = true;
  for (const auto& [addr, jetton_master] : jetton_masters) {
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
          << txn.quote(convert::to_raw_address(jetton_master.address)) << ","
          << jetton_master.total_supply << ","
          << TO_SQL_BOOL(jetton_master.mintable) << ","
          << TO_SQL_OPTIONAL_STRING(raw_admin_address, txn) << ","
          << (jetton_master.jetton_content ? txn.quote(content_to_json_string(jetton_master.jetton_content.value())) : "NULL") << ","
          << txn.quote(td::base64_encode(jetton_master.jetton_wallet_code_hash.as_slice())) << ","
          << jetton_master.last_transaction_lt << ","
          << txn.quote(td::base64_encode(jetton_master.code_hash.as_slice())) << ","
          << txn.quote(td::base64_encode(jetton_master.data_hash.as_slice()))
          << ")";
  }
  if (is_first) {
    return "";
  }
  query << " ON CONFLICT (address) DO UPDATE SET "
        << "total_supply = EXCLUDED.total_supply, "
        << "mintable = EXCLUDED.mintable, "
        << "admin_address = EXCLUDED.admin_address, "
        << "jetton_content = EXCLUDED.jetton_content, "
        << "jetton_wallet_code_hash = EXCLUDED.jetton_wallet_code_hash, "
        << "last_transaction_lt = EXCLUDED.last_transaction_lt, "
        << "code_hash = EXCLUDED.code_hash, " 
        << "data_hash = EXCLUDED.data_hash WHERE jetton_masters.last_transaction_lt < EXCLUDED.last_transaction_lt;\n";
  return query.str();
}

std::string InsertBatchPostgres::insert_jetton_wallets(pqxx::work &txn) {
  std::unordered_map<block::StdAddress, JettonWalletDataV2, AddressHasher> jetton_wallets;
  for (auto i = insert_tasks_.rbegin(); i != insert_tasks_.rend(); ++i) {
    const auto& task = *i;
    for (const auto& jetton_wallet : task.parsed_block_->get_accounts_v2<JettonWalletDataV2>()) {
      if (jetton_wallets.find(jetton_wallet.address) == jetton_wallets.end()) {
        jetton_wallets[jetton_wallet.address] = jetton_wallet;
      } else {
        if (jetton_wallets[jetton_wallet.address].last_transaction_lt < jetton_wallet.last_transaction_lt) {
          jetton_wallets[jetton_wallet.address] = jetton_wallet;
        }
      }
    }
  }

  std::unordered_set<block::StdAddress, AddressHasher> known_mintless_masters;

  std::ostringstream query;
  query << "INSERT INTO jetton_wallets (balance, address, owner, jetton, last_transaction_lt, code_hash, data_hash, mintless_is_claimed) VALUES ";
  bool is_first = true;
  for (const auto& [addr, jetton_wallet] : jetton_wallets) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    query << "("
          << (jetton_wallet.balance.not_null() ? jetton_wallet.balance->to_dec_string() : "NULL") << ","
          << txn.quote(convert::to_raw_address(jetton_wallet.address)) << ","
          << txn.quote(convert::to_raw_address(jetton_wallet.owner)) << ","
          << txn.quote(convert::to_raw_address(jetton_wallet.jetton)) << ","
          << jetton_wallet.last_transaction_lt << ","
          << txn.quote(td::base64_encode(jetton_wallet.code_hash.as_slice())) << ","
          << txn.quote(td::base64_encode(jetton_wallet.data_hash.as_slice())) << ","
          << TO_SQL_OPTIONAL_BOOL(jetton_wallet.mintless_is_claimed)
          << ")";
    if (jetton_wallet.mintless_is_claimed.has_value()) {
      known_mintless_masters.insert(jetton_wallet.jetton);
    }
  }
  if (is_first) {
    return "";
  }
  query << " ON CONFLICT (address) DO UPDATE SET "
        << "balance = EXCLUDED.balance, "
        << "owner = EXCLUDED.owner, "
        << "jetton = EXCLUDED.jetton, "
        << "last_transaction_lt = EXCLUDED.last_transaction_lt, "
        << "code_hash = EXCLUDED.code_hash, " 
        << "data_hash = EXCLUDED.data_hash, "
        << "mintless_is_claimed = EXCLUDED.mintless_is_claimed WHERE jetton_wallets.last_transaction_lt < EXCLUDED.last_transaction_lt;\n";

  if (!known_mintless_masters.empty()) {
    bool is_first = true;
    query << "INSERT INTO mintless_jetton_masters(address, is_indexed) VALUES ";
    for (const auto &addr : known_mintless_masters) {
      if (is_first) {
        is_first = false;
      } else {
        query << ", ";
      }
      LOG(INFO) << "Indexed mintless jetton: " << convert::to_raw_address(addr);
      query << "(" << txn.quote(convert::to_raw_address(addr)) << ", FALSE)";
    }
    query << " ON CONFLICT (address) DO NOTHING;\n";
  }
  return query.str();
}

std::string InsertBatchPostgres::insert_nft_collections(pqxx::work &txn) {
  std::unordered_map<block::StdAddress, NFTCollectionDataV2, AddressHasher> nft_collections;
  for (auto i = insert_tasks_.rbegin(); i != insert_tasks_.rend(); ++i) {
    const auto& task = *i;
    for (const auto& nft_collection : task.parsed_block_->get_accounts_v2<NFTCollectionDataV2>()) {
      if (nft_collections.find(nft_collection.address) == nft_collections.end()) {
        nft_collections[nft_collection.address] = nft_collection;
      } else {
        if (nft_collections[nft_collection.address].last_transaction_lt < nft_collection.last_transaction_lt) {
          nft_collections[nft_collection.address] = nft_collection;
        }
      }
    }
  }

  std::ostringstream query;
  query << "INSERT INTO nft_collections (address, next_item_index, owner_address, collection_content, last_transaction_lt, code_hash, data_hash) VALUES ";
  bool is_first = true;
  for (const auto& [addr, nft_collection] : nft_collections) {
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
    return "";
  }
  query << " ON CONFLICT (address) DO UPDATE SET "
        << "next_item_index = EXCLUDED.next_item_index, "
        << "owner_address = EXCLUDED.owner_address, "
        << "collection_content = EXCLUDED.collection_content, "
        << "last_transaction_lt = EXCLUDED.last_transaction_lt, "
        << "code_hash = EXCLUDED.code_hash, " 
        << "data_hash = EXCLUDED.data_hash WHERE nft_collections.last_transaction_lt < EXCLUDED.last_transaction_lt;\n";
  return query.str();
}

std::string InsertBatchPostgres::insert_nft_items(pqxx::work &txn) {
  std::unordered_map<block::StdAddress, NFTItemDataV2, AddressHasher> nft_items;
  for (auto i = insert_tasks_.rbegin(); i != insert_tasks_.rend(); ++i) {
    const auto& task = *i;
    for (const auto& nft_item : task.parsed_block_->get_accounts_v2<NFTItemDataV2>()) {
      if (nft_items.find(nft_item.address) == nft_items.end()) {
        nft_items[nft_item.address] = nft_item;
      } else {
        if (nft_items[nft_item.address].last_transaction_lt < nft_item.last_transaction_lt) {
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
  if (is_first) {
    return "";
  }
  query << " ON CONFLICT (address) DO UPDATE SET "
        << "init = EXCLUDED.init, "
        << "index = EXCLUDED.index, "
        << "collection_address = EXCLUDED.collection_address, "
        << "owner_address = EXCLUDED.owner_address, "
        << "content = EXCLUDED.content, "
        << "last_transaction_lt = EXCLUDED.last_transaction_lt, "
        << "code_hash = EXCLUDED.code_hash, "
        << "data_hash = EXCLUDED.data_hash WHERE nft_items.last_transaction_lt < EXCLUDED.last_transaction_lt;\n";
  return query.str();
}

std::string InsertBatchPostgres::insert_getgems_nft_sales(pqxx::work &txn) {
  std::unordered_map<block::StdAddress, GetGemsNftFixPriceSaleData, AddressHasher> nft_sales;
  for (auto i = insert_tasks_.rbegin(); i != insert_tasks_.rend(); ++i) {
    const auto& task = *i;
    for (const auto& nft_sale : task.parsed_block_->get_accounts_v2<GetGemsNftFixPriceSaleData>()) {
      if (nft_sales.find(nft_sale.address) == nft_sales.end()) {
        nft_sales[nft_sale.address] = nft_sale;
      } else {
        if (nft_sales[nft_sale.address].last_transaction_lt < nft_sale.last_transaction_lt) {
          nft_sales[nft_sale.address] = nft_sale;
        }
      }
    }
  }

  std::ostringstream query;
  query << "INSERT INTO getgems_nft_sales (address, is_complete, created_at, marketplace_address, nft_address, nft_owner_address, full_price, marketplace_fee_address, marketplace_fee, royalty_address, royalty_amount, last_transaction_lt, code_hash, data_hash) VALUES ";
  bool is_first = true;
  for (const auto& [addr, nft_sale] : nft_sales) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    std::optional<std::string> raw_nft_owner_address;
    if (nft_sale.nft_owner_address) {
      raw_nft_owner_address = convert::to_raw_address(nft_sale.nft_owner_address.value());
    }
    query << "("
          << txn.quote(convert::to_raw_address(nft_sale.address)) << ","
          << TO_SQL_BOOL(nft_sale.is_complete) << ","
          << nft_sale.created_at << ","
          << txn.quote(convert::to_raw_address(nft_sale.marketplace_address)) << ","
          << txn.quote(convert::to_raw_address(nft_sale.nft_address)) << ","
          << TO_SQL_OPTIONAL_STRING(raw_nft_owner_address, txn) << ","
          << (nft_sale.full_price.not_null() ? nft_sale.full_price->to_dec_string() : "NULL") << ","
          << txn.quote(convert::to_raw_address(nft_sale.marketplace_fee_address)) << ","
          << (nft_sale.marketplace_fee.not_null() ? nft_sale.marketplace_fee->to_dec_string() : "NULL") << ","
          << txn.quote(convert::to_raw_address(nft_sale.royalty_address)) << ","
          << (nft_sale.royalty_amount.not_null() ? nft_sale.royalty_amount->to_dec_string() : "NULL") << ","
          << nft_sale.last_transaction_lt << ","
          << txn.quote(td::base64_encode(nft_sale.code_hash.as_slice())) << ","
          << txn.quote(td::base64_encode(nft_sale.data_hash.as_slice()))
          << ")";
  }
  if (is_first) {
    return "";
  }
  query << " ON CONFLICT (address) DO UPDATE SET "
        << "is_complete = EXCLUDED.is_complete, "
        << "created_at = EXCLUDED.created_at, "
        << "marketplace_address = EXCLUDED.marketplace_address, "
        << "nft_address = EXCLUDED.nft_address, "
        << "nft_owner_address = EXCLUDED.nft_owner_address, "
        << "full_price = EXCLUDED.full_price, "
        << "marketplace_fee_address = EXCLUDED.marketplace_fee_address, "
        << "marketplace_fee = EXCLUDED.marketplace_fee, "
        << "royalty_address = EXCLUDED.royalty_address, "
        << "royalty_amount = EXCLUDED.royalty_amount, "
        << "last_transaction_lt = EXCLUDED.last_transaction_lt, "
        << "code_hash = EXCLUDED.code_hash, " 
        << "data_hash = EXCLUDED.data_hash WHERE getgems_nft_sales.last_transaction_lt < EXCLUDED.last_transaction_lt;\n";
  return query.str();
}

std::string InsertBatchPostgres::insert_getgems_nft_auctions(pqxx::work &txn) {
  std::unordered_map<block::StdAddress, GetGemsNftAuctionData, AddressHasher> nft_auctions;
  for (auto i = insert_tasks_.rbegin(); i != insert_tasks_.rend(); ++i) {
    const auto& task = *i;
    for (const auto& nft_auction : task.parsed_block_->get_accounts_v2<GetGemsNftAuctionData>()) {
      if (nft_auctions.find(nft_auction.address) == nft_auctions.end()) {
        nft_auctions[nft_auction.address] = nft_auction;
      } else {
        if (nft_auctions[nft_auction.address].last_transaction_lt < nft_auction.last_transaction_lt) {
          nft_auctions[nft_auction.address] = nft_auction;
        }
      }
    }
  }

  std::ostringstream query;
  query << "INSERT INTO getgems_nft_auctions (address, end_flag, end_time, mp_addr, nft_addr, nft_owner, last_bid, last_member, min_step, mp_fee_addr, mp_fee_factor, mp_fee_base, royalty_fee_addr, royalty_fee_factor, royalty_fee_base, max_bid, min_bid, created_at, last_bid_at, is_canceled, last_transaction_lt, code_hash, data_hash) VALUES ";
  bool is_first = true;
  for (const auto& [addr, nft_auction] : nft_auctions) {
    if (is_first) {
      is_first = false;
    } else {
      query << ", ";
    }
    std::optional<std::string> raw_nft_owner;
    if (nft_auction.nft_owner) {
      raw_nft_owner = convert::to_raw_address(nft_auction.nft_owner.value());
    }
    std::optional<std::string> raw_last_member;
    if (nft_auction.last_member) {
      raw_last_member = convert::to_raw_address(nft_auction.last_member.value());
    }
    query << "("
          << txn.quote(convert::to_raw_address(nft_auction.address)) << ","
          << TO_SQL_BOOL(nft_auction.end) << ","
          << nft_auction.end_time << ","
          << txn.quote(convert::to_raw_address(nft_auction.mp_addr)) << ","
          << txn.quote(convert::to_raw_address(nft_auction.nft_addr)) << ","
          << TO_SQL_OPTIONAL_STRING(raw_nft_owner, txn) << ","
          << (nft_auction.last_bid.not_null() ? nft_auction.last_bid->to_dec_string() : "NULL") << ","
          << TO_SQL_OPTIONAL_STRING(raw_last_member, txn) << ","
          << nft_auction.min_step << ","
          << txn.quote(convert::to_raw_address(nft_auction.mp_fee_addr)) << ","
          << nft_auction.mp_fee_factor << ","
          << nft_auction.mp_fee_base << ","
          << txn.quote(convert::to_raw_address(nft_auction.royalty_fee_addr)) << ","
          << nft_auction.royalty_fee_factor << ","
          << nft_auction.royalty_fee_base << ","
          << (nft_auction.max_bid.not_null() ? nft_auction.max_bid->to_dec_string() : "NULL") << ","
          << (nft_auction.min_bid.not_null() ? nft_auction.min_bid->to_dec_string() : "NULL") << ","
          << nft_auction.created_at << ","
          << nft_auction.last_bid_at << ","
          << TO_SQL_BOOL(nft_auction.is_canceled) << ","
          << nft_auction.last_transaction_lt << ","
          << txn.quote(td::base64_encode(nft_auction.code_hash.as_slice())) << ","
          << txn.quote(td::base64_encode(nft_auction.data_hash.as_slice()))
          << ")";
  }
  if (is_first) {
    return "";
  }
  query << " ON CONFLICT (address) DO UPDATE SET "
        << "end_flag = EXCLUDED.end_flag, "
        << "end_time = EXCLUDED.end_time, "
        << "mp_addr = EXCLUDED.mp_addr, "
        << "nft_addr = EXCLUDED.nft_addr, "
        << "nft_owner = EXCLUDED.nft_owner, "
        << "last_bid = EXCLUDED.last_bid, "
        << "last_member = EXCLUDED.last_member, "
        << "min_step = EXCLUDED.min_step, "
        << "mp_fee_addr = EXCLUDED.mp_fee_addr, "
        << "mp_fee_factor = EXCLUDED.mp_fee_factor, "
        << "mp_fee_base = EXCLUDED.mp_fee_base, "
        << "royalty_fee_addr = EXCLUDED.royalty_fee_addr, "
        << "royalty_fee_factor = EXCLUDED.royalty_fee_factor, "
        << "royalty_fee_base = EXCLUDED.royalty_fee_base, "
        << "max_bid = EXCLUDED.max_bid, "
        << "min_bid = EXCLUDED.min_bid, "
        << "created_at = EXCLUDED.created_at, "
        << "last_bid_at = EXCLUDED.last_bid_at, "
        << "is_canceled = EXCLUDED.is_canceled, "
        << "last_transaction_lt = EXCLUDED.last_transaction_lt, "
        << "code_hash = EXCLUDED.code_hash, " 
        << "data_hash = EXCLUDED.data_hash WHERE getgems_nft_auctions.last_transaction_lt < EXCLUDED.last_transaction_lt;\n";
  return query.str();
}

std::string InsertBatchPostgres::insert_jetton_transfers(pqxx::work &txn) {
  std::ostringstream query;
  query << "INSERT INTO jetton_transfers (tx_hash, tx_lt, tx_now, tx_aborted, query_id, amount, source, "
                                         "destination, jetton_wallet_address, jetton_master_address, response_destination, "
                                         "custom_payload, forward_ton_amount, forward_payload, trace_id) VALUES ";
  bool is_first = true;
  for (const auto& task : insert_tasks_) {
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
            << txn.quote(td::base64_encode(transfer.transaction_hash.as_slice())) << ","
            << transfer.transaction_lt << ","
            << transfer.transaction_now << ","
            << TO_SQL_BOOL(transfer.transaction_aborted) << ","
            << transfer.query_id << ","
            << (transfer.amount.not_null() ? transfer.amount->to_dec_string() : "NULL") << ","
            << txn.quote(transfer.source) << ","
            << txn.quote(transfer.destination) << ","
            << txn.quote(transfer.jetton_wallet) << ","
            << txn.quote(transfer.jetton_master) << ","
            << txn.quote(transfer.response_destination) << ","
            << TO_SQL_OPTIONAL_STRING(custom_payload_boc, txn) << ","
            << (transfer.forward_ton_amount.not_null() ? transfer.forward_ton_amount->to_dec_string() : "NULL") << ","
            << TO_SQL_OPTIONAL_STRING(forward_payload_boc, txn) << ","
            << txn.quote(td::base64_encode(transfer.trace_id.as_slice()))
            << ")";
    }
  }
  if (is_first) {
    return "";
  }
  query << " ON CONFLICT DO NOTHING;\n";

  // LOG(DEBUG) << "Running SQL query: " << query.str();
  // LOG(INFO) << "Jetton transfers query size: " << double(query.str().length()) / 1024 / 1024;
  txn.exec0(query.str());
  return "";
}

std::string InsertBatchPostgres::insert_jetton_burns(pqxx::work &txn) {
  std::ostringstream query;
  query << "INSERT INTO jetton_burns (tx_hash, tx_lt, tx_now, tx_aborted, query_id, owner, jetton_wallet_address, jetton_master_address, amount, response_destination, custom_payload, trace_id) VALUES ";
  bool is_first = true;
  for (const auto& task : insert_tasks_) {
    for (const auto& burn : task.parsed_block_->get_events<JettonBurn>()) {
      if (is_first) {
        is_first = false;
      } else {
        query << ", ";
      }

      auto custom_payload_boc_r = convert::to_bytes(burn.custom_payload);
      auto custom_payload_boc = custom_payload_boc_r.is_ok() ? custom_payload_boc_r.move_as_ok() : td::optional<std::string>{};

      query << "("
            << txn.quote(td::base64_encode(burn.transaction_hash.as_slice())) << ","
            << burn.transaction_lt << ","
            << burn.transaction_now << ","
            << TO_SQL_BOOL(burn.transaction_aborted) << ","
            << burn.query_id << ","
            << txn.quote(burn.owner) << ","
            << txn.quote(burn.jetton_wallet) << ","
            << txn.quote(burn.jetton_master) << ","
            << (burn.amount.not_null() ? burn.amount->to_dec_string() : "NULL") << ","
            << txn.quote(burn.response_destination) << ","
            << TO_SQL_OPTIONAL_STRING(custom_payload_boc, txn) << ","
            << txn.quote(td::base64_encode(burn.trace_id.as_slice()))
            << ")";
    }
  }
  if (is_first) {
    return "";
  }
  query << " ON CONFLICT DO NOTHING;\n";

  // LOG(DEBUG) << "Running SQL query: " << query.str();
  // LOG(INFO) << "Jetton burns query size: " << double(query.str().length()) / 1024 / 1024;
  txn.exec0(query.str());
  return "";
}

std::string InsertBatchPostgres::insert_nft_transfers(pqxx::work &txn) {
  std::ostringstream query;
  query << "INSERT INTO nft_transfers (tx_hash, tx_lt, tx_now, tx_aborted, query_id, nft_item_address, nft_item_index, nft_collection_address, old_owner, new_owner, response_destination, custom_payload, forward_amount, forward_payload, trace_id) VALUES ";
  bool is_first = true;
  for (const auto& task : insert_tasks_) {
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
            << txn.quote(td::base64_encode(transfer.transaction_hash.as_slice())) << ","
            << transfer.transaction_lt << ","
            << transfer.transaction_now << ","
            << TO_SQL_BOOL(transfer.transaction_aborted) << ","
            << transfer.query_id << ","
            << txn.quote(convert::to_raw_address(transfer.nft_item)) << ","
            << (transfer.nft_item_index.not_null() ? transfer.nft_item_index->to_dec_string() : "NULL") << ","
            << txn.quote(transfer.nft_collection) << ","
            << txn.quote(transfer.old_owner) << ","
            << txn.quote(transfer.new_owner) << ","
            << txn.quote(transfer.response_destination) << ","
            << TO_SQL_OPTIONAL_STRING(custom_payload_boc, txn) << ","
            << (transfer.forward_amount.not_null() ? transfer.forward_amount->to_dec_string() : "NULL") << ","
            << TO_SQL_OPTIONAL_STRING(forward_payload_boc, txn) << ","
            << txn.quote(td::base64_encode(transfer.trace_id.as_slice()))
            << ")";
    }
  }
  if (is_first) {
    return "";
  }
  query << " ON CONFLICT DO NOTHING;\n";

  // LOG(DEBUG) << "Running SQL query: " << query.str();
  // LOG(INFO) << "NFT transfers query size: " << double(query.str().length()) / 1024 / 1024;
  txn.exec0(query.str());

  return "";
}

#define B64HASH(x) (td::base64_encode((x).as_slice()))

std::string InsertBatchPostgres::insert_traces(pqxx::work &txn) {
  std::string full_query;
  std::ostringstream traces_query;
  std::ostringstream edges_query;

  traces_query << "INSERT INTO traces (trace_id, external_hash, mc_seqno_start, mc_seqno_end, "
                  "start_lt, start_utime, end_lt, end_utime, state, pending_edges_, edges_, nodes_) VALUES ";
  edges_query << "INSERT INTO trace_edges (trace_id, msg_hash, left_tx, right_tx, incomplete, broken) VALUES ";
  
  bool is_first_trace = true;
  bool is_first_edge = true;

  std::unordered_map<td::Bits256, schema::Trace> traces_map;
  std::unordered_map<std::pair<td::Bits256, td::Bits256>, schema::TraceEdge> edges_map;
  for (const auto& task : insert_tasks_) {
    for(auto &trace : task.parsed_block_->traces_) {
      {
        auto it = traces_map.find(trace.trace_id);
        if (it != traces_map.end() && it->second.end_lt < trace.end_lt) {
          it->second = trace;
        } else {
          traces_map.insert({trace.trace_id, trace});
        }
      }
      for(auto &edge : trace.edges) {
        auto key = std::make_pair(edge.trace_id, edge.msg_hash);
        auto it = edges_map.find(key);
        if (it != edges_map.end() && it->second.incomplete && !edge.incomplete) {
          it->second = edge;
        } else {
          edges_map.insert({key, edge});
        }
      }
    }
  }
  for(auto &[_, trace] : traces_map) {
    // trace
    if(is_first_trace) {
      is_first_trace = false;
    } else {
      traces_query << ", ";
    }
    traces_query << "("
                  << txn.quote(B64HASH(trace.trace_id)) << ","
                  << (trace.external_hash.has_value() ? txn.quote(B64HASH(trace.external_hash.value())) : "NULL" ) << ","
                  << trace.mc_seqno_start << ","
                  << trace.mc_seqno_end << ","
                  << trace.start_lt << ","
                  << trace.start_utime << ","
                  << trace.end_lt << ","
                  << trace.end_utime << ","
                  << txn.quote(stringify(trace.state)) << ","
                  << trace.pending_edges_ << ","
                  << trace.edges_ << ","
                  << trace.nodes_
                  << ")";
  }
  // edges
  for(auto &[_, edge] : edges_map) {
    if(is_first_edge) {
      is_first_edge = false;
    } else {
      edges_query << ", ";
    }
    edges_query << "("
                << txn.quote(B64HASH(edge.trace_id)) << ","
                << txn.quote(B64HASH(edge.msg_hash)) << ","
                << (edge.left_tx.has_value() ? txn.quote(B64HASH(edge.left_tx.value())) : "NULL" ) << ","
                << (edge.right_tx.has_value() ? txn.quote(B64HASH(edge.right_tx.value())) : "NULL" ) << ","
                << TO_SQL_BOOL(edge.incomplete) << ","
                << TO_SQL_BOOL(edge.broken)
                << ")";
  }
  if (!is_first_trace) {
    traces_query << " ON CONFLICT (trace_id) DO UPDATE SET "
                 << "mc_seqno_end = EXCLUDED.mc_seqno_end, "
                 << "end_lt = EXCLUDED.end_lt, "
                 << "end_utime = EXCLUDED.end_utime, "
                 << "state = EXCLUDED.state, "
                 << "pending_edges_ = EXCLUDED.pending_edges_, "
                 << "edges_ = EXCLUDED.edges_, "
                 << "nodes_ = EXCLUDED.nodes_ "
                 << "WHERE traces.end_lt < EXCLUDED.end_lt;\n";
    full_query = traces_query.str();
  }
  if (!is_first_edge) {
    edges_query << " ON CONFLICT (trace_id, msg_hash) DO UPDATE SET "
                << "trace_id = EXCLUDED.trace_id, "
                << "msg_hash = EXCLUDED.msg_hash, "
                << "left_tx = EXCLUDED.left_tx, "
                << "right_tx = EXCLUDED.right_tx, "
                << "incomplete = EXCLUDED.incomplete, "
                << "broken = EXCLUDED.broken "
                << "WHERE trace_edges.incomplete is true and EXCLUDED.incomplete is false and EXCLUDED.broken is false;\n";
    full_query += edges_query.str();
  }
  return full_query;
}

//
// InsertManagerPostgres
//
bool check_database_exists(const InsertManagerPostgres::Credential& credentials, const std::string& dbname) {
    try {
        pqxx::connection C(credentials.get_connection_string("postgres"));
        pqxx::work W(C);
        std::string query = "SELECT 1 FROM pg_database WHERE datname = " + W.quote(dbname);
        pqxx::result R = W.exec(query);
        return !R.empty();
    } catch (const std::exception &e) {
        LOG(ERROR) << "Failed to check database existance: " << e.what();
        return false;
    }
}


void InsertManagerPostgres::start_up() {
  LOG(INFO) << "Creating database...";

  if (!check_database_exists(credential_, credential_.dbname)) {
    try {
      {
        pqxx::connection c(credential_.get_connection_string("postgres"));
        pqxx::nontransaction N(c);
        N.exec0("create database " + credential_.dbname + ";");
      }
    } catch (const std::exception &e) {
      LOG(ERROR) << "Failed to create database: " << e.what();
      std::_Exit(1);
    }
  }

  LOG(INFO) << "Creating required types...";
  try {
    auto exec_query = [&] (const std::string& query) {
      try {
        pqxx::connection c(credential_.get_connection_string());
        pqxx::work txn(c);

        LOG(DEBUG) << "Executing query '" << query << "'";
        txn.exec0(query);
        txn.commit();
      } catch (const std::exception &e) {
        LOG(INFO) << "Skipping query '" << query << "': " << e.what();
      }
    };
    if (custom_types_) {
      exec_query("create extension if not exists pgton;");
    } else {
      exec_query("create domain tonhash as char(44);");
      exec_query("create domain tonaddr as varchar;");
    }

    exec_query("create type blockid as (workchain integer, shard bigint, seqno integer);");
    exec_query("create type blockidext as (workchain integer, shard bigint, seqno integer, root_hash tonhash, file_hash tonhash);");
    exec_query("create type account_status_type as enum('uninit', 'frozen', 'active', 'nonexist');");
    exec_query("create type descr_type as enum('ord', 'storage', 'tick_tock', 'split_prepare', 'split_install', 'merge_prepare', 'merge_install');");
    exec_query("create type status_change_type as enum('unchanged', 'frozen', 'deleted');");
    exec_query("create type skipped_reason_type as enum('no_state', 'bad_state', 'no_gas', 'suspended');");
    exec_query("create type bounce_type as enum('negfunds', 'nofunds', 'ok');");
    exec_query("create type trace_state as enum('complete', 'pending', 'broken');");
    exec_query("create type msg_direction as enum('out', 'in');");
    exec_query("create type trace_classification_state as enum('unclassified', 'failed', 'ok', 'broken');");
    exec_query("create type change_dns_record_details as (key varchar, value_schema varchar, value varchar, flags integer);");
    exec_query("create type peer_swap_details as (asset_in tonaddr, amount_in numeric, asset_out tonaddr, amount_out numeric);");
    exec_query("create type dex_transfer_details as (amount numeric, asset tonaddr, source tonaddr, destination tonaddr, source_jetton_wallet tonaddr, destination_jetton_wallet tonaddr);");
    exec_query("create type jetton_swap_details as (dex varchar, sender tonaddr, dex_incoming_transfer dex_transfer_details, dex_outgoing_transfer dex_transfer_details, peer_swaps peer_swap_details[]);");
    exec_query("create type ton_transfer_details as (content text, encrypted boolean);");
    exec_query("create type nft_transfer_details as (is_purchase boolean, price numeric, query_id numeric, custom_payload text, forward_payload text, forward_amount numeric, response_destination tonaddr, nft_item_index numeric);");
    exec_query("create type jetton_transfer_details as (response_destination tonaddr, forward_amount numeric, query_id numeric, custom_payload text, forward_payload text, comment text, is_encrypted_comment bool);");
    exec_query("create type nft_mint_details as (nft_item_index numeric);");
  }
  catch (const std::exception &e) {
    LOG(ERROR) << "Failed to run some of initial scripts: " << e.what();
    std::_Exit(1);
  }

  LOG(INFO) << "Creating tables...";
  try {
    pqxx::connection c(credential_.get_connection_string());
    pqxx::work txn(c);

    std::string query = "";
    query += (
      "create table if not exists blocks ("
      "workchain integer not null, "
      "shard bigint  not null, "
      "seqno integer not null, "
      "root_hash tonhash, "
      "file_hash tonhash, "
      "mc_block_workchain integer, "
      "mc_block_shard bigint, "
      "mc_block_seqno integer, "
      "global_id integer, "
      "version integer, "
      "after_merge boolean, "
      "before_split boolean, "
      "after_split boolean, "
      "want_merge boolean, "
      "want_split boolean, "
      "key_block boolean, "
      "vert_seqno_incr boolean, "
      "flags integer, "
      "gen_utime bigint, "
      "start_lt bigint, "
      "end_lt bigint, "
      "validator_list_hash_short integer, "
      "gen_catchain_seqno integer, "
      "min_ref_mc_seqno integer, "
      "prev_key_block_seqno integer, "
      "vert_seqno integer, "
      "master_ref_seqno integer, "
      "rand_seed tonhash, "
      "created_by tonhash, "
      "tx_count integer, "
      "prev_blocks blockid[], "
      "primary key (workchain, shard, seqno), "
      "foreign key (mc_block_workchain, mc_block_shard, mc_block_seqno) references blocks);\n"
    );

    query += (
      "create table if not exists shard_state ("
      "mc_seqno integer not null, "
      "workchain integer not null, "
      "shard bigint not null, "
      "seqno integer not null, "
      "primary key (mc_seqno, workchain, shard, seqno));"
    );

    query += (
      "create table if not exists transactions ("
      "account tonaddr not null, "
      "hash tonhash not null, "
      "lt bigint not null, "
      "block_workchain integer, "
      "block_shard bigint, "
      "block_seqno integer, "
      "mc_block_seqno integer, "
      "trace_id tonhash, "
      "prev_trans_hash tonhash, "
      "prev_trans_lt bigint, "
      "now integer, "
      "orig_status account_status_type, "
      "end_status account_status_type, "
      "total_fees bigint, "
      "account_state_hash_before tonhash, "
      "account_state_hash_after tonhash, "
      "descr descr_type, "
      "aborted boolean, "
      "destroyed boolean, "
      "credit_first boolean, "
      "is_tock boolean, "
      "installed boolean, "
      "storage_fees_collected bigint, "
      "storage_fees_due bigint, "
      "storage_status_change status_change_type, "
      "credit_due_fees_collected bigint, "
      "credit bigint, "
      "compute_skipped boolean, "
      "skipped_reason skipped_reason_type, "
      "compute_success boolean, "
      "compute_msg_state_used boolean, "
      "compute_account_activated boolean, "
      "compute_gas_fees bigint, "
      "compute_gas_used bigint, "
      "compute_gas_limit bigint, "
      "compute_gas_credit bigint, "
      "compute_mode smallint, "
      "compute_exit_code integer,"
      "compute_exit_arg integer,"
      "compute_vm_steps bigint,"
      "compute_vm_init_state_hash tonhash,"
      "compute_vm_final_state_hash tonhash,"
      "action_success boolean, "
      "action_valid boolean, "
      "action_no_funds boolean, "
      "action_status_change status_change_type, "
      "action_total_fwd_fees bigint, "
      "action_total_action_fees bigint, "
      "action_result_code int, "
      "action_result_arg int, "
      "action_tot_actions int, "
      "action_spec_actions int, "
      "action_skipped_actions int, "
      "action_msgs_created int, "
      "action_action_list_hash tonhash, "
      "action_tot_msg_size_cells bigint, "
      "action_tot_msg_size_bits bigint, "
      "bounce bounce_type, "
      "bounce_msg_size_cells bigint, "
      "bounce_msg_size_bits bigint, "
      "bounce_req_fwd_fees bigint, "
      "bounce_msg_fees bigint, "
      "bounce_fwd_fees bigint, "
      "split_info_cur_shard_pfx_len int, "
      "split_info_acc_split_depth int, "
      "split_info_this_addr tonaddr, "
      "split_info_sibling_addr tonaddr, "
      "primary key (hash, lt), "
      "foreign key (block_workchain, block_shard, block_seqno) references blocks);\n"
    );

    query += (
      "create table if not exists messages ("
      "tx_hash tonhash, "
      "tx_lt bigint, "
      "msg_hash tonhash, "
      "direction msg_direction, "
      "trace_id tonhash, "
      "source tonaddr, "
      "destination tonaddr, "
      "value bigint, "
      "fwd_fee bigint, "
      "ihr_fee bigint, "
      "created_lt bigint, "
      "created_at bigint, "
      "opcode integer, "
      "ihr_disabled boolean, "
      "bounce boolean, "
      "bounced boolean, "
      "import_fee bigint, "
      "body_hash tonhash, "
      "init_state_hash tonhash, "
      "primary key (tx_hash, tx_lt, msg_hash, direction), "
      "foreign key (tx_hash, tx_lt) references transactions);\n"
    );

    query += (
      "create table if not exists message_contents ("
      "hash tonhash not null primary key, "
      "body text);"
    );

    query += (
      "create table if not exists account_states ("
      "hash tonhash not null primary key, "
      "account tonaddr, "
      "balance bigint, "
      "account_status account_status_type, "
      "frozen_hash tonhash, "
      "data_hash tonhash, "
      "code_hash tonhash"
      ");\n"
    );

    query += (
      "create table if not exists latest_account_states ("
      "id bigserial not null, "
      "account tonaddr not null primary key, "
      "account_friendly tonaddr, "
      "hash tonhash not null, "
      "balance bigint, "
      "account_status account_status_type, "
      "timestamp integer, "
      "last_trans_hash tonhash, "
      "last_trans_lt bigint, "
      "frozen_hash tonhash, "
      "data_hash tonhash, "
      "code_hash tonhash, "
      "data_boc text, "
      "code_boc text);\n"
    );

    query += (
      "create table if not exists nft_collections ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "next_item_index numeric, "
      "owner_address tonaddr, "
      "collection_content jsonb, "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash);\n"
    );

    query += (
      "create table if not exists nft_items ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "init boolean, "
      "index numeric, "
      "collection_address tonaddr, "
      "owner_address tonaddr, "
      "content jsonb, "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash);\n"
    );

    query += (
      "create table if not exists nft_transfers ("
      "tx_hash tonhash not null, "
      "tx_lt bigint not null, "
      "tx_now integer not null, "
      "tx_aborted boolean not null, "
      "query_id numeric, "
      "nft_item_address tonaddr, "
      "nft_item_index numeric, "
      "nft_collection_address tonaddr, "
      "old_owner tonaddr, "
      "new_owner tonaddr, "
      "response_destination tonaddr, "
      "custom_payload text, "
      "forward_amount numeric, "
      "forward_payload text, "
      "trace_id tonhash, "
      "primary key (tx_hash, tx_lt), "
      "foreign key (tx_hash, tx_lt) references transactions);\n"
    );

    query += (
      "create table if not exists jetton_masters ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "total_supply numeric, "
      "mintable boolean, "
      "admin_address tonaddr, "
      "jetton_content jsonb, "
      "jetton_wallet_code_hash tonhash, "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash);\n"
    );

    query += (
      "create table if not exists jetton_wallets ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "balance numeric, "
      "owner tonaddr, "
      "jetton tonaddr, "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash, "
      "mintless_is_claimed boolean, "
      "mintless_amount numeric, "
      "mintless_start_from bigint, "
      "mintless_expire_at bigint);\n"
    );

    query += (
      "create table if not exists jetton_burns ( "
      "tx_hash tonhash not null, "
      "tx_lt bigint not null, "
      "tx_now integer not null, "
      "tx_aborted boolean not null, "
      "query_id numeric, "
      "owner tonaddr, "
      "jetton_wallet_address tonaddr, "
      "jetton_master_address tonaddr, "
      "amount numeric, "
      "response_destination tonaddr, "
      "custom_payload text, "
      "trace_id tonhash, "
      "primary key (tx_hash, tx_lt), "
      "foreign key (tx_hash, tx_lt) references transactions);\n"
    );

    query += (
      "create table if not exists jetton_transfers ("
      "tx_hash tonhash not null, "
      "tx_lt bigint not null, "
      "tx_now integer not null, "
      "tx_aborted boolean not null, "
      "query_id numeric, "
      "amount numeric, "
      "source tonaddr, "
      "destination tonaddr, "
      "jetton_wallet_address tonaddr, "
      "jetton_master_address tonaddr, "
      "response_destination tonaddr, "
      "custom_payload text, "
      "forward_ton_amount numeric, "
      "forward_payload text, "
      "trace_id tonhash, "
      "primary key (tx_hash, tx_lt), "
      "foreign key (tx_hash, tx_lt) references transactions);\n"
    );

    query += (
      "create table if not exists getgems_nft_sales ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "is_complete boolean, "
      "created_at bigint, "
      "marketplace_address tonaddr, "
      "nft_address tonaddr, "
      "nft_owner_address tonaddr, "
      "full_price numeric, "
      "marketplace_fee_address tonaddr, "
      "marketplace_fee numeric, "
      "royalty_address tonaddr, "
      "royalty_amount numeric, "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash);\n"
    );

    query += (
      "create table if not exists getgems_nft_auctions ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "end_flag boolean, "
      "end_time bigint, "
      "mp_addr tonaddr, "
      "nft_addr tonaddr, "
      "nft_owner tonaddr, "
      "last_bid numeric, "
      "last_member tonaddr, "
      "min_step bigint, "
      "mp_fee_addr tonaddr, "
      "mp_fee_factor bigint, "
      "mp_fee_base bigint, "
      "royalty_fee_addr tonaddr, "
      "royalty_fee_factor bigint, "
      "royalty_fee_base bigint, "
      "max_bid numeric, "
      "min_bid numeric, "
      "created_at bigint, "
      "last_bid_at bigint, "
      "is_canceled boolean, "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash);\n"
    );

    // traces
    query += (
      "create table if not exists traces ("
      "trace_id tonhash, "
      "external_hash tonhash, "
      "mc_seqno_start integer, "
      "mc_seqno_end integer, "
      "start_lt bigint, "
      "start_utime integer, "
      "end_lt bigint, "
      "end_utime integer, "
      "state trace_state, "
      "pending_edges_ bigint, "
      "edges_ bigint, "
      "nodes_ bigint, "
      "classification_state trace_classification_state default 'unclassified', "
      "primary key (trace_id)"
      ");\n"
    );

    query += (
      "create table if not exists trace_edges ("
      "trace_id tonhash, "
      "msg_hash tonhash, "
      "left_tx tonhash, "
      "right_tx tonhash, "
      "incomplete boolean, "
      "broken boolean, "
      "primary key (trace_id, msg_hash), "
      "foreign key (trace_id) references traces"
      ");\n"
    );
    
    query += (
      "create table if not exists actions ("
      "trace_id tonhash not null, "
      "action_id tonhash not null, "
      "start_lt bigint, "
      "end_lt bigint, "
      "start_utime bigint, "
      "end_utime bigint, "
      "source tonaddr, "
      "source_secondary tonaddr, "
      "destination tonaddr, "
      "destination_secondary tonaddr, "
      "asset tonaddr, "
      "asset_secondary tonaddr, "
      "asset2 tonaddr, "
      "asset2_secondary tonaddr, "
      "opcode bigint, "
      "tx_hashes tonhash[], "
      "type varchar, "
      "ton_transfer_data ton_transfer_details, "
      "value numeric, "
      "amount numeric, "
      "jetton_transfer_data jetton_transfer_details, "
      "nft_transfer_data nft_transfer_details, "
      "jetton_swap_data jetton_swap_details, "
      "change_dns_record_data change_dns_record_details, "
      "nft_mint_data nft_mint_details, "
      "success boolean default true, "
      "primary key (trace_id, action_id),"
      "foreign key (trace_id) references traces"
      ");\n"
    );

    query += (
      "create table if not exists mintless_jetton_masters ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "is_indexed boolean, "
      "custom_payload_api_uri varchar[]);\n"
    );

    LOG(DEBUG) << query;
    txn.exec0(query);
    txn.commit();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error while creating database: " << e.what();
    std::_Exit(1);
  }

  LOG(INFO) << "Creating required indexes...";
  try {
    pqxx::connection c(credential_.get_connection_string());
    pqxx::work txn(c);

    std::string query = "";
    
    // some necessary indexes
    query += (
      "create index if not exists traces_index_1 on traces (state);\n"
      "create index if not exists trace_edges_index_1 on trace_edges (incomplete);\n"
      "create index if not exists trace_unclassified_index on traces (state, start_lt) include (trace_id, nodes_) where (classification_state = 'unclassified');\n"
    );

    LOG(DEBUG) << query;
    txn.exec0(query);
    txn.commit();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error while creating required indexes in database: " << e.what();
    std::_Exit(1);
  }

  // create all indexes
  if (create_indexes_) {
    LOG(INFO) << "Creating all indexes...";
    try {
      pqxx::connection c(credential_.get_connection_string());
      pqxx::work txn(c);

      std::string query = "";
      
      // some necessary indexes
      query += (
        "create index if not exists blocks_index_1 on blocks (gen_utime asc);\n"
        "create index if not exists blocks_index_2 on blocks (mc_block_seqno asc);\n"
        "create index if not exists blocks_index_3 on blocks (seqno asc) where (workchain = '-1'::integer);\n"
        "create index if not exists blocks_index_4 on blocks (start_lt asc);\n"
        "create index if not exists transactions_index_1 on transactions (block_workchain, block_shard, block_seqno);\n"
        "create index if not exists transactions_index_2 on transactions (lt asc);\n"
        "create index if not exists transactions_index_3 on transactions (now asc, lt asc);\n"
        "create index if not exists transactions_index_4 on transactions (account, lt asc);\n"
        "create index if not exists transactions_index_5 on transactions (account, now asc, lt asc);\n"
        "create index if not exists transactions_index_6 on transactions (hash);\n"
        "create index if not exists transactions_index_7 on transactions (trace_id, lt asc);\n"
        "create index if not exists transactions_index_8 on transactions (mc_block_seqno asc, lt asc);\n"
        "create index if not exists messages_index_1 on messages (msg_hash);\n"
        "create index if not exists messages_index_2 on messages (source, created_lt asc);\n"
        "create index if not exists messages_index_3 on messages (destination, created_lt asc);\n"
        "create index if not exists messages_index_4 on messages (body_hash);\n"
        "create index if not exists messages_index_5 on messages (trace_id, tx_lt asc);\n"
        "create index if not exists messages_index_6 on messages (opcode, created_lt);\n"
        "create index if not exists latest_account_states_index_1 on latest_account_states (balance desc);\n"
        "create index if not exists latest_account_states_index_2 on latest_account_states (id asc);\n"
        "create index if not exists latest_account_states_address_book_index on latest_account_states (account) include (account_friendly, code_hash, account_status);\n"
        "create index if not exists jetton_masters_index_1 on jetton_masters (admin_address, id asc);\n"
        "create index if not exists jetton_masters_index_2 on jetton_masters (id asc);\n"
        "create index if not exists jetton_wallets_index_1 on jetton_wallets (owner, id asc);\n"
        "create index if not exists jetton_wallets_index_2 on jetton_wallets (jetton, id asc);\n"
        "create index if not exists jetton_wallets_index_3 on jetton_wallets (id asc);\n"
        "create index if not exists jetton_wallets_index_4 on jetton_wallets (jetton, balance desc);\n"
        "create index if not exists jetton_wallets_index_5 on jetton_wallets (owner, balance desc);\n"
        "create index if not exists jetton_transfers_index_1 on jetton_transfers (source, tx_now asc);\n"
        "create index if not exists jetton_transfers_index_2 on jetton_transfers (source, tx_lt asc);\n"
        "create index if not exists jetton_transfers_index_3 on jetton_transfers (destination, tx_lt asc);\n"
        "create index if not exists jetton_transfers_index_4 on jetton_transfers (destination, tx_now asc);\n"
        "create index if not exists jetton_transfers_index_6 on jetton_transfers (jetton_wallet_address, tx_lt asc);\n"
        "create index if not exists jetton_transfers_index_7 on jetton_transfers (jetton_master_address, tx_now asc);\n"
        "create index if not exists jetton_transfers_index_8 on jetton_transfers (jetton_master_address, tx_lt asc);\n"
        "create index if not exists jetton_transfers_index_9 on jetton_transfers (tx_now asc, tx_lt asc);\n"
        "create index if not exists jetton_transfers_index_10 on jetton_transfers (tx_lt asc);\n"
        "create index if not exists jetton_burns_index_1 on jetton_burns (owner, tx_now asc, tx_lt asc);\n"
        "create index if not exists jetton_burns_index_2 on jetton_burns (owner, tx_lt asc);\n"
        "create index if not exists jetton_burns_index_3 on jetton_burns (jetton_wallet_address, tx_now asc, tx_lt asc);\n"
        "create index if not exists jetton_burns_index_4 on jetton_burns (jetton_wallet_address, tx_lt asc);\n"
        "create index if not exists jetton_burns_index_5 on jetton_burns (jetton_master_address, tx_now asc, tx_lt asc);\n"
        "create index if not exists jetton_burns_index_6 on jetton_burns (jetton_master_address, tx_lt asc);\n"
        "create index if not exists jetton_burns_index_7 on jetton_burns (tx_now asc, tx_lt asc);\n"
        "create index if not exists jetton_burns_index_8 on jetton_burns (tx_lt asc);\n"
        "create index if not exists nft_collections_index_1 on nft_collections (owner_address, id asc);\n"
        "create index if not exists nft_collections_index_2 on nft_collections (id asc);\n"
        "create index if not exists nft_items_index_1 on nft_items (collection_address, index asc);\n"
        "create index if not exists nft_items_index_2 on nft_items (owner_address, collection_address asc, index asc);\n"
        "create index if not exists nft_items_index_3 on nft_items (id asc);\n"
        "-- create index if not exists nft_transfers_index_1 on nft_transfers (nft_item_address, tx_now asc, tx_lt asc);\n"
        "create index if not exists nft_transfers_index_2 on nft_transfers (nft_item_address, tx_lt asc);\n"
        "create index if not exists nft_transfers_index_3 on nft_transfers (nft_collection_address, tx_now asc);\n"
        "create index if not exists nft_transfers_index_4 on nft_transfers (nft_collection_address, tx_lt asc);\n"
        "create index if not exists nft_transfers_index_5 on nft_transfers (old_owner, tx_lt asc);\n"
        "-- create index if not exists nft_transfers_index_6 on nft_transfers (old_owner, tx_now asc, tx_lt asc);\n"
        "create index if not exists nft_transfers_index_7 on nft_transfers (new_owner, tx_lt asc);\n"
        "-- create index if not exists nft_transfers_index_8 on nft_transfers (new_owner, tx_now asc, tx_lt asc);\n"
        "create index if not exists nft_transfers_index_9 on nft_transfers (tx_lt asc);\n"
        "create index if not exists nft_transfers_index_10 on nft_transfers (tx_now asc, tx_lt asc);\n"
        "create index if not exists traces_index_1 on traces (state);\n"
        "create index if not exists trace_index_2a on traces (mc_seqno_end asc);\n"
        "-- create index if not exists traces_index_3 on traces (end_lt asc);\n"
        "-- create index if not exists traces_index_4 on traces (end_utime asc);\n"
        "-- create index if not exists traces_index_5 on traces (external_hash, end_lt asc);\n"
        "-- create index if not exists traces_index_6 on traces (external_hash, end_utime asc);\n"
        "create index if not exists traces_index_7 on traces (classification_state);\n"
        "create index if not exists trace_edges_index_1 on trace_edges (incomplete);\n"
        "-- create index if not exists trace_edges_index_2 on trace_edges (msg_hash);\n"
        "-- create index if not exists actions_index_1 on actions (trace_id, start_lt, end_lt);\n"
        "create index if not exists actions_index_2 on actions (action_id);"
      );

      LOG(DEBUG) << query;
      txn.exec0(query);
      txn.commit();
    } catch (const std::exception &e) {
      LOG(ERROR) << "Error while creating indexes in database: " << e.what();
      std::_Exit(1);
    }
  }

  // some migrations
  LOG(INFO) << "Running some migrations...";
  try {
    pqxx::connection c(credential_.get_connection_string());
    pqxx::work txn(c);

    std::string query = "";
    
    // some necessary indexes
    query += (
      "alter table jetton_wallets add column if not exists mintless_is_claimed boolean;\n"
      "alter table jetton_wallets add column if not exists mintless_amount numeric;\n"
      "alter table jetton_wallets add column if not exists mintless_start_from bigint;\n"
      "alter table jetton_wallets add column if not exists mintless_expire_at bigint;\n"
      "alter table mintless_jetton_masters add column if not exists custom_payload_api_uri varchar[];\n"
    );

    LOG(DEBUG) << query;
    txn.exec0(query);
    txn.commit();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error while running some migrations in database: " << e.what();
    std::_Exit(1);
  }

  // if success
  alarm_timestamp() = td::Timestamp::in(1.0);
}

void InsertManagerPostgres::set_max_data_depth(std::int32_t value) {
  LOG(INFO) << "InsertManagerPostgres max_data_depth set to " << value; 
  max_data_depth_ = value;
}

void InsertManagerPostgres::get_trace_assembler_state(td::Promise<schema::TraceAssemblerState> promise) {
  pqxx::connection c(credential_.get_connection_string());

  auto to_bits256 = [](std::string value) {
    auto R = td::base64_decode(value);
    if (R.is_error()) {
      LOG(ERROR) << "Failed to decode b64 string: " << value;
      std::_Exit(2);
    }
    return td::Bits256(td::ConstBitPtr(td::Slice(R.move_as_ok()).ubegin()));
  };
  try {
    schema::TraceAssemblerState state;
    {
      pqxx::work txn(c);
      std::string query = "select trace_id, external_hash, mc_seqno_start, mc_seqno_end, start_lt, start_utime, end_lt, end_utime, state, pending_edges_, edges_, nodes_ from traces where state = 'pending';";
      pqxx::result result = txn.exec(query);
      txn.commit();
      for (auto row : result) {
        schema::Trace trace;
        trace.trace_id = to_bits256(row[0].as<std::string>());
        if (!row[1].is_null()) {
          trace.external_hash = to_bits256(row[1].as<std::string>());
        }
        if (!row[2].is_null()) {
          trace.mc_seqno_start = row[2].as<std::int32_t>();
        }
        if (!row[3].is_null()) {
          trace.mc_seqno_end = row[3].as<std::int32_t>();
        }
        if (!row[4].is_null()) {
          trace.start_lt = row[4].as<std::uint64_t>();
        }
        if (!row[5].is_null()) {
          trace.start_utime = row[5].as<std::uint32_t>();
        }
        if (!row[6].is_null()) {
          trace.end_lt = row[6].as<std::uint64_t>();
        }
        if (!row[7].is_null()) {
          trace.end_utime = row[7].as<std::uint32_t>();
        }
        if (row[8].as<std::string>() != "pending") {
          LOG(ERROR) << "Error in request. Got non-pending trace!";
        }
        trace.state = schema::Trace::State::pending;
        if (!row[9].is_null()) {
          trace.pending_edges_ = row[9].as<std::int64_t>();
        }
        if (!row[10].is_null()) {
          trace.edges_ = row[10].as<std::int64_t>();
        }
        if (!row[11].is_null()) {
          trace.nodes_ = row[11].as<std::int64_t>();
        }

        state.pending_traces_.push_back(std::move(trace));
      }
    }
    {
      pqxx::work txn(c);
      std::string query = "select trace_id, msg_hash, left_tx, right_tx, incomplete, broken from trace_edges where incomplete;";
      pqxx::result result = txn.exec(query);
      txn.commit();
      for (auto row : result) {
        schema::TraceEdge edge;
        edge.trace_id = to_bits256(row[0].as<std::string>());
        edge.msg_hash = to_bits256(row[1].as<std::string>());
        if (!row[2].is_null()) {
          edge.left_tx = to_bits256(row[2].as<std::string>());
        }
        if (!row[3].is_null()) {
          edge.right_tx = to_bits256(row[3].as<std::string>());
        }
        edge.type = schema::TraceEdge::Type::ord;
        edge.incomplete = row[4].as<bool>();
        if (edge.incomplete != true) {
          LOG(ERROR) << "Error in request. Got non-pending edge!";
        }
        edge.broken = row[5].as<bool>();

        state.pending_edges_.push_back(std::move(edge));
      }
    }
    promise.set_value(std::move(state));
  } catch (const std::exception &e) {
    promise.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error selecting from PG: " << e.what()));
  }
}

void InsertManagerPostgres::create_insert_actor(std::vector<InsertTaskStruct> insert_tasks, td::Promise<td::Unit> promise) {
  td::actor::create_actor<InsertBatchPostgres>("insert_batch_postgres", credential_, std::move(insert_tasks), std::move(promise), max_data_depth_).release();
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
