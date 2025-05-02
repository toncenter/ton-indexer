#include <mutex>
#include "td/utils/JsonBuilder.h"
#include "InsertManagerPostgres.h"
#include "convert-utils.h"
#include "Statistics.h"


namespace pqxx
{

template<> struct nullness<schema::BlockReference> : pqxx::no_null<schema::BlockReference> {};

template<> struct string_traits<schema::BlockReference>
{
  static constexpr bool converts_to_string{true};
  static constexpr bool converts_from_string{false};

  static zview to_buf(char *begin, char *end, schema::BlockReference const &value) {
    return zview{
      begin,
      static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
  }
 
  static char *into_buf(char *begin, char *end, schema::BlockReference const &value) {
    std::ostringstream stream;
    stream << "(" << value.workchain << ", " << value.shard << ", " << value.seqno << ")";
    auto text = stream.str();
    if (pqxx::internal::cmp_greater_equal(std::size(text), end - begin))
      throw conversion_overrun{"Not enough buffer for schema::BlockReference."};
    std::memcpy(begin, text.c_str(), std::size(text) + 1);
    return begin + std::size(text) + 1;
  }
  static std::size_t size_buffer(schema::BlockReference const &value) noexcept {
    return 64;
  }
};

template<> struct nullness<td::RefInt256>
{
  static constexpr bool has_null{true};
 
  static constexpr bool always_null{false};
 
  static bool is_null(td::RefInt256 const &value) {
    return value.is_null();
  }
};

template<> struct string_traits<td::RefInt256>
{
  static constexpr bool converts_to_string{true};
  static constexpr bool converts_from_string{false};

  static zview to_buf(char *begin, char *end, td::RefInt256 const &value) {
    return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
  }
 
  static char *into_buf(char *begin, char *end, td::RefInt256 const &value) {
    auto text = value->to_dec_string();
    if (pqxx::internal::cmp_greater_equal(std::size(text), end - begin))
      throw conversion_overrun{"Not enough buffer for td::RefInt256."};
    std::memcpy(begin, text.c_str(), std::size(text) + 1);
    return begin + std::size(text) + 1;
  }
  static std::size_t size_buffer(td::RefInt256 const &value) noexcept {
    return 128;
  }
};

template<> struct nullness<block::StdAddress> : pqxx::no_null<block::StdAddress> {};

template<> struct string_traits<block::StdAddress>
{
  static constexpr bool converts_to_string{true};
  static constexpr bool converts_from_string{false};

  static zview to_buf(char *begin, char *end, block::StdAddress const &value) {
    return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
  }
 
  static char *into_buf(char *begin, char *end, block::StdAddress const &value) {
    std::ostringstream stream;
    stream << value.workchain << ":" << value.addr;
    auto text = stream.str();
    if (pqxx::internal::cmp_greater_equal(std::size(text), end - begin))
      throw conversion_overrun{"Not enough buffer for block::StdAddress."};
    std::memcpy(begin, text.c_str(), std::size(text) + 1);
    return begin + std::size(text) + 1;
  }
  static std::size_t size_buffer(block::StdAddress const &value) noexcept {
    return 80;
  }
};

template<> struct nullness<td::Bits256> : pqxx::no_null<td::Bits256> {};

template<> struct string_traits<td::Bits256>
{
  static constexpr bool converts_to_string{true};
  static constexpr bool converts_from_string{false};

  static zview to_buf(char *begin, char *end, td::Bits256 const &value) {
    return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
  }
 
  static char *into_buf(char *begin, char *end, td::Bits256 const &value) {
    auto text = td::base64_encode(value.as_slice());
    if (pqxx::internal::cmp_greater_equal(std::size(text), end - begin))
      throw conversion_overrun{"Not enough buffer for td::Bits256."};
    std::memcpy(begin, text.c_str(), std::size(text) + 1);
    return begin + std::size(text) + 1;
  }
  static std::size_t size_buffer(td::Bits256 const &value) noexcept {
    return 64;
  }
};

template<> struct nullness<vm::CellHash> : pqxx::no_null<vm::CellHash> {};

template<> struct string_traits<vm::CellHash>
{
  static constexpr bool converts_to_string{true};
  static constexpr bool converts_from_string{false};

  static zview to_buf(char *begin, char *end, vm::CellHash const &value) {
    return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
  }
 
  static char *into_buf(char *begin, char *end, vm::CellHash const &value) {
    auto text = td::base64_encode(value.as_slice());
    if (pqxx::internal::cmp_greater_equal(std::size(text), end - begin))
      throw conversion_overrun{"Not enough buffer for vm::CellHash."};
    std::memcpy(begin, text.c_str(), std::size(text) + 1);
    return begin + std::size(text) + 1;
  }
  static std::size_t size_buffer(vm::CellHash const &value) noexcept {
    return 64;
  }
};
}

class PopulateTableStream {
private:
    pqxx::work& txn_;
    std::string table_name_;
    std::initializer_list<std::string_view> column_names_;
    int batch_size_;
    bool with_copy_;
    bool is_first_row_{true};
    std::optional<pqxx::stream_to> copy_stream_;
    std::ostringstream insert_stream_;
    std::string conflict_clause_;

    bool conflict_clause_added_{false};
    
    enum class ConflictAction { None, DoNothing, DoUpdate };
    ConflictAction conflict_action_{ConflictAction::None};
    std::initializer_list<std::string_view> conflict_columns_;
    std::string update_condition_;
    
public:
    struct UpsertConfig {
        std::initializer_list<std::string_view> conflict_columns;
        std::string_view update_condition;
    };

    PopulateTableStream(
        pqxx::work& txn,
        std::string_view table_name,
        std::initializer_list<std::string_view> column_names,
        int batch_size,
        bool with_copy = false)
        : txn_(txn)
        , table_name_(table_name)
        , column_names_(column_names)
        , batch_size_(batch_size)
        , with_copy_(with_copy)
    {
        initializeStream();
    }

    void setConflictDoNothing() {
        if (with_copy_) {
            throw std::runtime_error("ON CONFLICT not supported with COPY mode");
        }
        conflict_action_ = ConflictAction::DoNothing;
        buildConflictClause();
    }

    void setConflictDoUpdate(std::initializer_list<std::string_view> conflict_columns, std::string_view update_condition) {
        if (with_copy_) {
            throw std::runtime_error("ON CONFLICT not supported with COPY mode");
        }
        conflict_action_ = ConflictAction::DoUpdate;
        conflict_columns_ = std::move(conflict_columns);
        update_condition_ = std::move(update_condition);
        buildConflictClause();
    }

private:
    void buildConflictClause() {
      std::ostringstream conflict_stream;
      
      if (conflict_action_ != ConflictAction::None) {
          conflict_stream << " ON CONFLICT ";
          
          if (conflict_columns_.size()) {
              conflict_stream << "(";
              auto it = conflict_columns_.begin();
              conflict_stream << *it++;
              for(; it != conflict_columns_.end(); ++it) {
                  conflict_stream << ", " << *it;
              }
              conflict_stream << ") ";
          }

          if (conflict_action_ == ConflictAction::DoNothing) {
              conflict_stream << "DO NOTHING";
          } else if (conflict_action_ == ConflictAction::DoUpdate) {
              conflict_stream << "DO UPDATE SET ";
              bool first = true;
              for (const auto& col : column_names_) {
                  if (!first) conflict_stream << ", ";
                  conflict_stream << col << " = EXCLUDED." << col;
                  first = false;
              }
              if (!update_condition_.empty()) {
                  conflict_stream << " WHERE " << update_condition_;
              }
          }
      }
      
      conflict_clause_ = conflict_stream.str();
    }

    void initializeStream() {
        if (with_copy_) {
            copy_stream_.emplace(pqxx::stream_to::table(txn_, {table_name_}, column_names_));
            return;
        }

        insert_stream_.str("");
        insert_stream_.clear();
        is_first_row_ = true;

        // Build INSERT part
        insert_stream_ << "INSERT INTO " << table_name_ << " (";
        bool first = true;
        for (const auto& col : column_names_) {
          if (!first) insert_stream_ << ", ";
          insert_stream_ << col;
          first = false;
        }
        insert_stream_ << ") VALUES ";
    }

public:
    template <typename ...T>
    void insert_row(std::tuple<T...> row) {
      if (std::tuple_size<decltype(row)>::value != column_names_.size()) {
        throw std::runtime_error("row size doesn't match column names size");
      }
      if (with_copy_) {
        copy_stream_->write_row(row);
        return;
      }

      if (conflict_clause_added_) {
        throw std::runtime_error("can't insert row after conflict clause");
      }

      if (!is_first_row_) {
        insert_stream_ << ",";
      }
      is_first_row_ = false;

      insert_stream_ << "(";
      bool first = true;
      std::apply([&](const auto&... args) {
        ((insert_stream_ << (first ? "" : ",") << txn_.quote(args), first = false), ...);
      }, row);
      insert_stream_ << ")";
    }

    std::string get_str() {
      if (with_copy_) {
        throw std::runtime_error("get_str not supported with COPY mode");
      }
      if (is_first_row_) {
        return "";
      }
      if (!conflict_clause_added_) {
        insert_stream_ << conflict_clause_ << ";";
        conflict_clause_added_ = true;
      }
      return insert_stream_.str();
    }

    void finish() {
      if (with_copy_) {
        copy_stream_->complete();
        return;
      }
      if (is_first_row_) {
        return;
      }
      
      txn_.exec0(get_str());
    }
};

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
    td::Timer connect_timer;
    pqxx::connection c(connection_string_);
    connect_timer.pause();

    td::Timer data_timer;
    pqxx::work txn(c);

    insert_blocks(txn, with_copy_);
    insert_shard_state(txn, with_copy_);
    insert_transactions(txn, with_copy_);
    insert_messages(txn, with_copy_);
    insert_account_states(txn, with_copy_);
    insert_jetton_transfers(txn, with_copy_);
    insert_jetton_burns(txn, with_copy_);
    insert_nft_transfers(txn, with_copy_);
    insert_traces(txn, with_copy_);
    data_timer.pause();
    td::Timer states_timer;
    std::string insert_under_mutex_query;
    insert_under_mutex_query += insert_jetton_masters(txn);
    insert_under_mutex_query += insert_jetton_wallets(txn);
    insert_under_mutex_query += insert_nft_collections(txn);
    insert_under_mutex_query += insert_nft_items(txn);
    insert_under_mutex_query += insert_getgems_nft_auctions(txn);
    insert_under_mutex_query += insert_getgems_nft_sales(txn);
    insert_under_mutex_query += insert_latest_account_states(txn);
    
    td::Timer commit_timer{true};
    {
      std::lock_guard<std::mutex> guard(latest_account_states_update_mutex);
      txn.exec0(insert_under_mutex_query);
      states_timer.pause();
      commit_timer.resume();
      txn.commit();
      commit_timer.pause();
    }

    for(auto& task : insert_tasks_) {
      task.promise_.set_value(td::Unit());
    }
    promise_.set_value(td::Unit());

    g_statistics.record_time(INSERT_BATCH_CONNECT, connect_timer.elapsed() * 1e3);
    g_statistics.record_time(INSERT_BATCH_EXEC_DATA, data_timer.elapsed() * 1e3);
    g_statistics.record_time(INSERT_BATCH_EXEC_STATES, states_timer.elapsed() * 1e3);
    g_statistics.record_time(INSERT_BATCH_COMMIT, commit_timer.elapsed() * 1e3);

    stop();
  } catch (const pqxx::integrity_constraint_violation &e) {
    LOG(WARNING) << "Error COPY to PG: " << e.what();
    LOG(WARNING) << "Apparently this block already exists in the database. Nevertheless we retry with INSERT ... ON CONFLICT ...";
    with_copy_ = false;
    g_statistics.record_count(INSERT_CONFLICT);
    alarm_timestamp() = td::Timestamp::now();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error inserting to PG: " << e.what();
    alarm_timestamp() = td::Timestamp::in(1.0);
  }
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

void InsertBatchPostgres::insert_blocks(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {
    "workchain", "shard", "seqno", "root_hash", "file_hash", "mc_block_workchain", "mc_block_shard", "mc_block_seqno",
    "global_id", "version", "after_merge", "before_split", "after_split", "want_merge", "want_split", "key_block",
    "vert_seqno_incr", "flags", "gen_utime", "start_lt", "end_lt", "validator_list_hash_short", "gen_catchain_seqno",
    "min_ref_mc_seqno", "prev_key_block_seqno", "vert_seqno", "master_ref_seqno", "rand_seed", "created_by", "tx_count", "prev_blocks"
  };
  PopulateTableStream stream(txn, "blocks", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  // Prepare data
  for (const auto& task : insert_tasks_) {
    for (const auto& block : task.parsed_block_->blocks_) {
      auto tuple = std::make_tuple(
        block.workchain,
        block.shard,
        block.seqno,
        block.root_hash,
        block.file_hash,
        block.mc_block_workchain,
        block.mc_block_shard,
        block.mc_block_seqno,
        block.global_id,
        block.version,
        block.after_merge,
        block.before_split,
        block.after_split,
        block.want_merge,
        block.want_split,
        block.key_block,
        block.vert_seqno_incr,
        block.flags,
        block.gen_utime,
        block.start_lt,
        block.end_lt,
        block.validator_list_hash_short,
        block.gen_catchain_seqno,
        block.min_ref_mc_seqno,
        block.prev_key_block_seqno,
        block.vert_seqno,
        block.master_ref_seqno,
        block.rand_seed,
        block.created_by,
        block.transactions.size(),
        block.prev_blocks
      );
      stream.insert_row(std::move(tuple));
    }
  }
  stream.finish();
}


void InsertBatchPostgres::insert_shard_state(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {
    "mc_seqno", "workchain", "shard", "seqno"
  };
  PopulateTableStream stream(txn, "shard_state", columns, 1000);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  for (const auto& task : insert_tasks_) {
    for (const auto& shard : task.parsed_block_->shard_state_) {
      auto tuple = std::make_tuple(
        shard.mc_seqno,
        shard.workchain,
        shard.shard,
        shard.seqno
      );
      stream.insert_row(std::move(tuple));
    }
  }
  stream.finish();
}

template<typename... Tuples>
using tuple_cat_t = decltype(std::tuple_cat(std::declval<Tuples>()...));

void InsertBatchPostgres::insert_transactions(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {
    "account", "hash", "lt", "block_workchain", "block_shard", "block_seqno", "mc_block_seqno", "trace_id",
    "prev_trans_hash", "prev_trans_lt", "now", "orig_status", "end_status", "total_fees", "total_fees_extra_currencies",
    "account_state_hash_before", "account_state_hash_after", "descr", "aborted", "destroyed", "credit_first", "is_tock",
    "installed", "storage_fees_collected", "storage_fees_due", "storage_status_change", "credit_due_fees_collected",
    "credit", "credit_extra_currencies", "compute_skipped", "skipped_reason", "compute_success", "compute_msg_state_used",
    "compute_account_activated", "compute_gas_fees", "compute_gas_used", "compute_gas_limit", "compute_gas_credit",
    "compute_mode", "compute_exit_code", "compute_exit_arg", "compute_vm_steps", "compute_vm_init_state_hash",
    "compute_vm_final_state_hash", "action_success", "action_valid", "action_no_funds", "action_status_change",
    "action_total_fwd_fees", "action_total_action_fees", "action_result_code", "action_result_arg", "action_tot_actions",
    "action_spec_actions", "action_skipped_actions", "action_msgs_created", "action_action_list_hash",
    "action_tot_msg_size_cells", "action_tot_msg_size_bits", "bounce", "bounce_msg_size_cells", "bounce_msg_size_bits",
    "bounce_req_fwd_fees", "bounce_msg_fees", "bounce_fwd_fees", "split_info_cur_shard_pfx_len", "split_info_acc_split_depth",
    "split_info_this_addr", "split_info_sibling_addr"
  };

  PopulateTableStream stream(txn, "transactions", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  using storage_ph_tuple = std::tuple<std::optional<td::RefInt256>, std::optional<td::RefInt256>, std::optional<std::string>>;
  auto store_storage_ph = [&](const schema::TrStoragePhase& storage_ph) -> storage_ph_tuple {
    return {storage_ph.storage_fees_collected, storage_ph.storage_fees_due, stringify(storage_ph.status_change)};
  };
  auto store_empty_storage_ph = [&]() -> storage_ph_tuple {
    return {std::nullopt, std::nullopt, std::nullopt};
  };

  using credit_ph_tuple = std::tuple<std::optional<td::RefInt256>, std::optional<td::RefInt256>, std::optional<std::string>>;
  auto store_credit_ph = [](const schema::TrCreditPhase& credit_ph) -> credit_ph_tuple {
    return {credit_ph.due_fees_collected, credit_ph.credit.grams, extra_currencies_to_json_string(credit_ph.credit.extra_currencies)};
  };
  auto store_empty_credit_ph = [&]() -> credit_ph_tuple {
    return {std::nullopt, std::nullopt, std::nullopt};
  };

  using compute_ph_tuple = std::tuple<std::optional<bool>, std::optional<std::string>, std::optional<bool>, std::optional<bool>, std::optional<bool>, 
                                      std::optional<td::RefInt256>, std::optional<uint64_t>, std::optional<uint64_t>, std::optional<size_t>, 
                                      std::optional<int32_t>, std::optional<int32_t>, std::optional<int32_t>, std::optional<uint32_t>, 
                                      std::optional<td::Bits256>, std::optional<td::Bits256>>;
  auto store_compute_ph = [&](const schema::TrComputePhase& compute_ph) -> compute_ph_tuple {
    return std::visit([&](auto&& arg) -> compute_ph_tuple {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, schema::TrComputePhase_skipped>) {
        return {true, stringify(arg.reason), std::nullopt, std::nullopt, std::nullopt, 
                std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
                std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
                std::nullopt, std::nullopt};
      } else if constexpr (std::is_same_v<T, schema::TrComputePhase_vm>) {
        return {false, std::nullopt, arg.success, arg.msg_state_used, arg.account_activated, 
                arg.gas_fees, arg.gas_used, arg.gas_limit, arg.gas_credit, 
                arg.mode, arg.exit_code, arg.exit_arg, arg.vm_steps, 
                arg.vm_init_state_hash, arg.vm_final_state_hash};
      } else {
        UNREACHABLE();
      }
    }, compute_ph);
  };
  auto store_empty_compute_ph = [&]() -> compute_ph_tuple {
    return {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
            std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
            std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
            std::nullopt, std::nullopt};
  };

  using action_ph_tuple = std::tuple<std::optional<bool>, std::optional<bool>, std::optional<bool>, std::optional<std::string>, 
                                     std::optional<td::RefInt256>, std::optional<td::RefInt256>, std::optional<int32_t>, std::optional<int32_t>, 
                                     std::optional<uint16_t>, std::optional<uint16_t>, std::optional<uint16_t>, std::optional<uint16_t>, 
                                     std::optional<td::Bits256>, std::optional<uint64_t>, std::optional<uint64_t>>;
  auto store_action_ph = [&](const schema::TrActionPhase& action) -> action_ph_tuple {
    return {action.success, action.valid, action.no_funds, stringify(action.status_change), 
            action.total_fwd_fees, action.total_action_fees, action.result_code, action.result_arg, 
            action.tot_actions, action.spec_actions, action.skipped_actions, action.msgs_created, 
            action.action_list_hash, action.tot_msg_size.cells, action.tot_msg_size.bits};
  };
  auto store_empty_action_ph = [&]() -> action_ph_tuple {
    return {std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
            std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
            std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
            std::nullopt, std::nullopt, std::nullopt};
  };

  using bounce_ph_tuple = std::tuple<std::optional<std::string>, std::optional<uint64_t>, std::optional<uint64_t>, 
                                     std::optional<td::RefInt256>, std::optional<td::RefInt256>, std::optional<td::RefInt256>>;
  auto store_bounce_ph = [&](const schema::TrBouncePhase& bounce) -> bounce_ph_tuple {
    return std::visit([&](auto&& arg) -> bounce_ph_tuple {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, schema::TrBouncePhase_negfunds>) {
        return {"negfunds", std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt};
      } else if constexpr (std::is_same_v<T, schema::TrBouncePhase_nofunds>) {
        return {"nofunds", arg.msg_size.cells, arg.msg_size.bits, arg.req_fwd_fees, std::nullopt, std::nullopt};
      } else if constexpr (std::is_same_v<T, schema::TrBouncePhase_ok>) {
        return {"ok", arg.msg_size.cells, arg.msg_size.bits, std::nullopt, arg.msg_fees, arg.fwd_fees};
      } else {
        UNREACHABLE();
      }
    }, bounce);
  };
  auto store_empty_bounce_ph = [&]() -> bounce_ph_tuple {
    return {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt};
  };

  using split_info_tuple = std::tuple<std::optional<uint32_t>, std::optional<uint32_t>, std::optional<td::Bits256>, std::optional<td::Bits256>>;
  auto store_split_info = [&](const schema::SplitMergeInfo& split_info) -> split_info_tuple {
    return {split_info.cur_shard_pfx_len, split_info.acc_split_depth, split_info.this_addr, split_info.sibling_addr};
  };
  auto store_empty_split_info = [&]() -> split_info_tuple {
    return {std::nullopt, std::nullopt, std::nullopt, std::nullopt};
  };

  using transaction_descr_begin_tuple = std::tuple<std::string, std::optional<bool>, std::optional<bool>, 
                                                   std::optional<bool>, std::optional<bool>, std::optional<bool>>;

  using transaction_descr = tuple_cat_t<transaction_descr_begin_tuple, storage_ph_tuple, credit_ph_tuple, 
                                        compute_ph_tuple, action_ph_tuple, bounce_ph_tuple, split_info_tuple>;

  auto store_transaction_descr_ord = [&](const schema::TransactionDescr_ord& descr) -> transaction_descr {
    auto begin_tuple = std::make_tuple("ord", descr.aborted, descr.destroyed, descr.credit_first, std::nullopt, std::nullopt);
    auto storage_ph = descr.storage_ph ? store_storage_ph(descr.storage_ph.value()) : store_empty_storage_ph();
    auto credit_ph = descr.credit_ph ? store_credit_ph(descr.credit_ph.value()) : store_empty_credit_ph();
    auto compute_ph = store_compute_ph(descr.compute_ph);
    auto action_ph = descr.action ? store_action_ph(descr.action.value()) : store_empty_action_ph();
    auto bounce_ph = descr.bounce ? store_bounce_ph(descr.bounce.value()) : store_empty_bounce_ph();
    auto split_info = store_empty_split_info();
    return std::tuple_cat(std::move(begin_tuple), std::move(storage_ph), std::move(credit_ph), 
                          std::move(compute_ph), std::move(action_ph), std::move(bounce_ph), std::move(split_info));
  };
  auto store_transaction_descr_storage = [&](const schema::TransactionDescr_storage& descr) -> transaction_descr {
    auto begin_tuple = std::make_tuple("storage", std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt);
    auto storage_ph = store_storage_ph(descr.storage_ph);
    auto credit_ph = store_empty_credit_ph();
    auto compute_ph = store_empty_compute_ph();
    auto action_ph = store_empty_action_ph();
    auto bounce_ph = store_empty_bounce_ph();
    auto split_info = store_empty_split_info();
    return std::tuple_cat(std::move(begin_tuple), std::move(storage_ph), std::move(credit_ph), 
                          std::move(compute_ph), std::move(action_ph), std::move(bounce_ph), std::move(split_info));
  };
  auto store_transaction_descr_tick_tock = [&](const schema::TransactionDescr_tick_tock& descr) -> transaction_descr {
    auto begin_tuple = std::make_tuple("tick_tock", descr.aborted, descr.destroyed, std::nullopt, descr.is_tock, std::nullopt);
    auto storage_ph = store_storage_ph(descr.storage_ph);
    auto credit_ph = store_empty_credit_ph();
    auto compute_ph = store_compute_ph(descr.compute_ph);
    auto action_ph = descr.action ? store_action_ph(descr.action.value()) : store_empty_action_ph();
    auto bounce_ph = store_empty_bounce_ph();
    auto split_info = store_empty_split_info();
    return std::tuple_cat(std::move(begin_tuple), std::move(storage_ph), std::move(credit_ph), 
                          std::move(compute_ph), std::move(action_ph), std::move(bounce_ph), std::move(split_info));
  };
  auto store_transaction_descr_split_prepare = [&](const schema::TransactionDescr_split_prepare& descr) -> transaction_descr {
    auto begin_tuple = std::make_tuple("split_prepare", descr.aborted, descr.destroyed, std::nullopt, std::nullopt, std::nullopt);
    auto storage_ph = descr.storage_ph ? store_storage_ph(descr.storage_ph.value()) : store_empty_storage_ph();
    auto credit_ph = store_empty_credit_ph();
    auto compute_ph = store_compute_ph(descr.compute_ph);
    auto action_ph = descr.action ? store_action_ph(descr.action.value()) : store_empty_action_ph();
    auto bounce_ph = store_empty_bounce_ph();
    auto split_info = store_split_info(descr.split_info);
    return std::tuple_cat(std::move(begin_tuple), std::move(storage_ph), std::move(credit_ph), 
                          std::move(compute_ph), std::move(action_ph), std::move(bounce_ph), std::move(split_info));
  };
  auto store_transaction_descr_split_install = [&](const schema::TransactionDescr_split_install& descr) -> transaction_descr {
    auto begin_tuple = std::make_tuple("split_install", std::nullopt, std::nullopt, std::nullopt, std::nullopt, descr.installed);
    auto storage_pt = store_empty_storage_ph();
    auto credit_ph = store_empty_credit_ph();
    auto compute_ph = store_empty_compute_ph();
    auto action_ph = store_empty_action_ph();
    auto bounce_ph = store_empty_bounce_ph();
    auto split_info = store_split_info(descr.split_info);
    return std::tuple_cat(std::move(begin_tuple), std::move(storage_pt), std::move(credit_ph), 
                          std::move(compute_ph), std::move(action_ph), std::move(bounce_ph), std::move(split_info));
  };
  auto store_transaction_descr_merge_prepare = [&](const schema::TransactionDescr_merge_prepare& descr) -> transaction_descr {
    auto begin_tuple = std::make_tuple("merge_prepare", descr.aborted, std::nullopt, std::nullopt, std::nullopt, std::nullopt);
    auto storage_ph = store_storage_ph(descr.storage_ph);
    auto credit_ph = store_empty_credit_ph();
    auto compute_ph = store_empty_compute_ph();
    auto action_ph = store_empty_action_ph();
    auto bounce_ph = store_empty_bounce_ph();
    auto split_info = store_split_info(descr.split_info);
    return std::tuple_cat(std::move(begin_tuple), std::move(storage_ph), std::move(credit_ph), 
                          std::move(compute_ph), std::move(action_ph), std::move(bounce_ph), std::move(split_info));
  };
  auto store_transaction_descr_merge_install = [&](const schema::TransactionDescr_merge_install& descr) -> transaction_descr {
    auto begin_tuple = std::make_tuple("merge_install", descr.aborted, descr.destroyed, std::nullopt, std::nullopt, std::nullopt);
    auto storage_ph = descr.storage_ph ? store_storage_ph(descr.storage_ph.value()) : store_empty_storage_ph();
    auto credit_ph = descr.credit_ph ? store_credit_ph(descr.credit_ph.value()) : store_empty_credit_ph();
    auto compute_ph = store_compute_ph(descr.compute_ph);
    auto action_ph = descr.action ? store_action_ph(descr.action.value()) : store_empty_action_ph();
    auto bounce_ph = store_empty_bounce_ph();
    auto split_info = store_split_info(descr.split_info);
    return std::tuple_cat(std::move(begin_tuple), std::move(storage_ph), std::move(credit_ph), 
                          std::move(compute_ph), std::move(action_ph), std::move(bounce_ph), std::move(split_info));
  };

  for (const auto& task : insert_tasks_) {
    for (const auto &blk : task.parsed_block_->blocks_) {
      for (const auto& transaction : blk.transactions) {
        auto tx_common_tuple = std::make_tuple(
          transaction.account,
          transaction.hash,
          transaction.lt,
          blk.workchain,
          blk.shard,
          blk.seqno,
          blk.mc_block_seqno,
          transaction.trace_id,
          transaction.prev_trans_hash,
          transaction.prev_trans_lt,
          transaction.now,
          stringify(transaction.orig_status),
          stringify(transaction.end_status),
          transaction.total_fees.grams,
          extra_currencies_to_json_string(transaction.total_fees.extra_currencies),
          transaction.account_state_hash_before,
          transaction.account_state_hash_after
        );
        auto descr_tuple = std::visit([&](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, schema::TransactionDescr_ord>) {
            return store_transaction_descr_ord(arg);
          } else if constexpr (std::is_same_v<T, schema::TransactionDescr_storage>) {
            return store_transaction_descr_storage(arg);
          } else if constexpr (std::is_same_v<T, schema::TransactionDescr_tick_tock>) {
            return store_transaction_descr_tick_tock(arg);
          } else if constexpr (std::is_same_v<T, schema::TransactionDescr_split_prepare>) {
            return store_transaction_descr_split_prepare(arg);
          } else if constexpr (std::is_same_v<T, schema::TransactionDescr_split_install>) {
            return store_transaction_descr_split_install(arg);
          } else if constexpr (std::is_same_v<T, schema::TransactionDescr_merge_prepare>) {
            return store_transaction_descr_merge_prepare(arg);
          } else if constexpr (std::is_same_v<T, schema::TransactionDescr_merge_install>) {
            return store_transaction_descr_merge_install(arg);
          } else {
            UNREACHABLE();
          }
        }, transaction.description);
        auto row_tuple = std::tuple_cat(std::move(tx_common_tuple), std::move(descr_tuple));
        stream.insert_row(std::move(row_tuple));
      }
    }
  }
  stream.finish();
}

void InsertBatchPostgres::insert_messages(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {"tx_hash", "tx_lt", "mc_seqno", "msg_hash", "direction", "trace_id", "source", "destination",
                                                "value", "value_extra_currencies", "fwd_fee", "ihr_fee", "created_lt", "created_at",
                                                "opcode", "ihr_disabled", "bounce", "bounced", "import_fee", "body_hash", "init_state_hash", "msg_hash_norm"};
  PopulateTableStream stream(txn, "messages", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  auto store_message = [&](const schema::Transaction& tx, const schema::Message& msg, const std::string_view& direction) {
    // msg.import_fee is defined by user and can be too large for bigint, so we need to check it
    // and if it is too large, we will insert NULL.
    // TODO: change bigint to numeric
    std::optional<int64_t> import_fee_val;
    if (msg.import_fee) {
      import_fee_val = msg.import_fee.value()->to_long();
      if (import_fee_val.value() == (~0ULL << 63)) {
        LOG(WARNING) << "Import fee of msg " << msg.hash.to_hex() << " is too large for bigint: " << msg.import_fee.value();
        import_fee_val = std::nullopt;
      }
    }
    auto tuple = std::make_tuple(
      tx.hash,
      tx.lt,
      tx.mc_seqno,
      msg.hash,
      direction,
      msg.trace_id,
      msg.source,
      msg.destination,
      msg.value ? std::make_optional(msg.value->grams) : std::nullopt,
      msg.value ? std::make_optional(extra_currencies_to_json_string(msg.value->extra_currencies)) : std::nullopt,
      msg.fwd_fee,
      msg.ihr_fee,
      msg.created_lt,
      msg.created_at,
      msg.opcode,
      msg.ihr_disabled,
      msg.bounce,
      msg.bounced,
      import_fee_val,
      msg.body->get_hash(),
      msg.init_state.not_null() ? std::make_optional(msg.init_state->get_hash()) : std::nullopt,
      (msg.hash_norm.has_value() && msg.hash_norm.value() != msg.hash) ? 
            std::make_optional(msg.hash_norm.value()) : std::nullopt
    );
    stream.insert_row(std::move(tuple));
  };

  std::vector<std::tuple<td::Bits256, std::string_view>> msg_bodies;
  // we lock the message bodies to prevent multiple parallel queries for the same message
  // otherwise it causes deadlocks
  auto lock_msg_body = [&](const td::Bits256& body_hash, const std::string& body_boc) {
    if (msg_bodies_in_progress.find(body_hash) == msg_bodies_in_progress.end()) {
      msg_bodies.push_back({body_hash, body_boc});
      msg_bodies_in_progress.insert(body_hash);
    }
  };

  {
    std::lock_guard<std::mutex> guard(messages_in_progress_mutex);
    for (const auto& task : insert_tasks_) {
      for (const auto &blk : task.parsed_block_->blocks_) {
        for (const auto& transaction : blk.transactions) {
          if (transaction.in_msg) {
            lock_msg_body(transaction.in_msg->body->get_hash().bits(), transaction.in_msg->body_boc);
            if (transaction.in_msg->init_state_boc) {
              lock_msg_body(transaction.in_msg->init_state->get_hash().bits(), transaction.in_msg->init_state_boc.value());
            }
          }
          for (const auto& msg : transaction.out_msgs) {
            lock_msg_body(msg.body->get_hash().bits(), msg.body_boc);
            if (msg.init_state_boc) {
              lock_msg_body(msg.init_state->get_hash().bits(), msg.init_state_boc.value());
            }
          }
        }
      }
    }
  }

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
  stream.finish();

  PopulateTableStream bodies_stream(txn, "message_contents", {"hash", "body"}, 1000, false);
  bodies_stream.setConflictDoNothing();

  for (const auto& [body_hash, body] : msg_bodies) {
    auto tuple = std::make_tuple(body_hash, body);
    bodies_stream.insert_row(std::move(tuple));
  }
  bodies_stream.finish();

  // unlock message bodies
  {
    std::lock_guard<std::mutex> guard(messages_in_progress_mutex);
    for (const auto& [body_hash, body] : msg_bodies) {
      msg_bodies_in_progress.erase(body_hash);
    }
  }
}

void InsertBatchPostgres::insert_account_states(pqxx::work &txn, bool with_copy) {
  PopulateTableStream stream(txn, "account_states", {
    "hash", "account", "balance", "balance_extra_currencies", "account_status", "frozen_hash", "code_hash", "data_hash"
  }, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  for (const auto& task : insert_tasks_) {
    for (const auto& account_state : task.parsed_block_->account_states_) {
      if (account_state.account_status == "nonexist") {
        // nonexist account state is inserted on DB initialization
        continue;
      }
      auto tuple = std::make_tuple(
        account_state.hash,
        account_state.account,
        account_state.balance.grams,
        extra_currencies_to_json_string(account_state.balance.extra_currencies),
        account_state.account_status,
        account_state.frozen_hash,
        account_state.code_hash,
        account_state.data_hash
      );
      stream.insert_row(std::move(tuple));
    }
  }
  
  stream.finish();
}

std::string InsertBatchPostgres::insert_latest_account_states(pqxx::work &txn) {
  std::initializer_list<std::string_view> columns = {
    "account", "account_friendly", "hash", "balance", "balance_extra_currencies", "account_status", "timestamp",
    "last_trans_hash", "last_trans_lt", "frozen_hash", "data_hash", "code_hash", "data_boc", "code_boc"
  };
  PopulateTableStream stream(txn, "latest_account_states", columns, 1000, false);
  stream.setConflictDoUpdate({"account"}, "latest_account_states.last_trans_lt < EXCLUDED.last_trans_lt");

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

  for (const auto& [_, account_state] : latest_account_states) {
    std::optional<std::string> code_str = std::nullopt;
    std::optional<std::string> data_str = std::nullopt;

    if (max_data_depth_ >= 0 && account_state.data.not_null() && (max_data_depth_ == 0 || account_state.data->get_depth() <= max_data_depth_)){
      auto data_res = vm::std_boc_serialize(account_state.data);
      if (data_res.is_ok()){
        data_str = td::base64_encode(data_res.move_as_ok());
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
  return stream.get_str();
}

std::string InsertBatchPostgres::insert_jetton_masters(pqxx::work &txn) {
  std::unordered_map<block::StdAddress, JettonMasterDataV2> jetton_masters;

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

  std::initializer_list<std::string_view> columns = {
    "address", "total_supply", "mintable", "admin_address", "jetton_content", 
    "jetton_wallet_code_hash", "last_transaction_lt", "code_hash", "data_hash"
  };

  PopulateTableStream stream(txn, "jetton_masters", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "jetton_masters.last_transaction_lt < EXCLUDED.last_transaction_lt");

  for (const auto& [addr, jetton_master] : jetton_masters) {
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
  return stream.get_str();
}

std::string InsertBatchPostgres::insert_jetton_wallets(pqxx::work &txn) {
  std::unordered_map<block::StdAddress, JettonWalletDataV2> jetton_wallets;
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

  std::unordered_set<block::StdAddress> known_mintless_masters;

  std::initializer_list<std::string_view> columns = {
    "balance", "address", "owner", "jetton", "last_transaction_lt", "code_hash", "data_hash", "mintless_is_claimed"
  };

  PopulateTableStream stream(txn, "jetton_wallets", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "jetton_wallets.last_transaction_lt < EXCLUDED.last_transaction_lt");

  for (const auto& [addr, jetton_wallet] : jetton_wallets) {
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
  
  std::string result = stream.get_str();
  if (!known_mintless_masters.empty()) {
    PopulateTableStream mintless_stream(txn, "mintless_jetton_masters", {"address", "is_indexed"}, 1000, false);
    mintless_stream.setConflictDoNothing();
  
    for (const auto &addr : known_mintless_masters) {
      auto tuple = std::make_tuple(addr, false);
      mintless_stream.insert_row(std::move(tuple));
    }
    result += mintless_stream.get_str();
  }

  return result;
}

std::string InsertBatchPostgres::insert_nft_collections(pqxx::work &txn) {
  std::unordered_map<block::StdAddress, NFTCollectionDataV2> nft_collections;
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

  std::initializer_list<std::string_view> columns = {
    "address", "next_item_index", "owner_address", "collection_content", "last_transaction_lt", "code_hash", "data_hash"
  };

  PopulateTableStream stream(txn, "nft_collections", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "nft_collections.last_transaction_lt < EXCLUDED.last_transaction_lt");

  for (const auto& [addr, nft_collection] : nft_collections) {
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
  return stream.get_str();
}

std::string InsertBatchPostgres::insert_nft_items(pqxx::work &txn) {
  std::unordered_map<block::StdAddress, NFTItemDataV2> nft_items;
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

  std::initializer_list<std::string_view> columns = {
    "address", "init", "index", "collection_address", "owner_address", "content", "last_transaction_lt", "code_hash", "data_hash"
  };
  
  PopulateTableStream stream(txn, "nft_items", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "nft_items.last_transaction_lt < EXCLUDED.last_transaction_lt");

  for (const auto& [addr, nft_item] : nft_items) {
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
  auto result = stream.get_str();

  std::initializer_list<std::string_view> dns_columns = {
    "nft_item_address", "nft_item_owner", "domain", "dns_next_resolver", "dns_wallet", "dns_site_adnl", "dns_storage_bag_id", "last_transaction_lt"
  };
  PopulateTableStream dns_stream(txn, "dns_entries", dns_columns, 1000, false);
  dns_stream.setConflictDoUpdate({"nft_item_address"}, "dns_entries.last_transaction_lt < EXCLUDED.last_transaction_lt");

  for (const auto& [addr, nft_item] : nft_items) {
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
  result += dns_stream.get_str();
  return result;
}

std::string InsertBatchPostgres::insert_getgems_nft_sales(pqxx::work &txn) {
  std::unordered_map<block::StdAddress, GetGemsNftFixPriceSaleData> nft_sales;
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

  std::initializer_list<std::string_view> columns = {
    "address", "is_complete", "created_at", "marketplace_address", "nft_address", "nft_owner_address", "full_price",
    "marketplace_fee_address", "marketplace_fee", "royalty_address", "royalty_amount", "last_transaction_lt", "code_hash", "data_hash"
  };
  PopulateTableStream stream(txn, "getgems_nft_sales", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "getgems_nft_sales.last_transaction_lt < EXCLUDED.last_transaction_lt");

  for (const auto& [addr, nft_sale] : nft_sales) {
    auto tuple = std::make_tuple(
      nft_sale.address,
      nft_sale.is_complete,
      nft_sale.created_at,
      nft_sale.marketplace_address,
      nft_sale.nft_address,
      nft_sale.nft_owner_address,
      nft_sale.full_price,
      nft_sale.marketplace_fee_address,
      nft_sale.marketplace_fee,
      nft_sale.royalty_address,
      nft_sale.royalty_amount,
      nft_sale.last_transaction_lt,
      nft_sale.code_hash,
      nft_sale.data_hash
    );
    stream.insert_row(std::move(tuple));
  }
  return stream.get_str();
}

std::string InsertBatchPostgres::insert_getgems_nft_auctions(pqxx::work &txn) {
  std::unordered_map<block::StdAddress, GetGemsNftAuctionData> nft_auctions;
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

  std::initializer_list<std::string_view> columns = {"address", "end_flag", "end_time", "mp_addr", "nft_addr", "nft_owner", 
    "last_bid", "last_member", "min_step", "mp_fee_addr", "mp_fee_factor", "mp_fee_base", "royalty_fee_addr", "royalty_fee_factor", 
    "royalty_fee_base", "max_bid", "min_bid", "created_at", "last_bid_at", "is_canceled", "last_transaction_lt", "code_hash", "data_hash"
  };
  PopulateTableStream stream(txn, "getgems_nft_auctions", columns, 1000, false);
  stream.setConflictDoUpdate({"address"}, "getgems_nft_auctions.last_transaction_lt < EXCLUDED.last_transaction_lt");

  for (const auto& [addr, nft_auction] : nft_auctions) {
    auto tuple = std::make_tuple(
      nft_auction.address,
      nft_auction.end,
      nft_auction.end_time,
      nft_auction.mp_addr,
      nft_auction.nft_addr,
      nft_auction.nft_owner,
      nft_auction.last_bid,
      nft_auction.last_member,
      nft_auction.min_step,
      nft_auction.mp_fee_addr,
      nft_auction.mp_fee_factor,
      nft_auction.mp_fee_base,
      nft_auction.royalty_fee_addr,
      nft_auction.royalty_fee_factor,
      nft_auction.royalty_fee_base,
      nft_auction.max_bid,
      nft_auction.min_bid,
      nft_auction.created_at,
      nft_auction.last_bid_at,
      nft_auction.is_canceled,
      nft_auction.last_transaction_lt,
      nft_auction.code_hash,
      nft_auction.data_hash
    );
    stream.insert_row(std::move(tuple));
  }
  return stream.get_str();
}

void InsertBatchPostgres::insert_jetton_transfers(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {
    "tx_hash", "tx_lt", "tx_now", "tx_aborted", "mc_seqno", "query_id", "amount", "source", "destination", "jetton_wallet_address",
    "jetton_master_address", "response_destination", "custom_payload", "forward_ton_amount", "forward_payload", "trace_id"
  };
  PopulateTableStream stream(txn, "jetton_transfers", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  for (const auto& task : insert_tasks_) {
    for (const auto& transfer : task.parsed_block_->get_events<JettonTransfer>()) {
      auto custom_payload_boc_r = convert::to_bytes(transfer.custom_payload);
      auto custom_payload_boc = custom_payload_boc_r.is_ok() ? custom_payload_boc_r.move_as_ok() : std::nullopt;

      auto forward_payload_boc_r = convert::to_bytes(transfer.forward_payload);
      auto forward_payload_boc = forward_payload_boc_r.is_ok() ? forward_payload_boc_r.move_as_ok() : std::nullopt;

      auto tuple = std::make_tuple(
        transfer.transaction_hash,
        transfer.transaction_lt,
        transfer.transaction_now,
        transfer.transaction_aborted,
        transfer.mc_seqno,
        transfer.query_id,
        transfer.amount,
        transfer.source,
        transfer.destination,
        transfer.jetton_wallet,
        transfer.jetton_master,
        transfer.response_destination,
        custom_payload_boc,
        transfer.forward_ton_amount,
        forward_payload_boc,
        transfer.trace_id
      );
      stream.insert_row(std::move(tuple));
    }
  }
  stream.finish();
}

void InsertBatchPostgres::insert_jetton_burns(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {
    "tx_hash", "tx_lt", "tx_now", "tx_aborted", "mc_seqno", "query_id", "owner", "jetton_wallet_address", "jetton_master_address",
    "amount", "response_destination", "custom_payload", "trace_id"
  };
  PopulateTableStream stream(txn, "jetton_burns", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  for (const auto& task : insert_tasks_) {
    for (const auto& burn : task.parsed_block_->get_events<JettonBurn>()) {
      auto custom_payload_boc_r = convert::to_bytes(burn.custom_payload);
      auto custom_payload_boc = custom_payload_boc_r.is_ok() ? custom_payload_boc_r.move_as_ok() : std::nullopt;

      auto tuple = std::make_tuple(
        burn.transaction_hash,
        burn.transaction_lt,
        burn.transaction_now,
        burn.transaction_aborted,
        burn.mc_seqno,
        burn.query_id,
        burn.owner,
        burn.jetton_wallet,
        burn.jetton_master,
        burn.amount,
        burn.response_destination,
        custom_payload_boc,
        burn.trace_id
      );

      stream.insert_row(std::move(tuple));
    }
  }
  stream.finish();
}

void InsertBatchPostgres::insert_nft_transfers(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {
    "tx_hash", "tx_lt", "tx_now", "tx_aborted", "mc_seqno", "query_id", "nft_item_address", "nft_item_index", "nft_collection_address",
    "old_owner", "new_owner", "response_destination", "custom_payload", "forward_amount", "forward_payload", "trace_id"
  };
  PopulateTableStream stream(txn, "nft_transfers", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  for (const auto& task : insert_tasks_) {
    for (const auto& transfer : task.parsed_block_->get_events<NFTTransfer>()) {
      auto custom_payload_boc_r = convert::to_bytes(transfer.custom_payload);
      auto custom_payload_boc = custom_payload_boc_r.is_ok() ? custom_payload_boc_r.move_as_ok() : std::nullopt;

      auto forward_payload_boc_r = convert::to_bytes(transfer.forward_payload);
      auto forward_payload_boc = forward_payload_boc_r.is_ok() ? forward_payload_boc_r.move_as_ok() : std::nullopt;

      auto tuple = std::make_tuple(
        transfer.transaction_hash,
        transfer.transaction_lt,
        transfer.transaction_now,
        transfer.transaction_aborted,
        transfer.mc_seqno,
        transfer.query_id,
        transfer.nft_item,
        transfer.nft_item_index,
        transfer.nft_collection,
        transfer.old_owner,
        transfer.new_owner,
        transfer.response_destination,
        custom_payload_boc,
        transfer.forward_amount,
        forward_payload_boc,
        transfer.trace_id
      );
      stream.insert_row(std::move(tuple));
    }
  }
  stream.finish();
}

void InsertBatchPostgres::insert_traces(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = { "trace_id", "external_hash", "mc_seqno_start", "mc_seqno_end", 
    "start_lt", "start_utime", "end_lt", "end_utime", "state", "pending_edges_", "edges_", "nodes_" };

  PopulateTableStream stream(txn, "traces", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoUpdate({"trace_id"}, "traces.end_lt < EXCLUDED.end_lt");
  }

  std::unordered_map<td::Bits256, schema::Trace> traces_map;
  for (const auto& task : insert_tasks_) {
    for(auto &trace : task.parsed_block_->traces_) {
      if (trace.state == schema::Trace::State::complete) {
        auto it = traces_map.find(trace.trace_id);
        if (it != traces_map.end() && it->second.end_lt < trace.end_lt) {
          it->second = trace;
        } else {
          traces_map.insert({trace.trace_id, trace});
        }
      }
    }
  }
  for(auto &[_, trace] : traces_map) {
    auto tuple = std::make_tuple(
      trace.trace_id,
      trace.external_hash,
      trace.mc_seqno_start,
      trace.mc_seqno_end,
      trace.start_lt,
      trace.start_utime,
      trace.end_lt,
      trace.end_utime,
      stringify(trace.state),
      trace.pending_edges_,
      trace.edges_,
      trace.nodes_
    );
    stream.insert_row(std::move(tuple));
  }
  stream.finish();
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

    exec_query("create type account_status_type as enum ('uninit', 'frozen', 'active', 'nonexist');");
    exec_query("create type blockid as (workchain integer, shard bigint, seqno integer);");
    exec_query("create type blockidext as (workchain integer, shard bigint, seqno integer, root_hash tonhash, file_hash tonhash);");
    exec_query("create type bounce_type as enum ('negfunds', 'nofunds', 'ok');");
    exec_query("create type descr_type as enum ('ord', 'storage', 'tick_tock', 'split_prepare', 'split_install', 'merge_prepare', 'merge_install');");
    exec_query("create type msg_direction as enum ('out', 'in');");
    exec_query("create type skipped_reason_type as enum ('no_state', 'bad_state', 'no_gas', 'suspended');");
    exec_query("create type status_change_type as enum ('unchanged', 'frozen', 'deleted');");
    exec_query("create type trace_classification_state as enum ('unclassified', 'failed', 'ok', 'broken');");
    exec_query("create type trace_state as enum ('complete', 'pending', 'broken');");
    exec_query("create type change_dns_record_details as (key varchar, value_schema varchar, value varchar, flags integer);");
    exec_query("create type dex_deposit_liquidity_details as (dex varchar, amount1 numeric, amount2 numeric, asset1 varchar, asset2 varchar, user_jetton_wallet_1 varchar, user_jetton_wallet_2 varchar, lp_tokens_minted numeric);");
    exec_query("create type dex_transfer_details as (amount numeric, asset tonaddr, source tonaddr, destination tonaddr, source_jetton_wallet tonaddr, destination_jetton_wallet tonaddr);");
    exec_query("create type dex_withdraw_liquidity_details as (dex varchar, amount1 numeric, amount2 numeric, asset1_out varchar, asset2_out varchar, user_jetton_wallet_1 varchar, user_jetton_wallet_2 varchar, dex_jetton_wallet_1 varchar, dex_jetton_wallet_2 varchar, lp_tokens_burnt numeric, dex_wallet_1 varchar, dex_wallet_2 varchar);");
    exec_query("create type jetton_transfer_details as(response_destination tonaddr, forward_amount numeric, query_id numeric, custom_payload text, forward_payload text, comment text, is_encrypted_comment boolean);");
    exec_query("create type nft_mint_details as (nft_item_index numeric);");
    exec_query("create type nft_transfer_details as(is_purchase boolean, price numeric, query_id numeric, custom_payload text, forward_payload text, forward_amount numeric, response_destination tonaddr, nft_item_index numeric);");
    exec_query("create type peer_swap_details as(asset_in tonaddr, amount_in numeric, asset_out tonaddr, amount_out numeric);");
    exec_query("create type jetton_swap_details as (dex varchar, sender tonaddr, dex_incoming_transfer dex_transfer_details, dex_outgoing_transfer dex_transfer_details, peer_swaps peer_swap_details[]);");
    exec_query("create type staking_details as (provider varchar, ts_nft varchar, tokens_burnt numeric, tokens_minted numeric);");
    exec_query("create type ton_transfer_details as (content text, encrypted boolean);");
    exec_query("create type multisig_create_order_details as (query_id numeric, order_seqno numeric, is_created_by_signer boolean, is_signed_by_creator boolean, creator_index numeric, expiration_date numeric, order_boc varchar);");
    exec_query("create type multisig_approve_details as (signer_index numeric, exit_code numeric);");
    exec_query("create type multisig_execute_details as (query_id numeric, order_seqno numeric, expiration_date numeric, approvals_num numeric, signers_hash varchar, order_boc varchar);");
    exec_query("create type vesting_send_message_details as (query_id numeric, message_boc varchar);");
    exec_query("create type vesting_add_whitelist_details as (query_id numeric, accounts_added varchar[]);");
    exec_query("create type evaa_supply_details as (sender_jetton_wallet varchar, recipient_jetton_wallet varchar, master_jetton_wallet varchar, master varchar, asset_id varchar, is_ton boolean);");
    exec_query("create type evaa_withdraw_details as (sender_jetton_wallet varchar, recipient_jetton_wallet varchar, master_jetton_wallet varchar, master varchar, fail_reason varchar, asset_id varchar);");
    exec_query("create type evaa_liquidate_details as (fail_reason text, debt_amount numeric, asset_id varchar);");
    exec_query("create type jvault_claim_details as (claimed_jettons varchar[], claimed_amounts numeric[]);");
    exec_query("create type jvault_stake_details as (period numeric, minted_stake_jettons numeric, stake_wallet varchar);");
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
      "total_fees_extra_currencies jsonb, "
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
      "credit_extra_currencies jsonb, "
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
      "mc_seqno integer, "
      "msg_hash tonhash, "
      "direction msg_direction, "
      "trace_id tonhash, "
      "source tonaddr, "
      "destination tonaddr, "
      "value bigint, "
      "value_extra_currencies jsonb, "
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
      "balance_extra_currencies jsonb, "
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
      "balance_extra_currencies jsonb, "
      "account_status account_status_type, "
      "timestamp integer, "
      "last_trans_hash tonhash, "
      "last_trans_lt bigint, "
      "frozen_hash tonhash, "
      "data_hash tonhash, "
      "code_hash tonhash, "
      "data_boc text, "
      "code_boc text) with (autovacuum_vacuum_scale_factor = 0.03);\n"
    );

    query += (
      "create table if not exists address_book ("
      "address tonaddr not null primary key, "
      "code_hash tonhash, "
      "domain varchar);\n"
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
      "mc_seqno integer, "
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
      "create table if not exists mintless_jetton_masters ("
      "id bigserial,"
      "address tonaddr not null primary key,"
      "is_indexed boolean,"
      "custom_payload_api_uri character varying[]);\n"
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
      "mc_seqno integer, "
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
      "mc_seqno integer, "
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
      "trace_id tonhash not null primary key, "
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
      "classification_state trace_classification_state default 'unclassified'"
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
      "dex_withdraw_liquidity_data dex_withdraw_liquidity_details, "
      "dex_deposit_liquidity_data dex_deposit_liquidity_details, "
      "staking_data staking_details, "
      "trace_end_lt bigint, "
      "trace_external_hash tonhash, "
      "trace_end_utime integer, "
      "mc_seqno_end integer, "
      "trace_mc_seqno_end integer, "
      "multisig_create_order_data multisig_create_order_details, "
      "multisig_approve_data multisig_approve_details, "
      "multisig_execute_data multisig_execute_details, "
      "vesting_send_message_data vesting_send_message_details, "
      "vesting_add_whitelist_data vesting_add_whitelist_details, "
      "evaa_supply_data evaa_supply_details, "
      "evaa_withdraw_data evaa_withdraw_details, "
      "evaa_liquidate_data evaa_liquidate_details, "
      "jvault_claim_data jvault_claim_details, "
      "jvault_stake_data jvault_stake_details, "
      "parent_action_id varchar, "
      "ancestor_type varchar[] default '{}', "
      "value_extra_currencies jsonb default '{}'::jsonb, "
      "primary key (trace_id, action_id)"
      ") with (autovacuum_vacuum_scale_factor = 0.03);\n"
    );

    query += (
      "create table if not exists action_accounts ("
      "action_id tonhash not null, "
      "trace_id tonhash not null, "
      "account tonaddr not null, "
      "trace_end_lt bigint not null, "
      "action_end_lt bigint not null, "
      "trace_end_utime integer, "
      "action_end_utime bigint, "
      "primary key (account, trace_end_lt, trace_id, action_end_lt, action_id)"
      ") with (autovacuum_vacuum_scale_factor = 0.03);\n"
    );

    query += (
      "create table if not exists dns_entries ("
      "nft_item_address tonaddr not null primary key, "
      "nft_item_owner tonaddr, "
      "domain varchar, "
      "dns_next_resolver tonaddr, "
      "dns_wallet tonaddr, "
      "dns_site_adnl varchar(64), "
      "dns_storage_bag_id varchar(64), "
      "last_transaction_lt bigint);\n"
    );

    query += "create table if not exists blocks_classified (mc_seqno integer not null primary key);\n";

    query += (
      "create unlogged table if not exists _classifier_tasks ("
      "id serial, "
      "mc_seqno integer, "
      "trace_id tonhash, "
      "pending boolean, "
      "claimed_at timestamp, "
      "start_after timestamp);\n"
    );

    query += (
      "create unlogged table if not exists _classifier_failed_traces ("
      "id serial, "
      "trace_id tonhash, "
      "broken boolean, "
      "error varchar);\n"
    );

    query += (
      "create table if not exists address_metadata ("
			"address varchar not null, "
			"type varchar not null, "
			"valid boolean default true, "
			"name varchar, "
			"description varchar, "
			"extra jsonb, "
			"symbol varchar, "
			"image varchar, "
			"updated_at bigint, "
			"expires_at bigint, "
			"constraint address_metadata_pk primary key (address, type));\n"
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
      "create index if not exists trace_unclassified_index on traces (state, start_lt) include (trace_id, nodes_) where (classification_state = 'unclassified'::trace_classification_state);\n"
      "create index if not exists _classifier_tasks_mc_seqno_idx on _classifier_tasks (mc_seqno desc);\n"
    );

    LOG(DEBUG) << query;
    txn.exec0(query);
    txn.commit();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error while creating required indexes in database: " << e.what();
    std::_Exit(1);
  }

  // create functions and triggers
  LOG(INFO) << "Creating SQL functions and triggers...";
  try {
    pqxx::connection c(credential_.get_connection_string());
    pqxx::work txn(c);

    std::string query = "";

    // rebuild broken traces
    query += (
      "create or replace function trace_get_root(transaction_hash tonhash)  "
      "returns tonhash parallel safe language plpgsql as $$ "
      "declare parent tonhash; current tonhash; msg tonhash; "
      "begin\n"
      "    current := transaction_hash;\n"
      "    parent := transaction_hash;\n"
      "    while parent is not NULL loop\n"
      "        select msg_hash into msg from messages where tx_hash = current and direction = 'in';\n"
      "        select tx_hash into parent from messages where msg_hash = msg and direction = 'out';\n"
      "        if parent is not null then current := parent; end if;\n"
      "    end loop;\n"
      "    return current;\n"
      "end; $$;\n"
      "create or replace function rebuild_trace(root_tx_hash tonhash) "
      "returns tonhash language plpgsql as $$ "
      "declare "
      "new_trace_id tonhash; "
      "flag bool; "
      "txs tonhash[]; "
      "new_trace_external_hash tonhash; "
      "new_trace_start_seqno int; "
      "new_trace_start_lt bigint; "
      "new_trace_start_utime int; "
      "new_trace_end_seqno int; "
      "new_trace_end_lt bigint; "
      "new_trace_end_utime int; "
      "new_trace_nodes int; "
      "new_trace_edges int; "
      "new_trace_pending_edges int;\n"
      "begin\n"
      "    new_trace_id := root_tx_hash;\n"
      "    select msg_hash,\n"
      "        source is null or source = '0:0000000000000000000000000000000000000000000000000000000000000000'\n"
      "            or source = '-1:0000000000000000000000000000000000000000000000000000000000000000'\n"
      "    into new_trace_external_hash, flag\n"
      "    from messages where tx_hash = root_tx_hash and direction = 'in';\n"
      "    if not flag then\n"
      "        new_trace_id := trace_get_root(root_tx_hash);\n"
      "        insert into broken_traces_roots(tx_hash)\n"
      "        values (new_trace_id)\n"
      "        on conflict do nothing;\n"
      "    end if;\n\n"
      "    -- get transactions\n"
      "    with recursive cte as (\n"
      "        select hash as tx_hash from transactions where hash = $1\n"
      "        union all\n"
      "        select M2.tx_hash\n"
      "        from messages M1\n"
      "            join cte TT on TT.tx_hash = M1.tx_hash\n"
      "            join messages M2\n"
      "            on M1.msg_hash = M2.msg_hash and M1.direction = 'out' and\n"
      "                M2.direction = 'in'\n"
      "    )\n"
      "    select array_agg(cte.tx_hash) into txs from cte;\n\n"
      "    -- get meta and update transactions\n"
      "    update transactions set trace_id = new_trace_id where hash = any(txs);\n"
      "    update messages set trace_id = new_trace_id where tx_hash = any(txs);\n\n"
      "    select\n"
      "        count(*), min(mc_block_seqno), max(mc_block_seqno), min(lt), max(lt), min(now), max(now)\n"
      "    into new_trace_nodes, new_trace_start_seqno, new_trace_end_seqno, new_trace_start_lt,\n"
      "        new_trace_end_lt, new_trace_start_utime, new_trace_end_utime\n"
      "    from transactions where trace_id = new_trace_id;\n\n"
      "    -- build edges\n"
      "    delete from traces where trace_id = new_trace_id;\n\n"
      "    insert into traces(trace_id) values (new_trace_id) on conflict do nothing;\n\n"
      "    select count(*), sum((incomplete)::int) into new_trace_edges, new_trace_pending_edges\n"
      "        from (select\n"
      "        new_trace_id as trace_id, msg_hash,\n"
      "        max(case when direction = 'out' then tx_hash end) as left_tx,\n"
      "        max(case when direction = 'in' then tx_hash end) as right_tx,\n"
      "        bool_or(source is not NULL and source = '0:0000000000000000000000000000000000000000000000000000000000000000'\n"
      "            and source = '-1:0000000000000000000000000000000000000000000000000000000000000000')\n"
      "        and max(case when direction = 'out' then tx_hash end) is null as incomplete,\n"
      "        bool_or(destination is not NULL) and max(case when direction = 'in' then tx_hash end) is null as broken\n"
      "    from messages\n"
      "    where trace_id = new_trace_id\n"
      "    group by trace_id, msg_hash) as A where A.trace_id = new_trace_id;\n\n"
      "    update traces set\n"
      "        external_hash=new_trace_external_hash,\n"
      "        mc_seqno_start=new_trace_start_seqno,\n"
      "        mc_seqno_end=new_trace_end_seqno,\n"
      "        start_lt=new_trace_start_lt,\n"
      "        start_utime=new_trace_start_utime,\n"
      "        end_lt=new_trace_end_lt,\n"
      "        end_utime=new_trace_end_utime,\n"
      "        state=(case when new_trace_pending_edges = 0 then 'complete' else 'pending' end)::trace_state,\n"
      "        pending_edges_=new_trace_pending_edges,\n"
      "        edges_=new_trace_edges,\n"
      "        nodes_=new_trace_nodes,\n"
      "        classification_state='unclassified'\n"
      "    where trace_id = new_trace_id;\n"
      "    return root_tx_hash;\n"
      "end $$;\n"
    );

    // classifier dispatcher trigger
    query += (
      "create or replace function on_new_mc_block_func() "
      "returns trigger language plpgsql as $$ "
      "begin\n"
      "insert into _classifier_tasks(mc_seqno, start_after)\n"
      "values (NEW.seqno, now() + interval '1 seconds');\n"
      "return null; \n"
      "end; $$;\n"
      "create or replace trigger on_new_mc_block "
      "after insert on blocks for each row when (new.workchain = '-1'::integer) "
      "execute procedure on_new_mc_block_func();\n"
    );

    
    LOG(DEBUG) << query;
    txn.exec0(query);
    txn.commit();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error while creating SQL functions and triggers in database: " << e.what();
    std::_Exit(1);
  }

  // some migrations
  if (run_migrations_) {
    LOG(INFO) << "Running some migrations...";
    try {
      pqxx::connection c(credential_.get_connection_string());
      pqxx::work txn(c);

      std::string query = "";
      
      query += (
        "alter table jetton_wallets add column if not exists mintless_is_claimed boolean;\n"
        "alter table jetton_wallets add column if not exists mintless_amount numeric;\n"
        "alter table jetton_wallets add column if not exists mintless_start_from bigint;\n"
        "alter table jetton_wallets add column if not exists mintless_expire_at bigint;\n"
        "alter table mintless_jetton_masters add column if not exists custom_payload_api_uri varchar[];\n"

        "alter table transactions add column if not exists total_fees_extra_currencies jsonb;\n"
        "alter table transactions add column if not exists credit_extra_currencies jsonb;\n"
        "alter table messages add column if not exists value_extra_currencies jsonb;\n"
        "alter table account_states add column if not exists balance_extra_currencies jsonb;\n"
        "alter table latest_account_states add column if not exists balance_extra_currencies jsonb;\n"

        "alter table messages add column if not exists msg_hash_norm tonhash;\n"

        "alter table actions add column if not exists parent_action_id varchar;\n"
        "alter table actions add column if not exists ancestor_type varchar[] default '{}';\n"
        "alter table actions add column if not exists multisig_create_order_data multisig_create_order_details;\n"
        "alter table actions add column if not exists multisig_approve_data multisig_approve_details;\n"
        "alter table actions add column if not exists multisig_execute_data multisig_execute_details;\n"
        "alter table actions add column if not exists vesting_send_message_data vesting_send_message_details;\n"
        "alter table actions add column if not exists vesting_add_whitelist_data vesting_add_whitelist_details;\n"
        "alter table actions add column if not exists evaa_supply_data evaa_supply_details;\n"
        "alter table actions add column if not exists evaa_withdraw_data evaa_withdraw_details;\n"
        "alter table actions add column if not exists evaa_liquidate_data evaa_liquidate_details;\n"
        "alter table actions add column if not exists jvault_claim_data jvault_claim_details;\n"
        "alter table actions add column if not exists jvault_stake_data jvault_stake_details;\n"
      );

      LOG(DEBUG) << query;
      txn.exec0(query);
      txn.commit();
    } catch (const std::exception &e) {
      LOG(ERROR) << "Error while running some migrations in database: " << e.what();
      std::_Exit(1);
    }
  } else {
    LOG(WARNING) << "Skipping migrations!";
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
        "create index if not exists blocks_index_1 on blocks (gen_utime);\n"
        "create index if not exists blocks_index_2 on blocks (mc_block_seqno);\n"
        "create index if not exists blocks_index_3 on blocks (seqno) where (workchain = '-1'::integer);\n"
        "create index if not exists blocks_index_4 on blocks (start_lt);\n"
        "create index if not exists dns_entries_index_3 on dns_entries (dns_wallet, length(domain));\n"
        "create index if not exists dns_entries_index_4 on dns_entries (nft_item_owner, length(domain)) include (domain) where ((nft_item_owner)::text = (dns_wallet)::text);\n"
        "create index if not exists jetton_masters_index_1 on jetton_masters (admin_address, id);\n"
        "create index if not exists jetton_masters_index_2 on jetton_masters (id);\n"
        "create index if not exists jetton_wallets_index_1 on jetton_wallets (owner, id);\n"
        "create index if not exists jetton_wallets_index_2 on jetton_wallets (jetton, id);\n"
        "create index if not exists jetton_wallets_index_3 on jetton_wallets (id);\n"
        "create index if not exists jetton_wallets_index_4 on jetton_wallets (jetton asc, balance desc);\n"
        "create index if not exists jetton_wallets_index_5 on jetton_wallets (owner asc, balance desc);\n"
        "create index if not exists latest_account_states_index_1 on latest_account_states (balance desc);\n"
        "create index if not exists latest_account_states_address_book_index on latest_account_states (account) include (account_friendly, code_hash, account_status);\n"
        "create index if not exists latest_account_states_index_2 on latest_account_states (id);\n"
        "create index if not exists nft_collections_index_1 on nft_collections (owner_address, id);\n"
        "create index if not exists nft_collections_index_2 on nft_collections (id);\n"
        "create index if not exists nft_items_index_1 on nft_items (collection_address, index);\n"
        "create index if not exists nft_items_index_2 on nft_items (owner_address, collection_address, index);\n"
        "create index if not exists nft_items_index_3 on nft_items (id);\n"
        "create index if not exists trace_unclassified_index on traces (state, start_lt) include (trace_id, nodes_) where (classification_state = 'unclassified'::trace_classification_state);\n"
        "create index if not exists traces_index_1 on traces (state);\n"
        "create index if not exists trace_index_2a on traces (mc_seqno_end);\n"
        "create index if not exists traces_index_7 on traces (classification_state);\n"
        "create index if not exists traces_index_3 on traces (end_lt desc, trace_id desc);\n"
        "create index if not exists traces_index_4 on traces (end_utime desc, trace_id desc);\n"
        "create index if not exists jetton_burns_index_1 on jetton_burns (owner, tx_now, tx_lt);\n"
        "create index if not exists jetton_burns_index_2 on jetton_burns (owner, tx_lt);\n"
        "create index if not exists jetton_burns_index_3 on jetton_burns (jetton_wallet_address, tx_now, tx_lt);\n"
        "create index if not exists jetton_burns_index_4 on jetton_burns (jetton_wallet_address, tx_lt);\n"
        "create index if not exists jetton_burns_index_5 on jetton_burns (jetton_master_address, tx_now, tx_lt);\n"
        "create index if not exists jetton_burns_index_6 on jetton_burns (jetton_master_address, tx_lt);\n"
        "create index if not exists jetton_burns_index_7 on jetton_burns (tx_now, tx_lt);\n"
        "create index if not exists jetton_burns_index_8 on jetton_burns (tx_lt);\n"
        "create index if not exists jetton_transfers_index_1 on jetton_transfers (source, tx_now);\n"
        "create index if not exists jetton_transfers_index_2 on jetton_transfers (source, tx_lt);\n"
        "create index if not exists jetton_transfers_index_3 on jetton_transfers (destination, tx_lt);\n"
        "create index if not exists jetton_transfers_index_4 on jetton_transfers (destination, tx_now);\n"
        "create index if not exists jetton_transfers_index_6 on jetton_transfers (jetton_wallet_address, tx_lt);\n"
        "create index if not exists jetton_transfers_index_7 on jetton_transfers (jetton_master_address, tx_now);\n"
        "create index if not exists jetton_transfers_index_8 on jetton_transfers (jetton_master_address, tx_lt);\n"
        "create index if not exists jetton_transfers_index_9 on jetton_transfers (tx_now, tx_lt);\n"
        "create index if not exists jetton_transfers_index_10 on jetton_transfers (tx_lt);\n"
        "create index if not exists messages_index_1 on messages (msg_hash);\n"
        "create index if not exists messages_index_5 on messages (trace_id, tx_lt);\n"
        "create index if not exists messages_index_2 on messages (source, created_lt);\n"
        "create index if not exists messages_index_6 on messages (opcode, created_lt);\n"
        "create index if not exists messages_index_8 on messages (created_at, msg_hash);\n"
        "create index if not exists messages_index_7 on messages (created_lt, msg_hash);\n"
        "create index if not exists messages_index_3 on messages (destination, created_lt);\n"
        "create index if not exists messages_index_4 on messages (body_hash);\n"
        "create index if not exists messages_index_9 on messages (msg_hash_norm) where msg_hash_norm is not null;\n"
        "create index if not exists nft_transfers_index_2 on nft_transfers (nft_item_address, tx_lt);\n"
        "create index if not exists nft_transfers_index_3 on nft_transfers (nft_collection_address, tx_now);\n"
        "create index if not exists nft_transfers_index_4 on nft_transfers (nft_collection_address, tx_lt);\n"
        "create index if not exists nft_transfers_index_5 on nft_transfers (old_owner, tx_lt);\n"
        "create index if not exists nft_transfers_index_7 on nft_transfers (new_owner, tx_lt);\n"
        "create index if not exists nft_transfers_index_9 on nft_transfers (tx_lt);\n"
        "create index if not exists nft_transfers_index_10 on nft_transfers (tx_now, tx_lt);\n"
        "create index if not exists transactions_index_1 on transactions (block_workchain, block_shard, block_seqno);\n"
        "create index if not exists transactions_index_2 on transactions (lt);\n"
        "create index if not exists transactions_index_3 on transactions (now, lt);\n"
        "create index if not exists transactions_index_4 on transactions (account, lt);\n"
        "create index if not exists transactions_index_5 on transactions (account, now, lt);\n"
        "create index if not exists transactions_index_6 on transactions (hash);\n"
        "create index if not exists transactions_index_8 on transactions (mc_block_seqno, lt);\n"
        "create index if not exists transactions_index_7 on transactions (trace_id, lt);\n"
        "create index if not exists _classifier_tasks_mc_seqno_idx on _classifier_tasks (mc_seqno desc);\n"
        "create index if not exists actions_index_3 on actions (end_lt);\n"
        "create index if not exists actions_index_2 on actions (action_id);\n"
        "create index if not exists actions_index_1 on actions (trace_id, start_lt, end_lt);\n"
        "create index if not exists actions_index_4 on actions (trace_end_lt);\n"
        "create index if not exists actions_index_5 on actions (trace_mc_seqno_end);\n"
        "create index if not exists action_accounts_index_1 on action_accounts (action_id);\n"
        "create index if not exists action_accounts_index_2 on action_accounts (trace_id, action_id);\n"
        "create index if not exists action_accounts_index_3 on action_accounts (account, trace_end_utime, trace_id, action_end_utime, action_id);"
      );

      LOG(DEBUG) << query;
      txn.exec0(query);
      txn.commit();
    } catch (const std::exception &e) {
      LOG(ERROR) << "Error while creating indexes in database: " << e.what();
      std::_Exit(1);
    }
  } else {
    LOG(WARNING) << "Skipping creation of indexes!";
  }

  LOG(INFO) << "Database is ready!";

  // if success
  alarm_timestamp() = td::Timestamp::in(1.0);
}

void InsertManagerPostgres::set_max_data_depth(std::int32_t value) {
  LOG(INFO) << "InsertManagerPostgres max_data_depth set to " << value; 
  max_data_depth_ = value;
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
