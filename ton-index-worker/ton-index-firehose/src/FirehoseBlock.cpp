#include "FirehoseBlock.h"

#include <algorithm>
#include <limits>
#include <map>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/util/json_util.h>
#include "td/utils/base64.h"
#include "td/utils/check.h"
#include "td/utils/misc.h"
#include "crypto/vm/boc.h"
#include "crypto/vm/cells/CellBuilder.h"
#include "crypto/vm/dict.h"
#include "convert-utils.h"
#include "DataParser.h"
#include "sf/ton/type/v1/type.pb.h"

namespace pb = sf::ton::type::v1;

// Field mappings mirror InsertBatchPostgres, so that a block carries what API v3
// returns for the same masterchain seqno.
namespace {

template <class... Ts>
struct overloaded : Ts... {
  using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

std::string bytes(const td::Bits256& value) {
  return value.as_slice().str();
}

std::uint64_t coins(const td::RefInt256& value) {
  return static_cast<std::uint64_t>(value->to_long());
}

std::string stringify(schema::AccountStatus status) {
  switch (status) {
    case schema::AccountStatus::frozen: return "frozen";
    case schema::AccountStatus::uninit: return "uninit";
    case schema::AccountStatus::active: return "active";
    case schema::AccountStatus::nonexist: return "nonexist";
  }
  UNREACHABLE();
}

std::string stringify(schema::AccStatusChange status_change) {
  switch (status_change) {
    case schema::AccStatusChange::acst_unchanged: return "unchanged";
    case schema::AccStatusChange::acst_frozen: return "frozen";
    case schema::AccStatusChange::acst_deleted: return "deleted";
  }
  UNREACHABLE();
}

std::string stringify(schema::ComputeSkipReason reason) {
  switch (reason) {
    case schema::ComputeSkipReason::cskip_no_state: return "no_state";
    case schema::ComputeSkipReason::cskip_bad_state: return "bad_state";
    case schema::ComputeSkipReason::cskip_no_gas: return "no_gas";
    case schema::ComputeSkipReason::cskip_suspended: return "suspended";
  }
  UNREACHABLE();
}

class Converter {
 public:
  explicit Converter(const ParsedBlock& parsed_block) : parsed_block_(parsed_block) {
    for (const auto& state : parsed_block.account_states_) {
      block_end_states_.emplace(state.hash, &state);
    }
  }

  td::Result<pb::Block> convert() {
    pb::Block out;
    const schema::Block* mc_block = nullptr;
    for (const auto& blk : parsed_block_.blocks_) {
      bool is_masterchain = blk.workchain == ton::masterchainId;
      TRY_STATUS(fill_block_data(is_masterchain ? out.mutable_masterchain_block() : out.add_shard_blocks(), blk));
      if (is_masterchain) {
        mc_block = &blk;
      }
    }
    if (mc_block == nullptr) {
      return td::Status::Error("masterchain block is missing");
    }
    std::sort(out.mutable_shard_blocks()->begin(), out.mutable_shard_blocks()->end(),
              [](const pb::BlockData& a, const pb::BlockData& b) {
                return std::make_tuple(a.header().workchain(), a.header().shard(), a.header().seqno()) <
                       std::make_tuple(b.header().workchain(), b.header().shard(), b.header().seqno());
              });

    for (const auto& shard : parsed_block_.shard_state_) {
      fill_block_id(out.add_shards(), shard.workchain, shard.shard, shard.seqno);
    }
    std::sort(out.mutable_shards()->begin(), out.mutable_shards()->end(),
              [](const pb::BlockId& a, const pb::BlockId& b) {
                return std::make_tuple(a.workchain(), a.shard(), a.seqno()) <
                       std::make_tuple(b.workchain(), b.shard(), b.seqno());
              });

    for (const auto& trace : parsed_block_.traces_) {
      if (trace.state != schema::Trace::State::pending) {
        fill_trace(out.add_traces(), trace);
      }
    }
    // traces_ is filled from a hash map
    std::sort(out.mutable_traces()->begin(), out.mutable_traces()->end(),
              [](const pb::Trace& a, const pb::Trace& b) { return a.trace_id() < b.trace_id(); });

    if (mc_block->key_block || mc_block->seqno == kFirstMasterchainSeqno) {
      TRY_STATUS(fill_config_params(out.mutable_config_params()));
    }
    return out;
  }

 private:
  static constexpr std::uint32_t kFirstMasterchainSeqno = 1;
  static constexpr std::size_t kMaxDataBocSize = 64 * 1024;

  const ParsedBlock& parsed_block_;
  std::map<td::Bits256, const schema::AccountState*> block_end_states_;
  std::map<td::Bits256, std::optional<schema::AccountState>> loaded_states_;

  td::Status fill_block_data(pb::BlockData* out, const schema::Block& blk) {
    TRY_STATUS(fill_block_header(out->mutable_header(), blk));

    std::vector<std::pair<std::string, const schema::Transaction*>> txs;
    txs.reserve(blk.transactions.size());
    for (const auto& tx : blk.transactions) {
      txs.emplace_back(convert::to_raw_address(tx.account), &tx);
    }
    std::sort(txs.begin(), txs.end(), [](const auto& a, const auto& b) {
      if (a.second->lt != b.second->lt) {
        return a.second->lt < b.second->lt;
      }
      return a.first < b.first;
    });

    // first and last transaction of every account, keyed by its raw address
    std::map<std::string, std::pair<const schema::Transaction*, const schema::Transaction*>> accounts;
    for (const auto& [account, tx] : txs) {
      auto [it, inserted] = accounts.try_emplace(account, tx, tx);
      if (!inserted) {
        it->second.second = tx;
      }
    }

    for (const auto& [account, tx] : txs) {
      bool first_in_block = accounts.at(account).first == tx;
      TRY_STATUS(fill_transaction(out->add_transactions(), blk, *tx, account, first_in_block));
    }
    for (const auto& [account, range] : accounts) {
      TRY_STATUS(fill_account_change(out->add_account_changes(), account, *range.first, *range.second));
    }
    return td::Status::OK();
  }

  td::Status fill_account_change(pb::AccountChange* out, const std::string& account,
                                 const schema::Transaction& first_tx, const schema::Transaction& last_tx) {
    out->set_account(account);
    const auto* before = committed_state(first_tx.account_state_hash_before, first_tx.account, true);
    const auto* after = committed_state(last_tx.account_state_hash_after, last_tx.account, false);
    fill_state(out->mutable_state_before(), first_tx.account_state_hash_before, before);
    fill_state(out->mutable_state_after(), last_tx.account_state_hash_after, after);
    if (after == nullptr) {
      return td::Status::OK();
    }
    // An unknown previous state counts as changed.
    if (after->code.not_null() && (before == nullptr || before->code_hash != after->code_hash)) {
      TRY_RESULT(code, serialize_boc(after->code));
      out->set_code(std::move(code));
    }
    if (after->data.not_null() && (before == nullptr || before->data_hash != after->data_hash)) {
      TRY_RESULT(data, serialize_boc(after->data));
      if (data.size() <= kMaxDataBocSize) {
        out->set_data(std::move(data));
      } else {
        out->set_data_omitted(true);
      }
    }
    return td::Status::OK();
  }

  static td::Result<std::string> serialize_boc(const td::Ref<vm::Cell>& cell) {
    TRY_RESULT(boc, vm::std_boc_serialize(cell, vm::BagOfCells::Mode::WithCRC32C));
    return boc.as_slice().str();
  }

  static void fill_trace(pb::Trace* out, const schema::Trace& trace) {
    out->set_trace_id(bytes(trace.trace_id));
    if (trace.external_hash) {
      out->set_external_hash(bytes(*trace.external_hash));
    }
    if (trace.external_hash_norm && trace.external_hash_norm != trace.external_hash) {
      out->set_external_hash_norm(bytes(*trace.external_hash_norm));
    }
    out->set_mc_seqno_start(trace.mc_seqno_start);
    out->set_mc_seqno_end(trace.mc_seqno_end);
    out->set_start_lt(trace.start_lt);
    out->set_start_utime(trace.start_utime);
    out->set_end_lt(trace.end_lt);
    out->set_end_utime(trace.end_utime);
    out->set_state(trace.state == schema::Trace::State::complete ? "complete" : "broken");
    out->set_transactions(static_cast<std::uint32_t>(trace.nodes_));
    out->set_messages(static_cast<std::uint32_t>(trace.edges_));
  }

  td::Status fill_config_params(google::protobuf::Map<std::int32_t, std::string>* out) {
    const auto& config = parsed_block_.mc_block_.config_;
    if (!config) {
      return td::Status::Error("masterchain config is missing");
    }
    td::Status status;
    vm::Dictionary params{config->get_root_cell(), 32};
    params.check_for_each([&](td::Ref<vm::CellSlice> value, td::ConstBitPtr key, int key_len) {
      auto boc = serialize_boc(value->prefetch_ref());
      if (boc.is_error()) {
        status = boc.move_as_error_prefix(PSLICE() << "config param " << key.get_int(key_len) << ": ");
        return false;
      }
      (*out)[static_cast<std::int32_t>(key.get_int(key_len))] = boc.move_as_ok();
      return true;
    });
    return status;
  }

  static td::Result<std::string> decode_base64(td::Slice value) {
    return td::base64_decode(value);
  }

  static void fill_currencies(google::protobuf::Map<std::uint32_t, std::string>* out,
                              const std::map<uint32_t, td::RefInt256>& currencies) {
    for (const auto& [id, amount] : currencies) {
      (*out)[id] = amount->to_dec_string();
    }
  }

  static void fill_block_id(pb::BlockId* out, std::int32_t workchain, std::int64_t shard, std::uint32_t seqno) {
    out->set_workchain(workchain);
    out->set_shard(static_cast<std::uint64_t>(shard));
    out->set_seqno(seqno);
  }

  static td::Status fill_block_header(pb::BlockHeader* out, const schema::Block& blk) {
    out->set_workchain(blk.workchain);
    out->set_shard(static_cast<std::uint64_t>(blk.shard));
    out->set_seqno(blk.seqno);
    TRY_RESULT(root_hash, decode_base64(blk.root_hash));
    out->set_root_hash(std::move(root_hash));
    TRY_RESULT(file_hash, decode_base64(blk.file_hash));
    out->set_file_hash(std::move(file_hash));
    out->set_global_id(blk.global_id);
    out->set_version(blk.version);
    out->set_after_merge(blk.after_merge);
    out->set_before_split(blk.before_split);
    out->set_after_split(blk.after_split);
    out->set_want_merge(blk.want_merge);
    out->set_want_split(blk.want_split);
    out->set_key_block(blk.key_block);
    out->set_vert_seqno_incr(blk.vert_seqno_incr);
    out->set_flags(blk.flags);
    out->set_gen_utime(blk.gen_utime);
    out->set_start_lt(blk.start_lt);
    out->set_end_lt(blk.end_lt);
    out->set_validator_list_hash_short(static_cast<std::uint32_t>(blk.validator_list_hash_short));
    out->set_gen_catchain_seqno(blk.gen_catchain_seqno);
    out->set_min_ref_mc_seqno(blk.min_ref_mc_seqno);
    out->set_prev_key_block_seqno(blk.prev_key_block_seqno);
    out->set_vert_seqno(blk.vert_seqno);
    out->set_master_ref_seqno(blk.master_ref_seqno.value_or(0));
    TRY_RESULT(rand_seed, decode_base64(blk.rand_seed));
    out->set_rand_seed(std::move(rand_seed));
    TRY_RESULT(created_by, decode_base64(blk.created_by));
    out->set_created_by(std::move(created_by));
    out->set_tx_count(static_cast<std::uint32_t>(blk.transactions.size()));
    fill_block_id(out->mutable_masterchain_block_ref(), ton::masterchainId, static_cast<std::int64_t>(ton::shardIdAll),
                  blk.mc_block_seqno.value_or(blk.seqno));
    for (const auto& prev : blk.prev_blocks) {
      fill_block_id(out->add_prev_blocks(), prev.workchain, prev.shard, prev.seqno);
    }
    return td::Status::OK();
  }

  static void fill_account_state(pb::AccountState* out, const schema::AccountState& state) {
    out->set_hash(bytes(state.hash));
    out->set_balance(coins(state.balance.grams));
    fill_currencies(out->mutable_extra_currencies(), state.balance.extra_currencies);
    out->set_account_status(state.account_status);
    if (state.frozen_hash) {
      out->set_frozen_hash(bytes(*state.frozen_hash));
    }
    if (state.code_hash) {
      out->set_code_hash(bytes(*state.code_hash));
    }
    if (state.data_hash) {
      out->set_data_hash(bytes(*state.data_hash));
    }
  }

  static const td::Bits256& account_none_hash() {
    static const td::Bits256 hash = [] {
      vm::CellBuilder cb;
      cb.store_zeroes(1);
      return td::Bits256{cb.finalize()->get_hash().bits()};
    }();
    return hash;
  }

  // The state a block starts from was committed by an earlier block. The cell db
  // keeps a cell while any kept state references it, so the state is there as
  // long as the node keeps the state of the previous block.
  std::optional<schema::AccountState> load_prior_state(const td::Bits256& hash, const block::StdAddress& account) {
    if (hash == account_none_hash()) {
      schema::AccountState state;
      state.hash = hash;
      state.account = account;
      state.account_status = "nonexist";
      state.balance = schema::CurrencyCollection{td::make_refint(0), {}};
      return state;
    }
    if (!parsed_block_.cell_db_reader_) {
      return std::nullopt;
    }
    auto cell = parsed_block_.cell_db_reader_->load_cell(hash.as_slice());
    if (cell.is_error()) {
      return std::nullopt;
    }
    td::Ref<vm::Cell> root = cell.move_as_ok();
    if (block::gen::t_Account.get_tag(vm::load_cell_slice(root)) != block::gen::Account::account) {
      return std::nullopt;
    }
    auto parsed = ParseQuery::parse_account(std::move(root), 0, td::Bits256::zero(), 0);
    if (parsed.is_error()) {
      LOG(WARNING) << "Failed to parse account state " << hash.to_hex() << ": " << parsed.move_as_error();
      return std::nullopt;
    }
    return parsed.move_as_ok();
  }

  // The full state for hash: committed at the end of a block of this round or,
  // when committed_earlier, by an earlier block. Null for the states between two
  // transactions of one block, known by hash only.
  const schema::AccountState* committed_state(const td::Bits256& hash, const block::StdAddress& account,
                                              bool committed_earlier) {
    if (auto it = block_end_states_.find(hash); it != block_end_states_.end()) {
      return it->second;
    }
    if (!committed_earlier) {
      return nullptr;
    }
    auto it = loaded_states_.find(hash);
    if (it == loaded_states_.end()) {
      it = loaded_states_.emplace(hash, load_prior_state(hash, account)).first;
    }
    return it->second ? &*it->second : nullptr;
  }

  static void fill_state(pb::AccountState* out, const td::Bits256& hash, const schema::AccountState* state) {
    if (state != nullptr) {
      fill_account_state(out, *state);
    } else {
      out->set_hash(bytes(hash));
    }
  }

  static td::Status fill_content(pb::MessageContent* out, const td::Ref<vm::Cell>& cell, td::Slice boc) {
    out->set_hash(cell->get_hash().as_slice().str());
    TRY_RESULT(body, decode_base64(boc));
    out->set_body(std::move(body));
    return td::Status::OK();
  }

  static td::Status fill_message(pb::Message* out, const schema::Message& msg) {
    out->set_hash(bytes(msg.hash));
    if (msg.source) {
      out->set_source(*msg.source);
    }
    if (msg.destination) {
      out->set_destination(*msg.destination);
    }
    if (msg.value) {
      out->set_value(coins(msg.value->grams));
      fill_currencies(out->mutable_value_extra_currencies(), msg.value->extra_currencies);
    }
    if (msg.fwd_fee) {
      out->set_fwd_fee(coins(*msg.fwd_fee));
    }
    if (msg.ihr_fee) {
      out->set_ihr_fee(coins(*msg.ihr_fee));
    }
    if (msg.extra_flags) {
      out->set_extra_flags((*msg.extra_flags)->to_dec_string());
    }
    if (msg.created_lt) {
      out->set_created_lt(*msg.created_lt);
    }
    if (msg.created_at) {
      out->set_created_at(*msg.created_at);
    }
    if (msg.opcode) {
      out->set_opcode(static_cast<std::uint32_t>(*msg.opcode));
    }
    if (msg.ihr_disabled) {
      out->set_ihr_disabled(*msg.ihr_disabled);
    }
    if (msg.bounce) {
      out->set_bounce(*msg.bounce);
    }
    if (msg.bounced) {
      out->set_bounced(*msg.bounced);
    }
    // import_fee is set by the sender and may not fit int64, the API leaves it empty then.
    if (msg.import_fee) {
      auto import_fee = (*msg.import_fee)->to_long();
      if (import_fee != std::numeric_limits<std::int64_t>::min()) {
        out->set_import_fee(static_cast<std::uint64_t>(import_fee));
      }
    }
    TRY_STATUS(fill_content(out->mutable_message_content(), msg.body, msg.body_boc));
    if (msg.init_state.not_null() && msg.init_state_boc) {
      TRY_STATUS(fill_content(out->mutable_init_state(), msg.init_state, *msg.init_state_boc));
    }
    if (msg.hash_norm && *msg.hash_norm != msg.hash) {
      out->set_hash_norm(bytes(*msg.hash_norm));
    }
    // Only internal messages carry a value, and only inbound external ones lack a source.
    out->set_type(msg.value ? "int" : (msg.source ? "ext_out" : "ext_in"));
    return td::Status::OK();
  }

  static void fill_storage_ph(pb::StoragePhase* out, const schema::TrStoragePhase& phase) {
    out->set_storage_fees_collected(coins(phase.storage_fees_collected));
    if (phase.storage_fees_due) {
      out->set_storage_fees_due(coins(*phase.storage_fees_due));
    }
    out->set_status_change(stringify(phase.status_change));
  }

  static void fill_credit_ph(pb::CreditPhase* out, const schema::TrCreditPhase& phase) {
    if (phase.due_fees_collected) {
      out->set_due_fees_collected(coins(*phase.due_fees_collected));
    }
    out->set_credit(coins(phase.credit.grams));
    fill_currencies(out->mutable_credit_extra_currencies(), phase.credit.extra_currencies);
  }

  static void fill_compute_ph(pb::ComputePhase* out, const schema::TrComputePhase& phase) {
    std::visit(overloaded{
                   [&](const schema::TrComputePhase_skipped& skipped) {
                     out->set_skipped(true);
                     out->set_reason(stringify(skipped.reason));
                   },
                   [&](const schema::TrComputePhase_vm& vm) {
                     out->set_skipped(false);
                     out->set_success(vm.success);
                     out->set_msg_state_used(vm.msg_state_used);
                     out->set_account_activated(vm.account_activated);
                     out->set_gas_fees(coins(vm.gas_fees));
                     out->set_gas_used(vm.gas_used);
                     out->set_gas_limit(vm.gas_limit);
                     if (vm.gas_credit) {
                       out->set_gas_credit(*vm.gas_credit);
                     }
                     out->set_mode(vm.mode);
                     out->set_exit_code(vm.exit_code);
                     if (vm.exit_arg) {
                       out->set_exit_arg(*vm.exit_arg);
                     }
                     out->set_vm_steps(vm.vm_steps);
                     out->set_vm_init_state_hash(bytes(vm.vm_init_state_hash));
                     out->set_vm_final_state_hash(bytes(vm.vm_final_state_hash));
                   },
               },
               phase);
  }

  static void fill_msg_size(pb::MsgSize* out, const schema::StorageUsed& size) {
    out->set_cells(size.cells);
    out->set_bits(size.bits);
  }

  static void fill_action_ph(pb::ActionPhase* out, const schema::TrActionPhase& phase) {
    out->set_success(phase.success);
    out->set_valid(phase.valid);
    out->set_no_funds(phase.no_funds);
    out->set_status_change(stringify(phase.status_change));
    if (phase.total_fwd_fees) {
      out->set_total_fwd_fees(coins(*phase.total_fwd_fees));
    }
    if (phase.total_action_fees) {
      out->set_total_action_fees(coins(*phase.total_action_fees));
    }
    out->set_result_code(phase.result_code);
    if (phase.result_arg) {
      out->set_result_arg(*phase.result_arg);
    }
    out->set_tot_actions(phase.tot_actions);
    out->set_spec_actions(phase.spec_actions);
    out->set_skipped_actions(phase.skipped_actions);
    out->set_msgs_created(phase.msgs_created);
    out->set_action_list_hash(bytes(phase.action_list_hash));
    fill_msg_size(out->mutable_tot_msg_size(), phase.tot_msg_size);
  }

  static void fill_bounce_ph(pb::BouncePhase* out, const schema::TrBouncePhase& phase) {
    std::visit(overloaded{
                   [&](const schema::TrBouncePhase_negfunds&) { out->set_type("negfunds"); },
                   [&](const schema::TrBouncePhase_nofunds& nofunds) {
                     out->set_type("nofunds");
                     fill_msg_size(out->mutable_msg_size(), nofunds.msg_size);
                     out->set_req_fwd_fees(coins(nofunds.req_fwd_fees));
                   },
                   [&](const schema::TrBouncePhase_ok& ok) {
                     out->set_type("ok");
                     fill_msg_size(out->mutable_msg_size(), ok.msg_size);
                     out->set_msg_fees(coins(ok.msg_fees));
                     out->set_fwd_fees(coins(ok.fwd_fees));
                   },
               },
               phase);
  }

  static void fill_split_info(pb::SplitInfo* out, const schema::SplitMergeInfo& info) {
    out->set_cur_shard_pfx_len(info.cur_shard_pfx_len);
    out->set_acc_split_depth(info.acc_split_depth);
    out->set_this_addr(bytes(info.this_addr));
    out->set_sibling_addr(bytes(info.sibling_addr));
  }

  static void fill_descr(pb::TransactionDescr* out, const schema::TransactionDescr& descr) {
    std::visit(overloaded{
                   [&](const schema::TransactionDescr_ord& d) {
                     out->set_type("ord");
                     out->set_aborted(d.aborted);
                     out->set_destroyed(d.destroyed);
                     out->set_credit_first(d.credit_first);
                     if (d.storage_ph) fill_storage_ph(out->mutable_storage_ph(), *d.storage_ph);
                     if (d.credit_ph) fill_credit_ph(out->mutable_credit_ph(), *d.credit_ph);
                     fill_compute_ph(out->mutable_compute_ph(), d.compute_ph);
                     if (d.action) fill_action_ph(out->mutable_action(), *d.action);
                     if (d.bounce) fill_bounce_ph(out->mutable_bounce(), *d.bounce);
                   },
                   [&](const schema::TransactionDescr_storage& d) {
                     out->set_type("storage");
                     fill_storage_ph(out->mutable_storage_ph(), d.storage_ph);
                   },
                   [&](const schema::TransactionDescr_tick_tock& d) {
                     out->set_type("tick_tock");
                     out->set_aborted(d.aborted);
                     out->set_destroyed(d.destroyed);
                     out->set_is_tock(d.is_tock);
                     fill_storage_ph(out->mutable_storage_ph(), d.storage_ph);
                     fill_compute_ph(out->mutable_compute_ph(), d.compute_ph);
                     if (d.action) fill_action_ph(out->mutable_action(), *d.action);
                   },
                   [&](const schema::TransactionDescr_split_prepare& d) {
                     out->set_type("split_prepare");
                     out->set_aborted(d.aborted);
                     out->set_destroyed(d.destroyed);
                     if (d.storage_ph) fill_storage_ph(out->mutable_storage_ph(), *d.storage_ph);
                     fill_compute_ph(out->mutable_compute_ph(), d.compute_ph);
                     if (d.action) fill_action_ph(out->mutable_action(), *d.action);
                     fill_split_info(out->mutable_split_info(), d.split_info);
                   },
                   [&](const schema::TransactionDescr_split_install& d) {
                     out->set_type("split_install");
                     out->set_installed(d.installed);
                     fill_split_info(out->mutable_split_info(), d.split_info);
                   },
                   [&](const schema::TransactionDescr_merge_prepare& d) {
                     out->set_type("merge_prepare");
                     out->set_aborted(d.aborted);
                     fill_storage_ph(out->mutable_storage_ph(), d.storage_ph);
                     fill_split_info(out->mutable_split_info(), d.split_info);
                   },
                   [&](const schema::TransactionDescr_merge_install& d) {
                     out->set_type("merge_install");
                     out->set_aborted(d.aborted);
                     out->set_destroyed(d.destroyed);
                     if (d.storage_ph) fill_storage_ph(out->mutable_storage_ph(), *d.storage_ph);
                     if (d.credit_ph) fill_credit_ph(out->mutable_credit_ph(), *d.credit_ph);
                     fill_compute_ph(out->mutable_compute_ph(), d.compute_ph);
                     if (d.action) fill_action_ph(out->mutable_action(), *d.action);
                     fill_split_info(out->mutable_split_info(), d.split_info);
                   },
               },
               descr);
  }

  td::Status fill_transaction(pb::Transaction* out, const schema::Block& blk, const schema::Transaction& tx,
                              const std::string& account, bool first_in_block) {
    out->set_account(account);
    out->set_hash(bytes(tx.hash));
    out->set_lt(tx.lt);
    out->set_now(tx.now);
    out->set_mc_block_seqno(blk.mc_block_seqno.value_or(blk.seqno));
    out->set_trace_id(bytes(tx.trace_id));
    out->set_prev_trans_hash(bytes(tx.prev_trans_hash));
    out->set_prev_trans_lt(tx.prev_trans_lt);
    out->set_orig_status(stringify(tx.orig_status));
    out->set_end_status(stringify(tx.end_status));
    out->set_total_fees(coins(tx.total_fees.grams));
    fill_currencies(out->mutable_total_fees_extra_currencies(), tx.total_fees.extra_currencies);
    fill_descr(out->mutable_description(), tx.description);
    fill_block_id(out->mutable_block_ref(), blk.workchain, blk.shard, blk.seqno);
    if (tx.in_msg) {
      TRY_STATUS(fill_message(out->mutable_in_msg(), *tx.in_msg));
    }
    for (const auto& msg : tx.out_msgs) {
      TRY_STATUS(fill_message(out->add_out_msgs(), msg));
    }
    fill_state(out->mutable_account_state_before(), tx.account_state_hash_before,
               committed_state(tx.account_state_hash_before, tx.account, first_in_block));
    fill_state(out->mutable_account_state_after(), tx.account_state_hash_after,
               committed_state(tx.account_state_hash_after, tx.account, false));
    return td::Status::OK();
  }
};

td::Result<std::string> serialize_deterministic(const google::protobuf::Message& message) {
  std::string out;
  {
    google::protobuf::io::StringOutputStream stream(&out);
    google::protobuf::io::CodedOutputStream coded(&stream);
    coded.SetSerializationDeterministic(true);
    if (!message.SerializeToCodedStream(&coded)) {
      return td::Status::Error("failed to serialize block");
    }
  }
  return out;
}

}  // namespace

td::Result<FirehoseBlock> make_firehose_block(const ParsedBlock& parsed_block) {
  TRY_RESULT(block, Converter(parsed_block).convert());

  const schema::BlockDataState* mc_state = nullptr;
  for (const auto& state : parsed_block.mc_block_.shard_blocks_diff_) {
    if (state.handle && state.handle->id().is_masterchain()) {
      mc_state = &state;
    }
  }
  if (mc_state == nullptr) {
    return td::Status::Error("masterchain block handle is missing");
  }
  auto prev = mc_state->handle->one_prev(true);

  const auto& header = block.masterchain_block().header();
  FirehoseBlock out;
  out.number = header.seqno();
  out.id = td::hex_encode(header.root_hash());
  out.parent_num = prev.seqno();
  out.parent_id = td::hex_encode(prev.root_hash.as_slice());
  out.gen_utime = header.gen_utime();
  TRY_RESULT_ASSIGN(out.payload, serialize_deterministic(block));
  return out;
}

td::Result<std::string> firehose_payload_to_json(const std::string& payload) {
  pb::Block block;
  if (!block.ParseFromString(payload)) {
    return td::Status::Error("failed to parse block");
  }
  google::protobuf::util::JsonPrintOptions options;
  options.preserve_proto_field_names = true;
  std::string out;
  auto status = google::protobuf::util::MessageToJsonString(block, &out, options);
  if (!status.ok()) {
    return td::Status::Error(status.ToString());
  }
  return out;
}
