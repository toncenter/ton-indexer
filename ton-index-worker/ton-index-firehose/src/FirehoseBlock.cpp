#include "FirehoseBlock.h"

#include <algorithm>
#include <map>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/util/json_util.h>
#include "td/utils/check.h"
#include "td/utils/misc.h"
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"
#include "crypto/block/block.h"
#include "crypto/vm/boc.h"
#include "crypto/vm/cells/CellBuilder.h"
#include "crypto/vm/dict.h"
#include "DataParser.h"
#include "sf/ton/type/v1/type.pb.h"

namespace pb = sf::ton::type::v1;

// Blocks are converted from their cells following block.tlb, see type.proto.
// Only trace ids, normalized message hashes and traces come from the parser.
namespace {

template <class... Ts>
struct overloaded : Ts... {
  using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

using td::Ref;
using vm::Cell;
using vm::CellSlice;

std::string bytes(const td::Bits256& value) {
  return value.as_slice().str();
}

// bits n: from the high bit of the first byte, the last byte padded with zero bits
std::string bit_string_bytes(const Ref<td::BitString>& bits) {
  if (bits.is_null()) {
    return {};
  }
  std::string out((bits->size() + 7) / 8, '\0');
  td::BitPtr{reinterpret_cast<unsigned char*>(out.data())}.copy_from(bits->cbits(), bits->size());
  return out;
}

std::string cell_hash(const Ref<Cell>& cell) {
  return cell->get_hash().as_slice().str();
}

std::string key_bytes(td::ConstBitPtr key) {
  td::Bits256 value;
  value.bits().copy_from(key, 256);
  return bytes(value);
}

std::optional<std::uint64_t> to_uint64(const td::RefInt256& value) {
  unsigned char buffer[8];
  if (value.is_null() || !value->export_bytes(buffer, sizeof(buffer), false)) {
    return std::nullopt;
  }
  std::uint64_t result = 0;
  for (auto byte : buffer) {
    result = result << 8 | byte;
  }
  return result;
}

const td::Bits256& account_none_hash() {
  static const td::Bits256 hash = [] {
    vm::CellBuilder cb;
    cb.store_zeroes(1);
    return td::Bits256{cb.finalize()->get_hash().bits()};
  }();
  return hash;
}

td::Result<std::string> serialize_boc(const Ref<Cell>& cell) {
  TRY_RESULT(boc, vm::std_boc_serialize(cell, vm::BagOfCells::Mode::WithCRC32C));
  return boc.as_slice().str();
}

pb::AccountStatus account_status(char status) {
  switch (status) {
    case block::gen::AccountStatus::acc_state_uninit: return pb::ACC_STATE_UNINIT;
    case block::gen::AccountStatus::acc_state_frozen: return pb::ACC_STATE_FROZEN;
    case block::gen::AccountStatus::acc_state_active: return pb::ACC_STATE_ACTIVE;
    case block::gen::AccountStatus::acc_state_nonexist: return pb::ACC_STATE_NONEXIST;
  }
  UNREACHABLE();
}

pb::AccStatusChange acc_status_change(schema::AccStatusChange status_change) {
  switch (status_change) {
    case schema::AccStatusChange::acst_unchanged: return pb::ACST_UNCHANGED;
    case schema::AccStatusChange::acst_frozen: return pb::ACST_FROZEN;
    case schema::AccStatusChange::acst_deleted: return pb::ACST_DELETED;
  }
  UNREACHABLE();
}

pb::ComputeSkipReason compute_skip_reason(schema::ComputeSkipReason reason) {
  switch (reason) {
    case schema::ComputeSkipReason::cskip_no_state: return pb::CSKIP_NO_STATE;
    case schema::ComputeSkipReason::cskip_bad_state: return pb::CSKIP_BAD_STATE;
    case schema::ComputeSkipReason::cskip_no_gas: return pb::CSKIP_NO_GAS;
    case schema::ComputeSkipReason::cskip_suspended: return pb::CSKIP_SUSPENDED;
  }
  UNREACHABLE();
}

// Maybe ^Cell
Ref<Cell> maybe_ref(const Ref<CellSlice>& cs) {
  return cs->prefetch_ulong(1) == 1 ? cs->prefetch_ref() : Ref<Cell>{};
}

void fill_shard_ident(pb::ShardIdent* out, ton::WorkchainId workchain, ton::ShardId shard) {
  out->set_shard_pfx_bits(ton::shard_prefix_length(shard));
  out->set_workchain_id(workchain);
  out->set_shard_prefix(shard & (shard - 1));
}

void fill_block_id_ext(pb::BlockIdExt* out, const ton::BlockIdExt& id) {
  fill_shard_ident(out->mutable_shard_id(), id.id.workchain, id.id.shard);
  out->set_seq_no(id.id.seqno);
  out->set_root_hash(bytes(id.root_hash));
  out->set_file_hash(bytes(id.file_hash));
}

void fill_trace(pb::Trace* out, const schema::Trace& trace) {
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
  out->set_transactions(static_cast<std::uint32_t>(trace.nodes_));
  out->set_messages(static_cast<std::uint32_t>(trace.edges_));
}

void fill_split_info(pb::SplitMergeInfo* out, const schema::SplitMergeInfo& info) {
  out->set_cur_shard_pfx_len(info.cur_shard_pfx_len);
  out->set_acc_split_depth(info.acc_split_depth);
  out->set_this_addr(bytes(info.this_addr));
  out->set_sibling_addr(bytes(info.sibling_addr));
}

void fill_msg_size(pb::StorageUsed* out, const schema::StorageUsed& size) {
  out->set_cells(size.cells);
  out->set_bits(size.bits);
}

class Converter {
 public:
  explicit Converter(const ParsedBlock& parsed_block) : parsed_block_(parsed_block) {
    for (const auto& blk : parsed_block.blocks_) {
      for (const auto& tx : blk.transactions) {
        trace_ids_.emplace(tx.hash, tx.trace_id);
        if (tx.in_msg && tx.in_msg->hash_norm && *tx.in_msg->hash_norm != tx.in_msg->hash) {
          norm_hashes_.emplace(tx.in_msg->hash, *tx.in_msg->hash_norm);
        }
      }
    }
  }

  td::Result<pb::Block> convert() {
    const auto& config = parsed_block_.mc_block_.config_;
    if (!config) {
      return td::Status::Error("masterchain config is missing");
    }
    global_version_ = config->get_global_version();
    pb::Block out;
    try {
      fill_block(&out);
    } catch (vm::VmError& e) {
      fail(td::Status::Error(PSLICE() << "malformed cell: " << e.get_msg()));
    }
    if (error_.is_error()) {
      return std::move(error_);
    }
    return out;
  }

 private:
  static constexpr std::size_t kMaxDataBocSize = 64 * 1024;

  // An account as converted, with the cells of its code, data and library.
  struct LoadedAccount {
    pb::Account account;
    Ref<Cell> code;
    Ref<Cell> data;
    Ref<Cell> library;
  };

  const ParsedBlock& parsed_block_;
  int global_version_{0};
  std::map<td::Bits256, td::Bits256> trace_ids_;
  std::map<td::Bits256, td::Bits256> norm_hashes_;
  std::map<td::Bits256, std::optional<LoadedAccount>> accounts_;
  // The first error: the fill functions record it and go on, convert() returns it.
  td::Status error_;

  void fail(td::Status error) {
    if (error_.is_ok()) {
      error_ = std::move(error);
    }
  }

  void fail_unpack(td::Slice what) {
    fail(td::Status::Error(PSLICE() << "failed to unpack " << what));
  }

  std::uint64_t coins(const td::RefInt256& value) {
    auto result = to_uint64(value);
    if (!result) {
      fail(td::Status::Error(PSLICE() << "value does not fit uint64: " << value));
      return 0;
    }
    return *result;
  }

  std::uint64_t grams(const Ref<CellSlice>& cs) {
    return coins(block::tlb::t_Grams.as_integer(*cs));
  }

  // Maybe Grams
  std::optional<std::uint64_t> maybe_grams(const Ref<CellSlice>& cs) {
    CellSlice value = *cs;
    if (value.fetch_ulong(1) != 1) {
      return std::nullopt;
    }
    return coins(block::tlb::t_Grams.as_integer_skip(value));
  }

  void fill_msg_addr_int(pb::MsgAddressInt* out, const Ref<CellSlice>& cs) {
    using block::gen::MsgAddressInt;
    switch (block::gen::t_MsgAddressInt.get_tag(*cs)) {
      case MsgAddressInt::addr_std: {
        MsgAddressInt::Record_addr_std rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("addr_std");
        }
        auto* addr = out->mutable_addr_std();
        fill_anycast(addr, rec.anycast);
        addr->set_workchain_id(rec.workchain_id);
        addr->set_address(bytes(rec.address));
        return;
      }
      case MsgAddressInt::addr_var: {
        MsgAddressInt::Record_addr_var rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("addr_var");
        }
        auto* addr = out->mutable_addr_var();
        fill_anycast(addr, rec.anycast);
        addr->set_addr_len(rec.addr_len);
        addr->set_workchain_id(rec.workchain_id);
        addr->set_address(bit_string_bytes(rec.address));
        return;
      }
    }
    fail_unpack("MsgAddressInt");
  }

  // Maybe Anycast of addr_std or addr_var
  template <class Addr>
  void fill_anycast(Addr* out, const Ref<CellSlice>& cs) {
    CellSlice value = *cs;
    if (value.fetch_ulong(1) != 1) {
      return;
    }
    block::gen::Anycast::Record anycast;
    if (!block::gen::t_Anycast.unpack(value, anycast)) {
      return fail_unpack("Anycast");
    }
    out->mutable_anycast()->set_depth(anycast.depth);
    out->mutable_anycast()->set_rewrite_pfx(bit_string_bytes(anycast.rewrite_pfx));
  }

  void fill_msg_addr_ext(pb::MsgAddressExt* out, const Ref<CellSlice>& cs) {
    using block::gen::MsgAddressExt;
    switch (block::gen::t_MsgAddressExt.get_tag(*cs)) {
      case MsgAddressExt::addr_none:
        out->mutable_addr_none();
        return;
      case MsgAddressExt::addr_extern: {
        MsgAddressExt::Record_addr_extern rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("addr_extern");
        }
        out->mutable_addr_extern()->set_len(rec.len);
        out->mutable_addr_extern()->set_external_address(bit_string_bytes(rec.external_address));
        return;
      }
    }
    fail_unpack("MsgAddressExt");
  }

  void fill_cell(pb::Cell* out, const Ref<Cell>& cell, bool with_boc) {
    out->set_hash(cell_hash(cell));
    if (with_boc) {
      auto boc = serialize_boc(cell);
      if (boc.is_error()) {
        return fail(boc.move_as_error());
      }
      out->set_boc(boc.move_as_ok());
    }
  }

  void fill_currencies(pb::CurrencyCollection* out, const td::RefInt256& grams, const Ref<Cell>& extra) {
    out->set_grams(coins(grams));
    vm::Dictionary dict{extra, 32};
    for (auto it = dict.begin(); !it.eof(); ++it) {
      auto id = static_cast<std::uint32_t>(td::BitArray<32>(it.cur_pos()).to_ulong());
      auto amount = block::tlb::t_VarUInteger_32.as_integer(it.cur_value());
      if (amount.is_null()) {
        return fail_unpack("extra currency amount");
      }
      (*out->mutable_other())[id] = amount->to_dec_string();
    }
  }

  void fill_currencies(pb::CurrencyCollection* out, const Ref<CellSlice>& cs) {
    td::RefInt256 grams;
    Ref<Cell> extra;
    if (!block::unpack_CurrencyCollection(cs, grams, extra)) {
      return fail_unpack("CurrencyCollection");
    }
    fill_currencies(out, grams, extra);
  }

  void fill_currencies(pb::CurrencyCollection* out, const schema::CurrencyCollection& value) {
    out->set_grams(coins(value.grams));
    for (const auto& [id, amount] : value.extra_currencies) {
      (*out->mutable_other())[id] = amount->to_dec_string();
    }
  }

  void fill_block(pb::Block* out) {
    std::vector<const schema::BlockDataState*> blocks;
    for (const auto& state : parsed_block_.mc_block_.shard_blocks_diff_) {
      blocks.push_back(&state);
    }
    std::sort(blocks.begin(), blocks.end(), [](const schema::BlockDataState* a, const schema::BlockDataState* b) {
      const auto& x = a->block_data->block_id().id;
      const auto& y = b->block_data->block_id().id;
      return std::make_tuple(x.workchain, x.shard, x.seqno) < std::make_tuple(y.workchain, y.shard, y.seqno);
    });
    bool has_masterchain_block = false;
    for (const auto* state : blocks) {
      bool is_masterchain = state->block_data->block_id().is_masterchain();
      has_masterchain_block |= is_masterchain;
      fill_block_data(is_masterchain ? out->mutable_masterchain_block() : out->add_shard_blocks(), *state);
    }
    if (!has_masterchain_block) {
      return fail(td::Status::Error("masterchain block is missing"));
    }

    // Every trace a transaction of the round joined is in traces_, so failing
    // on a broken one keeps its trace_id out of the block.
    for (const auto& trace : parsed_block_.traces_) {
      if (trace.state == schema::Trace::State::broken) {
        return fail(td::Status::Error(PSLICE() << "trace " << trace.trace_id.to_hex()
                                               << " is broken; if it starts before the TraceAssembler prewarm, "
                                                  "raise --prewarm"));
      }
      if (trace.state == schema::Trace::State::complete) {
        fill_trace(out->add_traces(), trace);
      }
    }
    // traces_ is filled from a hash map
    std::sort(out->mutable_traces()->begin(), out->mutable_traces()->end(),
              [](const pb::Trace& a, const pb::Trace& b) { return a.trace_id() < b.trace_id(); });
  }

  void fill_block_data(pb::BlockData* out, const schema::BlockDataState& state) {
    const auto& id = state.block_data->block_id();
    fill_block_id_ext(out->mutable_id(), id);
    block::gen::Block::Record blk;
    if (!tlb::unpack_cell(state.block_data->root_cell(), blk)) {
      return fail_unpack("Block");
    }
    out->set_global_id(blk.global_id);
    fill_block_info(out->mutable_info(), blk.info);
    fill_value_flow(out->mutable_value_flow(), blk.value_flow);
    fill_merkle_update(out->mutable_state_update(), blk.state_update);
    fill_block_extra(out->mutable_extra(), blk.extra);
  }

  void fill_block_info(pb::BlockInfo* out, const Ref<Cell>& cell) {
    block::gen::BlockInfo::Record info;
    if (!tlb::unpack_cell(cell, info)) {
      return fail_unpack("BlockInfo");
    }
    out->set_version(info.version);
    out->set_not_master(info.not_master);
    out->set_after_merge(info.after_merge);
    out->set_before_split(info.before_split);
    out->set_after_split(info.after_split);
    out->set_want_split(info.want_split);
    out->set_want_merge(info.want_merge);
    out->set_key_block(info.key_block);
    out->set_vert_seqno_incr(info.vert_seqno_incr);
    out->set_flags(info.flags);
    out->set_seq_no(info.seq_no);
    out->set_vert_seq_no(info.vert_seq_no);
    fill_shard_ident(out->mutable_shard(), info.shard);
    out->set_gen_utime(info.gen_utime);
    out->set_start_lt(info.start_lt);
    out->set_end_lt(info.end_lt);
    out->set_gen_validator_list_hash_short(info.gen_validator_list_hash_short);
    out->set_gen_catchain_seqno(info.gen_catchain_seqno);
    out->set_min_ref_mc_seqno(info.min_ref_mc_seqno);
    out->set_prev_key_block_seqno(info.prev_key_block_seqno);
    if (info.gen_software.not_null()) {
      block::gen::GlobalVersion::Record version;
      if (!tlb::csr_unpack(info.gen_software, version)) {
        return fail_unpack("GlobalVersion");
      }
      out->mutable_gen_software()->set_version(version.version);
      out->mutable_gen_software()->set_capabilities(version.capabilities);
    }
    if (info.master_ref.not_null()) {
      block::gen::BlkMasterInfo::Record master;
      if (!tlb::unpack_cell(info.master_ref, master)) {
        return fail_unpack("BlkMasterInfo");
      }
      fill_ext_blk_ref(out->mutable_master_ref()->mutable_master(), master.master);
    }
    fill_blk_prev_info(out->mutable_prev_ref(), info.prev_ref, info.after_merge);
    if (info.prev_vert_ref.not_null()) {
      fill_blk_prev_info(out->mutable_prev_vert_ref(), info.prev_vert_ref, false);
    }
  }

  void fill_shard_ident(pb::ShardIdent* out, const Ref<CellSlice>& cs) {
    block::gen::ShardIdent::Record shard;
    if (!tlb::csr_unpack(cs, shard)) {
      return fail_unpack("ShardIdent");
    }
    out->set_shard_pfx_bits(shard.shard_pfx_bits);
    out->set_workchain_id(shard.workchain_id);
    out->set_shard_prefix(shard.shard_prefix);
  }

  void fill_ext_blk_ref(pb::ExtBlkRef* out, const Ref<CellSlice>& cs) {
    block::gen::ExtBlkRef::Record ref;
    if (!tlb::csr_unpack(cs, ref)) {
      return fail_unpack("ExtBlkRef");
    }
    out->set_end_lt(ref.end_lt);
    out->set_seq_no(ref.seq_no);
    out->set_root_hash(bytes(ref.root_hash));
    out->set_file_hash(bytes(ref.file_hash));
  }

  void fill_blk_prev_info(pb::BlkPrevInfo* out, const Ref<Cell>& cell, bool after_merge) {
    if (after_merge) {
      block::gen::BlkPrevInfo::Record_prev_blks_info prev;
      if (!tlb::type_unpack_cell(cell, block::gen::BlkPrevInfo{1}, prev)) {
        return fail_unpack("prev_blks_info");
      }
      fill_ext_blk_ref(out->mutable_prev_blks_info()->mutable_prev1(), vm::load_cell_slice_ref(prev.prev1));
      fill_ext_blk_ref(out->mutable_prev_blks_info()->mutable_prev2(), vm::load_cell_slice_ref(prev.prev2));
    } else {
      block::gen::BlkPrevInfo::Record_prev_blk_info prev;
      if (!tlb::type_unpack_cell(cell, block::gen::BlkPrevInfo{0}, prev)) {
        return fail_unpack("prev_blk_info");
      }
      fill_ext_blk_ref(out->mutable_prev_blk_info()->mutable_prev(), prev.prev);
    }
  }

  void fill_value_flow(pb::ValueFlow* out, const Ref<Cell>& cell) {
    auto cs = vm::load_cell_slice_ref(cell);
    bool v2 = block::gen::t_ValueFlow.get_tag(*cs) == block::gen::ValueFlow::value_flow_v2;
    block::ValueFlow flow;
    if (!flow.unpack(cs)) {
      return fail_unpack("ValueFlow");
    }
    auto fill = [this](pb::CurrencyCollection* out, const block::CurrencyCollection& value) {
      fill_currencies(out, value.grams, value.extra);
    };
    fill(out->mutable_from_prev_blk(), flow.from_prev_blk);
    fill(out->mutable_to_next_blk(), flow.to_next_blk);
    fill(out->mutable_imported(), flow.imported);
    fill(out->mutable_exported(), flow.exported);
    fill(out->mutable_fees_collected(), flow.fees_collected);
    if (v2) {
      fill(out->mutable_burned(), flow.burned);
    }
    fill(out->mutable_fees_imported(), flow.fees_imported);
    fill(out->mutable_recovered(), flow.recovered);
    fill(out->mutable_created(), flow.created);
    fill(out->mutable_minted(), flow.minted);
  }

  void fill_merkle_update(pb::MerkleUpdate* out, const Ref<Cell>& cell) {
    CellSlice cs{vm::NoVm(), cell};
    td::Bits256 old_hash, new_hash;
    if (cs.special_type() != Cell::SpecialType::MerkleUpdate || !cs.advance(8) || !cs.fetch_bits_to(old_hash) ||
        !cs.fetch_bits_to(new_hash)) {
      return fail_unpack("MERKLE_UPDATE");
    }
    out->set_old_hash(bytes(old_hash));
    out->set_new_hash(bytes(new_hash));
    out->set_old_depth(static_cast<std::uint32_t>(cs.fetch_ulong(16)));
    out->set_new_depth(static_cast<std::uint32_t>(cs.fetch_ulong(16)));
  }

  void fill_block_extra(pb::BlockExtra* out, const Ref<Cell>& cell) {
    block::gen::BlockExtra::Record extra;
    if (!tlb::unpack_cell(cell, extra)) {
      return fail_unpack("BlockExtra");
    }

    block::tlb::Aug_InMsgDescr in_aug{global_version_};
    vm::AugmentedDictionary in_msgs{vm::load_cell_slice_ref(extra.in_msg_descr), 256, in_aug};
    bool ok = in_msgs.check_for_each_extra([&](Ref<CellSlice> value, Ref<CellSlice> aug, td::ConstBitPtr key, int) {
      auto* item = out->add_in_msg_descr();
      item->set_msg_hash(key_bytes(key));
      fill_in_msg(item->mutable_in_msg(), value);
      fill_import_fees(item->mutable_import_fees(), aug);
      return error_.is_ok();
    });
    if (!ok) {
      return fail_unpack("InMsgDescr");
    }

    block::tlb::Aug_OutMsgDescr out_aug{global_version_};
    vm::AugmentedDictionary out_msgs{vm::load_cell_slice_ref(extra.out_msg_descr), 256, out_aug};
    ok = out_msgs.check_for_each_extra([&](Ref<CellSlice> value, Ref<CellSlice> aug, td::ConstBitPtr key, int) {
      auto* item = out->add_out_msg_descr();
      item->set_msg_hash(key_bytes(key));
      fill_out_msg(item->mutable_out_msg(), value);
      fill_currencies(item->mutable_export_value(), aug);
      return error_.is_ok();
    });
    if (!ok) {
      return fail_unpack("OutMsgDescr");
    }

    vm::AugmentedDictionary account_blocks{vm::load_cell_slice_ref(extra.account_blocks), 256,
                                           block::tlb::aug_ShardAccountBlocks};
    ok = account_blocks.check_for_each_extra([&](Ref<CellSlice> value, Ref<CellSlice>, td::ConstBitPtr, int) {
      fill_account_block(out->add_account_blocks(), value);
      return error_.is_ok();
    });
    if (!ok) {
      return fail_unpack("ShardAccountBlocks");
    }

    out->set_rand_seed(bytes(extra.rand_seed));
    out->set_created_by(bytes(extra.created_by));
    if (auto custom = maybe_ref(extra.custom); custom.not_null()) {
      fill_mc_block_extra(out->mutable_custom(), custom);
    }
  }

  void fill_mc_block_extra(pb::McBlockExtra* out, const Ref<Cell>& cell) {
    block::gen::McBlockExtra::Record extra;
    if (!tlb::unpack_cell(cell, extra)) {
      return fail_unpack("McBlockExtra");
    }
    out->set_key_block(extra.key_block);

    // Workchains are signed: invert_first puts the keys in their order.
    vm::Dictionary shard_hashes{extra.shard_hashes, 32};
    bool ok = shard_hashes.check_for_each(
        [&](Ref<CellSlice> value, td::ConstBitPtr key, int key_len) {
          fill_shard_tree(out, value->prefetch_ref(), static_cast<ton::WorkchainId>(key.get_int(key_len)),
                          ton::shardIdAll);
          return error_.is_ok();
        },
        true);
    if (!ok) {
      return fail_unpack("ShardHashes");
    }

    vm::AugmentedDictionary shard_fees{extra.shard_fees, 96, block::tlb::aug_ShardFees};
    ok = shard_fees.check_for_each_extra(
        [&](Ref<CellSlice> value, Ref<CellSlice>, td::ConstBitPtr key, int) {
          auto* item = out->add_shard_fees();
          ::fill_shard_ident(item->mutable_shard(), static_cast<ton::WorkchainId>(key.get_int(32)),
                             (key + 32).get_uint(64));
          block::gen::ShardFeeCreated::Record fee;
          if (!tlb::csr_unpack(value, fee)) {
            fail_unpack("ShardFeeCreated");
            return false;
          }
          fill_currencies(item->mutable_shard_fee_created()->mutable_fees(), fee.fees);
          fill_currencies(item->mutable_shard_fee_created()->mutable_create(), fee.create);
          return error_.is_ok();
        },
        true);
    if (!ok) {
      return fail_unpack("ShardFees");
    }

    vm::Dictionary signatures{extra.r1.prev_blk_signatures, 16};
    unsigned long long index = 0;
    ok = signatures.check_for_each([&](Ref<CellSlice> value, td::ConstBitPtr key, int key_len) {
      if (key.get_uint(key_len) != index++) {
        fail_unpack("prev_blk_signatures: the keys are not the indexes");
        return false;
      }
      fill_signature_pair(out->add_prev_blk_signatures(), value);
      return error_.is_ok();
    });
    if (!ok) {
      return fail_unpack("prev_blk_signatures");
    }

    if (auto msg = maybe_ref(extra.r1.recover_create_msg); msg.not_null()) {
      fill_in_msg(out->mutable_recover_create_msg(), vm::load_cell_slice_ref(msg));
    }
    if (auto msg = maybe_ref(extra.r1.mint_msg); msg.not_null()) {
      fill_in_msg(out->mutable_mint_msg(), vm::load_cell_slice_ref(msg));
    }

    if (extra.key_block) {
      block::gen::ConfigParams::Record config;
      if (!tlb::csr_unpack(extra.config, config)) {
        return fail_unpack("ConfigParams");
      }
      auto* out_config = out->mutable_config();
      out_config->set_config_addr(bytes(config.config_addr));
      vm::Dictionary params{config.config, 32};
      ok = params.check_for_each([&](Ref<CellSlice> value, td::ConstBitPtr key, int key_len) {
        auto& param = (*out_config->mutable_config())[static_cast<std::int32_t>(key.get_int(key_len))];
        fill_cell(&param, value->prefetch_ref(), true);
        return error_.is_ok();
      });
      if (!ok) {
        return fail_unpack("ConfigParams config");
      }
    }
  }

  // BinTree ShardDescr: the path to a leaf gives its shard.
  void fill_shard_tree(pb::McBlockExtra* out, const Ref<Cell>& cell, ton::WorkchainId workchain,
                       ton::ShardId shard) {
    auto cs = vm::load_cell_slice(cell);
    if (cs.fetch_ulong(1) == 1) {  // bt_fork$1
      if (cs.size_refs() != 2) {
        return fail_unpack("bt_fork");
      }
      fill_shard_tree(out, cs.prefetch_ref(0), workchain, ton::shard_child(shard, true));
      fill_shard_tree(out, cs.prefetch_ref(1), workchain, ton::shard_child(shard, false));
      return;
    }
    auto* item = out->add_shard_hashes();
    ::fill_shard_ident(item->mutable_shard(), workchain, shard);
    fill_shard_descr(item->mutable_shard_descr(), cs);
  }

  void fill_shard_descr(pb::ShardDescr* out, CellSlice cs) {
    using block::gen::ShardDescr;
    switch (block::gen::t_ShardDescr.get_tag(cs)) {
      case ShardDescr::shard_descr_new: {
        ShardDescr::Record_shard_descr_new rec;
        if (!block::gen::t_ShardDescr.unpack(cs, rec)) {
          return fail_unpack("shard_descr_new");
        }
        fill_shard_descr_fields(out, rec);
        fill_currencies(out->mutable_fees_collected(), rec.r1.fees_collected);
        fill_currencies(out->mutable_funds_created(), rec.r1.funds_created);
        return;
      }
      case ShardDescr::shard_descr: {
        ShardDescr::Record_shard_descr rec;
        if (!block::gen::t_ShardDescr.unpack(cs, rec)) {
          return fail_unpack("shard_descr");
        }
        fill_shard_descr_fields(out, rec);
        fill_currencies(out->mutable_fees_collected(), rec.fees_collected);
        fill_currencies(out->mutable_funds_created(), rec.funds_created);
        return;
      }
    }
    fail_unpack("ShardDescr");
  }

  // The fields both constructors of ShardDescr keep in the same cell.
  template <class Record>
  void fill_shard_descr_fields(pb::ShardDescr* out, const Record& rec) {
    out->set_seq_no(rec.seq_no);
    out->set_reg_mc_seqno(rec.reg_mc_seqno);
    out->set_start_lt(rec.start_lt);
    out->set_end_lt(rec.end_lt);
    out->set_root_hash(bytes(rec.root_hash));
    out->set_file_hash(bytes(rec.file_hash));
    out->set_before_split(rec.before_split);
    out->set_before_merge(rec.before_merge);
    out->set_want_split(rec.want_split);
    out->set_want_merge(rec.want_merge);
    out->set_nx_cc_updated(rec.nx_cc_updated);
    out->set_flags(rec.flags);
    out->set_next_catchain_seqno(rec.next_catchain_seqno);
    out->set_next_validator_shard(rec.next_validator_shard);
    out->set_min_ref_mc_seqno(rec.min_ref_mc_seqno);
    out->set_gen_utime(rec.gen_utime);
    fill_split_merge_at(out->mutable_split_merge_at(), rec.split_merge_at);
  }

  void fill_split_merge_at(pb::FutureSplitMerge* out, const Ref<CellSlice>& cs) {
    using block::gen::FutureSplitMerge;
    switch (block::gen::t_FutureSplitMerge.get_tag(*cs)) {
      case FutureSplitMerge::fsm_none:
        out->mutable_fsm_none();
        return;
      case FutureSplitMerge::fsm_split: {
        FutureSplitMerge::Record_fsm_split rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("fsm_split");
        }
        out->mutable_fsm_split()->set_split_utime(rec.split_utime);
        out->mutable_fsm_split()->set_interval(rec.interval);
        return;
      }
      case FutureSplitMerge::fsm_merge: {
        FutureSplitMerge::Record_fsm_merge rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("fsm_merge");
        }
        out->mutable_fsm_merge()->set_merge_utime(rec.merge_utime);
        out->mutable_fsm_merge()->set_interval(rec.interval);
        return;
      }
    }
    fail_unpack("FutureSplitMerge");
  }

  void fill_signature_pair(pb::CryptoSignaturePair* out, const Ref<CellSlice>& cs) {
    block::gen::CryptoSignaturePair::Record pair;
    if (!tlb::csr_unpack(cs, pair)) {
      return fail_unpack("CryptoSignaturePair");
    }
    if (block::gen::t_CryptoSignature.get_tag(*pair.sign) != block::gen::CryptoSignature::cons1) {
      return fail(td::Status::Error("chained_signature is not supported"));
    }
    block::gen::CryptoSignature::Record_cons1 sign;
    block::gen::CryptoSignatureSimple::Record simple;
    if (!tlb::csr_unpack(pair.sign, sign) || !tlb::csr_unpack(sign.x, simple)) {
      return fail_unpack("CryptoSignatureSimple");
    }
    out->set_node_id_short(bytes(pair.node_id_short));
    auto* out_simple = out->mutable_sign()->mutable_crypto_signature_simple();
    out_simple->set_r(bytes(simple.R));
    out_simple->set_s(bytes(simple.s));
  }

  void fill_import_fees(pb::ImportFees* out, const Ref<CellSlice>& cs) {
    block::gen::ImportFees::Record fees;
    if (!tlb::csr_unpack(cs, fees)) {
      return fail_unpack("ImportFees");
    }
    out->set_fees_collected(grams(fees.fees_collected));
    fill_currencies(out->mutable_value_imported(), fees.value_imported);
  }

  void fill_in_msg(pb::InMsg* out, const Ref<CellSlice>& cs) {
    using block::gen::InMsg;
    switch (block::gen::t_InMsg.get_tag(*cs)) {
      case InMsg::msg_import_ext: {
        InMsg::Record_msg_import_ext rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_import_ext");
        }
        auto* msg = out->mutable_msg_import_ext();
        msg->set_msg_hash(cell_hash(rec.msg));
        msg->set_transaction_hash(cell_hash(rec.transaction));
        return;
      }
      case InMsg::msg_import_ihr: {
        InMsg::Record_msg_import_ihr rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_import_ihr");
        }
        auto* msg = out->mutable_msg_import_ihr();
        msg->set_msg_hash(cell_hash(rec.msg));
        msg->set_transaction_hash(cell_hash(rec.transaction));
        msg->set_ihr_fee(grams(rec.ihr_fee));
        fill_cell(msg->mutable_proof_created(), rec.proof_created, false);
        return;
      }
      case InMsg::msg_import_imm: {
        InMsg::Record_msg_import_imm rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_import_imm");
        }
        auto* msg = out->mutable_msg_import_imm();
        fill_msg_envelope(msg->mutable_in_msg(), rec.in_msg);
        msg->set_transaction_hash(cell_hash(rec.transaction));
        msg->set_fwd_fee(grams(rec.fwd_fee));
        return;
      }
      case InMsg::msg_import_fin: {
        InMsg::Record_msg_import_fin rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_import_fin");
        }
        auto* msg = out->mutable_msg_import_fin();
        fill_msg_envelope(msg->mutable_in_msg(), rec.in_msg);
        msg->set_transaction_hash(cell_hash(rec.transaction));
        msg->set_fwd_fee(grams(rec.fwd_fee));
        return;
      }
      case InMsg::msg_import_tr: {
        InMsg::Record_msg_import_tr rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_import_tr");
        }
        auto* msg = out->mutable_msg_import_tr();
        fill_msg_envelope(msg->mutable_in_msg(), rec.in_msg);
        fill_msg_envelope(msg->mutable_out_msg(), rec.out_msg);
        msg->set_transit_fee(grams(rec.transit_fee));
        return;
      }
      case InMsg::msg_discard_fin: {
        InMsg::Record_msg_discard_fin rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_discard_fin");
        }
        auto* msg = out->mutable_msg_discard_fin();
        fill_msg_envelope(msg->mutable_in_msg(), rec.in_msg);
        msg->set_transaction_id(rec.transaction_id);
        msg->set_fwd_fee(grams(rec.fwd_fee));
        return;
      }
      case InMsg::msg_discard_tr: {
        InMsg::Record_msg_discard_tr rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_discard_tr");
        }
        auto* msg = out->mutable_msg_discard_tr();
        fill_msg_envelope(msg->mutable_in_msg(), rec.in_msg);
        msg->set_transaction_id(rec.transaction_id);
        msg->set_fwd_fee(grams(rec.fwd_fee));
        fill_cell(msg->mutable_proof_delivered(), rec.proof_delivered, false);
        return;
      }
      case InMsg::msg_import_deferred_fin: {
        InMsg::Record_msg_import_deferred_fin rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_import_deferred_fin");
        }
        auto* msg = out->mutable_msg_import_deferred_fin();
        fill_msg_envelope(msg->mutable_in_msg(), rec.in_msg);
        msg->set_transaction_hash(cell_hash(rec.transaction));
        msg->set_fwd_fee(grams(rec.fwd_fee));
        return;
      }
      case InMsg::msg_import_deferred_tr: {
        InMsg::Record_msg_import_deferred_tr rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_import_deferred_tr");
        }
        auto* msg = out->mutable_msg_import_deferred_tr();
        fill_msg_envelope(msg->mutable_in_msg(), rec.in_msg);
        fill_msg_envelope(msg->mutable_out_msg(), rec.out_msg);
        return;
      }
    }
    fail_unpack("InMsg");
  }

  void fill_out_msg(pb::OutMsg* out, const Ref<CellSlice>& cs) {
    using block::gen::OutMsg;
    switch (block::gen::t_OutMsg.get_tag(*cs)) {
      case OutMsg::msg_export_ext: {
        OutMsg::Record_msg_export_ext rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_export_ext");
        }
        auto* msg = out->mutable_msg_export_ext();
        msg->set_msg_hash(cell_hash(rec.msg));
        msg->set_transaction_hash(cell_hash(rec.transaction));
        return;
      }
      case OutMsg::msg_export_imm: {
        OutMsg::Record_msg_export_imm rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_export_imm");
        }
        auto* msg = out->mutable_msg_export_imm();
        fill_msg_envelope(msg->mutable_out_msg(), rec.out_msg);
        msg->set_transaction_hash(cell_hash(rec.transaction));
        fill_in_msg(msg->mutable_reimport(), vm::load_cell_slice_ref(rec.reimport));
        return;
      }
      case OutMsg::msg_export_new: {
        OutMsg::Record_msg_export_new rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_export_new");
        }
        auto* msg = out->mutable_msg_export_new();
        fill_msg_envelope(msg->mutable_out_msg(), rec.out_msg);
        msg->set_transaction_hash(cell_hash(rec.transaction));
        return;
      }
      case OutMsg::msg_export_tr: {
        OutMsg::Record_msg_export_tr rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_export_tr");
        }
        auto* msg = out->mutable_msg_export_tr();
        fill_msg_envelope(msg->mutable_out_msg(), rec.out_msg);
        fill_in_msg(msg->mutable_imported(), vm::load_cell_slice_ref(rec.imported));
        return;
      }
      case OutMsg::msg_export_deq: {
        OutMsg::Record_msg_export_deq rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_export_deq");
        }
        auto* msg = out->mutable_msg_export_deq();
        fill_msg_envelope(msg->mutable_out_msg(), rec.out_msg);
        msg->set_import_block_lt(rec.import_block_lt);
        return;
      }
      case OutMsg::msg_export_deq_short: {
        OutMsg::Record_msg_export_deq_short rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_export_deq_short");
        }
        auto* msg = out->mutable_msg_export_deq_short();
        msg->set_msg_env_hash(bytes(rec.msg_env_hash));
        msg->set_next_workchain(rec.next_workchain);
        msg->set_next_addr_pfx(rec.next_addr_pfx);
        msg->set_import_block_lt(rec.import_block_lt);
        return;
      }
      case OutMsg::msg_export_tr_req: {
        OutMsg::Record_msg_export_tr_req rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_export_tr_req");
        }
        auto* msg = out->mutable_msg_export_tr_req();
        fill_msg_envelope(msg->mutable_out_msg(), rec.out_msg);
        fill_in_msg(msg->mutable_imported(), vm::load_cell_slice_ref(rec.imported));
        return;
      }
      case OutMsg::msg_export_deq_imm: {
        OutMsg::Record_msg_export_deq_imm rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_export_deq_imm");
        }
        auto* msg = out->mutable_msg_export_deq_imm();
        fill_msg_envelope(msg->mutable_out_msg(), rec.out_msg);
        fill_in_msg(msg->mutable_reimport(), vm::load_cell_slice_ref(rec.reimport));
        return;
      }
      case OutMsg::msg_export_new_defer: {
        OutMsg::Record_msg_export_new_defer rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_export_new_defer");
        }
        auto* msg = out->mutable_msg_export_new_defer();
        fill_msg_envelope(msg->mutable_out_msg(), rec.out_msg);
        msg->set_transaction_hash(cell_hash(rec.transaction));
        return;
      }
      case OutMsg::msg_export_deferred_tr: {
        OutMsg::Record_msg_export_deferred_tr rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_export_deferred_tr");
        }
        auto* msg = out->mutable_msg_export_deferred_tr();
        fill_msg_envelope(msg->mutable_out_msg(), rec.out_msg);
        fill_in_msg(msg->mutable_imported(), vm::load_cell_slice_ref(rec.imported));
        return;
      }
    }
    fail_unpack("OutMsg");
  }

  void fill_msg_envelope(pb::MsgEnvelope* out, const Ref<Cell>& cell) {
    using block::gen::MsgEnvelope;
    auto cs = vm::load_cell_slice_ref(cell);
    switch (block::gen::t_MsgEnvelope.get_tag(*cs)) {
      case MsgEnvelope::msg_envelope: {
        MsgEnvelope::Record_msg_envelope rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_envelope");
        }
        fill_interm_addr(out->mutable_cur_addr(), rec.cur_addr);
        fill_interm_addr(out->mutable_next_addr(), rec.next_addr);
        out->set_fwd_fee_remaining(grams(rec.fwd_fee_remaining));
        out->set_msg_hash(cell_hash(rec.msg));
        return;
      }
      case MsgEnvelope::msg_envelope_v2: {
        MsgEnvelope::Record_msg_envelope_v2 rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("msg_envelope_v2");
        }
        fill_interm_addr(out->mutable_cur_addr(), rec.cur_addr);
        fill_interm_addr(out->mutable_next_addr(), rec.next_addr);
        out->set_fwd_fee_remaining(grams(rec.fwd_fee_remaining));
        out->set_msg_hash(cell_hash(rec.msg));
        CellSlice emitted_lt = *rec.emitted_lt;
        if (emitted_lt.fetch_ulong(1) == 1) {
          out->set_emitted_lt(emitted_lt.fetch_ulong(64));
        }
        CellSlice metadata = *rec.metadata;
        if (metadata.fetch_ulong(1) == 1) {
          block::gen::MsgMetadata::Record md;
          if (!block::gen::t_MsgMetadata.unpack(metadata, md)) {
            return fail_unpack("MsgMetadata");
          }
          out->mutable_metadata()->set_depth(md.depth);
          fill_msg_addr_int(out->mutable_metadata()->mutable_initiator_addr(), md.initiator_addr);
          out->mutable_metadata()->set_initiator_lt(md.initiator_lt);
        }
        return;
      }
    }
    fail_unpack("MsgEnvelope");
  }

  void fill_interm_addr(pb::IntermediateAddress* out, const Ref<CellSlice>& cs) {
    using block::gen::IntermediateAddress;
    switch (block::gen::t_IntermediateAddress.get_tag(*cs)) {
      case IntermediateAddress::interm_addr_regular: {
        IntermediateAddress::Record_interm_addr_regular rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("interm_addr_regular");
        }
        out->mutable_interm_addr_regular()->set_use_dest_bits(rec.use_dest_bits);
        return;
      }
      case IntermediateAddress::interm_addr_simple: {
        IntermediateAddress::Record_interm_addr_simple rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("interm_addr_simple");
        }
        out->mutable_interm_addr_simple()->set_workchain_id(rec.workchain_id);
        out->mutable_interm_addr_simple()->set_addr_pfx(rec.addr_pfx);
        return;
      }
      case IntermediateAddress::interm_addr_ext: {
        IntermediateAddress::Record_interm_addr_ext rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("interm_addr_ext");
        }
        out->mutable_interm_addr_ext()->set_workchain_id(rec.workchain_id);
        out->mutable_interm_addr_ext()->set_addr_pfx(rec.addr_pfx);
        return;
      }
    }
    fail_unpack("IntermediateAddress");
  }

  void fill_account_block(pb::AccountBlock* out, const Ref<CellSlice>& cs) {
    block::gen::AccountBlock::Record rec;
    if (!tlb::csr_unpack(cs, rec)) {
      return fail_unpack("AccountBlock");
    }
    out->set_account_addr(bytes(rec.account_addr));
    vm::AugmentedDictionary transactions{vm::DictNonEmpty(), rec.transactions, 64,
                                         block::tlb::aug_AccountTransactions};
    bool ok = transactions.check_for_each_extra([&](Ref<CellSlice> value, Ref<CellSlice>, td::ConstBitPtr, int) {
      fill_transaction(out->add_transactions(), value->prefetch_ref(), true);
      return error_.is_ok();
    });
    if (!ok) {
      return fail_unpack("AccountBlock transactions");
    }
    block::gen::HASH_UPDATE::Record update;
    if (!tlb::type_unpack_cell(rec.state_update, block::gen::t_HASH_UPDATE_Account, update)) {
      return fail_unpack("HASH_UPDATE");
    }
    out->mutable_state_update()->set_old_hash(bytes(update.old_hash));
    out->mutable_state_update()->set_new_hash(bytes(update.new_hash));

    const auto* before = account_at(update.old_hash);
    const auto* after = account_at(update.new_hash);
    if (before == nullptr || after == nullptr) {
      return;  // account_at has recorded the error
    }
    *out->mutable_state_before() = before->account;
    *out->mutable_state_after() = after->account;
    if (!after->account.account().storage().state().has_account_active()) {
      return;
    }
    auto* state_init =
        out->mutable_state_after()->mutable_account()->mutable_storage()->mutable_state()->mutable_account_active();
    auto changed = [before](const Ref<Cell>& cell, Ref<Cell> LoadedAccount::*field) {
      return cell.not_null() && ((before->*field).is_null() || (before->*field)->get_hash() != cell->get_hash());
    };
    if (changed(after->code, &LoadedAccount::code)) {
      fill_cell(state_init->mutable_code(), after->code, true);
    }
    if (changed(after->data, &LoadedAccount::data)) {
      auto boc = serialize_boc(after->data);
      if (boc.is_error()) {
        return fail(boc.move_as_error());
      }
      if (boc.ok().size() <= kMaxDataBocSize) {
        state_init->mutable_data()->set_boc(boc.move_as_ok());
      } else {
        state_init->mutable_data()->set_boc_omitted(true);
      }
    }
    if (changed(after->library, &LoadedAccount::library)) {
      fill_cell(state_init->mutable_library(), after->library, true);
    }
  }

  // Accounts are loaded from the cell db by hash. The cell db keeps a cell while
  // any kept state references it, so the state a block starts from is there as
  // long as the node keeps the state of the previous block. A missing account
  // is an error: the block is not written without it.
  const LoadedAccount* account_at(const td::Bits256& hash) {
    auto it = accounts_.find(hash);
    if (it == accounts_.end()) {
      it = accounts_.emplace(hash, load_account(hash)).first;
    }
    return it->second ? &*it->second : nullptr;
  }

  std::optional<LoadedAccount> load_account(const td::Bits256& hash) {
    LoadedAccount account;
    if (hash == account_none_hash()) {
      account.account.mutable_account_none();
      return account;
    }
    if (!parsed_block_.cell_db_reader_) {
      fail(td::Status::Error("cell db reader is missing"));
      return std::nullopt;
    }
    auto cell = parsed_block_.cell_db_reader_->load_cell(hash.as_slice());
    if (cell.is_error()) {
      fail(cell.move_as_error_prefix(PSLICE() << "account state " << hash.to_hex() << ": "));
      return std::nullopt;
    }
    fill_account(&account, cell.move_as_ok());
    return account;
  }

  void fill_account(LoadedAccount* out, const Ref<Cell>& root) {
    auto cs = vm::load_cell_slice_ref(root);
    if (block::gen::t_Account.get_tag(*cs) == block::gen::Account::account_none) {
      out->account.mutable_account_none();
      return;
    }
    block::gen::Account::Record_account rec;
    if (!tlb::csr_unpack(cs, rec)) {
      return fail_unpack("Account");
    }
    auto* account = out->account.mutable_account();
    fill_msg_addr_int(account->mutable_addr(), rec.addr);
    fill_storage_info(account->mutable_storage_stat(), rec.storage_stat);

    block::gen::AccountStorage::Record storage;
    if (!tlb::csr_unpack(rec.storage, storage)) {
      return fail_unpack("AccountStorage");
    }
    auto* out_storage = account->mutable_storage();
    out_storage->set_last_trans_lt(storage.last_trans_lt);
    fill_currencies(out_storage->mutable_balance(), storage.balance);
    auto* state = out_storage->mutable_state();
    using block::gen::AccountState;
    switch (block::gen::t_AccountState.get_tag(*storage.state)) {
      case AccountState::account_uninit:
        state->mutable_account_uninit();
        return;
      case AccountState::account_frozen: {
        AccountState::Record_account_frozen frozen;
        if (!tlb::csr_unpack(storage.state, frozen)) {
          return fail_unpack("account_frozen");
        }
        state->mutable_account_frozen()->set_state_hash(bytes(frozen.state_hash));
        return;
      }
      case AccountState::account_active: {
        AccountState::Record_account_active active;
        if (!tlb::csr_unpack(storage.state, active)) {
          return fail_unpack("account_active");
        }
        fill_state_init(state->mutable_account_active(), active.x, false, out);
        return;
      }
    }
    fail_unpack("AccountState");
  }

  void fill_storage_info(pb::StorageInfo* out, const Ref<CellSlice>& cs) {
    block::gen::StorageInfo::Record info;
    if (!tlb::csr_unpack(cs, info)) {
      return fail_unpack("StorageInfo");
    }
    fill_storage_used(out->mutable_used(), info.used);
    using block::gen::StorageExtraInfo;
    switch (block::gen::t_StorageExtraInfo.get_tag(*info.storage_extra)) {
      case StorageExtraInfo::storage_extra_none:
        out->mutable_storage_extra()->mutable_storage_extra_none();
        break;
      case StorageExtraInfo::storage_extra_info: {
        StorageExtraInfo::Record_storage_extra_info extra;
        unsigned char dict_hash[32];
        if (!tlb::csr_unpack(info.storage_extra, extra) ||
            !extra.dict_hash->export_bytes(dict_hash, sizeof(dict_hash), false)) {
          return fail_unpack("storage_extra_info");
        }
        out->mutable_storage_extra()->mutable_storage_extra_info()->set_dict_hash(
            std::string(reinterpret_cast<const char*>(dict_hash), sizeof(dict_hash)));
        break;
      }
      default:
        return fail_unpack("StorageExtraInfo");
    }
    out->set_last_paid(info.last_paid);
    if (auto due_payment = maybe_grams(info.due_payment)) {
      out->set_due_payment(*due_payment);
    }
  }

  void fill_storage_used(pb::StorageUsed* out, const Ref<CellSlice>& cs) {
    block::gen::StorageUsed::Record used;
    if (!tlb::csr_unpack(cs, used)) {
      return fail_unpack("StorageUsed");
    }
    out->set_cells(block::tlb::t_VarUInteger_7.as_uint(*used.cells));
    out->set_bits(block::tlb::t_VarUInteger_7.as_uint(*used.bits));
  }

  // With cells set, keeps the cells of code, data and library there instead of
  // adding their BOCs.
  void fill_state_init(pb::StateInit* out, const Ref<CellSlice>& cs, bool with_bocs, LoadedAccount* cells) {
    block::gen::StateInit::Record init;
    if (!tlb::csr_unpack(cs, init)) {
      return fail_unpack("StateInit");
    }
    if (init.fixed_prefix_length->prefetch_ulong(1) == 1) {
      out->set_fixed_prefix_length(static_cast<std::uint32_t>(init.fixed_prefix_length->prefetch_ulong(6) & 0x1f));
    }
    if (init.special->prefetch_ulong(1) == 1) {
      auto tick_tock = init.special->prefetch_ulong(3);
      out->mutable_special()->set_tick(tick_tock & 2);
      out->mutable_special()->set_tock(tick_tock & 1);
    }
    auto code = maybe_ref(init.code);
    auto data = maybe_ref(init.data);
    auto library = maybe_ref(init.library);
    if (code.not_null()) {
      fill_cell(out->mutable_code(), code, with_bocs);
    }
    if (data.not_null()) {
      fill_cell(out->mutable_data(), data, with_bocs);
    }
    if (library.not_null()) {
      fill_cell(out->mutable_library(), library, with_bocs);
    }
    if (cells != nullptr) {
      cells->code = std::move(code);
      cells->data = std::move(data);
      cells->library = std::move(library);
    }
  }

  void fill_transaction(pb::Transaction* out, const Ref<Cell>& cell, bool with_trace_id) {
    block::gen::Transaction::Record tx;
    if (!tlb::unpack_cell(cell, tx)) {
      return fail_unpack("Transaction");
    }
    td::Bits256 hash{cell->get_hash().bits()};
    out->set_hash(bytes(hash));
    out->set_account_addr(bytes(tx.account_addr));
    out->set_lt(tx.lt);
    out->set_prev_trans_hash(bytes(tx.prev_trans_hash));
    out->set_prev_trans_lt(tx.prev_trans_lt);
    out->set_now(tx.now);
    out->set_outmsg_cnt(static_cast<std::uint32_t>(tx.outmsg_cnt));
    out->set_orig_status(account_status(tx.orig_status));
    out->set_end_status(account_status(tx.end_status));
    if (tx.r1.in_msg->prefetch_ulong(1) == 1) {
      fill_message(out->mutable_in_msg(), tx.r1.in_msg->prefetch_ref());
    }
    if (tx.outmsg_cnt > 0) {
      vm::Dictionary out_msgs{tx.r1.out_msgs, 15};
      for (int i = 0; i < tx.outmsg_cnt; i++) {
        auto msg = out_msgs.lookup_ref(td::BitArray<15>{i});
        if (msg.is_null()) {
          return fail_unpack("out_msgs");
        }
        fill_message(out->add_out_msgs(), msg);
      }
    }
    fill_currencies(out->mutable_total_fees(), tx.total_fees);
    block::gen::HASH_UPDATE::Record update;
    if (!tlb::type_unpack_cell(tx.state_update, block::gen::t_HASH_UPDATE_Account, update)) {
      return fail_unpack("HASH_UPDATE");
    }
    out->mutable_state_update()->set_old_hash(bytes(update.old_hash));
    out->mutable_state_update()->set_new_hash(bytes(update.new_hash));
    fill_descr(out->mutable_description(), tx.description);
    if (with_trace_id) {
      if (auto it = trace_ids_.find(hash); it != trace_ids_.end()) {
        out->set_trace_id(bytes(it->second));
      }
    }
  }

  void fill_message(pb::Message* out, const Ref<Cell>& cell) {
    out->set_hash(cell_hash(cell));
    block::gen::Message::Record message;
    if (!tlb::type_unpack_cell(cell, block::gen::t_Message_Any, message)) {
      return fail_unpack("Message");
    }
    fill_msg_info(out->mutable_info(), message.info);

    CellSlice init = *message.init;
    if (init.fetch_ulong(1) == 1) {
      // Either StateInit ^StateInit
      auto state_init =
          init.fetch_ulong(1) == 0 ? Ref<CellSlice>{true, init} : vm::load_cell_slice_ref(init.fetch_ref());
      fill_state_init(out->mutable_init(), state_init, true, nullptr);
    }

    // Either X ^X
    CellSlice body = *message.body;
    Ref<Cell> body_cell =
        body.fetch_ulong(1) == 1 ? body.fetch_ref() : vm::CellBuilder().append_cellslice(body).finalize();
    fill_cell(out->mutable_body(), body_cell, true);
    auto body_cs = vm::load_cell_slice(body_cell);
    if (body_cs.size() >= 32) {
      out->set_opcode(static_cast<std::uint32_t>(body_cs.prefetch_ulong(32)));
    }

    if (auto it = norm_hashes_.find(td::Bits256{cell->get_hash().bits()}); it != norm_hashes_.end()) {
      out->set_hash_norm(bytes(it->second));
    }
  }

  void fill_msg_info(pb::CommonMsgInfo* out, const Ref<CellSlice>& cs) {
    using block::gen::CommonMsgInfo;
    switch (block::gen::t_CommonMsgInfo.get_tag(*cs)) {
      case CommonMsgInfo::int_msg_info: {
        CommonMsgInfo::Record_int_msg_info rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("int_msg_info");
        }
        auto* info = out->mutable_int_msg_info();
        info->set_ihr_disabled(rec.ihr_disabled);
        info->set_bounce(rec.bounce);
        info->set_bounced(rec.bounced);
        fill_msg_addr_int(info->mutable_src(), rec.src);
        fill_msg_addr_int(info->mutable_dest(), rec.dest);
        fill_currencies(info->mutable_value(), rec.value);
        info->set_extra_flags(grams(rec.extra_flags));
        info->set_fwd_fee(grams(rec.fwd_fee));
        info->set_created_lt(rec.created_lt);
        info->set_created_at(rec.created_at);
        return;
      }
      case CommonMsgInfo::ext_in_msg_info: {
        CommonMsgInfo::Record_ext_in_msg_info rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("ext_in_msg_info");
        }
        auto* info = out->mutable_ext_in_msg_info();
        fill_msg_addr_ext(info->mutable_src(), rec.src);
        fill_msg_addr_int(info->mutable_dest(), rec.dest);
        if (auto import_fee = to_uint64(block::tlb::t_Grams.as_integer(*rec.import_fee))) {
          info->set_import_fee(*import_fee);
        }
        return;
      }
      case CommonMsgInfo::ext_out_msg_info: {
        CommonMsgInfo::Record_ext_out_msg_info rec;
        if (!tlb::csr_unpack(cs, rec)) {
          return fail_unpack("ext_out_msg_info");
        }
        auto* info = out->mutable_ext_out_msg_info();
        fill_msg_addr_int(info->mutable_src(), rec.src);
        fill_msg_addr_ext(info->mutable_dest(), rec.dest);
        info->set_created_lt(rec.created_lt);
        info->set_created_at(rec.created_at);
        return;
      }
    }
    fail_unpack("CommonMsgInfo");
  }

  // The phases come from the indexer's parser of TransactionDescr, which follows block.tlb.
  void fill_descr(pb::TransactionDescr* out, const Ref<Cell>& cell) {
    auto cs = vm::load_cell_slice(cell);
    auto descr = ParseQuery::process_transaction_descr(cs);
    if (descr.is_error()) {
      return fail(descr.move_as_error_prefix("TransactionDescr: "));
    }
    std::visit(overloaded{
                   [&](const schema::TransactionDescr_ord& d) {
                     auto* o = out->mutable_trans_ord();
                     o->set_credit_first(d.credit_first);
                     if (d.storage_ph) fill_storage_ph(o->mutable_storage_ph(), *d.storage_ph);
                     if (d.credit_ph) fill_credit_ph(o->mutable_credit_ph(), *d.credit_ph);
                     fill_compute_ph(o->mutable_compute_ph(), d.compute_ph);
                     if (d.action) fill_action_ph(o->mutable_action(), *d.action);
                     o->set_aborted(d.aborted);
                     if (d.bounce) fill_bounce_ph(o->mutable_bounce(), *d.bounce);
                     o->set_destroyed(d.destroyed);
                   },
                   [&](const schema::TransactionDescr_storage& d) {
                     fill_storage_ph(out->mutable_trans_storage()->mutable_storage_ph(), d.storage_ph);
                   },
                   [&](const schema::TransactionDescr_tick_tock& d) {
                     auto* o = out->mutable_trans_tick_tock();
                     o->set_is_tock(d.is_tock);
                     fill_storage_ph(o->mutable_storage_ph(), d.storage_ph);
                     fill_compute_ph(o->mutable_compute_ph(), d.compute_ph);
                     if (d.action) fill_action_ph(o->mutable_action(), *d.action);
                     o->set_aborted(d.aborted);
                     o->set_destroyed(d.destroyed);
                   },
                   [&](const schema::TransactionDescr_split_prepare& d) {
                     auto* o = out->mutable_trans_split_prepare();
                     fill_split_info(o->mutable_split_info(), d.split_info);
                     if (d.storage_ph) fill_storage_ph(o->mutable_storage_ph(), *d.storage_ph);
                     fill_compute_ph(o->mutable_compute_ph(), d.compute_ph);
                     if (d.action) fill_action_ph(o->mutable_action(), *d.action);
                     o->set_aborted(d.aborted);
                     o->set_destroyed(d.destroyed);
                   },
                   [&](const schema::TransactionDescr_split_install& d) {
                     auto* o = out->mutable_trans_split_install();
                     fill_split_info(o->mutable_split_info(), d.split_info);
                     block::gen::TransactionDescr::Record_trans_split_install rec;
                     if (!tlb::unpack_cell(cell, rec)) {
                       return fail_unpack("trans_split_install");
                     }
                     fill_transaction(o->mutable_prepare_transaction(), rec.prepare_transaction, false);
                     o->set_installed(d.installed);
                   },
                   [&](const schema::TransactionDescr_merge_prepare& d) {
                     auto* o = out->mutable_trans_merge_prepare();
                     fill_split_info(o->mutable_split_info(), d.split_info);
                     fill_storage_ph(o->mutable_storage_ph(), d.storage_ph);
                     o->set_aborted(d.aborted);
                   },
                   [&](const schema::TransactionDescr_merge_install& d) {
                     auto* o = out->mutable_trans_merge_install();
                     fill_split_info(o->mutable_split_info(), d.split_info);
                     block::gen::TransactionDescr::Record_trans_merge_install rec;
                     if (!tlb::unpack_cell(cell, rec)) {
                       return fail_unpack("trans_merge_install");
                     }
                     fill_transaction(o->mutable_prepare_transaction(), rec.prepare_transaction, false);
                     if (d.storage_ph) fill_storage_ph(o->mutable_storage_ph(), *d.storage_ph);
                     if (d.credit_ph) fill_credit_ph(o->mutable_credit_ph(), *d.credit_ph);
                     fill_compute_ph(o->mutable_compute_ph(), d.compute_ph);
                     if (d.action) fill_action_ph(o->mutable_action(), *d.action);
                     o->set_aborted(d.aborted);
                     o->set_destroyed(d.destroyed);
                   },
               },
               descr.ok());
  }

  void fill_storage_ph(pb::TrStoragePhase* out, const schema::TrStoragePhase& phase) {
    out->set_storage_fees_collected(coins(phase.storage_fees_collected));
    if (phase.storage_fees_due) {
      out->set_storage_fees_due(coins(*phase.storage_fees_due));
    }
    out->set_status_change(acc_status_change(phase.status_change));
  }

  void fill_credit_ph(pb::TrCreditPhase* out, const schema::TrCreditPhase& phase) {
    if (phase.due_fees_collected) {
      out->set_due_fees_collected(coins(*phase.due_fees_collected));
    }
    fill_currencies(out->mutable_credit(), phase.credit);
  }

  void fill_compute_ph(pb::TrComputePhase* out, const schema::TrComputePhase& phase) {
    std::visit(overloaded{
                   [&](const schema::TrComputePhase_skipped& skipped) {
                     out->mutable_tr_phase_compute_skipped()->set_reason(compute_skip_reason(skipped.reason));
                   },
                   [&](const schema::TrComputePhase_vm& vm) {
                     auto* o = out->mutable_tr_phase_compute_vm();
                     o->set_success(vm.success);
                     o->set_msg_state_used(vm.msg_state_used);
                     o->set_account_activated(vm.account_activated);
                     o->set_gas_fees(coins(vm.gas_fees));
                     o->set_gas_used(vm.gas_used);
                     o->set_gas_limit(vm.gas_limit);
                     if (vm.gas_credit) {
                       o->set_gas_credit(static_cast<std::uint32_t>(*vm.gas_credit));
                     }
                     o->set_mode(vm.mode);
                     o->set_exit_code(vm.exit_code);
                     if (vm.exit_arg) {
                       o->set_exit_arg(*vm.exit_arg);
                     }
                     o->set_vm_steps(vm.vm_steps);
                     o->set_vm_init_state_hash(bytes(vm.vm_init_state_hash));
                     o->set_vm_final_state_hash(bytes(vm.vm_final_state_hash));
                   },
               },
               phase);
  }

  void fill_action_ph(pb::TrActionPhase* out, const schema::TrActionPhase& phase) {
    out->set_success(phase.success);
    out->set_valid(phase.valid);
    out->set_no_funds(phase.no_funds);
    out->set_status_change(acc_status_change(phase.status_change));
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

  void fill_bounce_ph(pb::TrBouncePhase* out, const schema::TrBouncePhase& phase) {
    std::visit(overloaded{
                   [&](const schema::TrBouncePhase_negfunds&) { out->mutable_tr_phase_bounce_negfunds(); },
                   [&](const schema::TrBouncePhase_nofunds& nofunds) {
                     auto* o = out->mutable_tr_phase_bounce_nofunds();
                     fill_msg_size(o->mutable_msg_size(), nofunds.msg_size);
                     o->set_req_fwd_fees(coins(nofunds.req_fwd_fees));
                   },
                   [&](const schema::TrBouncePhase_ok& ok) {
                     auto* o = out->mutable_tr_phase_bounce_ok();
                     fill_msg_size(o->mutable_msg_size(), ok.msg_size);
                     o->set_msg_fees(coins(ok.msg_fees));
                     o->set_fwd_fees(coins(ok.fwd_fees));
                   },
               },
               phase);
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

  const auto& mc_block = block.masterchain_block();
  // a masterchain block has a single previous block
  const auto& prev = mc_block.info().prev_ref().prev_blk_info().prev();
  FirehoseBlock out;
  out.number = mc_block.info().seq_no();
  out.id = td::hex_encode(mc_block.id().root_hash());
  out.parent_num = prev.seq_no();
  out.parent_id = td::hex_encode(prev.root_hash());
  out.gen_utime = mc_block.info().gen_utime();
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
  // enums start with a meaningful zero value, such as ACC_STATE_UNINIT
  options.always_print_primitive_fields = true;
  std::string out;
  auto status = google::protobuf::util::MessageToJsonString(block, &out, options);
  if (!status.ok()) {
    return td::Status::Error(status.ToString());
  }
  return out;
}
