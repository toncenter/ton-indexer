#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "TraceUpdate.h"
#include "td/utils/crypto.h"
#include "td/utils/base64.h"
#include "vm/cells/CellBuilder.h"
#include "vm/dict.h"

namespace trace_bench {

inline td::Bits256 hash(const std::string& value) {
  td::Bits256 result;
  result.as_slice().copy_from(td::sha256(value));
  return result;
}

inline void address(vm::CellBuilder& builder, const block::StdAddress& account) {
  builder.store_long(4, 3).store_long(account.workchain, 8).store_bytes(account.addr.as_slice());
}

inline void coins(vm::CellBuilder& builder, std::uint32_t value) {
  unsigned bytes = 0;
  for (auto remaining = value; remaining; remaining >>= 8) ++bytes;
  builder.store_long(bytes, 4).store_long(value, bytes * 8);
}

inline td::Ref<vm::Cell> body(std::size_t bytes, std::uint64_t id) {
  td::Ref<vm::Cell> tail;
  while (bytes) {
    const auto part = std::min<std::size_t>(bytes, 120);
    vm::CellBuilder cell;
    cell.store_bytes(std::string(part, 'x'));
    if (tail.not_null()) cell.store_ref(tail);
    tail = cell.finalize();
    bytes -= part;
  }
  vm::CellBuilder first;
  first.store_long(0, 32).store_long(id, 64);  // TON transfer / comment body.
  if (tail.not_null()) first.store_ref(tail);
  return first.finalize();
}

inline td::Ref<vm::Cell> message(const block::StdAddress& source, const block::StdAddress& destination,
                                bool external, std::uint64_t id, std::size_t payload_bytes) {
  vm::CellBuilder builder;
  if (external) {
    builder.store_long(2, 2).store_long(0, 2);  // ext_in, addr_none
    address(builder, destination);
    builder.store_long(0, 4);  // import_fee
  } else {
    builder.store_long(4, 4);  // internal, ihr_disabled, no bounce
    address(builder, source);
    address(builder, destination);
    coins(builder, 1000000);
    builder.store_long(0, 1).store_long(0, 4).store_long(0, 4);  // currencies, fees
    builder.store_long(id, 64).store_long(1700000000, 32);
  }
  return builder.store_long(0, 1).store_long(1, 1).store_ref(body(payload_bytes, id)).finalize();
}

inline td::Ref<vm::Cell> transaction(const block::StdAddress& account, const td::Ref<vm::Cell>& in,
                                    const std::vector<td::Ref<vm::Cell>>& out, std::uint64_t lt) {
  vm::Dictionary messages(15);
  for (std::size_t i = 0; i < out.size(); ++i) {
    CHECK(messages.set_ref(td::BitArray<15>{static_cast<int>(i)}, out[i]));
  }
  vm::CellBuilder refs;
  refs.store_long(1, 1).store_ref(in);
  CHECK(messages.append_dict_to_bool(refs));
  const auto zero = hash("benchmark state hash");
  auto hashes = vm::CellBuilder().store_long(0x72, 8).store_bytes(zero.as_slice())
                    .store_bytes(zero.as_slice()).finalize();
  auto vm_phase = vm::CellBuilder().store_long(1, 3).store_long(100, 8)
                      .store_long(2, 3).store_long(10000, 16).store_long(0, 1)
                      .store_long(0, 8).store_long(0, 32).store_long(0, 1).store_long(10, 32)
                      .store_bytes(zero.as_slice()).store_bytes(zero.as_slice()).finalize();
  auto action = vm::CellBuilder().store_long(6, 3).store_long(0, 1).store_long(0, 2)
                    .store_long(0, 32).store_long(0, 1).store_long(out.size(), 16)
                    .store_long(0, 16).store_long(0, 16).store_long(out.size(), 16)
                    .store_bytes(zero.as_slice()).store_long(0, 6).finalize();
  auto description = vm::CellBuilder().store_long(0, 4).store_long(1, 1).store_long(0, 2)
                         .store_long(12, 4).store_long(0, 4).store_ref(vm_phase)
                         .store_long(1, 1).store_ref(action).store_long(0, 3).finalize();
  return vm::CellBuilder().store_long(7, 4).store_bytes(account.addr.as_slice()).store_long(lt, 64)
      .store_bytes(zero.as_slice()).store_long(0, 64).store_long(1700000000, 32)
      .store_long(out.size(), 15).store_long(10, 4).store_ref(refs.finalize())
      .store_long(0, 5).store_ref(hashes).store_ref(description).finalize();
}

inline block::Account account_state(const block::StdAddress& addr, std::uint64_t lt,
                                     const td::Bits256& transaction_hash) {
  block::Account account;
  account.code = vm::CellBuilder().store_long(0, 8).finalize();
  account.data = vm::CellBuilder().store_long(lt, 64).finalize();
  account.now_ = 1700000000;
  account.last_trans_lt_ = lt;
  account.last_trans_hash_ = transaction_hash;
  vm::CellBuilder state;
  state.store_long(1, 1);
  address(state, addr);
  state.store_long(0, 9).store_long(account.now_, 32).store_long(0, 1);  // StorageInfo
  state.store_long(lt, 64);
  coins(state, 1000000000);
  state.store_long(0, 1).store_long(1, 1);  // empty currencies, active account
  state.store_long(0, 2).store_long(1, 1).store_ref(account.code)
       .store_long(1, 1).store_ref(account.data).store_long(0, 1);
  account.total_state = state.finalize();
  return account;
}

struct SyntheticTrace {
  TraceUpdate pending;
  std::vector<TraceUpdate> confirmed, finalized;
  std::string key;
};

// Greater than the largest supported trace, keeping message ids and LTs unique.
inline constexpr std::uint64_t kTraceStride = 8192;

inline SyntheticTrace generate(std::uint64_t trace_id, std::size_t nodes, std::size_t account_count,
                                std::size_t payload_bytes, ton::BlockSeqno head_seqno, bool nonfinalized,
                                std::uint64_t seed) {
  const std::size_t head_nodes = nodes == 1 ? 1 : nodes / 2;
  std::vector<block::StdAddress> accounts;
  std::vector<td::Ref<vm::Cell>> messages, transactions;
  for (std::size_t i = 0; i < nodes; ++i) {
    accounts.emplace_back(0, hash("ton-trace-bench-account:" + std::to_string(seed) + ":" +
                                 std::to_string((trace_id * nodes + i) % account_count)));
    messages.push_back(message(accounts[i ? (i - 1) / 2 : 0], accounts[i], i == 0,
                                seed * 100000000 + trace_id * kTraceStride + i, payload_bytes));
  }
  for (std::size_t i = 0; i < nodes; ++i) {
    std::vector<td::Ref<vm::Cell>> out;
    for (auto child : {2 * i + 1, 2 * i + 2})
      if (child < nodes) out.push_back(messages[child]);
    transactions.push_back(transaction(accounts[i], messages[i], out, 1000000 + trace_id * kTraceStride + i));
  }
  const auto normalized = ext_in_msg_get_normalized_hash(messages[0]).move_as_ok();
  auto make_update = [&](FinalityState finality, bool tail) {
    TraceUpdate update;
    auto make_node = [&](std::size_t i) {
      auto node = std::make_unique<TraceNode>();
      node->node_id = messages[i]->get_hash().bits();
      node->address = accounts[i];
      node->transaction_root = transactions[i];
      node->finality_state = tail || i < head_nodes ? finality : FinalityState::Emulated;
      node->mc_block_seqno = head_seqno + (tail ? 1 : 0);
      node->block_id = ton::BlockId{0, ton::shardIdAll, 2 * head_seqno + (tail ? 1u : 0u)};
      return node;
    };
    auto make_fragment = [&](std::unique_ptr<TraceNode> root) {
      Trace trace;
      trace.root = std::move(root);
      trace.ext_in_msg_hash = messages[0]->get_hash().bits();
      trace.ext_in_msg_hash_norm = normalized;
      trace.root_tx_hash = transactions[0]->get_hash().bits();
      return trace;
    };
    if (tail) {
      for (std::size_t i = head_nodes; i < nodes; ++i) {
        auto fragment = make_fragment(make_node(i));
        fragment.committed_accounts.emplace(accounts[i], account_state(accounts[i], 1000000 + trace_id * kTraceStride + i,
                                                                        transactions[i]->get_hash().bits()));
        update.fragments.push_back(std::move(fragment));
      }
    } else {
      std::vector<std::unique_ptr<TraceNode>> tree(nodes);
      for (std::size_t i = nodes; i-- > 0;) {
        tree[i] = make_node(i);
        for (auto child : {2 * i + 1, 2 * i + 2})
          if (child < nodes) tree[i]->children.push_back(std::move(tree[child]));
      }
      auto fragment = make_fragment(std::move(tree[0]));
      if (finality != FinalityState::Emulated) {
        for (std::size_t i = 0; i < head_nodes; ++i)
          fragment.committed_accounts.insert_or_assign(accounts[i], account_state(accounts[i], 1000000 + trace_id * kTraceStride + i,
                                                                                   transactions[i]->get_hash().bits()));
      }
      update.fragments.push_back(std::move(fragment));
    }
    return update;
  };
  SyntheticTrace result;
  result.key = td::base64_encode(normalized.as_slice());
  result.finalized.push_back(make_update(FinalityState::Finalized, false));
  if (nodes > 1) result.finalized.push_back(make_update(FinalityState::Finalized, true));
  if (nonfinalized) {
    result.pending = make_update(FinalityState::Emulated, false);
    result.confirmed.push_back(make_update(FinalityState::Confirmed, false));
    if (nodes > 1) result.confirmed.push_back(make_update(FinalityState::Confirmed, true));
  }
  return result;
}

// A binary tree of short chains. Each block introduces one connected fragment;
// its parent lives in an earlier block. No future transactions enter the snapshot
// early, and total depth stays logarithmic in the number of fragments.
inline SyntheticTrace generate_growing(std::uint64_t trace_id, std::size_t nodes, std::size_t fragment_txs,
                                       std::size_t account_count, std::size_t payload_bytes,
                                       ton::BlockSeqno head_seqno, bool nonfinalized, std::uint64_t seed) {
  CHECK(nodes > 0 && fragment_txs > 0);
  std::vector<block::StdAddress> accounts;
  std::vector<td::Ref<vm::Cell>> messages, transactions;
  for (std::size_t i = 0; i < nodes; ++i) {
    accounts.emplace_back(0, hash("ton-trace-bench-account:" + std::to_string(seed) + ":" +
                                  std::to_string((trace_id * nodes + i) % account_count)));
    const auto step = i / fragment_txs;
    const auto parent = i % fragment_txs ? i - 1 : step ? ((step - 1) / 2 + 1) * fragment_txs - 1 : 0;
    messages.push_back(message(accounts[parent], accounts[i], i == 0,
                                seed * 100000000 + trace_id * kTraceStride + i, payload_bytes));
  }
  for (std::size_t i = 0; i < nodes; ++i) {
    std::vector<td::Ref<vm::Cell>> children;
    if ((i + 1) % fragment_txs && i + 1 < nodes) {
      children.push_back(messages[i + 1]);
    } else {
      const auto step = i / fragment_txs;
      for (auto child : {(2 * step + 1) * fragment_txs, (2 * step + 2) * fragment_txs})
        if (child < nodes) children.push_back(messages[child]);
    }
    transactions.push_back(transaction(accounts[i], messages[i], children, 1000000 + trace_id * kTraceStride + i));
  }
  const auto normalized = ext_in_msg_get_normalized_hash(messages[0]).move_as_ok();
  auto update = [&](std::size_t step, FinalityState finality) {
    Trace trace;
    trace.ext_in_msg_hash = messages[0]->get_hash().bits();
    trace.ext_in_msg_hash_norm = normalized;
    trace.root_tx_hash = transactions[0]->get_hash().bits();
    const auto begin = step * fragment_txs, end = std::min(nodes, begin + fragment_txs);
    for (auto i = end; i-- > begin;) {
      auto node = std::make_unique<TraceNode>();
      node->node_id = messages[i]->get_hash().bits();
      node->address = accounts[i];
      node->transaction_root = transactions[i];
      node->finality_state = finality;
      node->mc_block_seqno = head_seqno + step;
      node->block_id = ton::BlockId{0, ton::shardIdAll, ton::BlockSeqno(head_seqno + step)};
      if (trace.root) node->children.push_back(std::move(trace.root));
      trace.root = std::move(node);
    }
    if (finality != FinalityState::Emulated) {
      for (auto i = begin; i < end; ++i)
        trace.committed_accounts.insert_or_assign(accounts[i], account_state(accounts[i],
            1000000 + trace_id * kTraceStride + i, transactions[i]->get_hash().bits()));
    }
    return make_trace_update(std::move(trace), {});
  };
  SyntheticTrace result;
  result.key = td::base64_encode(normalized.as_slice());
  for (std::size_t step = 0; step * fragment_txs < nodes; ++step) {
    result.finalized.push_back(update(step, FinalityState::Finalized));
    if (nonfinalized) result.confirmed.push_back(update(step, FinalityState::Confirmed));
  }
  if (nonfinalized) result.pending = update(0, FinalityState::Emulated);
  return result;
}

}  // namespace trace_bench
