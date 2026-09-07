#pragma once

#include <optional>

#include "td/utils/base64.h"
#include "td/utils/tests.h"
#include "vm/cells/CellBuilder.h"
#include "vm/dict.h"

#include "TraceAssembler.h"

namespace trace_test {

inline td::Bits256 hash(char digit) {
  td::Bits256 result;
  CHECK(result.from_hex(std::string(64, digit)) == 256);
  return result;
}

inline std::string key(const td::Ref<vm::Cell>& cell) {
  return td::base64_encode(cell->get_hash().as_slice());
}

inline void address(vm::CellBuilder& builder) {
  builder.store_long(4, 3).store_long(0, 8).store_bytes(hash('a').as_slice());
}

inline td::Ref<vm::Cell> message(std::uint32_t marker, bool external = false) {
  vm::CellBuilder builder;
  if (external) {
    builder.store_long(2, 2).store_long(0, 2);  // ext_in, addr_none
    address(builder);
    builder.store_long(0, 4);  // import_fee
  } else {
    builder.store_long(4, 4);  // int_msg, ihr_disabled
    address(builder);
    address(builder);
    builder.store_long(0, 5).store_long(0, 8).store_long(marker, 64).store_long(1000, 32);
  }
  return builder.store_long(0, 2).store_long(marker, 32).finalize();  // no init, inline body
}

// Small, valid TL-B transactions exercise the real serializer, not cached or
// hand-written Redis payloads. Changing lt changes execution, not in-message id.
inline std::unique_ptr<TraceNode> node(td::Ref<vm::Cell> in, std::vector<td::Ref<vm::Cell>> out, FinalityState finality,
                                       std::uint64_t lt = 100) {
  vm::Dictionary messages(15);
  for (std::size_t i = 0; i < out.size(); ++i) {
    CHECK(messages.set_ref(td::BitArray<15>{static_cast<int>(i)}, out[i]));
  }
  vm::CellBuilder message_refs;
  message_refs.store_long(1, 1).store_ref(in);
  CHECK(messages.append_dict_to_bool(message_refs));
  auto hashes = vm::CellBuilder()
                    .store_long(0x72, 8)
                    .store_bytes(hash('0').as_slice())
                    .store_bytes(hash('0').as_slice())
                    .finalize();
  // trans_ord, no storage/credit/action/bounce, skipped compute, aborted.
  auto description = vm::CellBuilder().store_long(0, 11).store_long(1, 1).store_long(0, 2).finalize();
  auto transaction = vm::CellBuilder()
                         .store_long(7, 4)
                         .store_bytes(hash('a').as_slice())
                         .store_long(lt, 64)
                         .store_bytes(hash('0').as_slice())
                         .store_long(0, 64)
                         .store_long(1000, 32)
                         .store_long(out.size(), 15)
                         .store_long(0, 4)
                         .store_ref(message_refs.finalize())
                         .store_long(0, 5)
                         .store_ref(hashes)
                         .store_ref(description)
                         .finalize();
  auto result = std::make_unique<TraceNode>();
  result->node_id = in->get_hash().bits();
  result->address = block::StdAddress{0, hash('a')};
  result->transaction_root = std::move(transaction);
  result->mc_block_seqno = 100;
  result->block_id = ton::BlockId{0, ton::shardIdAll, 10};
  result->finality_state = finality;
  return result;
}

inline Trace trace(std::unique_ptr<TraceNode> root, const td::Ref<vm::Cell>& external,
                   std::optional<td::Bits256> root_transaction_hash = {}) {
  Trace result;
  result.root_tx_hash = root_transaction_hash.value_or(root->transaction_root->get_hash().bits());
  result.ext_in_msg_hash = external->get_hash().bits();
  result.ext_in_msg_hash_norm = hash('f');
  result.root = std::move(root);
  return result;
}

inline TraceTransition apply(ActiveTrace& state, Trace patch) {
  auto result = TraceAssembler().apply(state, patch, "trace");
  if (result.is_error()) {
    LOG(FATAL) << result.error();
  }
  auto transition = result.move_as_ok();
  if (transition.needs_redis_write) {
    state = transition.next_trace;
  }
  return transition;
}

}  // namespace trace_test
