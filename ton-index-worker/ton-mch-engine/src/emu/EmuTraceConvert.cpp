#include "EmuTraceConvert.h"

#include "DataParser.h"  // ParseQuery::parse_transaction (static parse seam)
#include "vm/boc.h"

namespace mch {

td::Result<std::vector<schema::Transaction>> emu_to_schema_txs(const EmuTraceView &view) {
  std::vector<schema::Transaction> txs;
  txs.reserve(view.nodes.size());
  for (std::size_t i = 0; i < view.nodes.size(); i++) {
    const EmuTxRef &node = view.nodes[i];
    if (!node.tx_boc || node.tx_boc->empty()) {
      return td::Status::Error(PSLICE() << "node " << i << " has no transaction data");
    }
    auto cell_result = vm::std_boc_deserialize(*node.tx_boc);
    if (cell_result.is_error()) {
      return cell_result.move_as_error_prefix(
          PSLICE() << "node " << i << " has an invalid transaction BOC: ");
    }
    auto tx_root = cell_result.move_as_ok();
    // Version 12 only selects message fields that no downstream consumer reads.
    auto r_tx = ParseQuery::parse_transaction(tx_root, node.address.workchain, 12);
    if (r_tx.is_error()) {
      return r_tx.move_as_error_prefix(PSLICE() << "node " << i << ": ");
    }
    schema::Transaction tx = r_tx.move_as_ok();
    tx.mc_seqno = node.mc_seqno;
    txs.push_back(std::move(tx));
  }
  return txs;
}

}  // namespace mch
