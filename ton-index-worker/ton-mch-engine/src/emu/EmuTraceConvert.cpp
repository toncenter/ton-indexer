#include "EmuTraceConvert.h"

#include "DataParser.h"  // ParseQuery::parse_transaction (static parse seam)

namespace mch {

td::Result<std::vector<schema::Transaction>> emu_to_schema_txs(const EmuTraceView &view,
                                                               int global_version) {
  std::vector<schema::Transaction> txs;
  txs.reserve(view.nodes.size());
  for (std::size_t i = 0; i < view.nodes.size(); i++) {
    const EmuTxRef &node = view.nodes[i];
    if (node.tx_root.is_null()) {
      return td::Status::Error(PSLICE() << "node " << i << " has no transaction cell");
    }
    auto r_tx = ParseQuery::parse_transaction(node.tx_root, node.address.workchain, global_version);
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
