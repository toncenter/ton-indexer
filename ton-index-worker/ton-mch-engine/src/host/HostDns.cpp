// DNS auction host fn (builders/dns.py). The purchase parent is inspected but
// deliberately remains outside the matcher pattern and consumed set.
#include "host/HostImpls.h"

#include "host/HostCommon.h"

#include "BlockTree.h"
#include "TraceLoader.h"

#include <optional>
#include <vector>

namespace mch {

EvalResult dns_purchase_buyer(BuildEnv &, const std::vector<Value> &args) {
  if (args.size() != 1) {
    return rt_fault("dns_purchase_buyer: bad arguments");
  }
  const Block *anchor = as_block(args[0]);
  if (anchor == nullptr) {
    return rt_ok(Value::null());
  }
  const Block *parent = anchor->previous_block;
  if (parent == nullptr || parent->btype != "call_contract") {
    return rt_ok(Value::null());
  }
  const Message *m = block_msg(parent);
  return rt_ok(account_from_opt(m != nullptr ? m->source : std::nullopt));
}

}  // namespace mch
