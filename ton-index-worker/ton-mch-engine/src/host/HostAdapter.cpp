// Typed host-authoring adapter (see host/HostAdapter.h).
#include "host/HostAdapter.h"

#include "BuildRuntime.h"
#include "host/HostCommon.h"

namespace mch {

EvalResult decode_consumed(const std::vector<Value> &args, const char *binding,
                           ConsumedBlocks &out) {
  if (args.size() != 1 || args[0].t != VType::List || args[0].items->empty()) {
    return rt_fault(std::string(binding) + ": bad arguments");
  }
  out.blocks.clear();
  for (const Value &v : *args[0].items) {
    const Block *b = as_block(v);
    if (b != nullptr) {
      out.blocks.push_back(b);
    }
  }
  if (out.blocks.empty()) {
    return rt_fault(std::string(binding) + ": empty consumed");
  }
  return rt_ok(Value::null());
}

EvalResult run_swap_host(BuildEnv &env, const std::vector<Value> &args, const char *binding,
                         SwapCore core) {
  ConsumedBlocks consumed;
  EvalResult decoded = decode_consumed(args, binding, consumed);
  if (decoded.faulted) {
    return decoded;
  }
  HostContext ctx{env, binding};
  HostResult<SwapRecord> r = core(ctx, consumed);
  switch (r.kind) {
    case HostResult<SwapRecord>::Kind::Value:
      return rt_ok(r.value.encode());
    case HostResult<SwapRecord>::Kind::Reject:
      // The typed Reject is the spec's `reject when r == null`, log it as one.
      return host_reject("typed core reject");
    case HostResult<SwapRecord>::Kind::Fault:
      return rt_fault(std::string(binding) + ": " + r.message);
  }
  return rt_fault(std::string(binding) + ": unreachable");
}

}  // namespace mch
