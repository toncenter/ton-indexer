#include "host/HostAdapter.h"

#include "BuildRuntime.h"
#include "host/HostCommon.h"

namespace mch {

EvalResult decode_consumed(const std::vector<Value> &args, const char *binding,
                           ConsumedBlocks &out) {
  if (args[0].t != VType::List || args[0].items->empty()) {
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

std::optional<ConsumedBlocks> decode_consumed_or_none(const std::vector<Value> &args) {
  if (args[0].t != VType::List || args[0].items->empty()) {
    return std::nullopt;
  }
  ConsumedBlocks out;
  for (const Value &v : *args[0].items) {
    if (const Block *b = as_block(v); b != nullptr) {
      out.blocks.push_back(b);
    }
  }
  if (out.blocks.empty()) {
    return std::nullopt;
  }
  return out;
}

}  // namespace mch
