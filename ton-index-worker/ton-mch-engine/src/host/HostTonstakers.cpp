#include "host/HostImpls.h"

#include "host/HostCommon.h"

#include "BlockTree.h"
#include "TraceLoader.h"
#include "btypes_gen.h"

#include <cstdint>
#include <optional>
#include <vector>

namespace mch {

// Walk previous_block upward from the NFT-burn notification. An already-built
// tonstakers_withdraw returns its `pool` field. Otherwise return the source of
// the start-asset-distribution call, continue only through the three
// burn/distribution opcodes below, and stop with null on any other block.
EvalResult tonstakers_pool_addr(BuildEnv &, const std::vector<Value> &args) {
  const Block *notification = as_block(args[0]);
  if (notification == nullptr) {
    return rt_ok(Value::null());
  }
  constexpr std::uint32_t kStartAssetDistribution = 0x1140A64F;
  constexpr std::uint32_t kNftBurnNotification = 0xED58B0B2;
  constexpr std::uint32_t kNftBurn = 0xF127FE4E;
  constexpr std::uint32_t kDistributedAsset = 0xDB3B8ABD;
  const Block *current = notification;
  while (true) {
    current = current->previous_block;
    if (current == nullptr) {
      break;
    }
    if (is_call_op(current, kStartAssetDistribution)) {
      const Message *m = block_msg(current);
      return rt_ok(account_from_opt(m != nullptr ? m->source : std::nullopt));
    }
    if (current->btype == mch::btype::kTonstakersWithdraw) {
      return rt_ok(data_field(current, "pool"));
    }
    if (is_call_op(current, kNftBurnNotification) || is_call_op(current, kNftBurn) ||
        is_call_op(current, kDistributedAsset)) {
      continue;
    }
    break;
  }
  return rt_ok(Value::null());
}

}  // namespace mch
