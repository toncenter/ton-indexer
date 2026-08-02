// Tonstakers host fns (builders/tonstakers.py). See host/HostImpls.h for the
// internal registry surface and HostRegistry.h for the public one.
#include "host/HostImpls.h"

#include "host/HostCommon.h"

#include "BlockTree.h"
#include "TraceLoader.h"

#include <cstdint>
#include <optional>
#include <vector>

namespace mch {

// builders/tonstakers.py _tonstakers_minted_nft(delayed): the minted request
// NFT's address off delayed.next_blocks. The btype probe is the LIVE arm since
// specs/nft_mint.mch landed: nft_mint carries priority 80, so it consumes the
// TONStakersInitNFT call into a generic nft_mint block before this matcher runs
// and the call_contract fallback below finds nothing. (It was a dead
// isinstance(NftMintBlock) check until 2026-07-30; the parity sweep could not
// see the gap because its prereq is the LEGACY NftMintBlockMatcher, whose
// product does satisfy an isinstance.) None/absent -> null, and the delayed arm
// then marks the block failed.
EvalResult tonstakers_minted_nft(BuildEnv &, const std::vector<Value> &args) {
  if (args.size() != 1) {
    return rt_fault("tonstakers_minted_nft: bad arguments");
  }
  const Block *delayed = as_block(args[0]);
  if (delayed == nullptr) {
    return rt_ok(Value::null());
  }
  constexpr std::uint32_t kTonstakersInitNft = 0x132F9A45;
  for (const Block *n : delayed->next_blocks) {
    if (n->btype == "nft_mint") {
      const Message *m = block_msg(n);
      return rt_ok(account_from_opt(m != nullptr ? m->destination : std::nullopt));
    }
  }
  for (const Block *n : delayed->next_blocks) {
    if (is_call_op(n, kTonstakersInitNft)) {
      const Message *m = block_msg(n);
      return rt_ok(account_from_opt(m != nullptr ? m->destination : std::nullopt));
    }
  }
  return rt_ok(Value::null());
}

// blocks/staking.py find_tonstakers_pool_addr(notification), reached through
// builders/tonstakers.py _tonstakers_pool_addr: walk `previous_block` upward
// from the NFT-burn notification. The walk can land on an already-built
// tonstakers_withdraw block, whose `pool` field is returned directly instead of
// continuing. Otherwise, return the source of the "start asset distribution"
// call, continue only through the three burn/distribution opcodes below, and
// stop with null on any other block. The unbounded topology navigation is why
// this remains a host fn.
EvalResult tonstakers_pool_addr(BuildEnv &, const std::vector<Value> &args) {
  if (args.size() != 1) {
    return rt_fault("tonstakers_pool_addr: bad arguments");
  }
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
    if (current->btype == "tonstakers_withdraw") {
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
