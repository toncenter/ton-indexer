# NFT mint — port of blocks/nft.py NftMintBlockMatcher (:437).
#
# The legacy matcher is a single-block matcher with no edges at all: `test_self`
# is `len(block.contract_deployments) == 1` and `build_block` merges only the
# anchor. So the whole port is one anchor node plus a five-field `out` over one
# interface lookup — no children, no parents, no parse, no build core.
#
# The deploy anchor
# -----------------
# `contract_deployments` is a Block property, not block data: it is the set of
# accounts whose transaction went `orig_status != active && end_status == active`
# over the block's event nodes (blocks/core.py `_find_contract_deployments`).
# Neither the leaf `data` map nor the message envelope carries it as one field,
# and an inline `where (.field == literal)` only tests block data against a
# literal, so the test is the single host `predicate`
# `single_contract_deploy`, the predicate anchor used for the
# motivating case (`len(block.contract_deployments) == 1`, NftMint-style),
# measured at 1.08x an opcode anchor.
#
# NOTE the deliberate difference from the `contract_deploy` SIDE-EFFECT block
# (basic_blocks.py, BlockTree.cpp): that one also excludes `orig_status ==
# frozen`, this one does not. A frozen -> active transaction therefore carries a
# deployment here and no `contract_deploy` child block. The predicate reproduces
# `_find_contract_deployments`, not the side-effect block's condition.
#
# Registration order
# ------------------
# `priority 80` — the legacy matcher is FIRST in `event_processing.matchers`,
# ahead of the priority-90 declarative jetton family, so it must run before both
# tiers. That order is what lets it claim a deploying leaf before the jetton /
# DEX matchers can absorb it, and it is what makes `btype nft_mint` available to
# the specs that bind it downstream (specs/tonco_liquidity.mch).
#
# opcode: signed envelope -> unsigned
# ----------------------------------
# The legacy build reads `event_nodes[0].get_opcode()`, which is
# `message.opcode & 0xFFFFFFFF` (tree_utils.py:45-49). The block `data` copy of
# it exists only on the call_contract/contract_deploy leaves — a ton_transfer
# leaf's data has no `opcode` key at all (basic_blocks.py, BlockTree.cpp
# `leaf_data`) — and the anchor here is deliberately btype-blind, so the value
# comes off the envelope instead, where BOTH engines carry the opcode SIGNED as
# stored. The ternary is that mask: null stays null (`null < 0` is false),
# a negative value gets 2^32 added, everything else passes through.
#
# Capture -> legacy role map:
#   @mint   the deploying block (`test_self` target, `block` in build_block)

predicate single_contract_deploy

matcher nft_mint {
    produces  nft_mint
    entry     @mint
    priority  80

    pred single_contract_deploy @mint

    # The deployed account: with exactly one deployment on a leaf block, the
    # legacy `next(iter(block.contract_deployments)).as_str()` IS this
    # transaction's account. Raw (un-normalized) lookup key, like builders/dns.py
    # and builders/teleitem_auction.py — `transaction.account` is already stored
    # in the raw-uppercase form `AccountId(...).as_str()` produces.
    let address = mint.msg.transaction.account
    let nft     = lookup nft_item(address)
    reject when nft == null
    let raw_op  = mint.msg.opcode
    out {
        source:     account(mint.msg.source)
        address:    account(address)
        index:      nft.index
        opcode:     (raw_op + 4294967296) if raw_op < 0 else raw_op
        collection: account(nft.collection_address)
    }
}
