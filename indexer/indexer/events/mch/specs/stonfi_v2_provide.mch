# Stonfi v2 provide-liquidity matcher corresponding to
# StonfiV2ProvideLiquidityMatcher.
#
# The source matcher pairs the two deposit branches in the matcher. A
# GenericMatcher over a previously produced dex_deposit_liquidity block hangs
# off the in-transfer's parent, and the
# second fire absorbs the first block plus performs the EmptyBlock proxy
# insertion inside build_block. Pairing and merging are host-side:
# here each deposit leg is matched independently and produces a class-tagged
# block (StonfiV2DepositLiquidity / StonfiV2DepositLiquidityPartial, both
# btype dex_deposit_liquidity like legacy); the host pass
# post_process_stonfi_v2_liquidity (blocks/liquidity.py) replays the legacy
# pairing, data merge, EmptyBlock proxy surgery, and condition-fail absorption
# over the produced blocks.
#
# `shape` is not used because pairing knowledge only exists in the host pass. A
# per-matcher shaper fires before the partner is known. Legacy placement is followed:
# surgery lives in the pass.
#
# Split rationale: both legs share one structural shape except the pool
# callback subtree (cb_add_liquidity -> LP mint | refund), which only the
# completing leg carries. `final` (callback mandatory) registers first and
# consumes completed legs; `partial` (no callback) picks up the rest,
# including the legacy edge where a cb call exists but has no LP/refund
# child (legacy's optional cb matcher fails and matches the leg without it).
# The partial builder rejects when a full callback subtree is present, so a
# final-builder rejection reproduces the legacy whole-matcher rejection
# instead of downgrading to a partial.
#
# In-transfer arms mirror the legacy OrMatcher order: raw pTON leg
# (JettonNotify <- 0x01f3835d), prebuilt jetton_transfer, and the raw
# notify-chain fallback behind a prebuilt transfer. Runs after
# JettonTransferBlockMatcher / PTonTransferMatcher, like legacy
# (event_processing order).
#
# FULLY DECLARATIVE build (no host builder). Each leg's single-block data is
# the same build_stonfi_v2_provide_core computed inline: the incoming jetton
# leg's `.data.asset`/`.data.sender_wallet` (a raw pTON leg carries neither ->
# ton_asset / null), the provide message's amount1/amount2/from_user (parsed),
# the LP-mint amount off the callback's JettonInternalTransfer (final only),
# and the pool off the provide envelope. The paired amount_2/asset_2/
# sender_wallet_2 stay null here — post_process_stonfi_v2_liquidity fills them
# when it merges the partner leg.
#
# Product is a generic Block(btype dex_deposit_liquidity) instead of the
# class-tagged StonfiV2DepositLiquidity[Partial]; the pairing pass dispatches
# dual (isinstance legacy classes OR btype+dex marker, see
# post_process_stonfi_v2_liquidity) so both the legacy and mch paths pair
# identically. The final/partial split is unchanged: `final` (callback
# mandatory) registers FIRST and consumes completed legs; `partial` picks up
# the rest.

opcode StonfiV2ProvideLiquidity = 0x37c096df
opcode StonfiV2AddLiquidity     = 0x50c6a654
opcode StonfiV2CbAddLiquidity   = 0x06ecd527
opcode JettonNotify             = 0x7362d09c
opcode JettonInternalTransfer   = 0x178d4519
opcode PTonTransfer             = 0x01f3835d

# The incoming deposit transfer feeding the router (legacy OrMatcher order):
# pTON leg (raw notify fronted by the 0x01f3835d call), a prebuilt jetton
# transfer, or a raw notify chain behind a prebuilt transfer.
rule in_leg = (
    op JettonNotify @notify_pton <- op PTonTransfer @in_pton
  | btype jetton_transfer @in_jt
  | op JettonNotify @notify_raw <- op JettonInternalTransfer @internal_raw <- btype jetton_transfer @in_jt_raw
)

matcher stonfi_v2_provide_final {
    produces dex_deposit_liquidity
    entry    @provide

    op StonfiV2ProvideLiquidity @provide
      -> { op StonfiV2AddLiquidity @add
             -> op StonfiV2CbAddLiquidity @cb
                  -> ( op JettonInternalTransfer @lp_transfer
                     | op StonfiV2AddLiquidity @refund ) }
      <- $in_leg

    parse provide     as StonfiV2ProvideLiquidity
    parse lp_transfer as JettonInternalTransfer
    let in_jt_block = in_jt ?? in_jt_raw
    failed when refund != null
    out {
        dex:             "stonfi_v2"
        amount_1:        amount(provide.body.amount1) if provide.body.amount1 > 0 else amount(provide.body.amount2)
        asset_1:         ton_asset() if in_jt_block == null else in_jt_block.data.asset
        sender:          account(provide.body.from_user)
        sender_wallet_1: null if in_jt_block == null else in_jt_block.data.sender_wallet
        amount_2:        null
        asset_2:         null
        sender_wallet_2: null
        pool:            account(provide.msg.destination)
        lp_tokens_minted: null if lp_transfer == null else amount(lp_transfer.body.amount)
    }
}

matcher stonfi_v2_provide_partial {
    produces dex_deposit_liquidity
    entry    @provide

    op StonfiV2ProvideLiquidity @provide
      -> { op StonfiV2AddLiquidity @add }
      <- $in_leg

    parse provide as StonfiV2ProvideLiquidity
    let in_jt_block = in_jt ?? in_jt_raw
    out {
        dex:             "stonfi_v2"
        amount_1:        amount(provide.body.amount1) if provide.body.amount1 > 0 else amount(provide.body.amount2)
        asset_1:         ton_asset() if in_jt_block == null else in_jt_block.data.asset
        sender:          account(provide.body.from_user)
        sender_wallet_1: null if in_jt_block == null else in_jt_block.data.sender_wallet
        amount_2:        null
        asset_2:         null
        sender_wallet_2: null
        pool:            account(provide.msg.destination)
        lp_tokens_minted: null
    }
}
