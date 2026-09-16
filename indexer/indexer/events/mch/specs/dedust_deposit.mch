# Dedust two-branch liquidity deposit — ported from blocks/liquidity.py
# DedustDepositBlockMatcher (final) + DedustDepositFirstAssetBlockMatcher
# (partial). Registration order inside this file mirrors event_processing:
# final first, partial second.
#
# Key deviation from the hand-written pair, forced by registration order: the
# legacy matchers run BEFORE JettonTransferBlockMatcher, so they match the
# user's deposit leg as a RAW JettonTransfer -> JettonInternalTransfer ->
# JettonNotify chain and build the vault's jetton-excess payouts themselves
# (labeled(..., JettonTransferBlockMatcher, call_build_block=True)). Here
# jetton_transfer has already run, so the deposit leg and the excess payouts
# arrive as prebuilt `btype jetton_transfer` blocks; the raw notify chain is
# kept as a fallback arm for legs the jetton matcher rejected. Consumed
# event-node sets are identical either way (the composite carries the same
# nodes, including per-node excess absorption).
#
# The implementation's opcode values and rejection/destroy shapes are
# authoritative; this file follows the code rather than the idealized example.
#
# Paired partial+final blocks are merged by host-side
# post_process_dedust_liquidity: both builders return the exact
# registered block classes the pass dispatches on
# (DedustDepositLiquidity / DedustDepositLiquidityPartial).
#
# The partial matcher's legacy guard (raise when final-deposit opcodes follow
# the topup, a failed-final leftover) lives in the shared build core and
# surfaces as a builder reject here.
#
# Flat parent chain `<- op Ask -> { maybe op Deploy } <- $deposit_leg`: ask gets
# both children_matchers=[deploy] and parent=Or(deposit legs), matching the
# source structure. The flat and grouped forms wire identically.
#
# Builders are consumed-set/tree-driven (Match._mch_consumed + previous_block
# navigation), same approach as dedust_swap/stonfi_v2; captures are kept for
# pattern fidelity only.

opcode JettonTransfer         = 0x0f8a7ea5
opcode JettonInternalTransfer = 0x178d4519
opcode JettonNotify           = 0x7362d09c

opcode DedustDepositTONToVault               = 0xd55e4686
opcode DedustAskLiquidityFactory             = 0xf04ec526
opcode DedustDeployDepositContract           = 0x9b3aa3fa
opcode DedustTopUpLiquidityDepositContract   = 0x54240fe5
opcode DedustDepositLiquidityToPool          = 0xb56b9598
opcode DedustDestroyLiquidityDepositContract = 0xaae79256
opcode DedustReturnExcessFromVault           = 0x6b0b787f
opcode DedustPayout                          = 0x474f86cf
opcode DedustRejection                       = 0xe1a36cd4

# The deposit leg feeding the vault: TON straight to the vault, a prebuilt
# jetton transfer, or the raw notify chain (fallback, see header).
rule deposit_leg = (
    op DedustDepositTONToVault @deposit_ton
  | btype jetton_transfer @deposit_transfer
  | op JettonNotify @deposit_notify <- op JettonInternalTransfer <- op JettonTransfer
)

# One excess-liquidity refund from a vault: optional jetton payout (prebuilt
# transfer) and/or optional TON payout. Legacy: ContractMatcher(optional,
# ReturnExcessFromVault, children=[jetton_excess(optional), ton_excess(optional)]).
rule vault_excess =
    maybe op DedustReturnExcessFromVault
      -> { maybe btype jetton_transfer
         ; maybe op DedustPayout }

# HYBRID declarative build. The reject/failed/`dex` wiring and the block
# assembly are declarative; the imperative residue (deposit-leg resolution via
# previous_block navigation, per-vault BOC parsing, jetton-wallet interface
# lookups, and the excess-payout scan over the consumed set) stays in two host
# fns that return the field record. Each fn returns null to reject,
# reproducing the builder's `return None` (including the partial matcher's
# final-deposit-opcodes-after-topup guard). Product is a generic
# Block(btype dedust_deposit_liquidity[_partial]); the distinct btypes let
# post_process_dedust_liquidity dual-dispatch (isinstance legacy classes OR
# btype) so both paths pair identically.

matcher dedust_deposit_final {
    produces dedust_deposit_liquidity
    entry    @pool

    op DedustDepositLiquidityToPool @pool
      -> { ( op JettonInternalTransfer @lp_transfer -> maybe op JettonNotify
           | op DedustRejection @rejection
               -> { maybe btype ton_transfer ; $vault_excess ; $vault_excess } )
         ; maybe op DedustDestroyLiquidityDepositContract
             -> { maybe btype ton_transfer ; $vault_excess ; $vault_excess } }
      <- op DedustTopUpLiquidityDepositContract
      <- op DedustAskLiquidityFactory
         -> { maybe op DedustDeployDepositContract }
         <- $deposit_leg

    let r = dedust_deposit_final_data(consumed)
    reject when r == null
    failed when not r.success
    out {
        dex:                  "dedust"
        sender:               r.sender
        pool_address:         r.pool_address
        deposit_contract:     r.deposit_contract
        lp_tokens_minted:     r.lp_tokens_minted
        asset_1:              r.asset_1
        asset_2:              r.asset_2
        amount_1:             r.amount_1
        amount_2:             r.amount_2
        target_asset_1:       r.target_asset_1
        target_amount_1:      r.target_amount_1
        target_asset_2:       r.target_asset_2
        target_amount_2:      r.target_amount_2
        user_jetton_wallet_1: r.user_jetton_wallet_1
        user_jetton_wallet_2: r.user_jetton_wallet_2
        vault_excesses:       r.vault_excesses
        success:              r.success
    }
}

matcher dedust_deposit_partial {
    produces dedust_deposit_liquidity_partial
    entry    @topup

    op DedustTopUpLiquidityDepositContract @topup
      <- op DedustAskLiquidityFactory
         -> { op DedustDeployDepositContract }
         <- $deposit_leg

    let r = dedust_deposit_partial_data(consumed)
    reject when r == null
    out {
        dex:                  "dedust"
        sender:               r.sender
        deposit_contract:     r.deposit_contract
        asset_1:              r.asset_1
        amount_1:             r.amount_1
        asset_2:              r.asset_2
        amount_2:             r.amount_2
        user_jetton_wallet_1: r.user_jetton_wallet_1
        user_jetton_wallet_2: r.user_jetton_wallet_2
        target_asset_1:       r.target_asset_1
        target_amount_1:      r.target_amount_1
        target_asset_2:       r.target_asset_2
        target_amount_2:      r.target_amount_2
    }
}
