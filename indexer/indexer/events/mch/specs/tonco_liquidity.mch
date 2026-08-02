# Tonco deposit liquidity — ported from blocks/liquidity.py
# ToncoDepositLiquidityMatcher.
#
# Like the legacy matcher (which runs after the jetton/nft matchers in
# event_processing), the pattern relies on prebuilt `btype jetton_transfer`
# and `btype nft_mint` blocks (prerequisites: NftMintBlockMatcher,
# JettonTransferBlockMatcher, PTonTransferMatcher).
#
# Shape notes:
# - the anchor's optional MinAndRefund child chain is wrapped in a one-item
#   children set so the parent edge stays attached to the anchor; a bare
#   `-> ... <-` chain is C009 — dedust_swap precedent;
# - each refund transfer carries `-> { maybe op PTonTransfer }`: legacy
#   output_ton_or_jetton has an optional PTonTransfer child_matcher;
# - `maybe op ExcessMessage` mirrors the legacy explicit children item — the
#   engine would absorb it as an excess sibling anyway, kept for fidelity;
# - the input alternative keeps legacy OrMatcher order: pton arm (JettonNotify
#   fed by a raw PTonTransfer call) first, plain jetton transfer second.
#
# HYBRID declarative build. The block assembly is declarative; the imperative
# residue (input-transfer resolution via previous_block, per-side BOC parsing
# of the AddLiquidity / NftPositionInit / JettonNotify forward payload, the two
# jetton-wallet interface lookups + pton-master asset classification, the
# amount1/amount2 ordering by which side was deposited, and the excess-payout
# pton navigation) stays in one host fn returning the field record — the
# ToncoDepositLiquidityData the shared core already builds. The `out` block
# flattens that record into the produced block's data map. The fn returns null
# to reject.
#
# Product is a generic Block(btype tonco_deposit_liquidity) with a dict data
# map (the legacy block carried a ToncoDepositLiquidityData dataclass); the
# serializer's _fill reads both via _dget.

opcode ToncoAccountV3AddLiquidity      = 0x3ebe5431
opcode ToncoPoolV3FundAccount          = 0x4468de77
opcode ToncoPoolV3MinAndRefund         = 0x81702ef8
opcode ToncoRouterV3PayTo              = 0xa1daa96d
opcode ToncoPoolV3StartBurn            = 0x530b5f2c
opcode ToncoPositionNftV3PositionBurn  = 0x46ca335a
opcode ToncoPoolV3Burn                 = 0xd73ac09d
opcode ToncoRouterV3CreatePool         = 0x2e3034ef
opcode ToncoPoolV3Init                 = 0x441c39ed
opcode JettonNotify                    = 0x7362d09c
opcode PTonTransfer                    = 0x01f3835d
opcode ExcessMessage                   = 0xd53276db

matcher tonco_deposit_liquidity {
    produces tonco_deposit_liquidity
    entry    @add_liquidity

    op ToncoAccountV3AddLiquidity @add_liquidity
      -> { maybe op ToncoPoolV3MinAndRefund
             -> { maybe op ToncoRouterV3PayTo @refund_router_call
                    -> { maybe btype jetton_transfer @excess_transfer_0
                           -> { maybe op PTonTransfer }
                       ; maybe btype jetton_transfer @excess_transfer_1
                           -> { maybe op PTonTransfer } }
                ; btype nft_mint @nft_mint
                ; maybe op ExcessMessage } }
      <- op ToncoPoolV3FundAccount
      <- ( op JettonNotify @ton_input <- op PTonTransfer @ton_input_addition
         | btype jetton_transfer @jetton_input )

    let r = tonco_deposit_liquidity_data(consumed)
    reject when r == null
    out {
        sender:            r.sender
        pool:              r.pool
        account_contract:  r.account_contract
        position_amount_1: r.position_amount_1
        position_amount_2: r.position_amount_2
        lp_tokens_minted:  r.lp_tokens_minted
        tick_lower:        r.tick_lower
        tick_upper:        r.tick_upper
        nft_index:         r.nft_index
        nft_address:       r.nft_address
        amount_1:          r.amount_1
        asset_1:           r.asset_1
        sender_wallet_1:   r.sender_wallet_1
        amount_2:          r.amount_2
        asset_2:           r.asset_2
        sender_wallet_2:   r.sender_wallet_2
        excesses:          r.excesses
    }
}


# -----------------------------------------------------------------------------
# Tonco deploy pool — blocks/liquidity.py:1871 ToncoDeployPoolBlockMatcher.
#
# A deployer asks the router to create a concentrated-liquidity pool; the router
# deploys it and initializes it, and the pool answers with excesses:
#
#   0x2e3034ef (deployer -> router)
#     -> 0x441c39ed (router -> pool)
#          -> maybe 0xd53276db
#
# FULLY DECLARATIVE: every field is an envelope address or a body scalar, so
# there is no host fn and no build core. `success` is the literal `true` the
# legacy build hardcodes — reaching the build at all means the init was found.
#
# TWO ABI LEAF TYPES, and this matcher is what needed them: `u16` for the
# three fee basis-point fields and `uint160` for the Q64.96 initial price. Both
# are native ABI scalar widths; both are plain
# `load_uint` on the Python side, so — like int24, unlike `bool` — they need no
# casting twin.
#
# ToncoPoolV3Init DOES need one (builders/tonco.py ToncoPoolV3InitTwin): its only
# read field, `pool_active`, is the schema's `bool` leaf, and pytoniq's
# load_bit() returns an INT. The legacy class is therefore not usable as the
# twin here, unlike the three withdraw-chain messages above. The twin stops
# right after `pool_active`, mirroring the ABI entry, which also drops the
# legacy class's unread tail (two content refs and a Maybe ^[minters] cell that
# has no schema form).
#
# ORDERING. 0x2e3034ef and 0x441c39ed are claimed by no other spec. Within this
# file the matcher sits between deposit and withdraw, exactly where
# event_processing.matchers puts it (deposit 64, deploy 65, withdraw 66). It
# binds no prebuilt block, so unlike its two neighbours it does not even depend
# on the priority-90 jetton specs.
# -----------------------------------------------------------------------------
matcher tonco_deploy_pool {
    produces tonco_deploy_pool
    entry    @create

    op ToncoRouterV3CreatePool @create
      -> op ToncoPoolV3Init @init
        -> maybe op ExcessMessage

    parse create as ToncoRouterV3CreatePool
    parse init   as ToncoPoolV3Init
    out {
        deployer:              account(create.msg.source)
        router:                account(create.msg.destination)
        pool:                  account(init.msg.destination)
        jetton0_router_wallet: account(create.body.jetton_wallet0)
        jetton1_router_wallet: account(create.body.jetton_wallet1)
        jetton0_minter:        account(create.body.minters.ref.jetton0_minter)
        jetton1_minter:        account(create.body.minters.ref.jetton1_minter)
        tick_spacing:          create.body.tick_spacing
        initial_price_x96:     create.body.initial_price_x96
        protocol_fee:          create.body.protocol_fee
        lp_fee_base:           create.body.lp_fee_base
        lp_fee_current:        create.body.lp_fee_current
        pool_active:           init.body.pool_active
        success:               true
    }
}


# Tonco withdraw liquidity — ported from blocks/liquidity.py
# ToncoWithdrawLiquidityMatcher, the third tonco liquidity matcher.
#
# Chain, straight off the legacy nested ContractMatchers:
#   StartBurn 0x530b5f2c (user -> Pool)                       <- anchor
#     PositionBurn 0x46ca335a (Pool -> position NFT)          # names the owner
#       PoolV3Burn 0xd73ac09d (NFT -> Pool)                   # authoritative index
#         RouterV3PayTo 0xa1daa96d (Pool -> Router)           # declares both slots
#           payout leg 1 (optional jetton_transfer -> optional PTonTransfer)
#           payout leg 2 (same)
#
# Hybrid declarative build, split along what the DSL can express.
#
# The declarative scalar half supplies sender, pool, burned-NFT index/address,
# liquidity_burnt, and tick range from the three parsed bodies. These values use
# the ABI declaration's uint128 and int24 leaves. Tick indices are
# signed and go negative, while share amounts overflow 64 bits. All three
# messages reuse their source class as the Python twin. The ABI entries cover the whole
# body (dropping what is unread), so accept/reject matches the twin exactly, and
# every leaf is a plain int on both sides — no casting twin (contrast the `bool`
# rule the dedust_v2 withdraw port had to follow).
#
# The payout half uses `tonco_withdraw_payouts` around the shared
# resolve_tonco_withdraw_payouts core (blocks/liquidity.py). Three constraints
# keep it host-side:
#   - ToncoRouterV3PayTo cannot be a schema message at all: its coinsinfo is a
#     Maybe ^Cell (there is no `maybe_ref { }` composite) and its two tail refs
#     are conditional on exit_code, which the schema grammar has no form for;
#   - a leg with NO transfer block falls back to the router's declared slot and
#     needs an async jetton-wallet interface lookup plus the pTON-master
#     classification to decide whether that slot is TON or a jetton. `lookup
#     jetton_wallet(...)` plus three address literals could express the lookup in
#     isolation (specs/pton_transfer.mch does exactly that), but the key it
#     would need comes out of the unparseable PayTo body;
#   - the final leg->slot reordering compares a resolved wallet against the
#     router's asset0 wallet, so it depends on both of the above.
# The legacy `additional_blocks` bookkeeping (the pTON call under a payout leg)
# is NOT needed here: `maybe op PTonTransfer` already consumes it, so the
# synthesized merge covers it — same as the deposit matcher above.
#
# DEVIATION (one, documented not worked around): the legacy build wraps the
# PoolV3Burn parse in try/except and keeps the anchor's burned_index when it
# throws, whereas a declarative `parse` REJECTS the whole match on a parse
# failure. The ABI entry parses the full contract-fixed body, so this can only
# diverge on a malformed 0xd73ac09d body, of which the corpus has none.
#
# ANCHOR / ORDERING. 0x530b5f2c, 0x46ca335a and 0xd73ac09d are claimed by no
# other spec, so this matcher steals nothing and nothing steals from it.
# 0xa1daa96d IS shared — the deposit matcher above binds it as its refund router
# call, and specs/tonco_swap.mch binds it under a 0xa7fb58f8 swap — but in all
# three it is a non-anchor child of a different parent, and one message has one
# parent, so the three uses are disjoint. Within this file the withdraw matcher
# is listed after the deposit one, mirroring event_processing.matchers (deposit
# 64, deploy-pool 65, withdraw 66). What IS load-bearing is the default priority
# 100: the payout legs are `btype jetton_transfer`, so the priority-90 jetton
# specs must have built them first.

matcher tonco_withdraw_liquidity {
    produces tonco_withdraw_liquidity
    entry    @start_burn

    op ToncoPoolV3StartBurn @start_burn
      -> op ToncoPositionNftV3PositionBurn @position_burn
        -> op ToncoPoolV3Burn @pool_burn
          -> op ToncoRouterV3PayTo
            -> { maybe btype jetton_transfer @payout_1
                   -> { maybe op PTonTransfer }
               ; maybe btype jetton_transfer @payout_2
                   -> { maybe op PTonTransfer } }

    parse start_burn    as ToncoPoolV3StartBurn
    parse position_burn as ToncoPositionNftV3PositionBurn
    parse pool_burn     as ToncoPoolV3Burn

    let r = tonco_withdraw_payouts(consumed)
    reject when r == null
    failed when r.failed
    out {
        sender:              account(position_burn.body.nft_owner)
        pool:                account(start_burn.msg.destination)
        burned_nft_index:    pool_burn.body.burned_index if pool_burn.body.burned_index != 0 else start_burn.body.burned_index
        burned_nft_address:  account(position_burn.msg.destination)
        liquidity_burnt:     amount(start_burn.body.liquidity_to_burn)
        tick_lower:          start_burn.body.tick_lower
        tick_upper:          start_burn.body.tick_upper
        amount1_out:         r.amount1_out
        asset1_out:          r.asset1_out
        dex_wallet_1:        r.dex_wallet_1
        dex_jetton_wallet_1: r.dex_jetton_wallet_1
        wallet1:             r.wallet1
        amount2_out:         r.amount2_out
        asset2_out:          r.asset2_out
        dex_wallet_2:        r.dex_wallet_2
        dex_jetton_wallet_2: r.dex_jetton_wallet_2
        wallet2:             r.wallet2
    }
}
