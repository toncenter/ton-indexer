# Coffee pool creation matchers corresponding to CoffeeCreatePoolCreatorMatcher
# and CoffeeCreatePoolMatcher. The second matcher anchors on the block produced
# by the first, so their order matters. Both are declarative.
#
# Shape. To create a pool the user funds it TWICE, once per asset, exactly like
# a deposit. Each leg (a jetton transfer, or a native 0xc0ffee02 call to the TON
# vault) makes the vault fire 0xc0ffee23 at the factory, which deploys the
# per-user POOL CREATOR contract (0xc0ffee22) — that is one
# `coffee_create_pool_creator` block per leg. When the SECOND leg lands, the
# creator has both amounts and asks the factory to create the pool
# (0xc0ffee25); the factory deploys the pool (0xc0ffee22 again), which mints LP
# jettons, logs 0xc0ffee33 and optionally notifies. So a two-asset creation
# produces TWO creator blocks and ONE `coffee_create_pool` that consumes the
# second one.
#
# The 0xC0FFEE03 arm is dead. The source parent alternative is
# `OR(jetton_transfer | 0xc0ffee02 | 0xc0ffee03)`, but the extra-currency arm is
# DEAD: `build_block` has no branch for it and falls into `else: return []`
# (liquidity.py:2447). Porting it literally would emit a block Python never
# emits. It is omitted here instead, which reproduces the legacy NET behaviour
# exactly. A 0xc0ffee03 parent makes the required parent edge fail, the match
# never happens, and nothing is consumed; legacy matched the parent and then
# clean-rejected, which also consumes nothing. The observable
# result, minus a branch that can only produce garbage.
#
# `provided_asset` and `sender_jetton_wallet` have
# no representation in the 0xc0ffee23 body, so they come off the leg — the
# prebuilt jetton_transfer's data, or TON/addr_none on the native arm. The
# remaining four (`amount`, `sender`, the two pool assets and the deposit
# recipient) are read from the ANCHOR body, where legacy reads them from the
# leg: for a jetton leg that means the CoffeeCreatePoolJetton sitting in the
# transfer's FORWARD PAYLOAD, and `parse CAPTURE as T` is node-only — it
# reads the captured block's own body and cannot descend into a payload, so a
# literal port would need a host fn.
#
# It does not need one, because the vault copies the leg into the request: the
# 0xc0ffee23 body carries the same amount, the same sender, the same PoolParams
# and the same creation_params the leg handed it. These four values must remain
# byte-equal to those read from the leg.
#
# NB the legacy `CoffeeCreatePoolCreatorRequest` class (messages/coffee.py:713)
# CANNOT be that reader: it parses `creation_params` inline where the wire has
# `creation_params:^PoolCreationParams`, so it throws on a real body. Nothing
# calls it (the legacy matcher only reads its `.opcode`), which is why the bug
# went unnoticed. The mch twin in mch/builders/coffee.py parses the ref.
#
# LEGACY DATA RESHAPED (blocks/liquidity.py, both dataclasses). The legacy
# blocks store `pool_params` as a live `PoolParams` PARSER OBJECT and
# `pool_creation_params` as a `PublicPoolCreationParams` one. A declarative
# build can only emit language values, and the parity sweep compares `data`
# key-for-key including type — so those two fields were replaced by what is
# actually consumed:
#   - `pool_params` -> `pool_first` / `pool_second`, the only two subfields any
#     reader touches (`_fill_coffee_create_pool_creator`'s asset2/asset2_secondary
#     and this file's asset-order swap);
#   - `pool_creation_params` -> DELETED from both blocks. It was written and
#     never read; only `pool_creation_params.public.recipient` was ever used, and
#     that already had its own field (`deposit_recipient`).
# Net effect on the emitted Action rows: none.
#
# ASSET ORDER (create_pool). The pool declares its own asset order in
# PoolParams; the creator block knows only which asset ITS leg provided. Legacy
# compares the two and, when the provided asset is the pool's SECOND, swaps both
# the amounts and the initiators so slot 1 is always the pool's first asset.
# When the provided asset matches NEITHER (cannot happen on a real pool, but the
# legacy build has the branch) it falls back to the pool's own order and swaps
# nothing. All three cases are reproduced.
#
# ORDERING. 0xc0ffee23 is claimed by no other spec, and `btype
# coffee_create_pool_creator` is produced only by the matcher above it in this
# file — source order inside the file is the tie-break that makes the serial
# pair work. The creator's parent alternative binds `btype jetton_transfer`, so
# the file must stay behind the priority-90 jetton specs; the default priority
# 100 gives that.

opcode CoffeeCreatePoolNative                = 0xC0FFEE02
opcode CoffeeDeploy                          = 0xC0FFEE22
opcode CoffeeCreatePoolCreatorRequest        = 0xC0FFEE23
opcode CoffeeCreatePoolRequest               = 0xC0FFEE25
opcode CoffeeDepositLiquiditySuccessfulEvent = 0xC0FFEE33
opcode CoffeeNotification                    = 0xC0FFEE36
opcode CoffeeNotificationCallback            = 0xB216D31F
opcode JettonInternalTransfer                = 0x178D4519
opcode ExcessMessage                         = 0xD53276DB

matcher coffee_create_pool_creator {
    produces coffee_create_pool_creator
    entry    @request

    # `-> { op CoffeeDeploy }` rather than `-> op CoffeeDeploy`: a children block
    # does not advance the chain cursor, so the `<-` leg edge still belongs to
    # the ANCHOR instead of tripping C009 on the deploy node, as in the
    # coffee_liquidity / dedust_deposit matchers. Legacy's single
    # `child_matcher` and a one-item children block differ only when several
    # 0xc0ffee22 children exist, which the factory never emits.
    op CoffeeCreatePoolCreatorRequest @request
      -> { op CoffeeDeploy @deploy }
      <- ( btype jetton_transfer @in_jt ^ op CoffeeCreatePoolNative @in_native )

    parse request as CoffeeCreatePoolCreatorRequest
    let pool_first  = asset_of(request.body.pool_first)
    let pool_second = asset_of(request.body.pool_second)
    out {
        sender:                account(request.body.sender)
        sender_jetton_wallet:  in_jt.data.sender_wallet ?? addr_none()
        amount:                amount(request.body.amount)
        provided_asset:        in_jt.data.asset ?? ton_asset()
        pool_creator_contract: account(deploy.msg.destination)
        deposit_recipient:     account(request.body.creation_params.ref.recipient)
        pool_first:            pool_first
        pool_second:           pool_second
    }
}

matcher coffee_create_pool {
    produces coffee_create_pool
    entry    @creator

    btype coffee_create_pool_creator @creator
      -> op CoffeeCreatePoolRequest @request
        -> op CoffeeDeploy @deploy
          -> { op JettonInternalTransfer -> maybe op ExcessMessage
             ; op CoffeeDepositLiquiditySuccessfulEvent @success
             ; maybe op CoffeeNotification -> maybe op CoffeeNotificationCallback }

    parse request as CoffeeCreatePoolRequest
    parse success as CoffeeDepositLiquiditySuccessfulEvent

    let provided  = creator.data.provided_asset
    let pool_1    = creator.data.pool_first
    let pool_2    = creator.data.pool_second
    let is_first  = provided == pool_1
    let is_second = provided == pool_2
    let initiator = account(request.body.tx_initiator)
    let a_1       = amount(request.body.amount1 ?? 0)
    let a_2       = amount(request.body.amount2 ?? 0)
    out {
        source:                creator.data.sender
        source_jetton_wallet:  creator.data.sender_jetton_wallet
        provided_asset:        provided
        amount:                creator.data.amount
        initiator_1:           initiator if is_second else creator.data.sender
        initiator_2:           creator.data.sender if is_second else initiator
        pool:                  account(deploy.msg.destination)
        asset_1:               provided if (is_first or is_second) else pool_1
        asset_2:               pool_1 if is_second else pool_2
        amount_1:              a_2 if is_second else a_1
        amount_2:              a_1 if is_second else a_2
        lp_tokens_minted:      amount(success.body.lp_amount ?? 0)
        pool_creator_contract: creator.data.pool_creator_contract
    }
}
