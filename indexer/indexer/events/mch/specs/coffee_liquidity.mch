# Coffee liquidity/vault matchers — ported from blocks/liquidity.py.
# CoffeeDepositLiquidityMatcher and CoffeeCreateVaultMatcher, declared in the
# legacy matchers-list order (deposit < create_vault). The third coffee
# liquidity matcher, CoffeeWithdrawLiquidityMatcher, lives in
# specs/coffee_withdraw.mch — split off for the same reason dedust and stonfi_v2
# keep their deposit and withdraw specs apart, plus a documented legacy
# divergence recorded in that file's header.
#
# Both builds here are FULLY DECLARATIVE: zero host fns, zero lookups. Coffee is
# the one family in the backlog with no parser gaps: its 2-bit asset stays an
# honest ABI union and the real ref groups remain visible to consumers.
#
# Anchor claims: 0xc0ffee27 (deposit) and 0xc0ffee06 (create vault) are claimed
# by no other spec, so this file's position in the built-in source list carries no
# tie-break.

opcode CoffeeDepositLiquidityNative           = 0xc0ffee04
opcode CoffeeCreateVault                      = 0xc0ffee06
opcode CoffeePayoutInternal                   = 0xc0ffee21
opcode CoffeeDeploy                           = 0xc0ffee22
opcode CoffeeCreateLiquidityDepositoryRequest = 0xc0ffee27
opcode CoffeeDepositLiquidityInternal         = 0xc0ffee29
opcode CoffeePayout                           = 0xc0ffee32
opcode CoffeeDepositLiquiditySuccessfulEvent  = 0xc0ffee33
opcode CoffeeNotification                     = 0xc0ffee36
opcode JettonInternalTransfer                 = 0x178d4519
opcode ExcessMessage                          = 0xd53276db
opcode ProvideWalletAddress                   = 0x2c76b973
opcode TakeWalletAddress                      = 0xd1735400

# ---------------------------------------------------------------------------
# coffee_deposit_liquidity — legacy CoffeeDepositLiquidityMatcher.
#
# A depository-creation request (the anchor) fed by the user's leg (TON straight
# to the vault, or a prebuilt `jetton_transfer`), deploying the depository. Once
# BOTH legs have landed the depository fires 0xc0ffee29, which mints the LP
# jettons (JettonInternalTransfer), logs 0xc0ffee33 and optionally pays a vault
# excess back. So a two-asset deposit produces TWO of these blocks: the first
# leg's has no 0xc0ffee29 subtree at all (pool/lp null), the second leg's has it.
#
# Legacy `child_matcher=deploy` is written here as a one-item children block
# (`-> { op CoffeeDeploy ... }`): a children block does not advance the chain
# cursor, so the `<-` in-transfer edge still belongs to the ANCHOR, as in the
# dedust_deposit matcher, instead of tripping
# C009_MIXED_CHILD_PARENT_CHAIN_UNSUPPORTED on the deploy node. With a single
# item the two lowerings differ only when several 0xc0ffee22 children exist
# (legacy would consume them all; its build reads only the first via
# get_labeled) — never observed.
#
# Slot assignment: the legacy build compares `str(in_asset)` against
# `str(pool_params.first/second)` and rejects when the incoming asset is in
# neither. `==` on two Asset values is the same test on both hosts (is_ton +
# has_jetton + normalized master string), so the comparison is declarative.
# `target_asset_1` is always the DEPOSITED side and `target_asset_2` the other —
# note the legacy swap in the `second` branch.
#
# Deviations from legacy, all documented rather than fixed:
#  - the `lp_transfer` fallback for lp_tokens_minted/pool_address is DEAD in the
#    legacy matcher (0xc0ffee33 and the internal transfer are both REQUIRED items
#    of the same children set, so the success event is present whenever the
#    transfer is) and is not reproduced;
#  - `vault_excesses` elements are 2-element LISTS here and tuples in the legacy
#    build; both index identically in the serializer (`excess[0]`/`excess[1]`)
#    and render identically, and the language has no tuple literal;
#  - the legacy `Asset(False, x.jetton_address)` coercion of a non-`Asset`
#    excess asset is dead under the declarative jetton family (data['asset'] is
#    always an `Asset`) and is not reproduced.
# ---------------------------------------------------------------------------
matcher coffee_deposit_liquidity {
    produces coffee_deposit_liquidity
    entry    @request

    op CoffeeCreateLiquidityDepositoryRequest @request
      -> { op CoffeeDeploy @deploy
             -> maybe op CoffeeDepositLiquidityInternal
                  -> { op JettonInternalTransfer -> maybe op ExcessMessage
                     ; op CoffeeDepositLiquiditySuccessfulEvent @success
                     ; maybe op CoffeePayoutInternal @payout
                         -> ( btype jetton_transfer @out_jt
                            ^ op CoffeePayout @out_call
                            ^ op CoffeeNotification @out_call ) } }
      <- ( op CoffeeDepositLiquidityNative @in_native ^ btype jetton_transfer @in_jt )

    parse request as CoffeeCreateLiquidityDepositoryRequest
    parse success as CoffeeDepositLiquiditySuccessfulEvent

    let in_asset   = ton_asset() if in_jt == null else in_jt.data.asset
    let in_wallet  = null if in_jt == null else in_jt.data.sender_wallet
    let pool_first = asset_of(request.body.pool_params.ref.pool_first)
    let pool_second = asset_of(request.body.pool_params.ref.pool_second)
    let is_first   = in_asset == pool_first
    reject when not (is_first or in_asset == pool_second)

    let in_amount = amount(request.body.amount)
    # The vault's excess refund — whichever arm of the payout terminal bound.
    let excess_jetton = [[out_jt.data.asset, out_jt.data.amount]] if out_jt != null else null
    let excess_ton    = [[ton_asset(), amount(out_call.msg.value ?? 0)]] if out_call != null else null
    out {
        dex:                  "coffee"
        # Unconditional `AccountId(...)` in the legacy build, so a body-level
        # addr_none stays present-but-empty rather than becoming null.
        sender:               account(request.body.sender) ?? addr_none()
        pool_address:         account(success.msg.source) if success != null else null
        deposit_contract:     account(deploy.msg.destination)
        vault_excesses:       excess_jetton ?? excess_ton ?? []
        asset_1:              in_asset if is_first else null
        amount_1:             in_amount if is_first else null
        user_jetton_wallet_1: in_wallet if is_first else null
        asset_2:              null if is_first else in_asset
        amount_2:             null if is_first else in_amount
        user_jetton_wallet_2: null if is_first else in_wallet
        target_asset_1:       pool_first if is_first else pool_second
        target_amount_1:      null
        target_asset_2:       pool_second if is_first else pool_first
        target_amount_2:      null
        lp_tokens_minted:     amount(success.body.lp_amount) if success != null else null
    }
}

# ---------------------------------------------------------------------------
# coffee_create_vault — legacy CoffeeCreateVaultMatcher.
#
# A vault creation request deploys the vault, which then discovers its jetton
# wallet via the TEP-89 pair (provide_wallet_address / take_wallet_address —
# the raw 0x2c76b973 / 0xd1735400 literals in the legacy matcher). Both
# discovery hops are optional (TON/extra-currency vaults skip them).
#
# The declarative build reads sender/amount from the anchor envelope and asset
# from its parsed body, vault from the captured CoffeeDeploy call's envelope.
# The produced block is the canonical generic block (btype + data map).
# ---------------------------------------------------------------------------
matcher coffee_create_vault {
    produces CoffeeCreateVaultBlock
    entry    @create

    op CoffeeCreateVault @create
      -> op CoffeeDeploy @deploy
      -> maybe op ProvideWalletAddress
      -> maybe op TakeWalletAddress

    parse create as CoffeeCreateVault
    out {
        sender: account(create.msg.source)
        vault:  account(deploy.msg.destination)
        asset:  asset_of(create.body.asset)
        amount: amount(create.msg.value)
    }
}
