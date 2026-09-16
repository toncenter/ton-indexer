# Tonstakers family — ported from blocks/staking.py. Three matchers, mirroring
# the legacy list order (event_processing.matchers): TONStakersDelayedWithdrawal
# (idx 1), TONStakersDeposit (idx 4), TONStakersWithdraw (idx 5).
#
# PRIORITY 85 on all three, and it is load-bearing on the withdraw matcher:
# JettonBurn 0x595f07bc is ALSO the anchor of specs/jettons_decl.mch's
# `jetton_burn`, which carries `priority 90`. Legacy registers
# TONStakersWithdrawMatcher (idx 5) ahead of JettonBurnBlockMatcher (idx 17), so
# without a priority < 90 the jetton family consumed the burn first and the whole
# withdraw family silently produced `jetton_burn` + loose call_contract rows
# instead — which is the row shape recorded in corpus fixtures GO-NI0Ej… /
# oND3Z5gM…. Per-spec testing cannot see this: it runs one spec's matchers in
# isolation, never the registered set.
# The other two anchors (0x47d54391, 0xdb3b8abd) are claimed by nothing else, so
# 85 is faithfulness rather than necessity there; W010 (three matchers sharing a
# non-default priority) is expected — their relative order falls back to source
# position, which is exactly the legacy order reproduced above, and their anchors
# and reachable node sets are disjoint so the order is unobservable anyway.

opcode JettonBurn                    = 0x595f07bc
opcode JettonBurnNotification        = 0x7bdd97de
opcode JettonInternalTransfer        = 0x178d4519
opcode JettonNotify                  = 0x7362d09c
opcode TONStakersDepositRequest      = 0x47d54391
opcode TONStakersWithdrawRequest     = 0x319b0cdc
opcode TONStakersPoolWithdrawal      = 0x0a77535c
opcode TONStakersMintNFT             = 0x1674b0a0
opcode TONStakersInitNFT             = 0x132f9a45
# Same wire opcode as TONStakersMintNFT; the pool reuses 0x1674b0a0 for the
# jetton mint and the request-NFT mint, disambiguated only by which request
# message parents it (blocks/messages/staking.py:25 vs :29).
opcode TONStakersMintJettons         = 0x1674b0a0
opcode TONStakersNftBurn             = 0xf127fe4e
opcode TONStakersNftBurnNotification = 0xed58b0b2
opcode TONStakersDistributedAsset    = 0xdb3b8abd

# ---------------------------------------------------------------------------
# tonstakers_delayed_withdrawal — legacy TONStakersDelayedWithdrawalMatcher.
#
# The SECOND half of a delayed withdrawal: the user burns the request NFT they
# were minted by `tonstakers_withdraw`'s delayed arm, and the pool pays out. Same
# produced btype as the immediate arm (`tonstakers_withdraw`), but with
# burnt_nft set and asset null.
#
# Anchored on the payout (TONStakersDistributedAsset) rather than the burn: it is
# the only message carrying the paid amount. The burn + notification are reached
# as a two-hop PARENT chain, exactly as legacy lowers them.
#
#   tonstakers_pool_addr(notification) — GAP: the pool address is found by
#     walking `previous_block` upward for an unbounded number of hops until a
#     0x1140a64f "start asset distribution" call (or an already-built
#     tonstakers_withdraw block) is hit. Block-topology navigation, and the walk
#     is unbounded, so it is deliberately outside the language. Shared
#     with legacy: both call blocks/staking.py::find_tonstakers_pool_addr.
# ---------------------------------------------------------------------------
matcher tonstakers_delayed_withdrawal {
    produces tonstakers_withdraw
    priority 85
    entry    @distributed

    op TONStakersDistributedAsset @distributed
      <- op TONStakersNftBurnNotification @notification
      <- op TONStakersNftBurn

    parse notification as TONStakersNftBurnNotification
    out {
        stake_holder: account(notification.body.owner)
        burnt_nft:    account(notification.msg.source)
        pool:         tonstakers_pool_addr(notification)
        tokens_burnt: amount(notification.body.amount)
        amount:       amount(distributed.msg.value)
        asset:        null
    }
}

# ---------------------------------------------------------------------------
# tonstakers_deposit — legacy TONStakersDepositMatcher.
#
#   DepositRequest 0x47d54391 (user -> Pool)
#     MintJettons 0x1674b0a0 (Pool -> tsTON minter)
#       JettonInternalTransfer 0x178d4519 (minter -> user's tsTON wallet)
#         JettonNotify 0x7362d09c                                    # optional
#         excess 0xd53276db                                # auto-absorbed
#
# The source declares the MintJettons node `optional=True`, but that branch is dead.
# so on paper a deposit with no mint leg still matches and builds a `failed`
# block with null wallet/tokens. The branch cannot emit because the next line
# builds `asset=Asset(False, transfer.get_message().source)` with NO null guard
# (blocks/staking.py:160), so `transfer is None` raises AttributeError, which
# BlockMatcher.try_build swallows into `return None`, a clean rejection that
# consumes nothing. The optional node is therefore effectively mandatory, and
# this pattern makes it mandatory outright: identical produced blocks, identical
# consumed set, one fewer impossible state. A literal port of the `optional`
# would have emitted stake_deposit rows Python never emits.
#
# `failed` stays the anchor's own flag (the only live case: the deposit request
# transaction itself aborted while the mint chain still formed), and it gates
# user_jetton_wallet / tokens_minted exactly as legacy does.
# ---------------------------------------------------------------------------
matcher tonstakers_deposit {
    produces tonstakers_deposit
    priority 85
    entry    @deposit

    op TONStakersDepositRequest @deposit
      -> op TONStakersMintJettons
        -> op JettonInternalTransfer @transfer
          -> maybe op JettonNotify

    parse transfer as JettonInternalTransfer
    let ok = not deposit.failed
    failed when deposit.failed
    out {
        source:             account(deposit.msg.source)
        user_jetton_wallet: account(transfer.msg.destination) if ok else null
        pool:               account(deposit.msg.destination)
        # 1 TON deposit fee, subtracted verbatim from the request's value.
        value:              amount(deposit.msg.value - 1000000000)
        tokens_minted:      amount(transfer.body.amount) if ok else null
        asset:              asset(transfer.msg.source)
    }
}

# ---------------------------------------------------------------------------
# tonstakers_withdraw — legacy TONStakersWithdrawMatcher. HYBRID declarative
# build. Anchors on the raw JettonBurn call (see the priority note at the top:
# it must beat the jetton family to it), then a fixed child chain to the pool's
# withdraw request, which forks into an immediate pool withdrawal or a delayed
# withdrawal that mints a request NFT.
#
# The produces switch follows an XOR of two distinct concrete heads. It
# dispatches the produced btype according to the bound arm:
# tonstakers_withdraw (immediate) vs tonstakers_withdraw_request (delayed). Both
# arms map their fields declaratively. The delayed arm peeks first for the
# already-produced nft_mint block, then for the raw init call fallback. Both
# candidates remain outside the consumed set.
# ---------------------------------------------------------------------------
matcher tonstakers_withdraw {
    produces tonstakers_withdraw | tonstakers_withdraw_request
    priority 85
    entry    @burn

    op JettonBurn @burn
      -> op JettonBurnNotification
        -> op TONStakersWithdrawRequest @request
          -> ( op TONStakersPoolWithdrawal @immediate_withdrawal
             ^ op TONStakersMintNFT @delayed_withdrawal
                 -> { maybe peek btype nft_mint @minted_block
                    ; maybe peek op TONStakersInitNFT @minted_call } )

    parse burn as JettonBurn
    let minted = (account(minted_block.msg.destination) ?? addr_none()) if minted_block != null else ((account(minted_call.msg.destination) ?? addr_none()) if minted_call != null else null)
    failed when burn.failed
    failed when immediate_withdrawal == null and minted == null
    produces switch {
        when immediate_withdrawal != null => tonstakers_withdraw out {
            stake_holder: account(burn.msg.source)
            burnt_nft:    null
            pool:         account(request.msg.destination)
            tokens_burnt: amount(burn.body.amount)
            amount:       amount(immediate_withdrawal.msg.value - request.msg.value)
            asset:        asset(request.msg.source)
        }
        else => tonstakers_withdraw_request out {
            source:       account(burn.msg.source)
            tsTON_wallet: account(burn.msg.destination)
            pool:         account(request.msg.destination)
            tokens_burnt: amount(burn.body.amount)
            minted_nft:   minted
            asset:        asset(request.msg.source)
        }
    }
}
