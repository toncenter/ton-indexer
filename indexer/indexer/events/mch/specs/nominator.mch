# Nominator pool withdraw matcher for an owner-initiated payout following a
# NominatorPoolProcessWithdrawRequests call. Its build is declarative.
#
# Legacy anchors on the call and loops over every ton_transfer child, building
# one NominatorPoolWithdrawRequestBlock per payout while leaving
# the call itself unconsumed (each payout's `previous_block` stays the call;
# `merge_blocks([transfer_block])` only, never the call).
#
# That fan-out (1 match -> N produced blocks, shared parent unconsumed) is not
# expressible by anchoring on the call. Each payout `ton_transfer` is the anchor.
# The parent call is checked via a host predicate on `.previous_block` (never a
# captured node), so it is never part of the consumed set and stays unconsumed
# exactly like legacy. The engine's normal per-block loop then produces one
# independent match per payout for any N, with identical output granularity.
#
# The pool address and the payout amount are both reachable without navigating
# to the (uncaptured) parent: the payout ton_transfer is sent BY the pool, so
# `payout.msg.source` is the pool and `payout.msg.destination` is the nominator.
# the pool block value equals the payout message value (TonTransferBlock.value =
# msg.value), so `payout.msg.value` is the amount. The NominatorPool interface
# check (legacy `get_interfaces` membership) is
# a registered nullable lookup keyed on the pool address, rejected when null.
#
# Aux merge (both new matchers): legacy takes pre_build_auto_append=True with
# include_excess/include_bounces defaulting True (staking.py:348,374), and
# basic_matchers.py:42-57 funnels both auxiliary opcodes through one list, so the
# excess and bounce are merged into the produced block, not appended standalone.
# The mch root default appends, so both are consumed optional children and both
# flags are off. nft_transfer.mch uses the same approach for excess alone.
#
# Auxiliary arity differs: `maybe op X` binds at most one block per item
# (engine.py:523-543, Walker.cpp:186-208), whereas the legacy true-form merges
# every matching excess/bounce (basic_matchers.py:49-52). A 'd'/'w' anchor with
# more than one excess or bounce therefore leaves the surplus unconsumed here.
# One message chain practically yields one of each, so this is accepted. If an
# A/B diff surfaces, this documented aux-arity divergence is the reason.

opcode ExcessMessage = 0xD53276DB

predicate nominator_pool_withdraw_parent

matcher nominator_pool_withdraw {
    produces nominator_pool_withdraw_request
    entry    @payout

    btype ton_transfer @payout where nominator_pool_withdraw_parent

    let pool_iface = lookup nominator_pool(payout.msg.source)
    reject when pool_iface == null
    out {
        source:        account(payout.msg.destination)
        pool:          account(payout.msg.source)
        payout_amount: amount(payout.msg.value)
    }
}

matcher nominator_pool_deposit {
    produces        nominator_pool_deposit
    entry           @deposit
    include_excess  false
    include_bounces false

    btype ton_transfer @deposit where (.comment == "d")
      -> { maybe op ExcessMessage @excess ; maybe op 0xffffffff @bounce }

    let pool_iface = lookup nominator_pool(deposit.msg.destination)
    reject when pool_iface == null
    failed when deposit.failed
    out {
        source: account(deposit.msg.source)
        pool:   account(deposit.msg.destination)
        value:  amount(deposit.msg.value)
    }
}

matcher nominator_pool_withdraw_request {
    produces        nominator_pool_withdraw_request
    shape           nominator_withdraw_absorb_payouts
    entry           @request
    include_excess  false
    include_bounces false

    btype ton_transfer @request where (.comment == "w")
      -> { maybe op ExcessMessage @excess ; maybe op 0xffffffff @bounce }

    let pool_iface = lookup nominator_pool(request.msg.destination)
    reject when pool_iface == null
    failed when request.failed
    out {
        source:        account(request.msg.source)
        pool:          account(request.msg.destination)
        payout_amount: nominator_withdraw_payout_amount(request)
    }
}
