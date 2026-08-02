# Coffee MEV-protect hold-funds matcher. It needs no host functions or parsers.
#
# The legacy matcher never reads a message body on either arm; every field comes
# off the envelope (TON arm) or off the prebuilt jetton_transfer's data (jetton
# arm). It is one matcher with a two-kind `test_self`:
#
#     (CallContractBlock and opcode == 0x6bc79e7e)
#  or (JettonTransferBlock and int(data["payload_opcode"], 16) == 0x6bc79e7e)
#
# Each arm uses a separate matcher.
# A mixed `(op X @m ^ btype jetton_transfer @m where …)` anchor group can only
# carry a NAMED host `where` predicate; an inline `where (expr)` on a group
# branch is C013_WHERE_EXPR_NOT_SYNC (compiler.py), because the
# IR anchor record has no slot for an expression. A single anchor would
# therefore cost a host predicate written twice (Python + C++) purely to test
# one data field against one string — exactly the dual-implementation liability
# the inline form exists to avoid. Splitting the arms keeps the payload test
# inline on a plain anchor node and avoids host code.
#
# The split is behaviourally inert: the two anchors are disjoint (a block is
# either a call_contract or a jetton_transfer), so at most one matcher can ever
# see a given block, both produce the same btype with the same field set, and
# their relative order carries nothing. Same construct as the
# telegram_nft_purchase / telegram_nft_assigned split.
#
# `payload_opcode` string equality == the legacy int comparison: the field is
# Python `hex(sum_type)` (messages/jettons.py:51) and C++ `"0x%x"`
# (parse/ParseJettons.cpp:105) — both lowercase, unpadded, so "0x6bc79e7e" is
# the one spelling either host can produce. A transfer with no forward payload
# has the key present-null (=> test false); a legacy `int(None, 16)` raises and
# is swallowed by the matcher's bare `except` (=> false too).
#
# THE PARENT. `GenericMatcher(optional=True, test_self_func=lambda b: True,
# child_matcher=ContractMatcher(CoffeeServiceFee))` — any parent block that also
# emitted the 0xc0ffeea0 service fee, optional. That is the `A <- maybe (any ->
# {…})` shape: an `any` head, and a children block after a parent edge attaching
# to the PARENT matcher, not the anchor, as in the telegram_nft matcher. On the
# corpus fixture the parent is the user wallet's ext-in
# driven call, whose two children are the fee and the anchor.
#
# `mev_contract_wallet` is hard null in the legacy build on BOTH arms (it is
# initialised to None and never assigned) — reproduced, not "fixed".
#
# ORDERING. 0x6bc79e7e is claimed by no other spec. The jetton arm anchors on
# `btype jetton_transfer`, shared with ethena_deposit, jvault_stake,
# stonfi_v1_swap, dedust_v2_swap/failed_swap and coffee_staking_deposit — and
# the legacy list puts CoffeeMevProtectHoldFundsMatcher AFTER every one of those
# except the staking family, so this file is listed near the END of
# the built-in source list, before specs/coffee_staking.mch. Being behind the
# priority-90 jetton specs (default priority 100) is what makes the
# jetton_transfer blocks exist at all.
#
# CoffeeMevProtectFailedSwapMatcher is not represented here. Its jetton arm needs a
# forward-payload parse the language cannot express (`parse` is node-only), and
# its serializer fill has no dispatch arm, so its result would contain only nulls.

opcode CoffeeMevProtectHoldFunds = 0x6BC79E7E
opcode CoffeeServiceFee          = 0xC0FFEEA0

# --- TON arm: the raw 0x6bc79e7e call to the MEV contract. -------------------
matcher coffee_mev_protect_hold_funds {
    produces coffee_mev_protect_hold_funds
    entry    @call

    op CoffeeMevProtectHoldFunds @call
      <- maybe ( any -> { op CoffeeServiceFee } )

    out {
        sender:              account(call.msg.source)
        mev_contract:        account(call.msg.destination)
        asset:               ton_asset()
        # Unconditional `AccountId(None)` in the legacy build: a present-but-
        # empty address, not a missing one.
        sender_wallet:       addr_none()
        mev_contract_wallet: null
        amount:              amount(call.msg.value)
    }
}

# --- jetton arm: a transfer whose forward payload IS the hold-funds op. ------
matcher coffee_mev_protect_hold_funds_jetton {
    produces coffee_mev_protect_hold_funds
    entry    @transfer

    btype jetton_transfer @transfer where (.payload_opcode == "0x6bc79e7e")
      <- maybe ( any -> { op CoffeeServiceFee } )

    out {
        sender:              transfer.data.sender
        mev_contract:        transfer.data.receiver
        asset:               transfer.data.asset
        sender_wallet:       transfer.data.sender_wallet
        mev_contract_wallet: null
        amount:              transfer.data.amount
    }
}
