# EVAA withdrawal matcher corresponding to EvaaWithdrawBlockMatcher.
#
# Differences from EvaaWithdrawBlockMatcher:
# - EVAA user-contract messages prefix the 32-bit opcode with a variable-length
#   header (version coins + maybe upgrade-info ref + 2 bits), so a plain `op`
#   node cannot see the opcode. Legacy uses EvaaContractWithHeaderMatcher
#   (test_self override); here those three nodes are `pred` nodes backed by
#   host predicates (evaa_user_withdraw_*) with exactly the legacy test logic.
#   Inline predicates cannot replace them because they require body parsing.
# - The EVAA service-comment ton_transfer (legacy evaa_action_comment_matcher,
#   a GenericMatcher over TonTransferBlock.comment) is `maybe pred
#   evaa_service_comment` — set membership over a block attribute is not
#   expressible as an inline field test either.
# - Legacy's withdraw_fail_on_user_excesses GenericMatcher (opcode-set
#   membership over 4 error opcodes) is an exclusive alternative of the four
#   concrete `op` nodes sharing one capture (@fail_excess); same block matched,
#   same consumed set, and only the winning branch binds.
# - `^` mirrors legacy's ExclusiveOrMatcher use. As an edge target the
#   exclusivity gate is try_build-inert, so branch order is
#   first-match-wins exactly like legacy.
#
# Capture -> legacy label map:
#   @master        (anchor)                      | @user      withdraw_on_user
#   @coll_ok       withdraw_collateralized_success
#   @jetton_payout jetton_payout                 | @ton_payout ton_payout
#   @coll_fail     withdraw_collateralized_fail
#   @fail_excess   withdraw_fail_on_user_excesses
# (withdraw_success / withdraw_fail_no_funds / withdraw_no_funds_excess are
# structural-only in the legacy build — no captures needed.)
#
# Captures are kept for pattern fidelity but the builder works off the
# consumed set (Match._mch_consumed): the compiled path's positional capture
# walker picks an alternative branch by its HEAD test, and @coll_ok/@coll_fail
# share the head `op EvaaWithdrawCollateralized`, so on fail traces it binds
# the wrong one (the v2 IR engine binds during the walk and is unaffected).
# Same situation as stonfi_v2_swap.mch / dedust_swap.mch.

opcode EvaaSupplySuccess                   = 0x11a
opcode EvaaSupplyFail                      = 0x11f
opcode EvaaWithdrawMaster                  = 0x2
opcode EvaaWithdrawCollateralized          = 0x211
opcode EvaaWithdrawSuccess                 = 0x211a
opcode EvaaWithdrawNoFundsExcess           = 0x211fe8
opcode EvaaWithdrawLockedExcess            = 0x21e6
opcode EvaaWithdrawNotCollateralizedExcess = 0x21e7
opcode EvaaWithdrawMissingPricesExcess     = 0x21e8
opcode EvaaWithdrawExecutionCrashed        = 0x21ec
opcode EvaaLiquidateMaster                 = 0x3
opcode EvaaLiquidateSatisfied              = 0x311
opcode EvaaLiquidateUnsatisfied            = 0x31f
opcode EvaaLiquidateSuccessReport          = 0x311d
opcode EvaaLiquidateFail                   = 0x311f

predicate evaa_user_withdraw_user
predicate evaa_user_withdraw_success
predicate evaa_user_withdraw_fail
predicate evaa_service_comment
predicate evaa_supply_anchor
predicate evaa_user_supply
predicate evaa_liquidate_anchor
predicate evaa_user_liquidate
predicate evaa_liquidate_success_header
predicate evaa_bounced_call

# EVAA supply matcher corresponding to EvaaSupplyBlockMatcher.
#
# HYBRID: declarative pattern, one host fn for the build. The shape is small and
# fully structural, but almost every field of the product is entangled with the
# TON-vs-jetton discriminator, so the build stays in
# build_evaa_supply_core — which the legacy matcher also calls, so the two
# cannot drift.
#
# ANCHOR. Supply has TWO wire forms: a plain 0x1 supply_master call (TON), or a
# jetton transfer whose FORWARD PAYLOAD is a supply_master. A single spec entry
# can only have one head, and `btype`/`op` cannot see inside a forward payload,
# so `pred evaa_supply_anchor` covers both exactly as the legacy test_self does
# (nft_mint.mch's `pred single_contract_deploy` is the same construct). Note the
# legacy test uses isinstance(JettonTransferBlock), which is dead under the IR
# engine — the predicate tests btype on both sides instead.
#
# `pred evaa_user_supply` is the same variable-header problem the three withdraw
# predicates have: the user contract prefixes its 32-bit opcode with version
# coins + a maybe upgrade-info ref + 2 bits, so a plain `op` node cannot see it.
#
# The outcome alternative is legacy's OrMatcher order verbatim (success, fail,
# jetton refund). The three heads are distinct, but the success branch carries a
# child, so the compiler leaves it an or-node — which is what legacy runs too.
# `maybe pred evaa_service_comment` under the success arm is the shared
# evaa_action_comment_matcher, already used by the withdraw matcher below.
#
# ORDERING. The 0x1 anchor opcode is claimed by no other spec (grepped over
# specs/), and the jetton arm binds a prebuilt `btype
# jetton_transfer`, so the default priority 100 — behind the priority-90 jetton
# specs — is what this needs, matching legacy (jetton matchers 16-19, evaa
# supply 54).

matcher evaa_supply {
    produces EvaaSupplyBlock
    entry    @master

    pred evaa_supply_anchor @master
      -> pred evaa_user_supply @user
        -> ( op EvaaSupplySuccess @success -> maybe pred evaa_service_comment
           | op EvaaSupplyFail @fail
           | btype jetton_transfer @refund )

    let r = evaa_supply_data(consumed)
    reject when r == null
    out {
        sender:                  r.sender
        recipient:               r.recipient
        recipient_contract:      r.recipient_contract
        amount:                  r.amount
        is_success:              r.is_success
        is_ton:                  r.is_ton
        asset_id:                r.asset_id
        sender_jetton_wallet:    r.sender_jetton_wallet
        recipient_jetton_wallet: r.recipient_jetton_wallet
        master_jetton_wallet:    r.master_jetton_wallet
        master:                  r.master
        asset:                   r.asset
    }
}

matcher evaa_withdraw {
    produces EvaaWithdrawBlock
    entry    @master

    op EvaaWithdrawMaster @master
      -> pred evaa_user_withdraw_user @user
      -> ( op EvaaWithdrawCollateralized @coll_ok
             -> { ( btype jetton_transfer @jetton_payout
                  ^ op EvaaWithdrawSuccess @ton_payout -> maybe pred evaa_service_comment )
                  pred evaa_user_withdraw_success -> btype ton_transfer }
         ^ op EvaaWithdrawCollateralized @coll_fail
             -> pred evaa_user_withdraw_fail
             -> op EvaaWithdrawNoFundsExcess
         ^ ( op EvaaWithdrawLockedExcess @fail_excess
           ^ op EvaaWithdrawNotCollateralizedExcess @fail_excess
           ^ op EvaaWithdrawMissingPricesExcess @fail_excess
           ^ op EvaaWithdrawExecutionCrashed @fail_excess ) )

    # Declarative build, field-for-field with build_evaa_withdraw_core.
    # The IR engine binds @coll_ok / @coll_fail / @fail_excess per the winning
    # exclusive branch (only the compiled path mis-bound the shared-head
    # @coll_ok/@coll_fail are not captured), so `coll_ok != null` is the
    # success test.
    # exactly like the core's `if withdraw_collateralized_success:` branch.
    parse master     as EvaaWithdrawMaster
    parse coll_ok    as EvaaWithdrawCollateralized     # null on the fail branches
    parse fail_excess as EvaaWithdrawFailExcess         # null unless a user-contract error fired
    let is_ton  = master.body.asset_id == 0x1A4219FE5E60D63AF2A3CC7DCE6FEC69B45C6B5718497A6148E7C232AC87BD8A
    let success = coll_ok != null
    # Legacy `return None` guards: a collateralized-success outcome must carry the
    # matching payout leg; a match with no outcome group is impossible structurally
    # but the last guard mirrors the core's trailing `return None`.
    reject when success and is_ton and ton_payout == null
    reject when success and not is_ton and jetton_payout == null
    reject when coll_ok == null and coll_fail == null and fail_excess == null
    out {
        owner:                   account(master.msg.source)
        owner_contract:          account(user.msg.destination)
        recipient:               account(master.body.recipient_address)
        master:                  account(master.msg.destination)
        asset_id:                master.body.asset_id
        is_ton:                  is_ton
        is_success:              success
        amount:                  coll_ok.body.withdraw_amount_current if success else master.body.amount
        asset:                   (ton_asset() if is_ton else jetton_payout.data.asset) if success else null
        recipient_jetton_wallet: account(jetton_payout.data.receiver_wallet) if (success and not is_ton) else null
        master_jetton_wallet:    account(jetton_payout.data.sender_wallet) if (success and not is_ton) else null
        fail_reason:             null if success else ("withdraw_no_funds_excess" if coll_fail != null else fail_excess.body.reason)
    }
}

# EVAA liquidation — the legacy structure verbatim. In particular, the two
# satisfied arms share 0x311, but only the success arm applies the variable
# user header before testing 0x311a; the fail arm tests plain 0x311f.
matcher evaa_liquidate {
    produces evaa_liquidate
    entry    @master

    pred evaa_liquidate_anchor @master
      -> ( pred evaa_user_liquidate @user
             -> ( op EvaaLiquidateSatisfied @satisfied_success
                    -> { pred evaa_liquidate_success_header
                           -> maybe pred evaa_service_comment
                         ( op EvaaLiquidateSuccessReport @finish
                         ^ op EvaaLiquidateFail @finish
                         ^ btype jetton_transfer @finish ) }
                ^ op EvaaLiquidateSatisfied @satisfied_fail
                    -> { op EvaaLiquidateFail
                         ( op EvaaLiquidateSuccessReport @finish
                         ^ op EvaaLiquidateFail @finish
                         ^ btype jetton_transfer @finish ) }
                ^ op EvaaLiquidateUnsatisfied @unsatisfied
                    -> ( op EvaaLiquidateSuccessReport @finish
                       ^ op EvaaLiquidateFail @finish
                       ^ btype jetton_transfer @finish ) )
           ^ ( pred evaa_bounced_call @immediate
             ^ btype jetton_transfer @immediate ) )

    let r = evaa_liquidate_data(consumed)
    reject when r == null
    out {
        liquidator:          r.liquidator
        borrower:            r.borrower
        borrower_contract:   r.borrower_contract
        collateral_asset_id: r.collateral_asset_id
        collateral_amount:   r.collateral_amount
        is_success:           r.is_success
        fail_reason:          r.fail_reason
        debt_amount:          r.debt_amount
        debt_asset_id:        r.debt_asset_id
    }
}
