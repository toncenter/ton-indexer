# tgBTC mint matcher corresponding to TgBTCMintBlockMatcher.
#
# Matcher shape
# The mint trace is a single linear call chain of arbitrary length:
#
#     op 0x3F781D24 (head) -> <N arbitrary-opcode hops> -> op 0xE42A879B (tail)
#                                                            -> { success log ;
#                                                                 jetton_mint }
#
# The intermediate hops carry protocol opcodes that vary per mint (short mints
# jump head -> ... -> tail in 3 hops; "very long" mints thread ~11 hops through
# opcodes 0xE92BD172 / 0xD303C8F4 / 0xEFFE02DA / 0x0A48515A that no fixed head
# could enumerate). The legacy matcher anchors on the TAIL (0xE42A879B) and then
# imperatively walks `previous_block` up to 20 hops to rediscover the head — a
# scan. This matcher anchors on the distinctive
# HEAD (0x3F781D24) and descends the chain with an `any`-headed recursion whose
# exclusive exit branch is the tail plus its two-item children set. Same
# consumed set, same produced block.
#
# Deviation from legacy (fixture-safe): the legacy walk also collects the
# non-path children (siblings) of every ancestor it passes. None of the four
# mint fixtures branch off the trunk — the chain is strictly linear — so the
# consumed set is identical. A hypothetical mint that forks mid-chain would have
# its off-trunk siblings consumed by legacy but not here; recording such a trace
# is out of scope (no live-DB access), and no known mint produces one.
#
# Builder: consumed-set-driven (Match._mch_consumed), like specs/evaa.mch. The
# three blocks the core needs each carry a unique signature within one match's
# consumed set — the head is the sole 0x3F781D24 call, the success log the sole
# 0x77A80EF3 call, the mint the sole `jetton_mint` block — so derivation is
# unambiguous. Captures are declared for pattern fidelity but not read (the
# exit-branch captures @tail/@log/@mint would be list bindings under recursion,
# and the compiled path's positional walker mis-binds same-position exit
# captures — the same reason evaa/stonfi_v2/dedust_swap drive off the consumed
# set).
#
# Capture -> legacy role map:
#   @head  (anchor, 0x3F781D24)  sender = head.msg.source
#   @tail  (0xE42A879B)          structural — the chain terminus
#   @log   (0x77A80EF3)          TgBTCMintEvent: amount/recipient/txid/teleport
#   @mint  (jetton_mint)         asset + recipient_wallet
#   @mid   (any)                 the intermediate hops (structural only)

# =============================================================================
# The rest of the family comprises mint, burn, new_key, and four log-only
# fallbacks.
#
# All six live in this file in registration order. The order matters: each
# log-only matcher anchors on an opcode its full sibling consumes. `tgbtc_mint`
# eats 0x77A80EF3, `tgbtc_burn` eats 0xCA444CE6, and `tgbtc_new_key` eats both
# 0x27756729 and 0x453443A6. A full matcher must
# claim its chain before the fallback can claim the naked log, or every complete
# trace would also emit a crippled `*_fallback` row. Equal priority (all default
# 100) ⇒ the tie-break is (file, source position), which is exactly what the
# single-file layout pins. No other spec declares any of these nine opcodes
# (grep over specs/), and nothing binds a tgbtc btype, so the file's slot in the
# built-in source list carries no cross-spec tie-break — only
# `tgbtc_burn`'s `btype jetton_burn` anchor matters, and that must stay behind
# the priority-90 jetton family that builds it (it does: default 100).
#
# Burn and new_key allow the same optional payout subtree,
# `0xBE44E7A6 -> { maybe 0x1A84C0E0 ; maybe ton_transfer }`, and both
# read nothing out of it; it exists only to widen the consumed set. Written out
# twice rather than hoisted to a `rule`: a rule reference cannot carry the
# attach-to-left context a children block needs (C008), so the inline form is
# the only one available.
#
# The payout call deploys its destination on every fixture, so
# a `ContractDeploy` hangs off its `children_blocks`. Unlike `tgbtc_mint` (whose
# legacy hand-walk sweeps those up explicitly) neither legacy burn nor legacy
# new_key collects them — both merge only `other_blocks + [block]` — so NO
# `shape tgbtc_deploy_absorb` here. `unwind_deployments` surfaces them later.
# =============================================================================

opcode TgBTCMintHead       = 0x3F781D24
opcode TgBTCMintTail       = 0xE42A879B
opcode TgBTCMintSuccessLog = 0x77A80EF3

opcode JettonBurnNotify     = 0x7BDD97DE
opcode TgBTCBurnNotify      = 0x587643A2
opcode TgBTCNewKeyRequest   = 0x690F357A
opcode TgBTCPayout          = 0xBE44E7A6
opcode TgBTCPayoutNotify    = 0x1A84C0E0
opcode TgBTCBurnLog         = 0xCA444CE6
opcode TgBTCNewKeyLog       = 0x27756729
opcode TgBTCDkgCompletedLog = 0x453443A6

matcher tgbtc_mint {
    produces TgBTCMintBlock
    shape    tgbtc_deploy_absorb
    entry    @head

    op TgBTCMintHead @head -> $mint_chain

    # HYBRID declarative build (C++-runnable). The core (log parse, endian-hex
    # bitcoin_txid, jetton_mint asset/wallet) is the one consumed-set-driven host
    # fn; every produced field maps from its namespace. The ContractDeploy
    # side-effect children ride on the shaper (see the builder module docstring).
    let d = tgbtc_mint_data(consumed)
    reject when d == null
    out {
        sender:            d.sender
        recipient:         d.recipient
        amount:            d.amount
        asset:             d.asset
        bitcoin_txid:      d.bitcoin_txid
        success:           d.success
        recipient_wallet:  d.recipient_wallet
        teleport_contract: d.teleport_contract
        crippled:          d.crippled
    }
}

# Descend the trunk: each hop is any block that is not the tail; recursion
# terminates on the tail node (exclusive exit branch), which carries the success
# log and the jetton_mint block as a children set.
rule mint_chain = (
    any @mid -> maybe $mint_chain
  ^ op TgBTCMintTail @tail
      -> { op TgBTCMintSuccessLog @log ; btype jetton_mint @mint }
)

# -----------------------------------------------------------------------------
# tgbtc_burn — blocks/tgbtc.py:210 TgBTCBurnBlockMatcher.
#
# A tgBTC peg-out: the user burns their tgBTC, the jetton master notifies the
# teleport contract, which logs the burn ext-out and (usually) pays the gas
# remainder back.
#
#     btype jetton_burn -> notify -> { burn log ; opt payout -> { … } }
#
# The notify head comes in two flavours, 0x7BDD97DE (the standard
# JettonBurnNotification, forwarded a SECOND time master -> teleport; the first
# wallet -> master hop is already inside the jetton_burn block) and 0x587643A2
# (the "strange notification" of the TDsLHsBM… fixture). `^` NOT `|`: both
# branches bind the SAME @log/@notify names, and a duplicated capture across
# alternatives is only legal when the compiler can prove exclusivity (R005).
# The children set is written out inside each branch rather than after the
# group, mirroring the legacy `OrMatcher([ContractMatcher(children_matchers=…),
# ContractMatcher(children_matchers=…)])` shape exactly.
#
# AMOUNT — reproduced, not fixed. Legacy takes the jetton_burn's amount when it
# is non-zero, else the log's, and when BOTH are present takes the MIN. The
# language has no `min`, so it is a ternary; `(chain ?? 0) == 0` is legacy's
# `not amount or amount == 0` for an int-or-None. The 2yH8U5KF… fixture is the
# case that pins it (yaml: "amount: 93066 # from burn msg").
#
# Fields other than the amount and the pegout address come off the jetton_burn
# block, NOT the log: the log's own `sender` is the teleport-side view and
# legacy never reads it here (see the TDsLHsBM… case, whose action source is the
# burn owner).
# -----------------------------------------------------------------------------
matcher tgbtc_burn {
    produces tgbtc_burn
    entry    @burn

    btype jetton_burn @burn
      -> ( op JettonBurnNotify @notify
             -> { op TgBTCBurnLog @log
                ; maybe op TgBTCPayout
                    -> { maybe op TgBTCPayoutNotify ; maybe btype ton_transfer } }
         ^ op TgBTCBurnNotify @notify
             -> { op TgBTCBurnLog @log
                ; maybe op TgBTCPayout
                    -> { maybe op TgBTCPayoutNotify ; maybe btype ton_transfer } } )

    let b = tgbtc_burn_log(log)
    reject when b == null
    let chain_amount = burn.data.amount.value
    let use_log      = (chain_amount ?? 0) == 0
    let min_amount   = chain_amount if chain_amount < b.amount else b.amount
    out {
        sender:         burn.data.owner
        jetton_wallet:  burn.data.jetton_wallet
        asset:          burn.data.asset
        amount:         amount(b.amount if use_log else min_amount)
        pegout_address: b.pegout
        crippled:       false
    }
}

# -----------------------------------------------------------------------------
# tgbtc_new_key — blocks/tgbtc.py:301 TgBTCNewKeyBlockMatcher.
#
# The validator set rotates its threshold key: the coordinator finishes DKG
# (logging 0x453443A6 ext-out) and, from the SAME transaction, sends the new key
# to the teleport contract, which logs 0x27756729 ext-out and funds the new
# pegout address.
#
#     op NewKeyRequest @k -> { new-key log ; opt payout } <- any @p -> { dkg log }
#
# The dkg log is a SIBLING of the anchor, so it is reached through the anchor's
# parent — legacy's `parent_matcher=GenericMatcher(test_self=True,
# child_matcher=<dkg log>)`, i.e. "the anchor's parent, whatever it is, must
# also have a dkg-log child". `any @p` is that untyped parent. The shape is
# This is legal because a children block does not advance the chain
# cursor, so `@k -> {…}` still leaves the parent edge attached to the anchor,
# and the trailing `-> {…}` attaches to @p — not the C009 shape, which needs a
# parent edge hanging off a node reached BY a child edge.
#
# Deviation, unobservable on the corpus: legacy's `child_matcher` consumes EVERY
# matching child of @p, this children block consumes the first. No trace carries
# two dkg logs in one transaction (there is one key per DKG round).
#
# `amount` is a RAW int here, not an Amount — legacy's TgBTCNewKeyData declares
# `amount: int` and the fill writes it to `action.amount` unwrapped.
# -----------------------------------------------------------------------------
matcher tgbtc_new_key {
    produces tgbtc_new_key
    entry    @k

    op TgBTCNewKeyRequest @k
      -> { op TgBTCNewKeyLog @log
         ; maybe op TgBTCPayout
             -> { maybe op TgBTCPayoutNotify ; maybe btype ton_transfer } }
      <- any @p
        -> { op TgBTCDkgCompletedLog @dkg }

    let n = tgbtc_new_key_log(log)
    let g = tgbtc_dkg_completed_log(dkg)
    reject when n == null or g == null
    out {
        coordinator_contract: account(log.msg.source)
        pubkey:               n.pubkey
        pegout_address:       n.pegout
        amount:               n.amount
        teleport_contract:    account(dkg.msg.source)
        timestamp:            g.timestamp
        crippled:             false
    }
}

# =============================================================================
# Log-only fallbacks (blocks/tgbtc.py:366-490). Each anchors on ONE external-out
# event log and consumes nothing else — the whole match is the single node, and
# the produced block is its full-sibling's block type with `crippled = true`, so
# the serializer appends `_fallback` to the action type. They exist for traces
# where the chain the full matcher wants is absent or unrecognisable.
#
# External-out logs ARE matchable on both engines: a destination-less out_msg
# becomes a real leaf EventNode with `btype == "call_contract"` and its opcode
# set, and the walker anchors on it.
#
# Each fallback must stay after its full sibling above; see the ordering note at
# the top of the file.
# =============================================================================

# mint fallback — same block type, same fill (which already appends `_fallback`
# off `crippled`) as tgbtc_mint. asset / recipient_wallet are unknowable without
# the jetton_mint leg, and the teleport contract collapses onto the log emitter.
matcher tgbtc_mint_log_only {
    produces TgBTCMintBlock
    entry    @log

    op TgBTCMintSuccessLog @log

    let m = tgbtc_mint_log(log)
    reject when m == null
    out {
        sender:            account(log.msg.source)
        recipient:         m.recipient
        amount:            m.amount
        asset:             null
        bitcoin_txid:      m.bitcoin_txid
        success:           true
        recipient_wallet:  null
        teleport_contract: account(log.msg.source)
        crippled:          true
    }
}

# burn fallback — the 35MtsOa0… fixture, whose log body is truncated mid-address:
# `tgbtc_burn_log` yields addr_none for both addresses instead of failing, which
# is the whole reason that parse is a host fn and not an `addr_py` schema row.
matcher tgbtc_burn_log_only {
    produces tgbtc_burn
    entry    @log

    op TgBTCBurnLog @log

    let b = tgbtc_burn_log(log)
    reject when b == null
    out {
        sender:         b.sender
        jetton_wallet:  null
        asset:          null
        amount:         amount(b.amount)
        pegout_address: b.pegout
        crippled:       true
    }
}

# new-key fallback — no dkg log in reach, so teleport_contract and timestamp are
# both null and the coordinator collapses onto the log emitter.
matcher tgbtc_new_key_log_only {
    produces tgbtc_new_key
    entry    @log

    op TgBTCNewKeyLog @log

    let n = tgbtc_new_key_log(log)
    reject when n == null
    out {
        coordinator_contract: account(log.msg.source)
        pubkey:               n.pubkey
        pegout_address:       n.pegout
        amount:               n.amount
        teleport_contract:    null
        timestamp:            null
        crippled:             true
    }
}

# dkg fallback — the one matcher here with NO full sibling: Python has no
# non-fallback producer of the `tgbtc_dkg_log` btype, and its fill hardcodes the
# `_fallback` suffix, so the block carries no `crippled` field at all. It fires
# on a dkg log the new-key matcher above did not claim (the _6GHAnnp8… fixture:
# the 0x690F357A request is there but emits no new-key log, so new_key rejects
# and the dkg log is left over).
matcher tgbtc_dkg_log_only {
    produces tgbtc_dkg_log
    entry    @log

    op TgBTCDkgCompletedLog @log

    let g = tgbtc_dkg_completed_log(log)
    reject when g == null
    out {
        coordinator_contract: account(log.msg.source)
        timestamp:            g.timestamp
        internal_pubkey:      g.pubkey
    }
}
