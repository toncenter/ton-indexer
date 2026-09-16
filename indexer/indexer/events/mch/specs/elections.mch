# Elector stake deposit / recover — port of blocks/elections.py
# ElectionDepositStakeBlockMatcher (:28) and ElectionRecoverStakeBlockMatcher
# (:50). Both are already declarative in shape (one anchor opcode plus one
# optional confirmation child, include_excess=False), so the patterns are
# literal transcriptions and neither build body needs a host core: no body is
# ever parsed, and every produced field comes off a message envelope.
#
# The elector gate
# ----------------
# Both anchors additionally require the destination to be the masterchain
# elector special account. The reference puts that test inside `test_self`; here
# it is a `reject when` on the raw envelope address, which is equivalent because
# a rejection consumes nothing and leaves the anchor for the next
# matcher, exactly as a false `test_self` would, and stays inside the language.
# It cannot be an inline `where (.field == literal)`: that form tests the
# candidate's `data` map, and a call_contract's `data.destination` is an
# AccountId, not the raw string the reference compares. The literal is safe to
# spell either way — the elector address is all digits, so the case-sensitivity
# of `==` on strings never comes into it.
#
# WHICH message carries the amount differs between the two, and is the only
# real asymmetry:
#   deposit — the REQUEST's value (what the staker sent), always present;
#   recover — the CONFIRMATION's value (what the elector paid back), so with no
#             confirmation there is no amount at all. The reference omits the
#             dict key in that case rather than storing None, and
#             `_fill_election_action` reads it with an `in` test, so the field
#             is declared optional (`amount?:`) to reproduce absent-vs-null.
#
# Both actions carry `source` and `amount` and nothing else — no composite
# column at all (block_tree_serializer.py:658 `_fill_election_action`), and both
# btypes are in `v1_ops` on both engines, so neither expands child rows.
#
# ORDERING. None of the four opcodes is claimed by any other spec, and neither
# matcher binds a block another matcher produces, so the file's position in
# the built-in source list carries no tie-break; it is listed where the file-name
# sort puts it. The reference registers the pair between ChangeDnsRecordMatcher
# and SubscriptionBlockMatcher, neither of which shares an anchor with it.
#
# Capture -> reference role map:
#   @req    the anchor call (`test_self` target, `block` in build_block)
#   @conf   the optional confirmation (legacy labeled('confirmation'))

opcode ElectorDepositStakeRequest      = 0x4E73744B
opcode ElectorDepositStakeConfirmation = 0xF374484C
opcode ElectorRecoverStakeRequest      = 0x47657424
opcode ElectorRecoverStakeConfirmation = 0xF96F7324

matcher election_deposit {
    produces        election_deposit
    entry           @req
    include_excess  false

    op ElectorDepositStakeRequest @req -> maybe op ElectorDepositStakeConfirmation @conf

    reject when req.msg.destination != "-1:3333333333333333333333333333333333333333333333333333333333333333"
    failed when conf == null

    out {
        amount:       amount(req.msg.value)
        stake_holder: account(req.msg.source)
    }
}

matcher election_recover {
    produces        election_recover
    entry           @req
    include_excess  false

    op ElectorRecoverStakeRequest @req -> maybe op ElectorRecoverStakeConfirmation @conf

    reject when req.msg.destination != "-1:3333333333333333333333333333333333333333333333333333333333333333"
    failed when conf == null

    out {
        amount?:      amount(conf.msg.value)
        stake_holder: account(req.msg.source)
    }
}
