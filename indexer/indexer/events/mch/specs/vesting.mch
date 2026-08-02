# Vesting send-message matcher — ported from blocks/vesting.py
# VestingSendMessageBlockMatcher. It is a linear two-node chain: the
# owner->vesting request plus its required response callback. The response is
# consumed (legacy child_matcher optional=False) but only the request feeds the
# build. `vesting_add_whitelist` below reuses that two-node shape 1:1.
#
# The declarative build has field-for-field parity with
# build_vesting_send_message_core in blocks/vesting.py. `message_boc_str` uses
# the b64 builtin over the parsed `message_cell` — b64 BOC-encodes a raw Cell
# before base64 (mirrors the legacy `message_cell.to_boc()`).
#
# `success` (upstream #430: the flag is true only when the message was really
# sent) is the one host fn — it scans the request TRANSACTION's out-messages for
# a matching destination + body hash, and transaction-level navigation is
# outside the language. See builders/vesting.py.

opcode VestingSendMessage         = 0xA7733ACD
opcode VestingSendMessageResponse = 0xF7733ACD

matcher vesting_send_message {
    produces VestingSendMessageBlock
    entry    @send

    op VestingSendMessage @send -> op VestingSendMessageResponse @response

    parse send as VestingSendMessage
    out {
        sender:              account(send.msg.source)
        vesting:             account(send.msg.destination)
        query_id:            send.body.query_id
        message_boc_str:     b64(send.body.message_cell)
        message_destination: account(send.body.message_destination)
        message_value:       amount(send.body.message_value)
        success:             vesting_message_was_sent(send, send.body)
    }
}

# -----------------------------------------------------------------------------
# vesting_add_whitelist — blocks/vesting.py:141 VestingAddWhiteListBlockMatcher.
#
# The vesting creator authorizes destinations the locked wallet may send to.
# Structurally the same two-node chain as the send above (request + required
# response callback, both consumed, only the request read), so the whole port is
# one parser plus one `out` block.
#
# THE PARSER stays hand-written (src/parse/ParseVesting.cpp): the whitelist is an
# UNBOUNDED chain of cells, one address per level, and `ref { }` in
# the protocol ABI rows are fixed-depth. It is not in the `--parse-dump` A/B table
# either — the dump renderer has no List form on either side (the same reason
# JVaultClaim's `dict_keys267` list is absent from it) — so its gate is the
# actions golden plus the legacy-vs-IR parity-sweep row, which compares
# `accounts_added` element by element.
#
# `?? addr_none()` on each mapped element is REQUIRED, not defensive: an
# addr_none entry reaches the two engines differently — C++ `load_address_py`
# yields an addr_none Account while the Python twin's attribute is a plain None,
# and `account(null)` short-circuits to null because builtins are null-strict.
# The legacy build writes `AccountId(None)` for that case, which is addr_none on
# both
# sides only with the coalesce. No corpus fixture carries one; the fixture's
# chain is a single real address.
# -----------------------------------------------------------------------------
opcode VestingAddWhiteList         = 0x7258A69B
opcode VestingAddWhiteListResponse = 0xF258A69B

matcher vesting_add_whitelist {
    produces vesting_add_whitelist
    entry    @add

    op VestingAddWhiteList @add -> op VestingAddWhiteListResponse @response

    parse add as VestingAddWhiteList
    out {
        adder:          account(add.msg.source)
        vesting:        account(add.msg.destination)
        query_id:       add.body.query_id
        accounts_added: map(add.body.addresses as a => account(a) ?? addr_none())
    }
}
