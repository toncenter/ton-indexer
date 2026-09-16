# pTON transfer matcher — port of blocks/jettons.py PTonTransferMatcher.
#
# A pTON ("proxy TON") wallet wraps native TON behind the jetton transfer
# interface: it accepts the ordinary 0x0f8a7ea5 body but never emits an internal
# transfer, so `jetton_transfer` (specs/jettons_decl.mch, whose pattern requires
# the internal leg) can never match one. Without this matcher the pTON leg of a
# DEX swap stays an unconsumed call_contract and every consumer that binds
# `btype jetton_transfer` — stonfi v1/v2, dedust, tonco — loses its TON-side leg.
#
# Same anchor opcode as `jetton_transfer`, so registration order decides: this
# spec is listed AFTER specs/jettons_decl.mch in the built-in source list and
# shares its `priority 90`, which reproduces the legacy list order (JettonTransferBlock-
# Matcher, then PTonTransferMatcher, both ahead of the swap matchers).
#
# The pTON master allowlist is what makes the match safe on an opcode this
# common: only a wallet whose master is one of the three canonical pTON masters
# qualifies. Same three addresses as host/DexPton.cpp `pton_masters()`, which the
# DEX hosts use to turn such a wallet into a TON asset.
#
# Field parity with the legacy build: `has_internal_transfer` is false and both
# wallet fields are null (there is no internal leg to name them from — the flag
# is what the stonfi v2 pattern keys its pton arms on), and the legacy build sets
# no `desired_amount`/`desired_forward_amount` here, so neither does this.

opcode JettonTransfer = 0x0f8a7ea5
opcode JettonNotify   = 0x7362d09c

matcher pton_transfer {
    produces JettonTransferBlock
    priority 90
    entry    @transfer

    op JettonTransfer @transfer -> maybe op JettonNotify @notify

    parse transfer as JettonTransfer
    let wallet = lookup jetton_wallet(transfer.msg.destination)
    reject when wallet == null
    let pton_v1   = wallet.jetton == "0:8CDC1D7640AD5EE326527FC1AD0514F468B30DC84B0173F0E155F451B4E11F7C"
    let pton_v2   = wallet.jetton == "0:671963027F7F85659AB55B821671688601CDCF1EE674FC7FBBB1A776A18D34A3"
    let tonco_wtton = wallet.jetton == "0:949C4C66760C002800E2FA3D8A3CA4E1C90A9373B53AE7472033483BF14CD95E"
    reject when not (pton_v1 or pton_v2 or tonco_wtton)
    failed when transfer.failed
    out {
        has_internal_transfer: false
        sender:                account(transfer.msg.source)
        sender_wallet:         null
        receiver:              account(transfer.body.destination)
        receiver_wallet:       null
        response_address:      account(transfer.body.response) ?? addr_none()
        forward_amount:        amount(transfer.body.forward_amount)
        query_id:              transfer.body.query_id
        asset:                 asset(wallet.jetton)
        amount:                amount(transfer.body.amount)
        forward_payload:       b64(transfer.body.forward_payload)
        custom_payload:        b64(transfer.body.custom_payload)
        comment:               transfer.body.comment
        encrypted_comment:     transfer.body.encrypted_comment
        payload_opcode:        transfer.body.payload_sum_type
        stonfi_swap_body:      transfer.body.stonfi_swap_body
    }
}
