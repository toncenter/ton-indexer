# Jetton family matchers with declarative builds.
#
# Same patterns as specs/jettons.mch, but the produced-block data/flags are
# expressed with build statements + `out {...}` instead of a registered `build`
# directive. Field-for-field parity with the legacy imperative cores in
# blocks/jettons.py (build_jetton_transfer_core / _get_jetton_burn_data /
# _get_jetton_mint_data). The separate specs/jettons.mch builder-tier
# conformance reference is excluded from the production source list to avoid
# duplicate anchors.
#
# Each matcher declares `priority 90` (< default 100) so it registers ahead of
# the priority-100 specs regardless of emission order.

opcode JettonTransfer         = 0x0f8a7ea5
opcode JettonInternalTransfer = 0x178d4519
opcode JettonNotify           = 0x7362d09c
opcode JettonBurn             = 0x595f07bc
opcode JettonBurnNotification = 0x7bdd97de
opcode JettonMint             = 0x642b7d07
opcode MinterJettonMint       = 0x00000015

# ---------------------------------------------------------------------------
# jetton_transfer — legacy build_jetton_transfer_core.
#
# amount/forward fall back from the internal-transfer body to the transfer body
# when the internal body cannot be parsed (soft parse + `??`). The receiver
# lookup rejects the match when absent; when the wallet owner disagrees with the
# transfer's declared destination the receiver is replaced with the wallet owner
# (ternary). `has_internal_transfer` is a literal true — the pattern requires the
# internal leg. Legacy marks the consumed anchor block `broken` on
# owner/receiver mismatch, which the build language cannot express (it can only
# flag the PRODUCED block); this does not affect produced-block parity because
# legacy leaves the produced block's broken flag False.
# ---------------------------------------------------------------------------
matcher jetton_transfer {
    produces JettonTransferBlock
    priority 90
    entry    @transfer

    op JettonTransfer @transfer
      -> op JettonInternalTransfer @internal
      -> maybe op JettonNotify @notify

    parse transfer as JettonTransfer
    parse internal  as JettonInternalTransfer
    let wallet   = lookup jetton_wallet(internal.msg.destination)
    reject when wallet == null
    let mismatch = account(wallet.owner) != account(transfer.body.destination)
    let amt      = internal.body.amount ?? transfer.body.amount
    let fwd      = internal.body.forward_ton_amount ?? transfer.body.forward_amount
    failed when transfer.failed or internal.failed
    out {
        has_internal_transfer:  true
        sender:                 account(transfer.msg.source)
        sender_wallet:          account(transfer.msg.destination)
        receiver:               account(wallet.owner) if mismatch else account(transfer.body.destination)
        receiver_wallet:        account(internal.msg.destination)
        response_address:       account(transfer.body.response) ?? addr_none()
        forward_amount:         amount(fwd)
        desired_forward_amount: amount(transfer.body.forward_amount)
        query_id:               transfer.body.query_id
        asset:                  asset(wallet.jetton)
        amount:                 amount(amt)
        desired_amount:         amount(transfer.body.amount)
        forward_payload:        b64(transfer.body.forward_payload)
        custom_payload:         b64(transfer.body.custom_payload)
        comment:                transfer.body.comment
        encrypted_comment:      transfer.body.encrypted_comment
        payload_opcode:         transfer.body.payload_sum_type
        stonfi_swap_body:       transfer.body.stonfi_swap_body
    }
}

# ---------------------------------------------------------------------------
# jetton_burn — legacy _get_jetton_burn_data.
#
# Wallet lookup (legacy asserts non-null; here `reject when` null — identical on
# every real trace). value_flow debits the owner by the burned amount
# (add_jetton with a NEGATIVE amount).
# ---------------------------------------------------------------------------
matcher jetton_burn {
    produces JettonBurnBlock
    priority 90
    entry    @burn

    op JettonBurn @burn -> maybe op JettonBurnNotification @notification

    parse burn as JettonBurn
    let wallet      = lookup jetton_wallet(burn.msg.destination)
    reject when wallet == null
    let burn_amount = burn.body.amount
    failed when burn.failed
    out {
        owner:         account(wallet.owner)
        jetton_wallet: account(burn.msg.destination)
        amount:        amount(burn_amount)
        asset:         asset(wallet.jetton)
    }
}

# ---------------------------------------------------------------------------
# jetton_mint — legacy _get_jetton_mint_data.
#
# Dual-opcode anchor. `parse mint as MinterJettonMint | JettonMint`: the minter
# body carries a trailing master_msg ref, so it parses only for the 0x15 anchor
# and falls back to JettonMint for 0x642b7d07 — matching legacy's opcode dispatch
# (to_address/ton_amount are read identically from either type).
#
# When the mint did not fail AND an internal-transfer leg is present, the mint
# resolved into a real credit: full data + failed from the internal leg.
# Otherwise it's the fallback shape (amount only for the 0x15 minter opcode)
# with failed = true.
# ---------------------------------------------------------------------------
matcher jetton_mint {
    produces JettonMintBlock
    priority 90
    entry    @mint

    (op JettonMint @mint ^ op MinterJettonMint @mint)
      -> maybe ( op JettonNotify @notify
               | op JettonInternalTransfer @internal -> maybe op JettonNotify @internal_notify )

    parse mint     as MinterJettonMint | JettonMint
    parse internal as JettonInternalTransfer
    let use_internal = not mint.failed and internal != null
    let rjwallet     = lookup jetton_wallet(internal.msg.destination)
    reject when use_internal and rjwallet == null
    failed when internal.failed if use_internal else true
    out {
        to:               account(rjwallet.owner) if use_internal else account(mint.body.to_address)
        to_jetton_wallet: account(rjwallet.address) if use_internal else null
        amount:           amount(internal.body.amount) if use_internal else (amount(mint.body.master_msg_jetton_amount) if mint.msg.opcode == 0x15 else null)
        ton_amount:       amount(mint.body.ton_amount)
        asset:            asset(rjwallet.jetton) if use_internal else asset(mint.msg.destination)
    }
}
