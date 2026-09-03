# Telegram-wallet signing-key rotation. The real request opcode is recovered
# from behind the 512-bit signature while constructing the call_contract leaf.
# This matcher stays last in the production manifest, mirroring
# ChangeWalletKeyMatcher's position in event_processing.matchers: every more
# specific composite matcher gets the first chance to consume the call.

opcode TgWalletChangePublicKeyInternal = 0xFBBA99C7
opcode TgWalletChangePublicKeyExternal = 0xFBBA99C8

matcher change_wallet_key {
    produces change_wallet_key
    entry    @request

    (op TgWalletChangePublicKeyInternal @request
     ^ op TgWalletChangePublicKeyExternal @request)

    # A valueless call_contract deliberately clears its block-level `failed`
    # flag. External wallet requests are valueless, so use the transaction's
    # unmodified status or an aborted key rotation would look successful.
    failed when (request.msg.transaction.aborted ?? false)
    out {
        source:      account(request.msg.source)
        destination: account(request.msg.destination)
        value:       amount(request.msg.value)
    }
}
