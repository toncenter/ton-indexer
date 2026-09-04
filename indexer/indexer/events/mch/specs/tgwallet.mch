# Telegram wallet (https://github.com/ton-blockchain/tg-wallet-contract) key
# rotation. The request comes either as an external (0xFBBA99C8) or, when a
# relayer pays the gas, as an internal message (0xFBBA99C7). Both bodies start
# with a 512-bit signature; the engine already puts the real request opcode on
# the call_contract leaf, so the anchor tests it directly. Externals have no
# source, so `source` stays null there. The relayed form also carries the
# engine-made gasless_request marker; change_wallet_key is a v1 op, so the
# marker is not expanded as a child row and is unwound onto the spine instead.
#
# Listed last in the source list: it only claims a change-key call that no
# composite matcher wanted.

opcode TgWalletChangePublicKeyInternal = 0xFBBA99C7
opcode TgWalletChangePublicKeyExternal = 0xFBBA99C8

matcher change_wallet_key {
    produces change_wallet_key
    entry    @req

    (op TgWalletChangePublicKeyInternal @req | op TgWalletChangePublicKeyExternal @req)

    # A valueless call_contract leaf clears its own `failed` flag (log/event
    # leaves), and wallet requests are valueless, so read the transaction's
    # status directly: an aborted key rotation must not look successful.
    # Deliberate departure from the python matcher, which copies the leaf flag.
    failed when (req.msg.transaction.aborted ?? false)

    out {
        source:      account(req.msg.source)
        destination: account(req.msg.destination)
        value:       amount(req.msg.value)
    }
}
