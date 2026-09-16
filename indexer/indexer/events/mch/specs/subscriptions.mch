# Wallet-plugin subscriptions, between elections and auction bids in the legacy list.
# The four opcodes are disjoint from every other production spec, and neither
# matcher binds a produced block, so the manifest slot carries no tie-break.

opcode Request              = 0x706C7567
opcode Response             = 0xF06C7567
opcode Payment              = 0x73756273
opcode WalletPluginDestruct = 0x64737472

matcher subscribe {
    produces subscribe
    entry    @resp

    op Response @resp -> { op Payment @pay } <- maybe op Request @req

    parse req as SubscriptionPaymentRequest
    failed when req != null and req.failed
    out {
        subscriber:   account(resp.msg.source)
        subscription: account(resp.msg.destination)
        beneficiary:  account(pay.msg.destination)
        amount:       amount(req.body.grams) if req != null else amount(resp.msg.value)
    }
}

matcher unsubscribe {
    produces unsubscribe
    entry    @destruct

    op WalletPluginDestruct @destruct -> maybe op WalletPluginDestruct @response

    out {
        subscriber:   account(destruct.msg.source)
        subscription: account(destruct.msg.destination)
        beneficiary:  account(response.msg.destination) if response != null else null
    }
}
