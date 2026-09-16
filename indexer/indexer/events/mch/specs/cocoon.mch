# `msg.opcode` is signed; use the masked `block.data.opcode` for u32 comparisons.
# Missing addresses are addr_none rather than null. worker_payout and grant_refund
# take `failed` from the payout transaction; the other matchers use the anchor.
# cocoon_expected_address preserves external-address formatting. Nested
# nullable parses recover the withdraw amount from the optional refund payload.

opcode CocoonPayoutPayload              = 0xA040AD28
opcode CocoonLastPayoutPayload          = 0xF5F26A36
opcode CocoonWorkerProxyRequest         = 0x4D725D2C
opcode CocoonPayout                     = 0xC59A7CD3
opcode CocoonReturnExcessesBack         = 0x2565934C
opcode CocoonExtProxyPayoutRequest      = 0x7610E6EB
opcode CocoonChargePayload              = 0xBB63FF93
opcode CocoonClientProxyRequest         = 0x65448FF4
opcode CocoonExtClientTopUp             = 0xF172E6C2
opcode CocoonRegisterProxy              = 0x927C7CB5
opcode CocoonUnregisterProxy            = 0x6D49EAF2
opcode CocoonOwnerClientRegister        = 0xC45F9F3B
opcode CocoonOwnerClientChangeSecretHash = 0xA9357034
opcode CocoonOwnerWalletSendMessage     = 0x9C69F376
opcode CocoonOwnerClientRequestRefund   = 0xFAFA6CC1
opcode CocoonGrantRefundPayload         = 0xEFD711E1
opcode CocoonOwnerClientIncreaseStake   = 0x6A1F6A60
opcode CocoonOwnerClientWithdraw        = 0xDA068E78

# The two anchor payloads have identical layouts; only payout_type differs.
matcher cocoon_worker_payout {
    produces cocoon_worker_payout
    entry    @start

    ( op CocoonPayoutPayload @start ^ op CocoonLastPayoutPayload @start )
      -> op CocoonWorkerProxyRequest @wreq
         -> { op CocoonPayout @payout
            ; maybe op CocoonReturnExcessesBack @excesses }

    parse start as CocoonLastPayoutPayload | CocoonPayoutPayload
    parse wreq  as CocoonWorkerProxyRequest
    failed when (payout.msg.transaction.aborted ?? false)
    out {
        proxy_contract:   account(start.msg.source) ?? addr_none()
        worker_contract:  account(start.msg.destination) ?? addr_none()
        worker_owner:     account(wreq.body.owner_address) ?? addr_none()
        payout_type:      "last" if start.data.opcode == 0xF5F26A36 else "regular"
        query_id:         start.body.query_id
        new_tokens:       start.body.new_tokens
        expected_address: cocoon_expected_address(start.body.expected_my_address)
        payout_amount:    amount(payout.msg.value)
        worker_state:     wreq.body.state
        worker_tokens:    wreq.body.tokens
    }
}

matcher cocoon_proxy_payout {
    produces cocoon_proxy_payout
    entry    @req

    op CocoonExtProxyPayoutRequest @req
      -> { op CocoonPayout @payout
         ; op CocoonReturnExcessesBack @excesses }

    parse req as CocoonExtProxyPayoutRequest
    failed when (payout.msg.transaction.aborted ?? false)
    out {
        proxy_contract:     account(req.msg.destination) ?? addr_none()
        proxy_owner:        account(payout.msg.destination) ?? addr_none()
        excesses_recipient: account(req.body.send_excesses_to) ?? addr_none()
        query_id:           req.body.query_id
    }
}

# The usually-aborted ClientProxyRequest child does not determine `failed`.
matcher cocoon_proxy_charge {
    produces cocoon_proxy_charge
    entry    @charge

    op CocoonChargePayload @charge
      -> { op CocoonReturnExcessesBack @excesses
         ; op CocoonClientProxyRequest @client_request }

    parse charge as CocoonChargePayload
    failed when (charge.msg.transaction.aborted ?? false)
    out {
        proxy_contract:   account(charge.msg.destination) ?? addr_none()
        client_contract:  account(client_request.msg.destination) ?? addr_none()
        query_id:         charge.body.query_id
        new_tokens_used:  charge.body.new_tokens_used
        expected_address: cocoon_expected_address(charge.body.expected_my_address)
    }
}

matcher cocoon_client_top_up {
    produces cocoon_client_top_up
    entry    @topup

    op CocoonExtClientTopUp @topup
      -> op CocoonClientProxyRequest @preq
         -> { op CocoonReturnExcessesBack @excesses }

    parse topup as CocoonExtClientTopUp
    failed when (topup.msg.transaction.aborted ?? false)
    out {
        client_contract: account(topup.msg.destination) ?? addr_none()
        proxy_contract:  account(preq.msg.destination) ?? addr_none()
        sender:          account(topup.body.send_excesses_to) ?? addr_none()
        query_id:        topup.body.query_id
        top_up_amount:   amount(topup.body.top_up_amount)
    }
}

matcher cocoon_register_proxy {
    produces cocoon_register_proxy
    entry    @reg

    op CocoonRegisterProxy @reg -> op CocoonReturnExcessesBack @excesses

    parse reg as CocoonRegisterProxy
    failed when (reg.msg.transaction.aborted ?? false)
    out {
        root_contract: account(reg.msg.destination) ?? addr_none()
        query_id:      reg.body.query_id
    }
}

matcher cocoon_unregister_proxy {
    produces cocoon_unregister_proxy
    entry    @unreg

    op CocoonUnregisterProxy @unreg -> op CocoonReturnExcessesBack @excesses

    parse unreg as CocoonUnregisterProxy
    failed when (unreg.msg.transaction.aborted ?? false)
    out {
        root_contract: account(unreg.msg.destination) ?? addr_none()
        query_id:      unreg.body.query_id
        seqno:         unreg.body.seqno
    }
}

matcher cocoon_client_register {
    produces cocoon_client_register
    entry    @reg

    op CocoonOwnerClientRegister @reg
      -> { op CocoonReturnExcessesBack @excesses
         ; op CocoonClientProxyRequest @client_request }

    parse reg as CocoonOwnerClientRegister
    failed when (reg.msg.transaction.aborted ?? false)
    out {
        client_contract: account(reg.msg.destination) ?? addr_none()
        owner:           account(reg.body.send_excesses_to) ?? addr_none()
        query_id:        reg.body.query_id
        nonce:           reg.body.nonce
    }
}

# new_secret_hash stays an integer; the serializer renders it as bare hex.
matcher cocoon_client_change_secret_hash {
    produces cocoon_client_change_secret_hash
    entry    @chg

    op CocoonOwnerClientChangeSecretHash @chg
      -> { op CocoonReturnExcessesBack @excesses
         ; op CocoonClientProxyRequest @client_request }

    parse chg as CocoonOwnerClientChangeSecretHash
    failed when (chg.msg.transaction.aborted ?? false)
    out {
        client_contract: account(chg.msg.destination) ?? addr_none()
        owner:           account(chg.body.send_excesses_to) ?? addr_none()
        query_id:        chg.body.query_id
        new_secret_hash: chg.body.new_secret_hash
    }
}

# The OwnerWalletSendMessage anchor arm is structurally dead: its only child is
# the refund call, so it cannot satisfy the required children set below.
# Keep the children block before the parent edge so it attaches to the anchor.
matcher cocoon_client_request_refund {
    produces cocoon_client_request_refund
    entry    @refund

    ( op CocoonOwnerWalletSendMessage @refund
    ^ op CocoonOwnerClientRequestRefund @refund )
      -> { op CocoonReturnExcessesBack @excesses
         ; op CocoonClientProxyRequest @client_request }
      <- maybe op CocoonOwnerWalletSendMessage @wallet

    parse refund as CocoonOwnerClientRequestRefund
    failed when (refund.msg.transaction.aborted ?? false)
    out {
        client_contract: account(refund.msg.destination) ?? addr_none()
        owner:           account(refund.body.send_excesses_to) ?? addr_none()
        query_id:        refund.body.query_id
        via_wallet:      wallet != null
    }
}

matcher cocoon_grant_refund {
    produces cocoon_grant_refund
    entry    @grant

    op CocoonGrantRefundPayload @grant
      -> op CocoonClientProxyRequest @client_request
         -> { op CocoonPayout @payout
            ; op CocoonReturnExcessesBack @excesses }

    parse grant as CocoonGrantRefundPayload
    failed when (payout.msg.transaction.aborted ?? false)
    out {
        proxy_contract:   account(grant.msg.destination) ?? addr_none()
        client_contract:  account(client_request.msg.destination) ?? addr_none()
        refund_recipient: account(payout.msg.destination) ?? addr_none()
        query_id:         grant.body.query_id
        new_tokens_used:  grant.body.new_tokens_used
        expected_address: cocoon_expected_address(grant.body.expected_my_address)
        payout_amount:    amount(payout.msg.value)
    }
}

matcher cocoon_client_increase_stake {
    produces cocoon_client_increase_stake
    entry    @inc

    op CocoonOwnerClientIncreaseStake @inc
      -> { op CocoonReturnExcessesBack @excesses
         ; op CocoonClientProxyRequest @client_request }

    parse inc as CocoonOwnerClientIncreaseStake
    failed when (inc.msg.transaction.aborted ?? false)
    out {
        owner:           account(inc.body.send_excesses_to) ?? addr_none()
        client_contract: account(inc.msg.destination) ?? addr_none()
        query_id:        inc.body.query_id
        new_stake:       amount(inc.body.new_stake)
    }
}

# Only excesses is optional; payout remains required. Nullable nested parses
# keep a malformed or absent refund payload non-fatal.
matcher cocoon_client_withdraw {
    produces cocoon_client_withdraw
    entry    @wd

    op CocoonOwnerClientWithdraw @wd
      -> op CocoonClientProxyRequest @client_request
         -> { maybe op CocoonReturnExcessesBack @excesses
            ; op CocoonPayout @payout }

    parse wd             as CocoonOwnerClientWithdraw
    parse client_request as CocoonClientProxyRequest
    # A refund grant rides inside the proxy request; anything else leaves the amount at 0.
    let granted = try parse client_request.body.payload as CocoonClientProxyRefundGranted
    failed when (wd.msg.transaction.aborted ?? false)
    out {
        owner:           account(wd.body.send_excesses_to) ?? addr_none()
        client_contract: account(wd.msg.destination) ?? addr_none()
        query_id:        wd.body.query_id
        withdraw_amount: amount(granted.coins ?? 0)
    }
}
