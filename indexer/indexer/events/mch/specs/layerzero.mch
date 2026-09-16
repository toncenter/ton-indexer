# LayerZero receive matcher with a declarative build. It uses one anchor and a
# nested children set, with no recursion.
#
# Structure:
#
#   op ChannelLzReceivePrepare @prepare          # anchor (test_self)
#     -> { maybe ton_transfer                     # excesses_transfer (optional)
#        ; op LayerzeroLzReceivePrepare
#            -> op ChannelLzReceiveLock
#            -> op LayerzeroLzReceiveExecute @execute
#                 -> { maybe op LayerZeroEventMsgBody          # even_1  (optional)
#                    ; op LayerZeroOappExecuteCallback @oapp_callback
#                        -> { maybe op LayerZeroEventMsgBody   # event_2 (optional)
#                           ; maybe ton_transfer } } }         # excess_transfer_2
#
# The source matcher reuses the label "execute_callback" on two nested,
# non-exclusive nodes: layerzero.py:363
# (LayerzeroLzReceiveExecute, 0x0c7b8418) and layerzero.py:368
# (LayerZeroOappExecuteCallback, 0xcaae25a1). Same-name captures are only legal
# across exclusive branches (R005), so the two nodes get distinct names here:
# @execute (outer) and @oapp_callback (inner). The legacy `get_labeled` returns
# the first label in traversal order, the outer @execute, so the build reads
# @execute (channel source, receiver-OApp destination) for the packet + envelope.
# @oapp_callback is bound for pattern fidelity but the legacy build never read it.
#
# Captures bind as scalars because the pattern is non-recursive. The build parses
# @execute's body (the OApp callback carries the LayerZero packet) and maps every
# LayerZeroReceiveData field straight off the two envelopes + the parsed packet.
#
# The address-form safety check compares the execute
# destination against the packet's dst_oapp with `dest[2:].lower() ==
# dst_oapp[2:].lower()`. The two carry the same 256-bit hash in different string
# forms. The envelope destination is `0:<hex>` (a TON address), while the packet
# dst_oapp is `0x<hex>` (a bare 256-bit hash). `account()` cannot bridge them:
# `account("0x<hex>")` raises AddressError (verified), so `account(a) == account(b)`
# rejects every real trace. String slicing/lowercasing is out of the expression
# language by design, so this stays a host `fn`.
# The `same_hash(addr_str, hash_str)` host function
# strips the two prefixes and compares the hex case-insensitively.

# =============================================================================
# The remaining family matchers are send, send_tokens, receive, commit_packet,
# and dvn_verify.
#
# All five live in this file. Only one pair is order-bearing:
# `layerzero_send_tokens` anchors on `btype layerzero_send`, the block
# `layerzero_send` produces, so it must stay after it. The other three carry
# pairwise-distinct anchor opcodes (0x22f3ac09 / 0x28b97077 / 0xa0d51a8d),
# claim no btype another matcher builds, and nothing consumes their products —
# so `layerzero_receive` keeping its historical first slot (rather than the
# legacy list's third) is inert. No opcode declared here appears in any other
# spec (grep over specs/), so the file's slot in the built-in source list
# carries no cross-spec tie-break. Default priority 100
# keeps send_tokens behind the priority-90 jetton family that builds the
# `jetton_transfer` parent it binds.
#
# Hand parsers. `LayerzeroChannelSendCallback`,
# `ChannelCommitPacket` and `UlnConnectionVerifyCallbackParser` are all
# kept hand-side because each emits a nested named object
# (LayerZeroPacket / LayerZeroPath / LayerZeroMDLzSend), reaches its payload
# through a raw `refs[0].refs[0].refs[0]` walk, and formats 256-bit fields as
# Python `hex()` minimal-digit strings. They join the hand table in
# src/parse/ParseLayerZero.cpp next to the receive parser and reuse its
# `lz_parse_packet` / `lz_hex_min` unchanged.
#
# `layerzero_dvn_verify.status` is one of "succeeded" /
# "nonce_out_of_range" / "dvn_not_configured" / "unknown_<decimal code>". The
# expression language has no string operations at all, so the mapping
# lives where the code is read — inside the parser, which emits both
# `status_code` (the raw uint32) and `status` (the mapped string), exactly like
# the Python class. No `produces switch`, no literal branch per code.
# =============================================================================

opcode ChannelLzReceivePrepare      = 0x22f3ac09
opcode LayerzeroLzReceivePrepare    = 0x97df404c
opcode ChannelLzReceiveLock         = 0xb7680bc6
opcode LayerzeroLzReceiveExecute    = 0x0c7b8418
opcode LayerZeroOappExecuteCallback = 0xcaae25a1
opcode LayerZeroEventMsgBody        = 0xe33b9873

opcode EndpointEndpointSend          = 0xdd4ea3b4
opcode ChannelChannelSend            = 0x536feb09
opcode MsglibConnectionSend          = 0x4002b790
opcode UlnUlnSend                    = 0x5de68393
opcode ChannelMsglibSendCallback     = 0x421c1a25
opcode LayerzeroChannelSendCallback  = 0xa2b5fbae

opcode UlnUlnCommitPacket                 = 0x28b97077
opcode UlnConnectionCommitPacket          = 0xf9d37b80
opcode EndpointEndpointCommitPacket       = 0x5dab749a
opcode ChannelCommitPacket                = 0x5388cd88
opcode MsglibConnectionCommitPacketCallback = 0x5e178f33

opcode DvnVerify                    = 0xa0d51a8d
opcode ProxyCallContract            = 0x09f047d9
opcode UlnUlnVerify                 = 0x994aaf4e
opcode UlnConnectionVerify          = 0x4ec8f80a
opcode UlnConnectionVerifyCallback  = 0x3cb38090

matcher layerzero_receive {
    produces LayerZeroReceiveBlock
    entry    @prepare

    op ChannelLzReceivePrepare @prepare
      -> { maybe btype ton_transfer
         ; op LayerzeroLzReceivePrepare
             -> op ChannelLzReceiveLock
             -> op LayerzeroLzReceiveExecute @execute
                  -> { maybe op LayerZeroEventMsgBody
                     ; op LayerZeroOappExecuteCallback @oapp_callback
                         -> { maybe op LayerZeroEventMsgBody
                            ; maybe btype ton_transfer } } }

    parse execute as LayerZeroOappExecuteCallback
    # Safety check from build_layerzero_receive_core: the execute destination must
    # be the same account as the packet's dst_oapp.
    reject when not layerzero_dst_oapp_matches(execute.msg.destination, execute.body.packet.path.dst_oapp)
    out {
        sender:  account(prepare.msg.source)
        channel: account(execute.msg.source)
        oapp:    account(execute.msg.destination)
        packet_data: {
            src_oapp: execute.body.packet.path.src_oapp
            dst_oapp: execute.body.packet.path.dst_oapp
            src_eid:  execute.body.packet.path.src_eid
            dst_eid:  execute.body.packet.path.dst_eid
            nonce:    execute.body.packet.nonce
            guid:     execute.body.packet.guid
            message:  execute.body.packet.message
        }
    }
}

# -----------------------------------------------------------------------------
# layerzero_send — blocks/layerzero.py:149 LayerZeroSendMatcher.
#
# The outbound half of a LayerZero message: the OApp calls the endpoint, the
# endpoint hands the packet down channel -> msglib connection -> ULN, and the
# ULN answers with a send callback that carries the fully-formed lzSend payload
# back up to the OApp.
#
#     0xdd4ea3b4 -> 0x536feb09 -> 0x4002b790 -> 0x5de68393 -> 0x421c1a25
#         -> { PacketSent event ; 5x maybe ton_transfer ; oapp callback -> {…} }
#
# The children set is written out with FIVE separate `maybe btype ton_transfer`
# items because the legacy matcher declares five (`excess_transfer_1..5`) — the
# ULN fans gas remainders out to the workers it paid, and the count varies per
# send (the two fixtures carry four and three). The language has no
# `zero_or_more` quantifier, so the legacy fixed-arity form is reproduced
# literally. Item order matters: the required PacketSent event goes first and
# the required oapp callback last, exactly as legacy declares them, so the
# optional slots in between can absorb whatever ton_transfers are present.
#
# msglib_send / send_callback carry legacy labels but are NOT captured here:
# the build reads neither. The legacy body computes
# `msglib = AccountId(msglib_send_block.get_message().destination)` and then
# THROWS IT AWAY — `msglib` in the produced data is `lz_send.send_msglib`, the
# 256-bit hex string from the payload, not that account. Quirk preserved; the
# 0x4002b790 node stays purely structural.
#
# Every field except the three envelope accounts comes off ONE parse of the
# oapp callback (`LayerzeroChannelSendCallback` -> `refs[0].refs[0].refs[0]` ->
# `LayerZeroMDLzSend` -> `LayerZeroPacket` -> `LayerZeroPath`). No host fn.
# -----------------------------------------------------------------------------
matcher layerzero_send {
    produces LayerZeroSendBlock
    entry    @endpoint_send

    op EndpointEndpointSend @endpoint_send
      -> op ChannelChannelSend @channel_send
        -> op MsglibConnectionSend
          -> op UlnUlnSend @uln_send
            -> op ChannelMsglibSendCallback
              -> { op LayerZeroEventMsgBody
                 ; maybe btype ton_transfer
                 ; maybe btype ton_transfer
                 ; maybe btype ton_transfer
                 ; maybe btype ton_transfer
                 ; maybe btype ton_transfer
                 ; op LayerzeroChannelSendCallback @oapp_callback
                     -> { maybe op LayerZeroEventMsgBody
                        ; maybe btype ton_transfer } }

    parse oapp_callback as LayerzeroChannelSendCallback
    out {
        initiator: account(endpoint_send.msg.source)
        packet_data: {
            src_oapp: oapp_callback.body.lz_send.packet.path.src_oapp
            dst_oapp: oapp_callback.body.lz_send.packet.path.dst_oapp
            src_eid:  oapp_callback.body.lz_send.packet.path.src_eid
            dst_eid:  oapp_callback.body.lz_send.packet.path.dst_eid
            nonce:    oapp_callback.body.lz_send.packet.nonce
            guid:     oapp_callback.body.lz_send.packet.guid
            message:  oapp_callback.body.lz_send.packet.message
        }
        send_request_id: oapp_callback.body.lz_send.send_request_id
        native_fee:      oapp_callback.body.lz_send.native_fee
        zro_fee:         oapp_callback.body.lz_send.zro_fee
        endpoint:        account(endpoint_send.msg.destination)
        channel:         account(channel_send.msg.destination)
        uln:             account(uln_send.msg.destination)
        msglib_manager:  oapp_callback.body.lz_send.send_msglib_manager
        msglib:          oapp_callback.body.lz_send.send_msglib
    }
}

# -----------------------------------------------------------------------------
# layerzero_send_tokens — blocks/layerzero.py:275 LayerZeroSendTokensMatcher.
#
# Pure composition, zero parsing: a `layerzero_send` whose PARENT is the jetton
# transfer that funded it, presented as one "sent tokens over LayerZero" action
# instead of a transfer plus an opaque send.
#
#     btype layerzero_send @send  <- btype jetton_transfer @jetton -> { OFTSent }
#
# `send.data` binds the whole record the matcher above produced and is written
# straight through — the legacy build likewise stores the SAME LayerZeroSendData
# instance rather than copying it field by field.
#
# SOURCE DOUBLE-WRITE, reproduced: `_fill_layerzero_send_tokens_action` sets
# `action.source` from the jetton sender, then calls the send fill (which
# overwrites it with the LayerZero initiator), then overwrites it AGAIN with the
# jetton sender — so the jetton sender wins. Both C++ and Python fills apply the
# writes in that order, which matters because for a token send the initiator is
# the OApp, not the user.
#
# DEVIATION (unobservable on the corpus): the legacy `child_matcher` on the
# jetton transfer consumes EVERY 0xe33b9873 child; the children block here
# consumes the first. A parent edge may only be followed by a CHILDREN BLOCK —
# the compiler silently drops a plain child edge in that position
# (compiler.py `_lower_parent_chain_from`) — so this is the only available form.
# The one fixture's jetton transfer has exactly one OFTSent event child.
# -----------------------------------------------------------------------------
matcher layerzero_send_tokens {
    produces LayerZeroSendTokensBlock
    entry    @send

    btype layerzero_send @send
      <- btype jetton_transfer @jetton
        -> { op LayerZeroEventMsgBody }

    out {
        sender:              jetton.data.sender
        oapp:                jetton.data.receiver
        sender_wallet:       jetton.data.sender_wallet
        oapp_wallet:         jetton.data.receiver_wallet
        asset:               jetton.data.asset
        amount:              jetton.data.amount
        layerzero_send_data: send.data
    }
}

# -----------------------------------------------------------------------------
# layerzero_commit_packet — blocks/layerzero.py:443 LayerZeroCommitPacketMatcher.
#
# The executor commits a verified packet: ULN -> ULN connection -> endpoint ->
# channel, and the channel logs the commit and calls the msglib connection back.
#
#     0x28b97077 -> 0xf9d37b80 -> 0x5dab749a -> 0x5388cd88
#         -> { maybe PacketVerified event ; maybe msglib callback -> maybe excess }
#
# msglib_callback is optional in the pattern and required by the build
# (`if not … or not msglib_callback_block: return []`, layerzero.py:488).
# This uses `maybe` plus `reject when … == null`, rather than a required node,
# because the two forms differ in what the
# engine does with a trace that lacks the callback. A required node fails the
# structural match, which under the clean-rejection rule is the same
# outcome — but the `maybe` + reject form also reproduces the legacy CONSUMED
# set on the paths where the callback exists and its own optional excess child
# does not. It also preserves the children_matchers consumed set.
#
# `uln` and `uln_connection` are both written from the ULN connection's
# destination (layerzero.py:506-507 assigns the same `uln_connection` local to
# both fields; the anchor's own destination, the real ULN, is never read).
# This matches LayerZeroCommitPacketMatcher.
# -----------------------------------------------------------------------------
matcher layerzero_commit_packet {
    produces LayerZeroCommitPacketBlock
    entry    @uln_commit

    op UlnUlnCommitPacket @uln_commit
      -> op UlnConnectionCommitPacket @uln_connection_commit
        -> op EndpointEndpointCommitPacket @endpoint_commit
          -> op ChannelCommitPacket @channel_commit
            -> { maybe op LayerZeroEventMsgBody
               ; maybe op MsglibConnectionCommitPacketCallback @msglib_callback
                   -> maybe btype ton_transfer }

    parse channel_commit as ChannelCommitPacket
    reject when msglib_callback == null
    out {
        sender:            account(uln_commit.msg.source)
        uln:               account(uln_connection_commit.msg.destination)
        uln_connection:    account(uln_connection_commit.msg.destination)
        endpoint:          account(endpoint_commit.msg.destination)
        channel:           account(channel_commit.msg.destination)
        msglib_connection: account(msglib_callback.msg.destination)
        packet_data: {
            src_oapp: channel_commit.body.packet.path.src_oapp
            dst_oapp: channel_commit.body.packet.path.dst_oapp
            src_eid:  channel_commit.body.packet.path.src_eid
            dst_eid:  channel_commit.body.packet.path.dst_eid
            nonce:    channel_commit.body.packet.nonce
            guid:     channel_commit.body.packet.guid
            message:  channel_commit.body.packet.message
        }
    }
}

# -----------------------------------------------------------------------------
# layerzero_dvn_verify — blocks/layerzero.py:527 LayerZeroDvnVerifyMatcher.
#
# One DVN of the OApp's security stack submits its attestation:
#
#     0xa0d51a8d -> 0x09f047d9 -> 0x994aaf4e -> 0x4ec8f80a
#         -> { maybe event ; maybe verify callback -> maybe excess }
#
# Same optional-in-pattern / required-in-build trap as commit_packet, same
# treatment (`maybe` + `reject`): layerzero.py:566 refuses to build without the
# 0x3cb38090 callback, which is where BOTH payload fields live.
#
# `nonce` and `status` come from `md::VerificationStatus` behind the callback's
# MdObj ref — a u116 header + i234 filler the parser validates before reading
# (a mismatch raises, i.e. rejects). The status string mapping is in the parser;
# see the file header.
# -----------------------------------------------------------------------------
matcher layerzero_dvn_verify {
    produces LayerZeroDvnVerifyBlock
    entry    @dvn_verify

    op DvnVerify @dvn_verify
      -> op ProxyCallContract @proxy_call
        -> op UlnUlnVerify @uln_verify
          -> op UlnConnectionVerify @uln_connection_verify
            -> { maybe op LayerZeroEventMsgBody
               ; maybe op UlnConnectionVerifyCallback @verify_callback
                   -> maybe btype ton_transfer }

    parse verify_callback as UlnConnectionVerifyCallbackParser
    reject when verify_callback == null
    out {
        sender:         account(dvn_verify.msg.source)
        nonce:          verify_callback.body.nonce
        status:         verify_callback.body.status
        dvn:            account(dvn_verify.msg.destination)
        proxy:          account(proxy_call.msg.destination)
        uln:            account(uln_verify.msg.destination)
        uln_connection: account(uln_connection_verify.msg.destination)
    }
}
