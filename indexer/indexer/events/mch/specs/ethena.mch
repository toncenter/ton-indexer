# Ethena deposit matcher — ported from blocks/ethena_staking.py
# EthenaDepositBlockMatcher. Anchors on an already-built `jetton_transfer`
# block (first btype-anchored matcher in the spec set — the engine anchor test
# is btype-set membership) whose child is the pool's `jetton_mint`.
# The withdrawal request is the second matcher in this file, below.
#
# In the declarative build, the hardcoded USDe/tsUSDe asset gate is two
# `reject when` clauses (Asset compares case-insensitively against the master
# address literal — a TON/absent asset raises, i.e. rejects, exactly like the
# legacy None guards). Every field is read off the composed jetton_transfer /
# jetton_mint blocks' produced `data` through the `.data` accessor.

matcher ethena_deposit {
    produces EthenaDepositBlock
    entry    @transfer

    btype jetton_transfer @transfer -> btype jetton_mint @mint

    reject when transfer.data.asset != "0:086FA2A675F74347B08DD4606A549B8FDB98829CB282BC1949D3B12FBAED9DCC"
    reject when mint.data.asset     != "0:D0E545323C7ACB7102653C073377F7E3C67F122EB94D430A250739F109D4A57D"
    out {
        source:             account(transfer.data.sender)
        user_jetton_wallet: account(transfer.data.sender_wallet)
        pool:               account(transfer.data.receiver)
        value:              amount(transfer.data.amount)
        tokens_minted:      amount(mint.data.amount)
        asset:              mint.data.asset
        source_asset:       transfer.data.asset
    }
}

# -----------------------------------------------------------------------------
# ethena_withdrawal_request — blocks/ethena_staking.py:58
# EthenaWithdrawalRequestBlockMatcher. Serializes to `stake_withdrawal_request`
# with `staking_data{provider: ethena}` (an existing composite; no new column).
#
# The user sends USDe to the pool and gets back a TIMELOCKED tsUSDe position:
#
#   0x0f8a7ea5 -> 0x178d4519 -> 0x7362d09c -> 0x642b7d07 -> 0xb2583ed5
#   transfer     internal      notify        mint(tsUSDe)  timelocked transfer
#
# a `child_sequence_matcher` in the reference, i.e. a plain required chain here.
#
# WIDE ANCHOR + `priority 85`, and both are load-bearing. `op JettonTransfer`
# is the raw 0x0f8a7ea5 CALL and `op JettonMint` the raw 0x642b7d07 one, so the
# whole chain has to be claimed BEFORE the priority-90 jetton family turns those
# calls into `jetton_transfer` / `jetton_mint` blocks. The legacy list interleaves
# them — JettonTransferBlockMatcher 14, THIS 48, JettonMintBlockMatcher 49,
# EthenaDepositBlockMatcher 50 — which a single priority per spec file cannot
# reproduce, because the mch jetton family is one file at one priority. 85 puts
# this matcher ahead of all three jetton matchers instead of between two of them.
#
# That deviation is UNOBSERVABLE, and by construction rather than by luck. Guard
# 3 below requires the source wallet's jetton MASTER to equal the internal
# transfer's DESTINATION — i.e. the tsUSDe leg lands on the master, not on a
# jetton wallet. build_jetton_transfer_core rejects exactly that shape: it looks
# the destination up with get_jetton_wallet() and returns None when the address
# is not a wallet. So no trace can satisfy this matcher's guards AND be a
# transfer the jetton family would have taken; whichever runs first, the other
# declines. The corpus confirms it — the sweep runs this spec on every fixture
# carrying 0x0f8a7ea5.
#
# Within the file it is listed AFTER the deposit, the reverse of the legacy list
# (withdrawal 48, deposit 50). Unobservable too, and for the mirror-image reason:
# the deposit anchors on a `btype jetton_transfer` BLOCK, which exists only where
# the jetton family matched — exactly where this matcher's guards do not hold.
#
# 0xb2583ed5 is a timelocked-transfer opcode whose BODY is an ordinary
# JettonInternalTransfer. `parse` is registry-keyed by NAME, and the single
# logical ABI row dispatches honestly across 0x178d4519 and 0xb2583ed5
# (verified against the corpus body: amount/from/response all read correctly).
#
# THE FOUR GUARDS are the legacy build's identity checks, one `reject when` each:
# both wallets must be owned by the sender, the source wallet must be a wallet of
# the USDe master the internal transfer landed on, and the receiver wallet must be
# a wallet of the tsUSDe master that emitted the timelocked transfer. They are
# written `account(a) != account(b)` (the nft_sale idiom) rather than as raw
# string compares so the two sides normalize identically.
#
# DEVIATION (documented, not worked around): an unresolvable jetton-wallet
# interface makes the legacy build raise AttributeError out of build_block, which
# drops the WHOLE trace to basic actions; the two `== null` rejects here decline
# only this match and let the rest of the trace classify. Same treatment as
# specs/dedust_withdraw.mch and the tonco withdraw host fn.
# -----------------------------------------------------------------------------
opcode JettonTransfer          = 0x0f8a7ea5
opcode JettonInternalTransfer  = 0x178d4519
opcode JettonNotify            = 0x7362d09c
opcode JettonMint              = 0x642b7d07
opcode EthenaTimelockTransfer  = 0xb2583ed5

matcher ethena_withdrawal_request {
    produces ethena_withdrawal_request
    entry    @transfer
    priority 85

    op JettonTransfer @transfer
      -> op JettonInternalTransfer @internal_transfer
        -> op JettonNotify
          -> op JettonMint @mint
            -> op EthenaTimelockTransfer @ts_usde_transfer

    parse internal_transfer as JettonInternalTransfer
    parse ts_usde_transfer  as JettonInternalTransfer
    let src_wallet_info = lookup jetton_wallet(transfer.msg.destination)
    let dst_wallet_info = lookup jetton_wallet(ts_usde_transfer.msg.destination)
    reject when src_wallet_info == null
    reject when dst_wallet_info == null
    reject when account(src_wallet_info.owner)  != account(transfer.msg.source)
    reject when account(dst_wallet_info.owner)  != account(transfer.msg.source)
    reject when account(src_wallet_info.jetton) != account(internal_transfer.msg.destination)
    reject when account(dst_wallet_info.jetton) != account(ts_usde_transfer.msg.source)
    out {
        source:          account(transfer.msg.source)
        source_wallet:   account(transfer.msg.destination)
        receiver_wallet: account(ts_usde_transfer.msg.destination)
        pool:            account(mint.msg.source)
        asset:           account(src_wallet_info.jetton)
        amount:          amount(internal_transfer.body.amount)
        ts_usde_amount:  amount(ts_usde_transfer.body.amount)
    }
}
