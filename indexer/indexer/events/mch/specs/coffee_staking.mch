# Coffee staking — port of the three staking matchers in blocks/staking.py,
# declared in the legacy matchers-list order (deposit 82, withdraw 83, claim
# rewards 84). All three are FULLY DECLARATIVE: no host fns, no lookups.
#
# deposit
# -------
# A jetton transfer into the staking pool; the pool answers 0xf9471134, logs a
# duplicate of it ext-out, and mints the position NFT. The anchor is a BARE
# `btype jetton_transfer` (the legacy `test_self` is a naked
# `isinstance(block, JettonTransferBlock)`) — the widest anchor in the family,
# which is why the required 0xf9471134 child does all the discrimination.
# `btype nft_mint` binds the block the priority-80 specs/nft_mint.mch builds, so
# both `minted_item_address` and `minted_item_index` come off it for free.
#
# withdraw
# --------
# THREE MESSAGE TYPES, ONE OPCODE. staking_position_withdraw_1/2/3 all ride
# 0xcb03bfaf, so the pattern is a chain of three identical `op` nodes and the
# three graph roles are told apart by NAME. Separate protocol-local ABI files
# keep the distinct w2/w3 layouts under the shared prefix. Only w2 and w3 have
# ABI rows — w1's body is never read, the legacy build takes only
# `get_message().destination` from it.
#
# The w2 children set is `[w3, maybe log]` where BOTH items share the head
# 0xcb03bfaf. It resolves because item 1 carries the required jetton_transfer
# child: the children algorithm tries candidates in canonical min_lt
# order, the ext-out log comes first and fails item 1 for want of that child,
# the real w3 matches, and the optional log then takes item 2.
#
# NAMING NOTE, reproduced not fixed: `pool` is `withdraw_2.msg.source` and
# `nft_address` is `withdraw_1.msg.destination` — the SAME account (the position
# NFT), because w1 goes user -> position and w2 goes position -> master. The
# legacy field names are misleading; the values are what they are.
#
# claim rewards
# -------------
# An admin highload wallet calls 0xb30c7310 on the pool, which pays the reward
# out as a jetton transfer and logs a duplicate of the claim ext-out.
#
# The legacy `test_self` also requires `len(block.next_blocks) > 0`. Dropped: it
# is implied by the required `btype jetton_transfer` child, so reproducing it
# would cost a host predicate for a test the pattern already makes. A block with
# no children fails the children set either way.
#
# `admin` is read but never serialized (the fill drops it — "always the same
# highload and we have no basic field for it"); it stays in the block data
# because the legacy block carries it and the parity sweep compares data
# key-for-key.
#
# ORDERING. 0xf9471134 / 0xcb03bfaf / 0xb30c7310 are claimed by no other spec.
# What IS load-bearing is the deposit's bare `btype jetton_transfer` anchor,
# shared with ethena_deposit, jvault_stake, stonfi_v1_swap, dedust_v2_swap/
# failed_swap and coffee_mev_protect_hold_funds_jetton: the legacy list puts
# CoffeeStakingDepositMatcher after every one of them, so this file is LAST in
# the built-in source list. The default priority 100 also keeps it behind the
# priority-90 jetton family (which builds the anchor) and the priority-80
# nft_mint spec (which builds the child it binds).

opcode CoffeeStakingDeposit             = 0xF9471134
opcode CoffeeStakingPositionWithdraw    = 0xCB03BFAF
opcode CoffeeStakingClaimRewards        = 0xB30C7310

matcher coffee_staking_deposit {
    produces coffee_staking_deposit
    entry    @transfer

    btype jetton_transfer @transfer
      -> op CoffeeStakingDeposit @pool_call
        -> { maybe op CoffeeStakingDeposit
           ; btype nft_mint @nft }

    out {
        source:              transfer.data.sender
        user_jetton_wallet:  transfer.data.sender_wallet
        pool_jetton_wallet:  transfer.data.receiver_wallet
        pool:                account(pool_call.msg.destination)
        value:               amount(transfer.data.amount)
        minted_item_address: nft.data.address
        minted_item_index:   nft.data.index
        asset:               transfer.data.asset
    }
}

matcher coffee_staking_withdraw {
    produces coffee_staking_withdraw
    entry    @withdraw_1

    op CoffeeStakingPositionWithdraw @withdraw_1
      -> op CoffeeStakingPositionWithdraw @withdraw_2
        -> { op CoffeeStakingPositionWithdraw @withdraw_3
               -> btype jetton_transfer @transfer
           ; maybe op CoffeeStakingPositionWithdraw }

    parse withdraw_2 as CoffeeStakingPositionWithdraw2
    parse withdraw_3 as CoffeeStakingPositionWithdraw3
    out {
        source:             account(withdraw_2.body.owner)
        pool:               account(withdraw_2.msg.source)
        asset:              transfer.data.asset
        amount:             amount(withdraw_3.body.jetton_amount ?? 0)
        user_jetton_wallet: transfer.data.receiver_wallet
        pool_jetton_wallet: account(withdraw_3.body.jetton_wallet)
        nft_address:        account(withdraw_1.msg.destination)
        nft_index:          withdraw_2.body.nft_id
        points:             withdraw_2.body.points ?? 0
    }
}

matcher coffee_staking_claim_rewards {
    produces coffee_staking_claim_rewards
    entry    @claim

    op CoffeeStakingClaimRewards @claim
      -> { btype jetton_transfer @transfer
         ; maybe op CoffeeStakingClaimRewards }

    parse claim as CoffeeStakingClaimRewards
    out {
        admin:                   account(claim.msg.source)
        recipient:               account(claim.body.receiver)
        pool:                    account(claim.msg.destination)
        asset:                   transfer.data.asset
        amount:                  amount(claim.body.jetton_amount ?? 0)
        pool_jetton_wallet:      account(claim.body.jetton_wallet)
        recipient_jetton_wallet: transfer.data.receiver_wallet
    }
}
