from __future__ import annotations

from pytoniq_core import Address, Slice, Builder


def load_address_list(slice: Slice) -> list[Address]:
    addr_dict = (
        slice.load_dict(267,
                    key_deserializer=lambda x: Builder().store_bits(x).end_cell().to_slice().load_address(),
                    value_deserializer=None) or {}
    )
    return list(addr_dict.keys())


class JVaultReceiveJettons:
    # ?? -> stake_wallet
    # receive_jettons#d68a4ac1
    #     query_id:uint64
    #     min_deposit:Coins
    #     max_deposit:Coins
    #     unstake_commission:uint16
    #     unstake_fee:Coins
    #     whitelist:(HashmapE 267 Bit) // jetton_wallet_address (MsgAddressStd -> nothing (int1))
    #     received_jettons:Coins
    # = InternalMsgBody;

    opcode = 0xD68A4AC1

    query_id: int
    min_deposit: int
    max_deposit: int
    unstake_commission: int
    unstake_fee: int
    whitelist: list[Address]
    received_jettons: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.min_deposit = slice.load_coins() or 0
        self.max_deposit = slice.load_coins() or 0
        self.unstake_commission = slice.load_uint(16)
        self.unstake_fee = slice.load_coins() or 0
        self.whitelist = load_address_list(slice)
        self.received_jettons = slice.load_coins() or 0


class JVaultUnstakeJettons:
    # user -> stake_wallet
    # unstake_jettons#0x499a9262
    #     query_id:uint64
    #     jettons_to_unstake:Coins
    #     force_unstake:Bool
    # = InternalMsgBody;
    opcode = 0x499A9262

    query_id: int
    jettons_to_unstake: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.jettons_to_unstake = slice.load_coins() or 0


class JVaultClaim:
    # user -> stake_wallet
    # claim_rewards_msg#78d9f109
    #     query_id:uint64
    #     jettons_to_claim:(HashmapE 267 Bit)  // jetton_wallet_address (MsgAddressStd -> nothing (int1))
    # = InternalMsgBody;
    opcode = 0x78D9F109

    query_id: int
    lock_period: int
    jettons_to_claim: list[Address]

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.jettons_to_claim = load_address_list(slice)


class JVaultSendClaimedRewards:
    # stake_wallet -> staking_pool
    opcode = 0x44BC1FE3


class JVaultRequestUpdateRewards:
    # stake_wallet -> staking_pool
    opcode = 0xF5C5BAA3


class JVaultUpdateRewards:
    # staking_pool -> stake_wallet
    opcode = 0xAE9307CE


class JVaultRequestUpdateReferrer:
    opcode = 0x55C35B40


class JVaultUpdateReferrer:
    opcode = 0x076EE4E0


class JVaultSetData:
    # staking_pool -> refferer_wallet
    opcode = 0x383411EA
