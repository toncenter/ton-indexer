from __future__ import annotations

from pytoniq_core import Cell, Slice, Address

from indexer.events.blocks.utils.ton_utils import AccountId


class TgBTCMintEvent:
    # External-out msg
    # mint#0x77a80ef3
    #   amount:Coins
    #   recipient_address:MsgAddress
    #   bitcoin_txid:uint256
    # = MsgBody;

    amount: int
    recipient_address: AccountId
    bitcoin_txid: int

    opcode = 0x77A80EF3

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.amount = slice.load_coins() or 0
        self.recipient_address = AccountId(slice.load_address())
        self.bitcoin_txid = slice.load_uint(256)


class TgBTCBurnEvent:
    # burn#0xca444ce6
    #   amount:Coins
    #   sender_address:MsgAddress
    #   pegout_address:MsgAddress
    # = MsgBody;

    amount: int
    sender_address: AccountId
    pegout_address: AccountId

    opcode = 0xCA444CE6

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.amount = slice.load_coins() or 0
        sender = None
        pegout = None
        try:
            sender = slice.load_address()
            pegout = slice.load_address()
        except:
            pass
        self.sender_address = (
            AccountId(sender) if isinstance(sender, Address) else AccountId(None)
        )
        self.pegout_address = (
            AccountId(pegout) if isinstance(pegout, Address) else AccountId(None)
        )


class TgBTCNewKeyEvent:
    # event_id:0x27756729 amount:Coins new_internal_pubkey:uint256 pegout_address:MsgAddress

    amount: int
    new_internal_pubkey: int
    pegout_address: AccountId

    opcode = 0x27756729

    def __init__(self, slice: Slice):
        slice.load_uint(32)  # load and discard opcode
        self.amount = slice.load_coins() or 0
        self.new_internal_pubkey = slice.load_uint(256)
        self.pegout_address = AccountId(slice.load_address())


class TgBTCDkgCompletedEvent:
    # dkg_completed — event_id:0x453443a6 timestamp:u64 internal_pubkey:uint256

    timestamp: int
    internal_pubkey: int

    opcode = 0x453443A6

    def __init__(self, slice: Slice):
        slice.load_uint(32)  # load and discard opcode
        self.timestamp = slice.load_uint(64)
        self.internal_pubkey = slice.load_uint(256)
