from __future__ import annotations

import base64

from pytoniq_core import Slice, WalletMessage, CurrencyCollection, Cell, StateInit, Address
from pytoniq_core.tlb.transaction import CommonMsgInfo, TransactionError


class InternalMsgInfo(CommonMsgInfo):
    """
    int_msg_info$0 ihr_disabled:Bool bounce:Bool bounced:Bool
    src:MsgAddressInt dest:MsgAddressInt
    value:CurrencyCollection ihr_fee:Grams fwd_fee:Grams
    created_lt:uint64 created_at:uint32 = CommonMsgInfo;
    """
    def __init__(self, ihr_disabled: bool, bounce: bool, bounced: bool, src: Address, dest: Address,
                 value: CurrencyCollection, ihr_fee: int, fwd_fee: int, created_lt: int, created_at: int):
        super().__init__()
        self.ihr_disabled = ihr_disabled
        self.bounce = bounce
        self.bounced = bounced
        self.src = src
        self.dest = dest
        self.value = value
        self.value_coins: int = value.grams
        self.ihr_fee = ihr_fee
        self.fwd_fee = fwd_fee
        self.created_lt = created_lt
        self.created_at = created_at

    @classmethod
    def deserialize(cls, cell_slice: Slice, with_extra_currencies: bool = False):
        tag = cell_slice.load_bit()
        if tag:
            raise TransactionError(f'InternalMsgInfo deserialization error unknown prefix tag: {tag}')
        if with_extra_currencies:
            return cls(
                ihr_disabled=cell_slice.load_bool(),
                bounce=cell_slice.load_bool(),
                bounced=cell_slice.load_bool(),
                src=cell_slice.load_address(),
                dest=cell_slice.load_address(),
                value=CurrencyCollection.deserialize(cell_slice),
                ihr_fee=cell_slice.load_coins(),
                fwd_fee=cell_slice.load_coins(),
                created_lt=cell_slice.load_uint(64),
                created_at=cell_slice.load_uint(32)
            )
        else:
            return cls(
                ihr_disabled=cell_slice.load_bool(),
                bounce=cell_slice.load_bool(),
                bounced=cell_slice.load_bool(),
                src=cell_slice.load_address(),
                dest=cell_slice.load_address(),
                value=CurrencyCollection(cell_slice.load_coins(), None),
                ihr_fee=cell_slice.load_coins(),
                fwd_fee=cell_slice.load_coins(),
                created_lt=cell_slice.load_uint(64),
                created_at=cell_slice.load_uint(32)
            )

class PayloadMessage:
    def __init__(self, cell: Cell):
        cell_slice = cell.to_slice()
        cp = cell_slice.copy()
        self.info = None
        try:
            tag = cell_slice.preload_bit()
            if not tag:  # 0
                self.info=InternalMsgInfo.deserialize(cell_slice, True)
        except Exception as e:
            cell_slice = cp
            tag = cell_slice.preload_bit()
            if not tag:  # 0
                self.info = InternalMsgInfo.deserialize(cell_slice, False)
        if self.info is None:
            return
        maybe = cell_slice.load_bit()
        init = None
        if maybe:
            either = cell_slice.load_bit()
            if either:  # right
                init = StateInit.deserialize(cell_slice.load_ref().begin_parse())
            else:  # left
                init = StateInit.deserialize(cell_slice)
        either = cell_slice.load_bit()
        if either:  # right
            body = cell_slice.load_ref()
        else:  # left
            body = cell_slice.to_cell()

        self.body = body
        self.opcode = None
        try:
            s = body.to_slice()
            opcode = s.load_uint(32)
            self.opcode = opcode
        except Exception as e:
            pass
        self.init = init
        self.hash = base64.b64encode(cell.hash).decode()

class WalletV3ExternalMessage:
    def __init__(self, slice: Slice):
        self.signature = slice.load_bits(512)
        self.subwallet_id = slice.load_uint(32)
        self.valid_until = slice.load_uint(32)
        self.seqno = slice.load_uint(32)
        self.payload = []
        while slice.remaining_refs > 0:
            self.payload.append(PayloadMessage(slice.load_ref()))

class WalletV4ExternalMessage:
    def __init__(self, slice: Slice):
        self.signature = slice.load_bits(512)
        self.subwallet_id = slice.load_uint(32)
        self.valid_until = slice.load_uint(32)
        self.seqno = slice.load_uint(32)
        self.op = slice.load_uint(8)
        self.payload = []
        while slice.remaining_refs > 0:
            self.payload.append(PayloadMessage(slice.load_ref()))

class WalletV5R1ExternalMessage:
    def __init__(self, slice: Slice):
        self.opcode = slice.load_uint(32)
        self.wallet_id = slice.load_uint(32)
        self.valid_until = slice.load_uint(32)
        self.seqno = slice.load_uint(32)
        self.payload = []
        current_ref = slice.load_maybe_ref()
        while current_ref:
            s = current_ref.to_slice()
            if s.remaining_bits == 0:
                break
            current_ref = s.load_ref()
            self.payload.append(PayloadMessage(s.load_ref()))

# Telegram wallet (https://github.com/ton-blockchain/tg-wallet-contract) request opcodes.
# Requests arrive either as externals (the "E" ones) or, when someone else pays for the gas,
# as internal messages (the "I" ones).
TG_WALLET_SEND_ONE_MESSAGE_INTERNAL = 0x63896E74
TG_WALLET_SEND_ONE_MESSAGE_EXTERNAL = 0x63896E75
TG_WALLET_SEND_BULK_MESSAGES_INTERNAL = 0x73896E74
TG_WALLET_SEND_BULK_MESSAGES_EXTERNAL = 0x73896E75
TG_WALLET_CHANGE_PUBLIC_KEY_INTERNAL = 0xFBBA99C7
TG_WALLET_CHANGE_PUBLIC_KEY_EXTERNAL = 0xFBBA99C8

TG_WALLET_REQUEST_OPCODES = frozenset({
    TG_WALLET_SEND_ONE_MESSAGE_INTERNAL,
    TG_WALLET_SEND_ONE_MESSAGE_EXTERNAL,
    TG_WALLET_SEND_BULK_MESSAGES_INTERNAL,
    TG_WALLET_SEND_BULK_MESSAGES_EXTERNAL,
    TG_WALLET_CHANGE_PUBLIC_KEY_INTERNAL,
    TG_WALLET_CHANGE_PUBLIC_KEY_EXTERNAL,
})

# signature(512) + opcode(32) + SeqnoHeader(96) = 640 bits, plus the boc header
_TG_WALLET_MIN_REQUEST_LEN = 80


def get_tg_wallet_request_opcode(body: bytes | None) -> int | None:
    """
    A request to a Telegram wallet starts with a 512 bit signature, so the first 32 bits of its
    body - the ones stored as the message opcode - are just a piece of that signature. Return the
    real opcode that follows the signature, or None if the body is not a tg-wallet request.
    """
    if body is None or len(body) < _TG_WALLET_MIN_REQUEST_LEN:
        return None
    try:
        slice = Slice.one_from_boc(body)
        slice.skip_bits(512)
        opcode = slice.load_uint(32)
    except Exception:
        return None
    return opcode if opcode in TG_WALLET_REQUEST_OPCODES else None


class WalletTgExternalMessage:
    """
    Telegram wallet (https://github.com/ton-blockchain/tg-wallet-contract).

    The body is a SignedRequest: the signature goes first (unlike wallet v5, where it is at the
    end), then the request itself. Messages come either inline (SendOneMessageRequestE) or as a
    tolk-encoded array<MessageToSend> (SendBulkMessagesRequestE): uint8 len + maybe ref to the
    first chunk, every chunk being a maybe ref to the next one followed by its items
    (sendMode:uint8 + ^messageCell).
    """
    SEND_ONE_MESSAGE = TG_WALLET_SEND_ONE_MESSAGE_EXTERNAL
    SEND_BULK_MESSAGES = TG_WALLET_SEND_BULK_MESSAGES_EXTERNAL
    CHANGE_PUBLIC_KEY = TG_WALLET_CHANGE_PUBLIC_KEY_EXTERNAL

    def __init__(self, slice: Slice):
        self.signature = slice.load_bits(512)
        self.opcode = slice.load_uint(32)
        if self.opcode not in (self.SEND_ONE_MESSAGE, self.SEND_BULK_MESSAGES, self.CHANGE_PUBLIC_KEY):
            raise ValueError(f'unknown tg-wallet request: {self.opcode:#010x}')
        self.subwallet_id = slice.load_uint(32)
        self.valid_until = slice.load_uint(32)
        self.seqno = slice.load_uint(32)
        self.payload = []
        if self.opcode == self.SEND_ONE_MESSAGE:
            slice.load_uint(8)  # send mode
            self.payload.append(PayloadMessage(slice.load_ref()))
        elif self.opcode == self.SEND_BULK_MESSAGES:
            slice.load_uint(8)  # number of messages, checked by the contract itself
            chunk = slice.load_maybe_ref()
            while chunk is not None:
                s = chunk.to_slice()
                chunk = s.load_maybe_ref()
                for _ in range(s.remaining_refs):
                    s.load_uint(8)  # send mode
                    self.payload.append(PayloadMessage(s.load_ref()))

def extract_payload_from_wallet_message(body: bytes) -> tuple[list[PayloadMessage], str|None]:
    # the v3/v4 parsers accept almost any body, so the ones checking an opcode go first
    wallets = [WalletTgExternalMessage, WalletV3ExternalMessage, WalletV4ExternalMessage, WalletV5R1ExternalMessage]
    wallet_types = ["tg-wallet", "v3", "v4", "v5r1"]
    external_message = None
    wallet_type = None
    
    for i, wallet_class in enumerate(wallets):
        try:
            slice = Slice.one_from_boc(body)
            external_message = wallet_class(slice)
            wallet_type = wallet_types[i]
            break
        except Exception:
            pass
            
    if external_message is None:
        return [], None
        
    return external_message.payload, wallet_type