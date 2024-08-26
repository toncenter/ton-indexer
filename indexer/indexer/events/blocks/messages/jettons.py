from __future__ import annotations

from pytoniq_core import Slice, Address, Cell


class JettonTransfer:
    opcode = 0x0f8a7ea5
    query_id: int
    amount: int
    destination: Address
    response: Address
    custom_payload: bytes | None
    forward_amount: int
    comment: str | None
    encrypted_comment: bool
    forward_payload: bytes | None
    stonfi_swap_body: dict | None

    def __init__(self, boc: Slice):
        boc.load_uint(32)  # opcode
        self.query_id = boc.load_uint(64)
        self.amount = boc.load_coins()
        self.destination = boc.load_address()
        self.response = boc.load_address()
        custom_payload = boc.load_maybe_ref()
        if custom_payload:
            self.custom_payload = custom_payload.to_boc(hash_crc32=True)
        else:
            self.custom_payload = None
        self.forward_amount = boc.load_coins()
        self.comment = None
        self.encrypted_comment = False
        self.payload_sum_type = None
        self.stonfi_swap_body = None
        payload_slice = boc.load_ref().to_slice() if boc.load_bool() else boc.copy()
        self._load_forward_payload(payload_slice)

    def _load_forward_payload(self, payload_slice: Slice):
        if payload_slice.remaining_bits == 0:
            self.forward_payload = None
            return
        else:
            self.forward_payload = payload_slice.to_cell().to_boc(hash_crc32=True)
        if payload_slice.remaining_bits < 32:
            self.sum_type = "Unknown"
            return
        sum_type = payload_slice.load_uint(32)
        self.payload_sum_type = hex(sum_type)
        # noinspection PyBroadException
        try:
            if sum_type == 0:
                self.sum_type = "TextComment"
                self.comment = payload_slice.load_str()
            elif sum_type == 0x2167da4b:
                self.sum_type = "EncryptedTextComment"
                self.comment = payload_slice.load_str()
                self.encrypted_comment = True
            elif sum_type == 0x25938561:
                self.stonfi_swap_body = {
                    'jetton_wallet': payload_slice.load_address(),
                    'min_amount': payload_slice.load_coins(),
                    'user_address': payload_slice.load_address()
                }
            else:
                self.sum_type = "Unknown"
        except Exception:
            self.sum_type = "Unknown"


class JettonBurn:
    opcode = 0x595f07bc

    query_id: int
    amount: int
    response_destination: Address

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.amount = slice.load_coins()
        self.response_destination = slice.load_address()


class JettonBurnNotification:
    opcode = 0x7bdd97de


class JettonInternalTransfer:
    opcode = 0x178d4519

    query_id: int
    amount: int
    from_address: Address
    response_address: Address
    forward_ton_amount: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.amount = slice.load_uint(16)
        self.from_address = slice.load_address()
        self.response_address = slice.load_address()
        self.forward_ton_amount = slice.load_uint(16)


class JettonNotify:
    opcode = 0x7362d09c
