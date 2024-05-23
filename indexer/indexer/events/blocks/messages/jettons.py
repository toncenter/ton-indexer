from pytoniq_core import Slice, Address


class JettonTransfer:
    opcode = 0x0f8a7ea5

    def __init__(self, boc: Slice):
        boc.load_uint(32)  # opcode
        self.queryId = boc.load_uint(64)
        self.amount = boc.load_coins()
        self.destination = boc.load_address()
        self.response = boc.load_address()
        self.custom_payload = boc.load_maybe_ref()
        self.forward_amount = boc.load_coins()
        self.forward_payload = boc.load_maybe_ref()


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


class JettonNotify:
    opcode = 0x7362d09c
