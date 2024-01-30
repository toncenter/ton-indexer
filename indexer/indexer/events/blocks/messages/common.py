from pytoniq_core import Slice


class ExcessMessage:
    opcode = 0xd53276db


class TonTransferMessage:
    opcode = 0

    def __init__(self, boc: Slice):
        boc.load_uint(32)  # opcode
        self.message = None
        if boc.remaining_bits >= 8:
            self.message = boc.load_snake_bytes()
