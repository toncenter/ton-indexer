from pytoniq_core import Slice


class ExcessMessage:
    opcode = 0xd53276db


class TonTransferMessage:
    opcode = 0
    encrypted_opcode = 0x2167da4b

    def __init__(self, boc: Slice):
        if boc.remaining_bits == 0 or boc.remaining_bits == 1:
            self.encrypted = False
            self.comment = None
            return
        op = boc.load_uint(32)  # opcode
        if op & 0xFFFFFFFF == TonTransferMessage.encrypted_opcode:
            self.encrypted = True
        else:
            self.encrypted = False
        self.comment = None
        if boc.remaining_bits >= 8 and not boc.remaining_bits % 8 and boc.remaining_refs in (0, 1):
            self.comment = boc.load_snake_bytes()
