from __future__ import annotations

from pytoniq_core import Address, Cell, ExternalAddress, Slice, InternalMsgInfo, MessageAny

# TODO: (when interfaces done)
# - Vesting Contract Deployment Matcher
# - Vesting Top-Up Matcher


class VestingSendMessage:
    # owner -> vesting
    opcode = 0xA7733ACD
    response_opcode = 0xF7733ACD

    query_id: int
    send_mode: int
    message_cell: Cell
    message_destination: Address
    message_value: int
    message_body_hash: bytes | None

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.send_mode = slice.load_uint(8)
        self.message_cell = slice.load_ref()
        try:
            msg = MessageAny.deserialize(self.message_cell.begin_parse())
            self.message_destination = msg.info.dest
            self.message_value = msg.info.value.grams
            self.message_body_hash = msg.body.hash
        except Exception:
            # Fallback for messages MessageAny cannot fully parse: at least the
            # int_msg_info header (destination/value) is required by the contract.
            msg_info = InternalMsgInfo.deserialize(self.message_cell.begin_parse())
            self.message_destination = msg_info.dest
            self.message_value = msg_info.value.grams
            self.message_body_hash = None


class VestingAddWhiteList:
    # vesting_creator -> vesting
    opcode = 0x7258A69B
    response_opcode = 0xF258A69B

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.addresses: list[Address | ExternalAddress | None] = []
        current_slice = slice
        while current_slice.remaining_refs > 0:
            self.addresses.append(current_slice.load_address())
            current_slice = current_slice.load_ref().begin_parse()
        self.addresses.append(current_slice.load_address())
