from __future__ import annotations

import hashlib

from pytoniq_core import Address, Cell, StateInit


def is_state_init_address(address: Address, code_hash: bytes, code_depth: int, data: Cell) -> bool:
    """The address is the hash of a StateInit with only code and data, so it proves both."""
    state_init = (bytes([2, 1, 0b00110100]) + code_depth.to_bytes(2, 'big') + data.get_depth().to_bytes(2, 'big')
                  + code_hash + data.hash)
    return hashlib.sha256(state_init).digest() == address.hash_part


def state_init_data(address: str, state_init_boc: str) -> Cell | None:
    try:
        state_init = Cell.one_from_boc(state_init_boc)
        if state_init.hash != Address(address).hash_part:
            return None
        return StateInit.deserialize(state_init.begin_parse()).data
    except Exception:
        return None
