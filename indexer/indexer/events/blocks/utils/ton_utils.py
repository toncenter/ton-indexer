from __future__ import annotations

from decimal import Decimal

import psycopg2
from pytoniq_core import Address, ExternalAddress

from indexer.core.database import Transaction


class Asset:
    is_ton: bool
    is_jetton: bool
    jetton_address: AccountId | None

    def __init__(self, is_ton: bool, jetton_address: Address | AccountId | str | None = None):
        self.is_ton = is_ton
        self.is_jetton = jetton_address is not None
        if isinstance(jetton_address, str):
            self.jetton_address = AccountId(jetton_address)
        elif isinstance(jetton_address, Address):
            self.jetton_address = AccountId(jetton_address)
        else:
            self.jetton_address = jetton_address

    def to_json(self):
        if self.is_ton:
            return "TON"
        else:
            return self.jetton_address.to_json()

    def __repr__(self):
        return self.to_json()

    def __eq__(self, other):
        if isinstance(other, Asset):
            return self.is_ton == other.is_ton and self.jetton_address == other.jetton_address
        elif isinstance(other, str):
            return self.jetton_address.as_str().lower() == other.lower() or (self.is_ton and other.lower() == "ton")
        elif other is None:
            return self.is_ton == True and self.jetton_address is None


def is_failed(tx: Transaction):
    description = tx.description
    if "compute_ph" in description:
        compute_type = description["compute_ph"]["type"]
        if compute_type == "skipped":
            return False
        elif compute_type == "vm":
            return description["compute_ph"]["exit_code"] != 0


class AccountId:
    def __init__(self, address: str | Address | ExternalAddress | None):
        if address is None:
            self.address = None
        elif isinstance(address, ExternalAddress):
            raise ValueError("ExternalAddress is not supported")
        elif isinstance(address, str):
            if address == "addr_none":
                self.address = None
            else:
                self.address = Address(address)
        elif isinstance(address, AccountId):
            self.address = address.address
        else:
            self.address = address

    def __repr__(self):
        return self.address.to_str(False) if self.address else "addr_none"

    def __eq__(self, other):
        if isinstance(other, AccountId):
            return self.as_str() == other.as_str()
        elif isinstance(other, str):
            try:
                return self.as_str() == AccountId(other).as_str()
            except:
                return False
        return self.address == other.address

    def __hash__(self):
        return hash(self.as_bytes())

    def as_bytes(self):
        if self.address is None:
            return None
        return self.address.wc.to_bytes(32, byteorder="big", signed=True) + self.address.hash_part

    def as_str(self):
        if self.address is None:
            return None
        return self.address.to_str(False).upper()

    def to_json(self):
        return self.as_str()


class Amount:
    value: int

    def __init__(self, value: int | Amount):
        if isinstance(value, Amount):
            self.value = value.value
        else:
            self.value = value

    def __repr__(self):
        return str(self.value)

    def to_json(self):
        return self.value

    def __eq__(self, other):
        if isinstance(other, Amount):
            return self.value == other.value
        elif isinstance(other, int):
            return self.value == other
        return False

def convert_amount(amount):
    return psycopg2.extensions.AsIs(Decimal(amount.value))  # Converts to Decimal

# Register adapter for psycopg2
psycopg2.extensions.register_adapter(Amount, convert_amount)
