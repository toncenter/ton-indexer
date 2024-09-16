from __future__ import annotations

from pytoniq_core import Slice, Address


class SubscriptionPaymentRequest:
    opcode = 0x706c7567

    def __init__(self, slice: Slice):
        slice.load_uint(32)  # opcode
        self.query_id = slice.load_uint(64)
        self.grams = slice.load_coins()


class SubscriptionPaymentRequestResponse:
    opcode = 0xf06c7567


class SubscriptionPayment:
    opcode = 0x73756273


class WalletPluginDestruct:
    opcode = 0x64737472
