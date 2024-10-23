from pytoniq_core import Slice

from indexer.core.database import Message, Trace
from indexer.events.blocks.messages import JettonNotify


def extract_target_wallet_stonfi_v2_swap(message: Message) -> set[str]:
    accounts = set()
    slice = Slice.one_from_boc(message.message_content.body)
    slice.skip_bits(32 + 64)  # opcode + query_id
    slice.load_coins()
    slice.load_address()
    payload = slice.load_ref().to_slice() if slice.load_bit() else slice.copy()
    if payload.remaining_bits > 0:
        opcode = payload.load_uint(32)
        if opcode == 0x6664de2a:
            address = payload.load_address()
            accounts.add(address.to_str(False).upper())
    return accounts

def extract_additional_addresses(traces: list[Trace]) -> set[str]:
    accounts = set()
    for trace in traces:
        for tx in trace.transactions:
            for msg in tx.messages:
                try:
                    if msg.opcode == JettonNotify.opcode:
                        accounts.update(extract_target_wallet_stonfi_v2_swap(msg))
                except Exception:
                    pass
    return accounts