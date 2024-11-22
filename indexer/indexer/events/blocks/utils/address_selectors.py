from pytoniq_core import Slice

from indexer.core.database import Message, Transaction
from indexer.events.blocks.messages import JettonNotify, JettonTransfer

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

def extract_target_wallet_stonfi_swap(message: Message) -> set[str]:
    accounts = set()
    jetton_transfer = JettonTransfer(Slice.one_from_boc(message.message_content.body))
    if jetton_transfer.stonfi_swap_body:
        accounts.add(jetton_transfer.stonfi_swap_body['jetton_wallet'].to_str(is_user_friendly=False).upper())
    return accounts

def extract_additional_addresses(tx: Transaction) -> set[str]:
    accounts = set()
    for msg in tx.messages:
        opcode = msg.opcode
        if opcode < 0:
            opcode = 0xFFFFFFFF
        try:
            if opcode == JettonTransfer.opcode:
                accounts.update(extract_target_wallet_stonfi_swap(msg))
            if opcode == JettonNotify.opcode:
                accounts.update(extract_target_wallet_stonfi_v2_swap(msg))
        except Exception:
            pass
    return accounts