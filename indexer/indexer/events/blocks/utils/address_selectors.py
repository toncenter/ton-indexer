from pytoniq_core import Slice

from indexer.core.database import Message, Transaction
from indexer.events.blocks.messages import JettonNotify, JettonTransfer


def extract_target_wallet_stonfi_swap(message: Message) -> set[str]:
    accounts = set()
    jetton_transfer = JettonTransfer(Slice.one_from_boc(message.message_content.body))
    if jetton_transfer.stonfi_swap_body:
        accounts.add(jetton_transfer.stonfi_swap_body['jetton_wallet'].to_str(is_user_friendly=False).upper())
    return accounts

def extract_additional_addresses(tx: Transaction) -> set[str]:
    accounts = set()
    for msg in tx.messages:
        try:
            if msg.opcode == JettonTransfer.opcode or (msg.opcode & 0xFFFFFFFF == JettonTransfer.opcode):
                accounts.update(extract_target_wallet_stonfi_swap(msg))
        except Exception:
            pass
    return accounts