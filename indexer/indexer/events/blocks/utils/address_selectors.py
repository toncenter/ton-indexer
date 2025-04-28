from pytoniq_core import Slice

from indexer.core.database import Message, Transaction, Trace
from indexer.events.blocks.messages import JettonNotify, JettonTransfer, StonfiSwapV2, JVaultUnstakeJettons
from indexer.events.blocks.messages.externals import extract_payload_from_wallet_message
from indexer.events.interface_repository import ExtraAccountRequest


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

def extract_pool_wallets_stonfi_v2(message: Message) -> set[str]:
    accounts = set()
    stonfi_swap_msg = StonfiSwapV2(Slice.one_from_boc(message.message_content.body))
    accounts.update(stonfi_swap_msg.get_pool_accounts_recursive())
    return accounts


def extract_addresses_from_external(message: Message) -> set[str]:
    if message.source is not None:
        return set()
    accounts = set()
    payloads, _ = extract_payload_from_wallet_message(message.message_content.body)
    for payload in payloads:
        if payload.info is None:
            continue
        opcode = payload.opcode & 0xFFFFFFFF
        if payload.info.dest is not None:
            accounts.add(payload.info.dest.to_str(is_user_friendly=False).upper())
        try:
            if opcode == JettonTransfer.opcode:
                msg = JettonTransfer(payload.body.to_slice())
                accounts.add(msg.destination.to_str(is_user_friendly=False).upper())
        except Exception:
            pass
    return accounts

def extract_additional_addresses(tx: Transaction) -> set[str]:
    accounts = set()
    for msg in tx.messages:
        if msg.opcode is None:
            continue
        opcode = msg.opcode & 0xFFFFFFFF
        try:
            if msg.source is None:
                accounts.update(extract_addresses_from_external(msg))
            if opcode == JettonTransfer.opcode:
                accounts.update(extract_target_wallet_stonfi_swap(msg))
            if opcode == JettonNotify.opcode:
                accounts.update(extract_target_wallet_stonfi_v2_swap(msg))
            if opcode == StonfiSwapV2.opcode:
                accounts.update(extract_pool_wallets_stonfi_v2(msg))
        except Exception:
            pass
    return accounts

def extract_extra_accounts_data_requests(tx: Transaction) -> set[ExtraAccountRequest]:
    requests = set()
    for msg in tx.messages:
        if msg.opcode is None:
            continue
        opcode = msg.opcode & 0xFFFFFFFF
        try:
            if opcode == JVaultUnstakeJettons.opcode and msg.destination is not None:
                requests.add(ExtraAccountRequest(msg.destination, request='data_boc'))
        except Exception:
            pass
    return requests

def extract_accounts_from_trace(trace: Trace) -> tuple[set[str], set[ExtraAccountRequest]]:
    accounts = set()
    extra_data_requests = set()
    for tx in trace.transactions:
        accounts.add(tx.account)
        accounts.update(extract_additional_addresses(tx))
        extra_data_requests.update(extract_extra_accounts_data_requests(tx))
    return accounts, extra_data_requests