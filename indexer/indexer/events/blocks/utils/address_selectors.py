from pytoniq_core import Slice

from indexer.core.database import Message, Transaction, Trace
from indexer.events.blocks.messages import JettonNotify, JettonTransfer, StonfiSwapV2, JVaultUnstakeJettons, \
    JVaultUnstakeRequest, ToncoRouterV3PayTo, ToncoPoolV3FundAccountPayload, ToncoPoolV3SwapPayload
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

def extract_target_wallet_tonco_swap(message: Message) -> set[str]:
    accounts = set()
    jetton_notify = JettonNotify(Slice.one_from_boc(message.message_content.body))
    if jetton_notify.forward_payload_cell:
        payload_slice = jetton_notify.forward_payload_cell.to_slice()
        if payload_slice.remaining_bits > 32:
            opcode = payload_slice.preload_uint(32)
            if opcode == ToncoPoolV3SwapPayload.payload_opcode:
                payload_info = ToncoPoolV3SwapPayload(payload_slice)
                accounts.update(payload_info.get_target_wallets_recursive())
    return accounts

def extract_other_jetton_tonco_deposit(message: Message) -> set[str]:
    accounts = set()
    jetton_notify = JettonNotify(Slice.one_from_boc(message.message_content.body))
    if jetton_notify.forward_payload_cell:
        payload_slice = jetton_notify.forward_payload_cell.to_slice()
        if payload_slice.remaining_bits > 32:
            opcode = payload_slice.preload_uint(32)
            if opcode == ToncoPoolV3FundAccountPayload.payload_opcode:
                payload_info = ToncoPoolV3FundAccountPayload(payload_slice)
                accounts.add(payload_info.get_other_jetton_wallet())
    return accounts

def extract_jetton_tonco_payout(message: Message) -> set[str]:
    payout_msg = ToncoRouterV3PayTo(Slice.one_from_boc(message.message_content.body))
    accounts = set(payout_msg.get_jetton_wallets())
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
                accounts.update(extract_target_wallet_tonco_swap(msg))
                accounts.update(extract_other_jetton_tonco_deposit(msg))
            if opcode == StonfiSwapV2.opcode:
                accounts.update(extract_pool_wallets_stonfi_v2(msg))
            if opcode == ToncoRouterV3PayTo.opcode:
                accounts.update(extract_jetton_tonco_payout(msg))
        except Exception:
            pass
    return accounts

def derive_accounts_from_stake_pool(extra_data: dict):
    data_boc = extra_data['data_boc']
    try:
        slice = Slice.one_from_boc(data_boc)
        slice.skip_bits(1+32)
        slice.load_address() # admin_address
        slice.load_address() # creator_address
        lock_wallet_address = slice.load_address()
        extra_data['lock_wallet_address'] = lock_wallet_address.to_str(is_user_friendly=False).upper()
        return [], {lock_wallet_address.to_str(is_user_friendly=False).upper()}
    except:
        return [], set()

def derive_accounts_from_stake_wallet(extra_data: dict):
    data_boc = extra_data['data_boc']
    try:
        slice = Slice.one_from_boc(data_boc)
        pool = slice.load_address()
        extra_data['pool'] = pool.to_str(is_user_friendly=False).upper()
        new_request = ExtraAccountRequest(account=pool.to_str(is_user_friendly=False).upper(),
                                          request_type='data_boc',
                                          callback=derive_accounts_from_stake_pool)
        return [new_request], {pool.to_str(is_user_friendly=False).upper()}
    except:
        return [], set()

def extract_from_jvault_unstake_data(message: Message) -> ExtraAccountRequest:
    return ExtraAccountRequest(account=message.destination,
                               request_type='data_boc',
                               callback=derive_accounts_from_stake_wallet)


def extract_extra_accounts_data_requests(tx: Transaction) -> set[ExtraAccountRequest]:
    requests = set()
    for msg in tx.messages:
        if msg.opcode is None:
            continue
        opcode = msg.opcode & 0xFFFFFFFF
        try:
            if opcode in [JVaultUnstakeJettons.opcode, JVaultUnstakeRequest.opcode] and msg.destination is not None:
                requests.add(extract_from_jvault_unstake_data(msg))
        except Exception as e:
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