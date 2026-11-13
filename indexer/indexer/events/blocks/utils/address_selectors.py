from pytoniq_core import Slice

from indexer.core.database import Message, Transaction, Trace
from indexer.events.blocks.messages import JettonNotify, JettonTransfer, StonfiSwapV2, JVaultUnstakeJettons, \
    JVaultUnstakeRequest, ToncoRouterV3PayTo, ToncoPoolV3FundAccountPayload, ToncoPoolV3SwapPayload, \
    NftOwnershipAssigned, NftTransfer
from indexer.events.blocks.messages.externals import extract_payload_from_wallet_message
from indexer.events.blocks.messages.nft_offer import NftOfferCancel, NftOfferCancelByBuyer, NftOfferData, \
    parse_nft_offer_state_init
from indexer.events.blocks.messages.fixed_sale import FixedSaleBuy, FixedSaleCancel, FixedSaleChangePrice, \
    FixedSaleData
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
            if msg.source is not None and msg.destination is not None and msg.init_state is not None:
                offer = parse_nft_offer_state_init(msg.destination, msg.init_state.body)
                if offer is not None:
                    accounts.add(offer.nft.to_str(is_user_friendly=False).upper())
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

def derive_accounts_from_nft_offer(extra_data: dict):
    try:
        nft = NftOfferData(Slice.one_from_boc(extra_data['data_boc'])).nft
        return [], {nft.to_str(is_user_friendly=False).upper()}
    except Exception:
        return [], set()

def derive_accounts_from_fixed_sale(extra_data: dict):
    try:
        nft = FixedSaleData(Slice.one_from_boc(extra_data['data_boc'])).nft
        return [], {nft.to_str(is_user_friendly=False).upper()}
    except Exception:
        return [], set()

# The payout and balance release distinguish offer acceptance from ordinary NFT and auction transfers.
def _sends_nft_back(tx: Transaction, nft: str | None) -> bool:
    out = [m for m in tx.messages if m.direction == 'out']
    return (nft is not None and sum(m.opcode is None for m in out) == 2
            and any(m.opcode is not None and m.destination == nft and m.opcode & 0xFFFFFFFF == NftTransfer.opcode
                    for m in out))


def extract_extra_accounts_data_requests(tx: Transaction) -> set[ExtraAccountRequest]:
    requests = set()
    for msg in tx.messages:
        if msg.opcode is None:
            continue
        opcode = msg.opcode & 0xFFFFFFFF
        try:
            if opcode in [JVaultUnstakeJettons.opcode, JVaultUnstakeRequest.opcode] and msg.destination is not None:
                requests.add(extract_from_jvault_unstake_data(msg))
            is_offer_cancel = ((opcode == NftOfferCancelByBuyer.opcode and msg.source is not None)
                               or (opcode == NftOfferCancel.opcode and msg.source is None))
            if is_offer_cancel and msg.destination is not None:
                requests.add(ExtraAccountRequest(account=msg.destination,
                                                 request_type='data_boc',
                                                 callback=derive_accounts_from_nft_offer))
            is_sale_call = opcode in (FixedSaleBuy.opcode, FixedSaleChangePrice.opcode, FixedSaleCancel.opcode)
            if is_sale_call and msg.source is not None and msg.destination is not None:
                requests.add(ExtraAccountRequest(account=msg.destination,
                                                 request_type='data_boc',
                                                 callback=derive_accounts_from_fixed_sale))
            if opcode == NftOwnershipAssigned.opcode and msg.direction == 'in' and _sends_nft_back(tx, msg.source):
                requests.add(ExtraAccountRequest(account=msg.destination, request_type='data_boc'))
            # A sale takes the NFT without a reply; its terms are in its data.
            if (opcode == NftOwnershipAssigned.opcode and msg.direction == 'in' and not tx.aborted
                    and tx.compute_exit_code == 0 and not any(m.direction == 'out' for m in tx.messages)):
                requests.add(ExtraAccountRequest(account=msg.destination, request_type='data_boc'))
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