import base64
import hashlib
import random
from typing import Tuple, List

from indexer.core.database import Action
from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.core import Block
from indexer.events.blocks.dns import ChangeDnsRecordBlock, DeleteDnsRecordBlock
from indexer.events.blocks.jettons import JettonTransferBlock, JettonBurnBlock
from indexer.events.blocks.nft import NftTransferBlock, NftMintBlock
from indexer.events.blocks.subscriptions import SubscriptionBlock, UnsubscribeBlock
from indexer.events.blocks.swaps import JettonSwapBlock


def _calc_action_id(block: Block) -> str:
    msg_hashes = list(set(n.message.msg_hash for n in block.event_nodes))
    msg_hashes.sort()
    h = hashlib.sha256(",".join(msg_hashes).encode())
    return base64.b64encode(h.digest()).decode()


def _base_block_to_action(block: Block, trace_id: str) -> Action:
    action_id = _calc_action_id(block)
    tx_hashes = list(set(n.message.tx_hash for n in block.event_nodes))
    return Action(
        trace_id=trace_id,
        type=block.btype,
        action_id=action_id,
        tx_hashes=tx_hashes,
        start_lt=block.min_lt,
        end_lt=block.max_lt,
        start_utime=block.min_utime,
        end_utime=block.max_utime,
        success=not block.failed)


def _fill_call_contract_action(block: CallContractBlock, action: Action):
    action.opcode = block.opcode
    action.value = block.data['value'].value
    action.source = block.data['source'].as_str() if block.data['source'] is not None else None
    action.destination = block.data['destination'].as_str() if block.data['destination'] is not None else None


def _fill_ton_transfer_action(block: TonTransferBlock, action: Action):
    action.value = block.value
    action.source = block.data['source'].as_str()
    if block.data['destination'] is None:
        print("Something very wrong", block.event_nodes[0].message.trace_id)
    action.destination = block.data['destination'].as_str()
    content = block.data['comment'].replace("\u0000", "") if block.data['comment'] is not None else None
    action.ton_transfer_data = {'content': content, 'encrypted': block.data['encrypted']}


def _fill_jetton_transfer_action(block: JettonTransferBlock, action: Action):
    action.source = block.data['sender'].as_str()
    action.source_secondary = block.data['sender_wallet'].as_str()
    action.destination = block.data['receiver'].as_str()
    action.destination_secondary = block.data['receiver_wallet'].as_str() if 'receiver_wallet' in block.data else None
    action.value = block.data['amount'].value
    asset = block.data['asset']
    if asset is None or asset.is_ton:
        action.asset = None
    else:
        action.asset = asset.jetton_address.as_str()
    action.jetton_transfer_data = {
        'query_id': block.data['query_id'],
        'response_address': block.data['response_address'].as_str() if block.data[
                                                                           'response_address'] is not None else None,
        'forward_amount': block.data['forward_amount'].value,
    }


def _fill_nft_transfer_action(block: NftTransferBlock, action: Action):
    if 'prev_owner' in block.data and block.data['prev_owner'] is not None:
        action.source = block.data['prev_owner'].as_str()
    action.destination = block.data['new_owner'].as_str()
    action.asset = block.data['nft']['address'].as_str()
    if block.data['nft']['collection'] is not None:
        action.asset_secondary = block.data['nft']['collection']['address'].as_str()
    action.nft_transfer_data = {
        'query_id': block.data['query_id'],
        'is_purchase': block.data['is_purchase'],
        'price': block.data['price'].value if 'price' in block.data and block.data['is_purchase'] else None,
    }


def _fill_nft_mint_action(block: NftMintBlock, action: Action):
    if block.data["source"]:
        action.source = block.data["source"].as_str()
    action.destination = block.data["address"].as_str()
    action.asset = action.destination
    if block.data["collection"]:
        action.asset_secondary = block.data["collection"].as_str()


def _convert_peer_swap(peer_swap: dict) -> dict:
    in_obj = peer_swap['in']
    out_obj = peer_swap['out']
    return {
        'amount_in': in_obj['amount'].value,
        'asset_in': in_obj['asset'].jetton_address.as_str() if in_obj['asset'].jetton_address is not None else None,
        'amount_out': out_obj['amount'].value,
        'asset_out': out_obj['asset'].jetton_address.as_str() if out_obj['asset'].jetton_address is not None else None,
    }


def _fill_jetton_swap_action(block: JettonSwapBlock, action: Action):
    action.source = block.data['sender'].as_str()
    action.source_secondary = block.data['sender_wallet'].as_str() if block.data['sender_wallet'] is not None else None
    action.destination = block.data['sender'].as_str()
    action.destination_secondary = block.data['receiver_wallet'].as_str() if block.data['receiver_wallet'] is not None \
        else None

    if block.data['in']['asset'].jetton_address is not None:
        action.asset = block.data['in']['asset'].jetton_address.as_str()

    if block.data['out']['asset'].jetton_address is not None:
        action.asset2 = block.data['out']['asset'].jetton_address.as_str()

    action.jetton_swap_data = {
        'dex': block.data['dex'],
        'amount_in': block.data['in']['amount'].value,
        'amount_out': block.data['out']['amount'].value,
        'peer_swaps': [_convert_peer_swap(x) for x in block.data['peer_swaps']],
    }


def _fill_jetton_burn_action(block: JettonBurnBlock, action: Action):
    action.source = block.data['owner'].as_str()
    action.source_secondary = block.data['jetton_wallet'].as_str()
    action.asset = block.data['asset'].jetton_address.as_str()
    action.value = block.data['amount'].value


def _fill_change_dns_record_action(block: ChangeDnsRecordBlock, action: Action):
    action.source = block.data['source'].as_str() if block.data['source'] is not None else None
    action.destination = block.data['destination'].as_str()
    dns_record_data = block.data['value']
    data = {
        'value_schema': dns_record_data['schema'],
        'flags': None,
        'address': None,
        'key': block.data['key'].hex(),
    }
    if data['value_schema'] in ('DNSNextResolver', 'DNSSmcAddress'):
        data['address'] = dns_record_data['address'].as_str()
    elif data['value_schema'] == 'DNSAdnlAddress':
        data['address'] = dns_record_data['address'].hex()
        data['flags'] = dns_record_data['flags']
    if data['value_schema'] == 'DNSSmcAddress':
        data['flags'] = dns_record_data['flags']
    if data['value_schema'] == 'DNSText':
        data['dns_text'] = dns_record_data['dns_text']
    action.change_dns_record_data = data


def _fill_delete_dns_record_action(block: DeleteDnsRecordBlock, action: Action):
    action.source = block.data['source'].as_str() if block.data['source'] is not None else None
    action.destination = block.data['destination'].as_str()
    data = {
        'value_schema': None,
        'flags': None,
        'address': None,
        'key': block.data['key'].hex(),
    }
    action.change_dns_record_data = data


def _fill_subscribe_action(block: SubscriptionBlock, action: Action):
    action.source = block.data['subscriber'].as_str()
    action.source_secondary = block.data['beneficiary'].as_str() if block.data['beneficiary'] is not None else None
    action.destination = block.data['subscription'].as_str()
    action.value = block.data['amount'].value


def _fill_unsubscribe_action(block: UnsubscribeBlock, action: Action):
    action.source = block.data['subscriber'].as_str()
    action.source_secondary = block.data['beneficiary'].as_str() if block.data['beneficiary'] is not None else None
    action.destination = block.data['subscription'].as_str()


def _fill_election_action(block: Block, action: Action):
    action.source = block.data['stake_holder'].as_str()
    action.value = block.data['amount'].value if 'amount' in block.data else None


# noinspection PyCompatibility,PyTypeChecker
def block_to_action(block: Block, trace_id: str) -> Action:
    action = _base_block_to_action(block, trace_id)
    match block.btype:
        case 'call_contract':
            _fill_call_contract_action(block, action)
        case 'ton_transfer':
            _fill_ton_transfer_action(block, action)
        case 'jetton_transfer':
            _fill_jetton_transfer_action(block, action)
        case 'nft_transfer':
            _fill_nft_transfer_action(block, action)
        case 'nft_mint':
            _fill_nft_mint_action(block, action)
        case 'jetton_burn':
            _fill_jetton_burn_action(block, action)
        case 'jetton_swap':
            _fill_jetton_swap_action(block, action)
        case 'change_dns':
            _fill_change_dns_record_action(block, action)
        case 'delete_dns':
            _fill_delete_dns_record_action(block, action)
        case 'subscribe':
            _fill_subscribe_action(block, action)
        case 'unsubscribe':
            _fill_unsubscribe_action(block, action)
        case 'election_deposit' | 'election_recover':
            _fill_election_action(block, action)

    return action
