from __future__ import annotations

import base64
import hashlib
import logging

from indexer.core.database import Action, Trace
from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.core import Block
from indexer.events.blocks.dns import ChangeDnsRecordBlock, DeleteDnsRecordBlock, DnsRenewBlock
from indexer.events.blocks.jettons import (
    JettonMintBlock,
)
from indexer.events.blocks.jettons import JettonTransferBlock, JettonBurnBlock
from indexer.events.blocks.liquidity import (
    DedustDepositLiquidityPartial,
    DedustDepositLiquidity,
)
from indexer.events.blocks.nft import NftTransferBlock, NftMintBlock
from indexer.events.blocks.staking import TONStakersDepositBlock, TONStakersWithdrawRequestBlock, \
    TONStakersWithdrawBlock, NominatorPoolWithdrawRequestBlock, NominatorPoolDepositBlock
from indexer.events.blocks.subscriptions import SubscriptionBlock, UnsubscribeBlock
from indexer.events.blocks.swaps import JettonSwapBlock
from indexer.events.blocks.utils import AccountId, Asset

logger = logging.getLogger(__name__)

def _addr(addr: AccountId | Asset | None) -> str | None:
    if addr is None:
        return None
    if isinstance(addr, Asset):
        return addr.jetton_address.as_str() if addr.jetton_address is not None else None
    else:
        return addr.as_str()


def _calc_action_id(block: Block) -> str:
    root_event_node = min(block.event_nodes, key=lambda n: n.get_lt())
    key = ""
    if root_event_node.message is not None:
        key = root_event_node.message.msg_hash
    else:
        key = root_event_node.get_tx_hash()
    key += block.btype
    h = hashlib.sha256(key.encode())
    return base64.b64encode(h.digest()).decode()


def _base_block_to_action(block: Block, trace_id: str) -> Action:
    action_id = _calc_action_id(block)
    tx_hashes = list(set(n.get_tx_hash() for n in block.event_nodes))
    mc_seqno_end = max(n.get_tx().mc_block_seqno for n in block.event_nodes if n.get_tx() is not None)
    accounts = []
    for n in block.event_nodes:
        if n.is_tick_tock:
            accounts.append(n.tick_tock_tx.account)
        else:
            accounts.append(n.message.transaction.account)

    action = Action(
        trace_id=trace_id,
        type=block.btype,
        action_id=action_id,
        tx_hashes=tx_hashes,
        start_lt=block.min_lt,
        end_lt=block.max_lt,
        start_utime=block.min_utime,
        end_utime=block.max_utime,
        success=not block.failed,
        mc_seqno_end=mc_seqno_end,
        value_extra_currencies=dict(),
    )
    action._accounts = accounts
    return action


def _fill_call_contract_action(block: CallContractBlock, action: Action):
    action.opcode = block.opcode
    action.value = block.data['value'].value
    action.source = block.data['source'].as_str() if block.data['source'] is not None else None
    action.destination = block.data['destination'].as_str() if block.data['destination'] is not None else None
    extra_currencies = block.data['extra_currencies'] if 'extra_currencies' in block.data else None
    if extra_currencies is not None:
        action.value_extra_currencies = extra_currencies
    else:
        action.value_extra_currencies = dict()


def _fill_ton_transfer_action(block: TonTransferBlock, action: Action):
    action.value = block.value
    action.source = block.data['source'].as_str()
    if block.data['destination'] is None:
        print("Something very wrong", block.event_nodes[0].message.trace_id)
    action.destination = block.data['destination'].as_str()
    content = block.data['comment'].replace("\u0000", "") if block.data['comment'] is not None else None
    action.ton_transfer_data = {'content': content, 'encrypted': block.data['encrypted']}
    extra_currencies = block.data['extra_currencies'] if 'extra_currencies' in block.data else None
    if extra_currencies is not None:
        action.value_extra_currencies = extra_currencies
    else:
        action.value_extra_currencies = dict()


def _fill_jetton_transfer_action(block: JettonTransferBlock, action: Action):
    action.source = block.data['sender'].as_str()
    action.source_secondary = _addr(block.data['sender_wallet'])
    action.destination = block.data['receiver'].as_str()
    action.destination_secondary = _addr(block.data['receiver_wallet']) if 'receiver_wallet' in block.data else None
    action.amount = block.data['amount'].value
    asset = block.data['asset']
    if asset is None or asset.is_ton:
        action.asset = None
    else:
        action.asset = asset.jetton_address.as_str()
    comment = None
    if block.data['comment'] is not None:
        if block.data['encrypted_comment']:
            comment = base64.b64encode(block.data['comment']).decode('utf-8')
        else:
            comment = block.data['comment'].decode('utf-8', errors='backslashreplace').replace("\u0000", "")
    action.jetton_transfer_data = {
        'query_id': block.data['query_id'],
        'response_destination': block.data['response_address'].as_str() if block.data[
                                                                           'response_address'] is not None else None,
        'forward_amount': block.data['forward_amount'].value,
        'custom_payload': block.data['custom_payload'],
        'forward_payload': block.data['forward_payload'],
        'comment': comment,
        'is_encrypted_comment': block.data['encrypted_comment']
    }


def _fill_nft_transfer_action(block: NftTransferBlock, action: Action):
    if 'prev_owner' in block.data and block.data['prev_owner'] is not None:
        action.source = block.data['prev_owner'].as_str()
    action.destination = block.data['new_owner'].as_str()
    action.asset_secondary = block.data['nft']['address'].as_str()
    if block.data['nft']['collection'] is not None:
        action.asset = block.data['nft']['collection']['address'].as_str()
    action.nft_transfer_data = {
        'query_id': block.data['query_id'],
        'is_purchase': block.data['is_purchase'],
        'price': block.data['price'].value if 'price' in block.data and block.data['is_purchase'] else None,
        'nft_item_index': block.data['nft']['index'],
        'forward_amount': block.data['forward_amount'].value if block.data['forward_amount'] is not None else None,
        'custom_payload': block.data['custom_payload'],
        'forward_payload': block.data['forward_payload'],
        'response_destination': block.data['response_destination'].as_str() if block.data['response_destination'] else None,
    }


def _fill_nft_mint_action(block: NftMintBlock, action: Action):
    if block.data["source"]:
        action.source = block.data["source"].as_str()
    action.destination = block.data["address"].as_str()
    action.asset_secondary = action.destination
    action.opcode = block.data['opcode']
    if block.data["collection"]:
        action.asset = block.data["collection"].as_str()
    action.nft_mint_data = {
        'nft_item_index': block.data["index"],
    }


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
    dex_incoming_transfer = {
        'amount': block.data['dex_incoming_transfer']['amount'].value,
        'source': _addr(block.data['dex_incoming_transfer']['source']),
        'source_jetton_wallet': _addr(block.data['dex_incoming_transfer']['source_jetton_wallet']),
        'destination': _addr(block.data['dex_incoming_transfer']['destination']),
        'destination_jetton_wallet': _addr(block.data['dex_incoming_transfer']['destination_jetton_wallet']),
        'asset': _addr(block.data['dex_incoming_transfer']['asset'])
    }
    dex_outgoing_transfer = {
        'amount': block.data['dex_outgoing_transfer']['amount'].value,
        'source': _addr(block.data['dex_outgoing_transfer']['source']),
        'source_jetton_wallet': _addr(block.data['dex_outgoing_transfer']['source_jetton_wallet']),
        'destination': _addr(block.data['dex_outgoing_transfer']['destination']),
        'destination_jetton_wallet': _addr(block.data['dex_outgoing_transfer']['destination_jetton_wallet']),
        'asset': _addr(block.data['dex_outgoing_transfer']['asset'])
    }
    action.asset = dex_incoming_transfer['asset']
    action.asset2 = dex_outgoing_transfer['asset']
    if block.data['dex'] in ('stonfi_v2', 'dedust'):
        action.asset = _addr(block.data['source_asset'])
        action.asset2 = _addr(block.data['destination_asset'])
    action.source = dex_incoming_transfer['source']
    action.source_secondary = dex_incoming_transfer['source_jetton_wallet']
    action.destination = dex_outgoing_transfer['destination']
    action.destination_secondary = dex_outgoing_transfer['destination_jetton_wallet']
    if 'destination_wallet' in block.data and block.data['destination_wallet'] is not None:
        action.destination_secondary = _addr(block.data['destination_wallet'])
    if 'destination_asset' in block.data and block.data['destination_asset'] is not None:
        action.asset2 = _addr(block.data['destination_asset'])

    action.jetton_swap_data = {
        'dex': block.data['dex'],
        'sender': _addr(block.data['sender']),
        'dex_incoming_transfer': dex_incoming_transfer,
        'dex_outgoing_transfer': dex_outgoing_transfer,
    }
    if 'peer_swaps' in block.data and block.data['peer_swaps'] is not None:
        action.jetton_swap_data['peer_swaps'] = [_convert_peer_swap(swap) for swap in block.data['peer_swaps']]

def _fill_dex_deposit_liquidity(block: Block, action: Action):
    action.source = _addr(block.data['sender'])
    action.destination = _addr(block.data['pool'])
    action.dex_deposit_liquidity_data = {
        "dex": block.data['dex'],
        "amount1": block.data['amount_1'].value if block.data['amount_1'] is not None else None,
        "amount2": block.data['amount_2'].value if block.data['amount_2'] is not None else None,
        "asset1": _addr(block.data['asset_1']),
        "asset2": _addr(block.data['asset_2']),
        "user_jetton_wallet_1": _addr(block.data['sender_wallet_1']),
        "user_jetton_wallet_2": _addr(block.data['sender_wallet_2']),
        "lp_tokens_minted": block.data['lp_tokens_minted'].value if block.data['lp_tokens_minted'] is not None else None
    }

def _fill_dex_withdraw_liquidity(block: Block, action: Action):
    action.source = _addr(block.data['sender'])
    action.source_secondary = _addr(block.data['sender_wallet'])
    action.destination = _addr(block.data['pool'])
    action.asset = _addr(block.data['asset'])
    action.dex_withdraw_liquidity_data = {
        "dex": block.data['dex'],
        "amount1" : block.data['amount1_out'].value if block.data['amount1_out'] is not None else None,
        "amount2" : block.data['amount2_out'].value if block.data['amount2_out'] is not None else None,
        'asset1_out' : _addr(block.data['asset1_out']),
        'asset2_out' : _addr(block.data['asset2_out']),
        'user_jetton_wallet_1' : _addr(block.data['wallet1']),
        'user_jetton_wallet_2' : _addr(block.data['wallet2']),
        'dex_jetton_wallet_1': _addr(block.data['dex_jetton_wallet_1']),
        'dex_wallet_1': _addr(block.data['dex_wallet_1']),
        'dex_wallet_2': _addr(block.data['dex_wallet_2']),
        'dex_jetton_wallet_2': _addr(block.data['dex_jetton_wallet_2']),
        'is_refund' : block.data['is_refund'],
        'lp_tokens_burnt': block.data['lp_tokens_burnt'].value if block.data['lp_tokens_burnt'] is not None else None
    }

def _fill_jetton_burn_action(block: JettonBurnBlock, action: Action):
    action.source = block.data['owner'].as_str()
    action.source_secondary = block.data['jetton_wallet'].as_str()
    action.asset = block.data['asset'].jetton_address.as_str()
    action.amount = block.data['amount'].value


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
        data['value'] = dns_record_data['address'].as_str()
    elif data['value_schema'] == 'DNSAdnlAddress':
        data['value'] = dns_record_data['address'].hex()
        data['flags'] = dns_record_data['flags']
    elif data["value_schema"] == 'DNSStorageAddress':
        data['value'] = dns_record_data['address'].hex()
    if data['value_schema'] == 'DNSSmcAddress':
        data['flags'] = dns_record_data['flags']
    if data['value_schema'] == 'DNSText':
        data['value'] = dns_record_data['dns_text']
    action.change_dns_record_data = data
    action.asset = _addr(block.data['collection_address'])

def _fill_delete_dns_record_action(block: DeleteDnsRecordBlock, action: Action):
    action.source = block.data['source'].as_str() if block.data['source'] is not None else None
    action.destination = block.data['destination'].as_str()
    data = {
        'value_schema': None,
        'flags': None,
        'address': None,
        'key': block.data['key'].hex(),
    }
    action.asset = _addr(block.data['collection_address'])
    action.change_dns_record_data = data

def _fill_tonstakers_deposit_action(block: TONStakersDepositBlock, action: Action):
    action.type = 'stake_deposit'
    action.source = _addr(block.data.source)
    action.destination = _addr(block.data.pool)
    action.amount = block.data.value.value
    action.asset = _addr(block.data.asset)
    action.staking_data = {
        'provider': 'tonstakers',
        'tokens_minted': block.data.tokens_minted.value if block.data.tokens_minted else None
    }

def _fill_dns_renew_action(block: DnsRenewBlock, action: Action):
    action.source = _addr(block.data['source'])
    action.destination = _addr(block.data['destination'])
    action.asset = _addr(block.data['collection_address'])

def _fill_tonstakers_withdraw_request_action(block: TONStakersWithdrawRequestBlock, action: Action):
    action.source = _addr(block.data.source)
    action.source_secondary = _addr(block.data.tsTON_wallet)
    action.destination = _addr(block.data.pool)
    action.amount = block.data.tokens_burnt.value
    action.type = 'stake_withdrawal_request'
    action.asset = _addr(block.data.asset)
    action.staking_data = {
        'provider': 'tonstakers',
        'ts_nft': _addr(block.data.minted_nft)
    }

def _fill_tonstakers_withdraw_action(block: TONStakersWithdrawBlock, action: Action):
    action.source = _addr(block.data.stake_holder)
    action.destination = _addr(block.data.pool)
    action.amount = block.data.amount.value
    action.type = 'stake_withdrawal'
    action.staking_data = {
        'provider': 'tonstakers',
        'ts_nft': _addr(block.data.burnt_nft),
        'tokens_burnt': block.data.tokens_burnt.value if block.data.tokens_burnt is not None else None,
    }
    action.asset = _addr(block.data.asset)

def _fill_subscribe_action(block: SubscriptionBlock, action: Action):
    action.source = block.data['subscriber'].as_str()
    action.destination = block.data['beneficiary'].as_str() if block.data['beneficiary'] is not None else None
    action.destination_secondary = block.data['subscription'].as_str()
    action.amount = block.data['amount'].value


def _fill_unsubscribe_action(block: UnsubscribeBlock, action: Action):
    action.source = block.data['subscriber'].as_str()
    action.destination = block.data['beneficiary'].as_str() if block.data['beneficiary'] is not None else None
    action.destination_secondary = block.data['subscription'].as_str()


def _fill_election_action(block: Block, action: Action):
    action.source = block.data['stake_holder'].as_str()
    action.amount = block.data['amount'].value if 'amount' in block.data else None


def _fill_auction_bid_action(block: Block, action: Action):
    action.source = block.data['bidder'].as_str()
    action.destination = block.data['auction'].as_str()
    action.asset_secondary = block.data['nft_address'].as_str()
    action.asset = _addr(block.data['nft_collection'])
    action.nft_transfer_data = {
        'nft_item_index': block.data['nft_item_index'],
    }
    action.value = block.data['amount'].value

def _fill_dedust_deposit_liquidity_action(block: DedustDepositLiquidity, action: Action):
    action.type='dex_deposit_liquidity'
    action.source = _addr(block.data["sender"])
    action.destination = _addr(block.data["pool_address"])
    action.destination_secondary = _addr(block.data["deposit_contract"])
    action.dex_deposit_liquidity_data = {
        "dex": block.data["dex"],
        "asset1": _addr(block.data["asset_1"].jetton_address),
        "amount1": block.data["amount_1"].value,
        "asset2": _addr(block.data["asset_2"].jetton_address),
        "amount2": block.data["amount_2"].value,
        "user_jetton_wallet_1": _addr(block.data["user_jetton_wallet_1"]),
        "user_jetton_wallet_2": _addr(block.data["user_jetton_wallet_2"]),
        "lp_tokens_minted": block.data["lp_tokens_minted"].value,
    }

def _fill_dedust_deposit_liquidity_partial_action(block: DedustDepositLiquidityPartial, action: Action):
    action.type='dex_deposit_liquidity'
    action.source = _addr(block.data["sender"])
    action.destination_secondary = _addr(block.data["deposit_contract"])
    action.dex_deposit_liquidity_data = {
        "dex": block.data["dex"],
        "asset1": _addr(block.data["asset_1"].jetton_address),
        "amount1": block.data["amount_1"].value,
        "asset2": _addr(block.data["asset_2"].jetton_address),
        "amount2": block.data["amount_2"].value,
        "user_jetton_wallet_1": _addr(block.data["user_jetton_wallet_1"]),
        "user_jetton_wallet_2": _addr(block.data["user_jetton_wallet_2"]),
        "lp_tokens_minted": None,
    }

def _fill_jetton_mint_action(block: JettonMintBlock, action: Action):
    action.destination = _addr(block.data["to"])
    action.destination_secondary = _addr(block.data["to_jetton_wallet"])
    action.asset = _addr(block.data["asset"].jetton_address)
    action.amount = block.data["amount"].value if block.data["amount"] is not None else None
    action.value = block.data["ton_amount"].value if block.data["ton_amount"] is not None else None

def _fill_nominator_pool_deposit_action(block: NominatorPoolDepositBlock, action: Action):
    action.type = 'stake_deposit'
    action.source = block.data.source.as_str()
    action.destination = block.data.pool.as_str()
    action.amount = block.data.value.value
    action.staking_data = {
        'provider': 'nominator'
    }

def _fill_nominator_pool_withdraw_request_action(block: NominatorPoolWithdrawRequestBlock, action: Action):
    if block.data.payout_amount is None:
        action.type = 'stake_withdrawal_request'
    else:
        action.type = 'stake_withdrawal'
        action.amount = block.data.payout_amount.value
    action.staking_data = {
        'provider': 'nominator'
    }
    action.source = block.data.source.as_str()
    action.destination = block.data.pool.as_str()

def _fill_tick_tock_action(block: Block, action: Action):
    action.source = _addr(block.data['account'])

# noinspection PyCompatibility,PyTypeChecker
def block_to_action(block: Block, trace_id: str, trace: Trace | None = None) -> Action:
    action = _base_block_to_action(block, trace_id)
    if trace is not None:
        action.trace_end_lt = trace.end_lt
        action.trace_end_utime = trace.end_utime
        action.trace_external_hash = trace.external_hash
        action.trace_mc_seqno_end = trace.mc_seqno_end
    match block.btype:
        case 'call_contract' | 'contract_deploy':
            _fill_call_contract_action(block, action)
        case 'ton_transfer':
            _fill_ton_transfer_action(block, action)
        case "nominator_pool_deposit":
            _fill_nominator_pool_deposit_action(block, action)
        case "nominator_pool_withdraw_request":
            _fill_nominator_pool_withdraw_request_action(block, action)
        case "dedust_deposit_liquidity":
            _fill_dedust_deposit_liquidity_action(block, action)
        case "dedust_deposit_liquidity_partial":
            _fill_dedust_deposit_liquidity_partial_action(block, action)
        case "jetton_transfer":
            _fill_jetton_transfer_action(block, action)
        case 'nft_transfer':
            _fill_nft_transfer_action(block, action)
        case 'nft_mint':
            _fill_nft_mint_action(block, action)
        case 'jetton_burn':
            _fill_jetton_burn_action(block, action)
        case "jetton_mint":
            _fill_jetton_mint_action(block, action)
        case "jetton_swap":
            _fill_jetton_swap_action(block, action)
        case 'change_dns':
            _fill_change_dns_record_action(block, action)
        case 'delete_dns':
            _fill_delete_dns_record_action(block, action)
        case 'renew_dns':
            _fill_dns_renew_action(block, action)
        case "tonstakers_deposit":
            _fill_tonstakers_deposit_action(block, action)
        case "tonstakers_withdraw_request":
            _fill_tonstakers_withdraw_request_action(block, action)
        case "tonstakers_withdraw":
            _fill_tonstakers_withdraw_action(block, action)
        case "subscribe":
            _fill_subscribe_action(block, action)
        case 'dex_deposit_liquidity':
            _fill_dex_deposit_liquidity(block, action)
        case 'dex_withdraw_liquidity':
            _fill_dex_withdraw_liquidity(block, action)
        case 'unsubscribe':
            _fill_unsubscribe_action(block, action)
        case 'election_deposit' | 'election_recover':
            _fill_election_action(block, action)
        case 'auction_bid':
            _fill_auction_bid_action(block, action)
        case 'tick_tock':
            _fill_tick_tock_action(block, action)
        case _:
            logger.warning(f"Unknown block type {block.btype} for trace {trace_id}")
    # Fill accounts
    action._accounts.append(action.source)
    action._accounts.append(action.source_secondary)
    action._accounts.append(action.destination)
    action._accounts.append(action.destination_secondary)

    # Fill extended tx hashes
    extended_tx_hashes = set(action.tx_hashes)
    if block.initiating_event_node is not None:
        extended_tx_hashes.add(block.initiating_event_node.get_tx_hash())
        if not block.initiating_event_node.is_tick_tock:
            acc = block.initiating_event_node.message.transaction.account
            if acc not in action._accounts:
                logging.debug(f"Initiating transaction ({block.initiating_event_node.get_tx_hash()}) account not in accounts. Trace id: {trace_id}. Action id: {action.action_id}")
            action._accounts.append(acc)
    action.tx_hashes = list(extended_tx_hashes)

    action._accounts = list(set(a for a in action._accounts if a is not None))
    return action

basic_ops = [ 'call_contract',
              'contract_deploy',
              'jetton_burn',
              'tick_tock',
              'jetton_transfer',
              'nft_transfer',
              'nft_mint',
              'jetton_mint',
              'ton_transfer',
              ]

def serialize_blocks(blocks: list[Block], trace_id, trace: Trace = None, parent_acton_id = None, serialize_child_actions=True) -> tuple[list[Action], str]:
    actions = []
    action_ids = []
    state = 'ok'
    for block in blocks:
        if block.btype != 'root':
            if block.btype == 'call_contract' and block.event_nodes[0].message.destination is None:
                continue
            if block.btype == 'empty':
                continue
            if block.btype == 'call_contract' and block.event_nodes[0].message.source is None:
                continue
            if block.broken:
                state = 'broken'
            action = block_to_action(block, trace_id, trace)
            if parent_acton_id is not None and action.action_id == parent_acton_id:
                continue
            action.parent_action_id = parent_acton_id
            action_ids.append(action.action_id)
            actions.append(action)
            if serialize_child_actions:
                if block.btype not in basic_ops:
                    child_actions, child_state = serialize_blocks(block.children_blocks, trace_id, trace, action.action_id, serialize_child_actions)
                    for child_action in child_actions:
                        if child_action.type == 'contract_deploy':
                            continue
                        if child_action.ancestor_type is None:
                            child_action.ancestor_type = []
                        child_action.ancestor_type.append(block.btype)
                        child_action.ancestor_type = list(set(child_action.ancestor_type))
                        if child_action.action_id not in action_ids:
                            action_ids.append(child_action.action_id)
                            actions.append(child_action)
                        else:
                            raise Exception(f"Duplicate action id {child_action.action_id} in trace {trace_id}")
                    if child_state != 'ok':
                        state = child_state
    return actions, state