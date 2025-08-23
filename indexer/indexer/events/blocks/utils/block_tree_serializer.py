from __future__ import annotations

import base64
import hashlib
import logging

from indexer.core.database import Action, Trace
from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.core import Block
from indexer.events.blocks.dns import (
    ChangeDnsRecordBlock,
    DeleteDnsRecordBlock,
    DnsRenewBlock,
)
from indexer.events.blocks.evaa import (
    EvaaLiquidateBlock,
    EvaaSupplyBlock,
    EvaaWithdrawBlock,
)
from indexer.events.blocks.jettons import (
    JettonBurnBlock,
    JettonMintBlock,
    JettonTransferBlock,
)
from indexer.events.blocks.jvault import (
    JVaultClaimBlock,
    JVaultStakeBlock,
    JVaultUnstakeBlock, JVaultUnstakeRequestBlock,
)
from indexer.events.blocks.liquidity import (
    CoffeeCreatePoolBlock,
    CoffeeCreatePoolCreatorBlock,
    CoffeeCreateVaultBlock,
    DedustDepositLiquidity,
    DedustDepositLiquidityPartial,
    ToncoDeployPoolBlock,
    ToncoDepositLiquidityBlock,
    ToncoWithdrawLiquidityBlock,
)
from indexer.events.blocks.multisig import (
    MultisigApproveBlock,
    MultisigCreateOrderBlock,
    MultisigExecuteBlock,
)
from indexer.events.blocks.auction import NftPutOnSaleBlock, NftPutOnAuctionBlock
from indexer.events.blocks.nft import NftDiscoveryBlock, NftMintBlock, NftTransferBlock
from indexer.events.blocks.staking import (
    CoffeeStakingClaimRewardsBlock,
    CoffeeStakingDepositBlock,
    CoffeeStakingWithdrawBlock,
    NominatorPoolDepositBlock,
    NominatorPoolWithdrawRequestBlock,
    TONStakersDepositBlock,
    TONStakersWithdrawBlock,
    TONStakersWithdrawRequestBlock,
)
from indexer.events.blocks.subscriptions import SubscriptionBlock, UnsubscribeBlock
from indexer.events.blocks.swaps import JettonSwapBlock
from indexer.events.blocks.utils import AccountId, Asset, Amount
from indexer.events.blocks.vesting import (
    VestingAddWhiteListBlock,
    VestingSendMessageBlock,
)
from indexer.events.blocks.tgbtc import TgBTCDkgLogBlock, TgBTCMintBlock, TgBTCBurnBlock, TgBTCNewKeyBlock

logger = logging.getLogger(__name__)

def _addr(addr: AccountId | Asset | None) -> str | None:
    if addr is None:
        return None
    if isinstance(addr, Asset):
        return addr.jetton_address.as_str() if addr.jetton_address is not None else None
    else:
        return addr.as_str()

def _value(amount: Amount | None):
    if amount is None:
        return None
    return amount.value


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
    action.accounts = accounts
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
    marketplace = None
    real_prev_owner = None
    marketplace_address = None
    if 'marketplace' in block.data and block.data['marketplace'] is not None:
        marketplace = block.data['marketplace']
    if 'real_prev_owner' in block.data and block.data['real_prev_owner'] is not None:
        real_prev_owner = block.data['real_prev_owner']
    if 'marketplace_address' in block.data and block.data['marketplace_address'] is not None:
        marketplace_address = block.data['marketplace_address']
    action.nft_transfer_data = {
        'query_id': block.data['query_id'],
        'is_purchase': block.data['is_purchase'],
        'price': block.data['price'].value if 'price' in block.data and block.data['is_purchase'] else None,
        'nft_item_index': block.data['nft']['index'],
        'forward_amount': block.data['forward_amount'].value if block.data['forward_amount'] is not None else None,
        'custom_payload': block.data['custom_payload'],
        'forward_payload': block.data['forward_payload'],
        'response_destination': block.data['response_destination'].as_str() if block.data['response_destination'] else None,
        'marketplace': marketplace,
        'marketplace_address': _addr(marketplace_address),
        'real_prev_owner': _addr(real_prev_owner),
        'payout_amount': _value(block.data['payout_amount']) if 'payout_amount' in block.data else None,
        'payout_comment_encrypted': block.data.get('payout_comment_encrypted', None),
        'payout_comment_encoded': block.data.get('payout_comment_encoded', None),
        'payout_comment': block.data.get('payout_comment', None),
    }

def _fill_nft_discovery_action(block: NftDiscoveryBlock, action: Action):
    action.source = _addr(block.data.sender)
    action.asset = _addr(block.data.result_collection)
    action.asset_secondary = _addr(block.data.nft)
    action.nft_transfer_data = {
        "nft_item_index": block.data.result_index,
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


def _fill_nft_put_on_sale_action(block: NftPutOnSaleBlock, action: Action):
    action.source = _addr(block.data.owner)
    action.source_secondary = _addr(block.data.listing_address)
    action.destination = _addr(block.data.sale_address)
    action.destination_secondary = _addr(block.data.marketplace)
    action.asset = _addr(block.data.nft_collection)
    action.asset_secondary = _addr(block.data.nft_address)
    action.nft_listing_data = {
        'nft_item_index': block.data.nft_index,
        'full_price': _value(block.data.full_price),
        'marketplace_fee': _value(block.data.marketplace_fee),
        'royalty_amount': _value(block.data.royalty_amount),
        'marketplace_fee_address': _addr(block.data.marketplace_fee_address),
        'royalty_address': _addr(block.data.royalty_address),
        # Auction fields set to null
        'mp_fee_factor': None,
        'mp_fee_base': None,
        'royalty_fee_base': None,
        'max_bid': None,
        'min_bid': None,
    }


def _fill_nft_put_on_auction_action(block: NftPutOnAuctionBlock, action: Action):
    action.source = _addr(block.data.owner)
    action.source_secondary = _addr(block.data.listing_address)
    action.destination = _addr(block.data.auction_address)
    action.destination_secondary = _addr(block.data.marketplace)
    action.asset = _addr(block.data.nft_collection)
    action.asset_secondary = _addr(block.data.nft_address)
    action.nft_listing_data = {
        'nft_item_index': block.data.nft_index,
        'mp_fee_factor': _value(block.data.mp_fee_factor),
        'mp_fee_base': _value(block.data.mp_fee_base),
        'royalty_fee_base': _value(block.data.royalty_fee_base),
        'max_bid': _value(block.data.max_bid),
        'min_bid': _value(block.data.min_bid),
        'marketplace_fee_address': _addr(block.data.mp_fee_address),  # unified field
        'royalty_address': _addr(block.data.royalty_fee_addr),  # unified field
        # Sale fields set to null
        'full_price': None,
        'marketplace_fee': None,
        'royalty_amount': None,
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
    if block.data['dex'] in ('stonfi_v2', 'dedust', 'tonco'):
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
    min_out_amount = None
    if 'min_out_amount' in block.data:
        min_out_amount = block.data['min_out_amount'].value if block.data['min_out_amount'] is not None else None
    action.jetton_swap_data = {
        'dex': block.data['dex'],
        'sender': _addr(block.data['sender']),
        'dex_incoming_transfer': dex_incoming_transfer,
        'dex_outgoing_transfer': dex_outgoing_transfer,
        'min_out_amount': min_out_amount
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

def _fill_tonco_withdraw_liquidity(block: ToncoWithdrawLiquidityBlock, action: Action):
    action.type = 'dex_withdraw_liquidity'
    d = block.data
    action.source = _addr(d.sender)
    action.source_secondary = None # tonco uses NFT, no LP jetton wallet for sender
    action.destination = _addr(d.pool)
    action.asset = None # lp asset is nft, not a jetton, so primary asset on action is None
    action.dex_withdraw_liquidity_data = {
        "dex": "tonco",
        "amount1": d.amount1_out.value if d.amount1_out is not None else None,
        "amount2": d.amount2_out.value if d.amount2_out is not None else None,
        'asset1_out': _addr(d.asset1_out),
        'asset2_out': _addr(d.asset2_out),
        'user_jetton_wallet_1': _addr(d.wallet1),
        'user_jetton_wallet_2': _addr(d.wallet2),
        'dex_jetton_wallet_1': _addr(d.dex_jetton_wallet_1),
        'dex_wallet_1': _addr(d.dex_wallet_1),
        'dex_wallet_2': _addr(d.dex_wallet_2),
        'dex_jetton_wallet_2': _addr(d.dex_jetton_wallet_2),
        'lp_tokens_burnt': d.liquidity_burnt.value if d.liquidity_burnt is not None else None,
        'burned_nft_index': d.burned_nft_index,
        'burned_nft_address': _addr(d.burned_nft_address),
        'tick_lower': d.tick_lower,
        'tick_upper': d.tick_upper
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
    vault_excesses = []
    for excess in block.data["vault_excesses"]:
        vault_excesses.append({
            'asset': _addr(excess[0]),
            'amount': excess[1].value,
        })
    action.dex_deposit_liquidity_data = {
        "dex": block.data["dex"],
        "asset1": _addr(block.data["asset_1"]),
        "amount1": block.data["amount_1"].value if block.data["amount_1"] is not None else None,
        "asset2": _addr(block.data["asset_2"]),
        "amount2": block.data["amount_2"].value if block.data["amount_2"] is not None else None,
        "user_jetton_wallet_1": _addr(block.data["user_jetton_wallet_1"]),
        "user_jetton_wallet_2": _addr(block.data["user_jetton_wallet_2"]),
        "lp_tokens_minted": block.data["lp_tokens_minted"].value if block.data["lp_tokens_minted"] is not None else None,
        "target_asset_1": _addr(block.data["target_asset_1"]),
        "target_amount_1": block.data["target_amount_1"].value if block.data["target_amount_1"] is not None else None,
        "target_asset_2": _addr(block.data["target_asset_2"]),
        "target_amount_2": block.data["target_amount_2"].value if block.data["target_amount_2"] is not None else None,
        "vault_excesses": vault_excesses
    }

def _fill_dedust_deposit_liquidity_partial_action(block: DedustDepositLiquidityPartial, action: Action):
    action.type='dex_deposit_liquidity'
    action.source = _addr(block.data["sender"])
    action.destination_secondary = _addr(block.data["deposit_contract"])
    action.dex_deposit_liquidity_data = {
        "dex": block.data["dex"],
        "asset1": _addr(block.data["asset_1"]),
        "amount1": block.data["amount_1"].value if block.data["amount_1"] is not None else None,
        "asset2": _addr(block.data["asset_2"]),
        "amount2": block.data["amount_2"].value if block.data["amount_2"] is not None else None,
        "user_jetton_wallet_1": _addr(block.data["user_jetton_wallet_1"]),
        "user_jetton_wallet_2": _addr(block.data["user_jetton_wallet_2"]),
        "lp_tokens_minted": None,
        "target_asset_1": _addr(block.data["target_asset_1"]),
        "target_amount_1": block.data["target_amount_1"].value if block.data["target_amount_1"] is not None else None,
        "target_asset_2": _addr(block.data["target_asset_2"]),
        "target_amount_2": block.data["target_amount_2"].value if block.data["target_amount_2"] is not None else None,
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

def _fill_evaa_supply_action(block: EvaaSupplyBlock, action: Action):
    action.source = _addr(block.data.sender)
    action.source_secondary = _addr(block.data.sender_jetton_wallet)
    action.destination = _addr(block.data.recipient)
    action.destination_secondary = _addr(block.data.recipient_contract)
    action.amount = block.data.amount
    action.asset = _addr(block.data.asset)
    action.success = block.data.is_success
    if block.failed:
        action.success = False
    action.evaa_supply_data = {
        "is_ton": block.data.is_ton,
        "asset_id": hex(block.data.asset_id) if block.data.asset_id is not None else None,
        "master": _addr(block.data.master),
        "recipient_jetton_wallet": _addr(block.data.recipient_jetton_wallet) if block.data.recipient_jetton_wallet else None,
        "master_jetton_wallet": _addr(block.data.master_jetton_wallet) if block.data.master_jetton_wallet else None
    }

def _fill_evaa_withdraw_action(block: EvaaWithdrawBlock, action: Action):
    action.source = _addr(block.data.owner)
    action.destination = _addr(block.data.recipient)
    action.destination_secondary = _addr(block.data.owner_contract)
    action.amount = block.data.amount
    action.asset = _addr(block.data.asset)
    action.success = block.data.is_success
    if block.failed:
        action.success = False
    action.evaa_withdraw_data = {
        "is_ton": block.data.is_ton,
        "recipient_jetton_wallet": _addr(block.data.recipient_jetton_wallet) if block.data.recipient_jetton_wallet else None,
        "master_jetton_wallet": _addr(block.data.master_jetton_wallet) if block.data.master_jetton_wallet else None,
        "fail_reason": block.data.fail_reason,
        "master": _addr(block.data.master),
        "asset_id": hex(block.data.asset_id) if block.data.asset_id is not None else None,
    }


def _fill_evaa_liquidate_action(block: EvaaLiquidateBlock, action: Action):
    action.source = str(block.data.liquidator)
    action.destination = str(block.data.borrower)
    action.destination_secondary = str(block.data.borrower_contract) if block.data.borrower_contract else None
    action.asset = str(block.data.collateral_asset_id)
    action.amount = block.data.collateral_amount
    action.success = block.data.is_success
    action.evaa_liquidate_data = {
        'asset_id': hex(block.data.collateral_asset_id) if block.data.collateral_asset_id is not None else None,
        "fail_reason": block.data.fail_reason,
        "debt_amount": block.data.debt_amount
    }
def _fill_jvault_stake(block: JVaultStakeBlock, action: Action):
    action.source = _addr(block.data.sender)
    action.source_secondary = _addr(block.data.sender_wallet)
    action.asset = _addr(block.data.asset)
    action.destination = _addr(block.data.staking_pool)
    action.amount = block.data.staked_amount
    action.jvault_stake_data = {
        "period": block.data.period,
        "stake_wallet": _addr(block.data.stake_wallet),
    }


def _fill_jvault_unstake(block: JVaultUnstakeBlock, action: Action):
    action.source = _addr(block.data.sender)
    action.source_secondary = _addr(block.data.stake_wallet)
    action.destination = _addr(block.data.staking_pool)
    action.amount = block.data.unstaked_amount
    action.opcode = block.data.exit_code
    action.asset = _addr(block.data.asset)
    action.asset2 = _addr(block.data.jvault_asset)


def _fill_jvault_claim(block: JVaultClaimBlock, action: Action):
    action.source = _addr(block.data.sender)
    action.source_secondary = _addr(block.data.stake_wallet)
    action.destination = _addr(block.data.staking_pool)
    action.jvault_claim_data = {
        "claimed_jettons": list(map(_addr, block.data.claimed_jettons)),
        "claimed_amounts": block.data.claimed_amounts,
    }


def _fill_jvault_unstake_request(block: JVaultUnstakeRequestBlock, action: Action):
    action.source = _addr(block.data.sender)
    action.source_secondary = _addr(block.data.stake_wallet)
    action.destination = _addr(block.data.staking_pool)
    action.amount = block.data.requested_amount
    action.asset = _addr(block.data.asset)
    action.asset2 = _addr(block.data.jvault_asset)
    action.opcode = block.data.exit_code


def _fill_multisig_create_order(block: MultisigCreateOrderBlock, action: Action):
    action.source = _addr(block.data.created_by)
    action.destination = _addr(block.data.multisig)
    action.destination_secondary = _addr(block.data.order_contract_address)
    action.multisig_create_order_data = {
        "query_id": block.data.query_id,
        "order_seqno": block.data.order_seqno,
        "is_created_by_signer": block.data.is_created_by_signer,
        "is_signed_by_creator": block.data.creator_approved,
        "creator_index": block.data.creator_index,
        "expiration_date": block.data.expiration_date,
        "order_boc": block.data.order_boc_str,
    }
    action.accounts.extend(block.data.signers)


def _fill_multisig_approve(block: MultisigApproveBlock, action: Action):
    action.source = _addr(block.data.signer)
    action.destination = _addr(block.data.order)
    action.success = block.data.success
    action.multisig_approve_data = {
        "signer_index": block.data.signer_index,
        "exit_code": block.data.exit_code,
    }
    action.accounts.extend(block.data.signers)


def _fill_multisig_execute(block: MultisigExecuteBlock, action: Action):
    action.source = _addr(block.data.order_contract_address)
    action.destination = _addr(block.data.multisig)
    action.success = block.data.success
    action.multisig_execute_data = {
        "query_id": block.data.query_id,
        "order_seqno": block.data.order_seqno,
        "expiration_date": block.data.expiration_date,
        "approvals_num": block.data.approvals_num,
        "signers_hash": block.data.signers_hash_str,
        "order_boc": block.data.order_boc_str,
    }
    action.accounts.extend(block.data.signers)


def _fill_vesting_send_message(block: VestingSendMessageBlock, action: Action):
    action.source = _addr(block.data.sender)
    action.destination = _addr(block.data.vesting)
    action.destination_secondary = _addr(
        block.data.message_destination
    )  # where the msg was sent to
    action.amount = block.data.message_value.value  # the value of the msg
    action.vesting_send_message_data = {
        "query_id": block.data.query_id,
        "message_boc": block.data.message_boc_str,
    }


def _fill_vesting_add_whitelist(block: VestingAddWhiteListBlock, action: Action):
    action.source = _addr(block.data.adder)
    action.destination = _addr(block.data.vesting)
    action.vesting_add_whitelist_data = {
        "query_id": block.data.query_id,
        "accounts_added": list(map(_addr, block.data.accounts_added)),
    }

def _fill_tonco_deploy_pool(block: ToncoDeployPoolBlock, action: Action):
    d = block.data
    action.success = d.success
    action.source = _addr(d.deployer)
    action.destination = _addr(d.router)
    action.destination_secondary = _addr(d.pool)
    action.tonco_deploy_pool_data = {
        "jetton0_router_wallet": _addr(d.jetton0_router_wallet),
        "jetton1_router_wallet": _addr(d.jetton1_router_wallet),
        "jetton0_minter": _addr(d.jetton0_minter),
        "jetton1_minter": _addr(d.jetton1_minter),
        "tick_spacing": d.tick_spacing,
        "initial_price_x96": d.initial_price_x96,
        "protocol_fee": d.protocol_fee,
        "lp_fee_base": d.lp_fee_base,
        "lp_fee_current": d.lp_fee_current,
        "pool_active": d.pool_active,
    }

def _fill_tgbtc_mint_action(block: TgBTCMintBlock, action: Action):
    action.type = 'tgbtc_mint'
    if block.data.crippled:
        action.type += '_fallback'
    action.source = _addr(block.data.sender)
    action.destination = _addr(block.data.recipient)
    action.amount = block.data.amount
    action.asset = _addr(block.data.asset)
    action.success = block.data.success
    action.asset_secondary = block.data.bitcoin_txid
    action.source_secondary = _addr(block.data.teleport_contract)
    action.destination_secondary = _addr(block.data.recipient_wallet)


def _fill_tgbtc_burn_action(block: TgBTCBurnBlock, action: Action):
    action.type = 'tgbtc_burn'
    if block.data.crippled:
        action.type += '_fallback'
    action.source = _addr(block.data.sender)
    action.source_secondary = _addr(block.data.jetton_wallet)
    action.destination = _addr(block.data.pegout_address)
    action.amount = block.data.amount.value
    action.asset = _addr(block.data.asset)


def _fill_tgbtc_new_key_action(block: TgBTCNewKeyBlock, action: Action):
    action.type = 'tgbtc_new_key'
    if block.data.crippled:
        action.type += '_fallback'
    action.source = _addr(block.data.teleport_contract)
    action.source_secondary = block.data.pubkey
    action.destination = _addr(block.data.coordinator_contract)
    action.destination_secondary = _addr(block.data.pegout_address)
    action.amount = block.data.amount
    action.value = block.data.timestamp

def _fill_tgbtc_dkg_log_action(block: TgBTCDkgLogBlock, action: Action):
    action.type = 'tgbtc_dkg_log_fallback'
    action.source = _addr(block.data.coordinator_contract)
    action.asset = block.data.internal_pubkey
    action.value = block.data.timestamp


def _fill_tonco_deposit_liquidity_action(block: ToncoDepositLiquidityBlock, action: Action):
    action.type = 'dex_deposit_liquidity'
    action.source = _addr(block.data.sender)
    action.source_secondary = _addr(block.data.sender_wallet_1 or block.data.sender_wallet_2)
    action.destination = _addr(block.data.pool)
    action.destination_secondary = _addr(block.data.account_contract)
    vault_excesses = []
    if block.data.excesses:
        for excess in block.data.excesses:
            vault_excesses.append({
                'asset': _addr(excess[0]),
                'amount': excess[1].value,
            })
    actual_asset_1 = None
    actual_amount_1 = None
    actual_asset_2 = None
    actual_amount_2 = None
    for (amt, asset) in [(block.data.amount_1, block.data.asset_1), (block.data.amount_2, block.data.asset_2)]:
        if amt is None:
            continue
        if actual_amount_1 is None:
            actual_amount_1 = amt
            actual_asset_1 = asset
        else:
            actual_amount_2 = amt
            actual_asset_2 = asset
    action.dex_deposit_liquidity_data = {
        "dex": "tonco",
        "amount1": actual_amount_1.value if actual_amount_1 else None,
        "amount2": actual_amount_2.value if actual_amount_2 else None,
        "asset1": _addr(actual_asset_1),
        "asset2": _addr(actual_asset_2),
        "user_jetton_wallet_1": _addr(block.data.sender_wallet_1),
        "user_jetton_wallet_2": _addr(block.data.sender_wallet_2),
        "lp_tokens_minted": block.data.lp_tokens_minted.value if block.data.lp_tokens_minted else None,
        "tick_lower": block.data.tick_lower,
        "tick_upper": block.data.tick_upper,
        "nft_index": block.data.nft_index,
        "nft_address": _addr(block.data.nft_address),
        "target_amount_1": block.data.position_amount_1.value,
        "target_amount_2": block.data.position_amount_2.value,
        "target_asset_1": _addr(block.data.asset_1),
        "target_asset_2": _addr(block.data.asset_2),
        "vault_excesses": vault_excesses,
    }

def _fill_coffee_create_vault(block: CoffeeCreateVaultBlock, action: Action):
    action.source = _addr(block.data.sender)
    action.destination = _addr(block.data.vault)
    action.asset = _addr(block.data.asset)
    action.value = block.data.amount.value

def _fill_coffee_create_pool_creator(block: CoffeeCreatePoolCreatorBlock, action: Action):
    action.source = _addr(block.data.sender)
    action.source_secondary = _addr(block.data.sender_jetton_wallet)
    action.destination = _addr(block.data.deposit_recipient)
    action.destination_secondary = _addr(block.data.pool_creator_contract)
    action.asset = _addr(block.data.provided_asset)
    action.asset2 = _addr(block.data.pool_params.first)
    action.asset2_secondary = _addr(block.data.pool_params.second)
    action.amount = block.data.amount.value

def _fill_coffee_create_pool(block: CoffeeCreatePoolBlock, action: Action):
    action.source = _addr(block.data.source)
    action.source_secondary = _addr(block.data.source_jetton_wallet)
    action.amount = block.data.amount.value
    action.asset = _addr(block.data.asset_1)
    action.asset2 = _addr(block.data.asset_2)
    action.destination = _addr(block.data.pool)
    action.destination_secondary = _addr(block.data.pool_creator_contract)
    action.coffee_create_pool_data = {
        "amount_1": block.data.amount_1.value if block.data.amount_1 else None,
        "amount_2": block.data.amount_2.value if block.data.amount_2 else None,
        "initiator_1": _addr(block.data.initiator_1),
        "initiator_2": _addr(block.data.initiator_2),
        "provided_asset": _addr(block.data.provided_asset),
        "lp_tokens_minted": block.data.lp_tokens_minted.value if block.data.lp_tokens_minted else None,
    }

def _fill_coffee_mev_protect_hold_funds(block: Block, action: Action):
    action.source = _addr(block.data['sender'])
    action.source_secondary = _addr(block.data['sender_wallet'])
    action.destination = _addr(block.data['mev_contract'])
    action.destination_secondary = _addr(block.data['mev_contract_wallet'])
    action.asset = _addr(block.data['asset'])
    action.amount = block.data['amount'].value

def _fill_coffee_mev_protect_failed_swap(block: Block, action: Action):
    action.destination = _addr(block.data['recipient'])
    action.asset = _addr(block.data['asset'])

def _fill_coffee_staking_deposit(block: CoffeeStakingDepositBlock, action: Action):
    action.source = _addr(block.data.source)
    action.source_secondary = _addr(block.data.user_jetton_wallet)
    action.destination = _addr(block.data.pool)
    action.destination_secondary = _addr(block.data.pool_jetton_wallet)
    action.asset = _addr(block.data.asset)
    action.amount = block.data.value.value
    action.coffee_staking_deposit_data = {
        "minted_item_address": _addr(block.data.minted_item_address),
        "minted_item_index": block.data.minted_item_index,
    }

def _fill_coffee_staking_withdraw(block: CoffeeStakingWithdrawBlock, action: Action):
    action.source = _addr(block.data.source)
    action.source_secondary = _addr(block.data.user_jetton_wallet)
    action.destination = _addr(block.data.pool)
    action.destination_secondary = _addr(block.data.pool_jetton_wallet)
    action.asset = _addr(block.data.asset)
    action.amount = block.data.amount.value
    action.coffee_staking_withdraw_data = {
        "nft_address": _addr(block.data.nft_address),
        "nft_index": block.data.nft_index,
        "points": block.data.points,
    }

def _fill_coffee_staking_claim_rewards(block: CoffeeStakingClaimRewardsBlock, action: Action):
    # don't store admin since it's always the same highload
    #  and we don't have a basic field for it
    action.source = _addr(block.data.pool)
    action.source_secondary = _addr(block.data.pool_jetton_wallet)
    action.destination = _addr(block.data.recipient)
    action.destination_secondary = _addr(block.data.recipient_jetton_wallet)
    action.asset = _addr(block.data.asset)
    action.amount = block.data.amount.value

# noinspection PyCompatibility,PyTypeChecker
def block_to_action(block: Block, trace_id: str, trace: Trace | None = None) -> Action:
    action = _base_block_to_action(block, trace_id)
    if trace is not None:
        action.trace_end_lt = trace.end_lt
        action.trace_end_utime = trace.end_utime
        action.trace_external_hash = trace.external_hash
        action.trace_external_hash_norm = trace.external_hash_norm
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
        case "coffee_deposit_liquidity":
            _fill_dedust_deposit_liquidity_action(block, action)  # use dedust's filler
        case "dedust_deposit_liquidity_partial":
            _fill_dedust_deposit_liquidity_partial_action(block, action)
        case "tonco_deposit_liquidity":
            _fill_tonco_deposit_liquidity_action(block, action)
        case "jetton_transfer":
            _fill_jetton_transfer_action(block, action)
        case 'nft_transfer':
            _fill_nft_transfer_action(block, action)
        case 'nft_mint':
            _fill_nft_mint_action(block, action)
        case 'nft_put_on_sale':
            _fill_nft_put_on_sale_action(block, action)
        case 'nft_put_on_auction':
            _fill_nft_put_on_auction_action(block, action)
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
        case 'jvault_unstake':
            _fill_jvault_unstake(block, action)
        case 'jvault_stake':
            _fill_jvault_stake(block, action)
        case 'jvault_claim':
            _fill_jvault_claim(block, action)
        case 'jvault_unstake_request':
            _fill_jvault_unstake_request(block, action)
        case 'multisig_create_order':
            _fill_multisig_create_order(block, action)
        case 'multisig_approve':
            _fill_multisig_approve(block, action)
        case 'multisig_execute':
            _fill_multisig_execute(block, action)
        case 'nft_discovery':
            _fill_nft_discovery_action(block, action)
        case 'evaa_supply':
            _fill_evaa_supply_action(block, action)
        case 'evaa_withdraw':
            _fill_evaa_withdraw_action(block, action)
        case 'evaa_liquidate':
            _fill_evaa_liquidate_action(block, action)
        case 'vesting_send_message':
            _fill_vesting_send_message(block, action)
        case 'vesting_add_whitelist':
            _fill_vesting_add_whitelist(block, action)
        case 'tonco_deploy_pool':
            _fill_tonco_deploy_pool(block, action)
        case 'tgbtc_mint':
            _fill_tgbtc_mint_action(block, action)
        case 'tgbtc_burn':
            _fill_tgbtc_burn_action(block, action)
        case 'tgbtc_new_key':
            _fill_tgbtc_new_key_action(block, action)
        case 'tgbtc_dkg_log':
            _fill_tgbtc_dkg_log_action(block, action)
        case 'tick_tock':
            _fill_tick_tock_action(block, action)
        case 'tonco_withdraw_liquidity':
            _fill_tonco_withdraw_liquidity(block, action)
        case 'coffee_create_vault':
            _fill_coffee_create_vault(block, action)
        case 'coffee_create_pool_creator':
            _fill_coffee_create_pool_creator(block, action)
        case 'coffee_create_pool':
            _fill_coffee_create_pool(block, action)
        case 'coffee_mev_protect_hold_funds':
            _fill_coffee_mev_protect_hold_funds(block, action)
        case 'coffee_staking_deposit':
            _fill_coffee_staking_deposit(block, action)
        case 'coffee_staking_withdraw':
            _fill_coffee_staking_withdraw(block, action)
        case 'coffee_staking_claim_rewards':
            _fill_coffee_staking_claim_rewards(block, action)
        case _:
            logger.warning(f"Unknown block type {block.btype} for trace {trace_id}")
    # Fill accounts
    action.accounts.append(action.source)
    action.accounts.append(action.source_secondary)
    if not block.is_ghost_block:
        action.accounts.append(action.destination)
        action.accounts.append(action.destination_secondary)

    # Fill extended tx hashes
    extended_tx_hashes = set(action.tx_hashes)
    if block.initiating_event_node is not None:
        extended_tx_hashes.add(block.initiating_event_node.get_tx_hash())
        if not block.initiating_event_node.is_tick_tock:
            acc = block.initiating_event_node.message.transaction.account
            if acc not in action.accounts:
                logging.debug(f"Initiating transaction ({block.initiating_event_node.get_tx_hash()}) account not in accounts. Trace id: {trace_id}. Action id: {action.action_id}")
            action.accounts.append(acc)
    action.tx_hashes = list(extended_tx_hashes)

    action.accounts = list(set(a for a in action.accounts if a is not None))
    return action


v1_ops = [
    'call_contract',
    'contract_deploy',
    'jetton_burn',
    'tick_tock',
    'jetton_transfer',
    'nft_transfer',
    'nft_mint',
    'jetton_mint',
    'ton_transfer',
    'stake_deposit',
    'stake_withdrawal',
    'stake_withdrawal_request',
    'dex_deposit_liquidity',
    'jetton_swap',
    'change_dns',
    'delete_dns',
    'renew_dns',
    'subscribe',
    'dex_withdraw_liquidity',
    'unsubscribe',
    'election_deposit',
    'election_recover',
    'auction_bid',
    'nominator_pool_deposit',
    'nominator_pool_withdraw_request',
    'dedust_deposit_liquidity',
    'dedust_deposit_liquidity_partial',
    'tonstakers_deposit',
    'tonstakers_withdraw_request',
    'tonstakers_withdraw',
    'tonco_deposit_liquidity',
    'tonco_withdraw_liquidity',
    'coffee_deposit_liquidity'
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
                if block.btype not in v1_ops:
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

def create_unknown_action(trace: Trace) -> Action:
    logger.debug("Creating unknown action for " + trace.trace_id)
    tx_hashes = [n.hash for n in trace.transactions]
    failed = any(n.aborted for n in trace.transactions)
    action = Action(
        trace_id=trace.trace_id,
        type="unknown",
        action_id=trace.trace_id,
        tx_hashes=tx_hashes,
        start_lt=trace.start_lt,
        end_lt=trace.end_lt,
        start_utime=trace.start_utime,
        end_utime=trace.end_utime,
        success=not failed,
        mc_seqno_end=trace.mc_seqno_end,
        value_extra_currencies=dict(),
        trace_end_lt=trace.end_lt,
        trace_end_utime=trace.end_utime,
        trace_external_hash=trace.external_hash,
        trace_mc_seqno_end=trace.mc_seqno_end
    )
    action.accounts = list(set([n.account for n in trace.transactions]))
    return action