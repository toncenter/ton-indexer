from __future__ import annotations

from indexer.core.database import Message
from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, child_sequence_matcher, ContractMatcher, \
    BlockTypeMatcher, OrMatcher
from indexer.events.blocks.core import Block, SingleLevelWrapper
from indexer.events.blocks.jettons import JettonTransferBlock
from indexer.events.blocks.labels import labeled, LabelBlock
from indexer.events.blocks.messages import DedustPayout, DedustPayoutFromPool, DedustSwapPeer, DedustSwapExternal, \
    DedustSwap, StonfiV2PayTo, JettonNotify, PTonTransfer
from indexer.events.blocks.messages import StonfiSwapMessage, StonfiPaymentRequest, DedustSwapNotification
from indexer.events.blocks.utils import AccountId, Asset, Amount
from indexer.events.blocks.utils.address_selectors import extract_target_wallet_stonfi_v2_swap
from indexer.events.blocks.utils.block_utils import find_call_contracts, find_messages

stonfi_swap_ok_exit_code = 0xc64370e5
stonfi_swap_ok_ref_exit_code = 0x45078540
stonfi_swap_no_liq_exit_code = 0x5ffe1295
stonfi_swap_reserve_err_exit_code = 0x38976e9b
stonfi_sender_related_exit_codes = [
    stonfi_swap_reserve_err_exit_code,
    stonfi_swap_no_liq_exit_code,
    stonfi_swap_ok_exit_code
]

class JettonSwapBlock(Block):
    def __init__(self, data):
        super().__init__('jetton_swap', [], data)

    def __repr__(self):
        return f"jetton_swap {self.data}"


async def _get_block_data(block, other_blocks):
    swap_call_block = next(x for x in other_blocks if isinstance(x, CallContractBlock) and
                           x.opcode == StonfiSwapMessage.opcode)
    swap_message = StonfiSwapMessage(swap_call_block.get_body())
    payment_requests_messages = [(StonfiPaymentRequest(x.get_body()), x) for x in
                                 find_call_contracts(other_blocks, StonfiPaymentRequest.opcode)]
    assert len(payment_requests_messages) > 0
    if len(payment_requests_messages) > 2:
        print("Multiple payment requests found ", swap_call_block.event_nodes[0].message.trace_id)

    out_amt = None
    out_addr = None
    ref_amt = None
    ref_addr = None
    outgoing_jetton_transfer = None
    in_jetton_transfer = swap_call_block.previous_block
    success_swap = False
    # Find payment request and outgoing jetton transfer
    for payment_request, payment_request_block in payment_requests_messages:
        if payment_request.amount0_out > 0:
            amount = payment_request.amount0_out
            addr = payment_request.token0_out
        else:
            amount = payment_request.amount1_out
            addr = payment_request.token1_out
        if payment_request.exit_code in stonfi_sender_related_exit_codes:
            success_swap = (payment_request.exit_code == stonfi_swap_ok_exit_code)
            if out_amt is None:
                outgoing_jetton_transfer = next(b for b in payment_request_block.next_blocks
                                                if isinstance(b, JettonTransferBlock))
                out_amt = amount
                out_addr = addr
            elif out_amt < amount:
                outgoing_jetton_transfer = next(b for b in payment_request_block.next_blocks
                                                if isinstance(b, JettonTransferBlock))
                ref_amt = out_amt
                ref_addr = out_addr
                out_amt = amount
                out_addr = addr
        elif payment_request.exit_code == stonfi_swap_ok_ref_exit_code:
            ref_amt = amount
            ref_addr = addr
    actual_out_addr = out_addr
    if isinstance(block, JettonTransferBlock) and block.jetton_transfer_message.stonfi_swap_body is not None:
        out_addr = block.jetton_transfer_message.stonfi_swap_body['jetton_wallet']
    out_wallet = await context.interface_repository.get().get_jetton_wallet(out_addr.to_str(False).upper())
    actual_out_wallet = await context.interface_repository.get().get_jetton_wallet(actual_out_addr.to_str(False).upper())
    dex_in_wallet = await context.interface_repository.get().get_jetton_wallet(
        swap_message.token_wallet.to_str(False).upper())
    actual_out_jetton = AccountId(actual_out_wallet.jetton) if actual_out_wallet is not None else None
    out_jetton = AccountId(out_wallet.jetton) if out_wallet is not None else None
    in_jetton = AccountId(dex_in_wallet.jetton) if dex_in_wallet is not None else None

    in_source_jetton_wallet = None
    if in_jetton_transfer.data['has_internal_transfer']:
        in_source_jetton_wallet = in_jetton_transfer.data['sender_wallet']

    out_destination_jetton_wallet = None
    if outgoing_jetton_transfer.data['has_internal_transfer']:
        out_destination_jetton_wallet = outgoing_jetton_transfer.data['receiver_wallet']

    incoming_transfer = {
        'asset': Asset(is_ton=in_jetton is None, jetton_address=in_jetton),
        'amount': Amount(swap_message.amount),
        'source': AccountId(swap_message.from_user_address),
        'source_jetton_wallet': in_source_jetton_wallet,
        'destination': AccountId(dex_in_wallet.owner),
        'destination_jetton_wallet': AccountId(swap_message.token_wallet),
    }

    outgoing_transfer = {
        'asset': Asset(is_ton=actual_out_jetton is None, jetton_address=actual_out_jetton),
        'amount': Amount(out_amt),
        'source': outgoing_jetton_transfer.data['sender'],
        'source_jetton_wallet': outgoing_jetton_transfer.data['sender_wallet']
    }
    if out_destination_jetton_wallet is not None:
        outgoing_transfer['destination_jetton_wallet'] = out_destination_jetton_wallet
        outgoing_transfer['destination'] = outgoing_jetton_transfer.data['receiver']
    elif in_jetton_transfer.data['stonfi_swap_body'] is not None:
        outgoing_transfer['destination'] = AccountId(in_jetton_transfer.data['stonfi_swap_body']['user_address'])
        outgoing_transfer['destination_jetton_wallet'] = None
    else:
        outgoing_transfer['destination'] = AccountId(swap_message.from_user_address)
        outgoing_transfer['destination_jetton_wallet'] = None
    if out_jetton:
        target_asset = Asset(is_ton=out_jetton is None, jetton_address=out_jetton)
    else:
        target_asset = None
    return {
        'dex': 'stonfi',
        'sender': AccountId(swap_message.from_user_address),
        'receiver': AccountId(swap_message.from_real_user),
        'dex_incoming_transfer': incoming_transfer,
        'dex_outgoing_transfer': outgoing_transfer,
        'destination_asset': target_asset,
        'destination_wallet': AccountId(out_addr) if out_addr is not None else None,
        'referral_amount': Amount(ref_amt),
        'referral_address': AccountId(ref_addr) if ref_addr is not None else None,
        'peer_swaps': [],
        'success': success_swap
    }


class StonfiSwapBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(parent_matcher=None, optional=False,
                         child_matcher=child_sequence_matcher([
                             ContractMatcher(opcode=StonfiSwapMessage.opcode),
                             ContractMatcher(opcode=StonfiPaymentRequest.opcode),
                             BlockTypeMatcher(block_type='jetton_transfer')
                         ]))

    def test_self(self, block: Block):
        return isinstance(block, JettonTransferBlock)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        data = await _get_block_data(block, other_blocks)
        new_block = JettonSwapBlock(data)
        if not data['success']:
            new_block.failed = True
        include = [block]
        include.extend(other_blocks)
        new_block.merge_blocks(include)
        return [new_block]


class DedustPeerBlockMatcher(BlockMatcher):

    def __init__(self):
        super().__init__(parent_matcher=None, optional=False, child_matcher=None,
                         children_matchers=[ContractMatcher(opcode=DedustSwapNotification.opcode),
                                            ContractMatcher(opcode=DedustPayoutFromPool.opcode,
                                                            child_matcher=OrMatcher([
                                                                ContractMatcher(opcode=DedustPayout.opcode),
                                                                BlockTypeMatcher(block_type='jetton_transfer')
                                                            ]),
                                                            optional=True)])

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == DedustSwapPeer.opcode

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        wrapper = SingleLevelWrapper()
        wrapper.wrap([block] + other_blocks)
        return [wrapper]


class StonfiV2SwapBlockMatcher(BlockMatcher):
    swap_opcode = 0x6664de2a
    pay_to_opcode = 0x657b54f5
    pay_vault_opcode = 0x63381632
    deposit_ref_fee_opcode = 0x0490f09b

    def __init__(self):
        ref_payout_matcher = labeled('ref_payout', ContractMatcher(
            opcode=self.pay_vault_opcode,
            optional=True,
            child_matcher=ContractMatcher(opcode=self.deposit_ref_fee_opcode, include_excess=True)))

        payout_matcher = labeled('payout', ContractMatcher(self.pay_to_opcode, child_matcher=ref_payout_matcher))

        peer_swap_matcher = labeled('peer_swap', ContractMatcher(self.swap_opcode,
                                                                 child_matcher=payout_matcher,
                                                                 optional=True))

        payout_matcher.child_matcher = OrMatcher([
            labeled('out_transfer', BlockTypeMatcher(block_type='jetton_transfer',
                                                     child_matcher=peer_swap_matcher,
                                                     optional=True)),
            peer_swap_matcher])

        in_pton_transfer = ContractMatcher(opcode=JettonNotify.opcode,
                                           parent_matcher=labeled('in_transfer',
                                                                  ContractMatcher(opcode=0x01f3835d)))

        in_transfer = OrMatcher([labeled('in_transfer', BlockTypeMatcher(block_type='jetton_transfer')),
                                 in_pton_transfer])

        super().__init__(parent_matcher=in_transfer, optional=False,
                         children_matchers=[ref_payout_matcher, payout_matcher])

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == self.swap_opcode

    async def _get_target_asset_from_notification(self, message: Message):
        try:
            address = next(iter(extract_target_wallet_stonfi_v2_swap(message)), None)
            if address is None:
                return None
            jetton_wallet = await context.interface_repository.get().get_jetton_wallet(address)
            if jetton_wallet is not None:
                return Asset(is_ton=False, jetton_address=jetton_wallet.jetton)
        except Exception:
            return None

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        blocks = sorted(other_blocks, key=lambda x: x.min_lt)
        peer_swap_blocks = [block]
        in_transfer = None
        out_transfer = None
        ref_payout = None
        for b in blocks:
            if isinstance(b, LabelBlock):
                match b.label:
                    case 'peer_swap':
                        peer_swap_blocks.append(b.block)
                    case 'payout':
                        peer_swap_blocks[-1] = (peer_swap_blocks[-1], b.block)
                    case 'ref_payout':
                        ref_payout = b.block
                    case 'out_transfer':
                        out_transfer = b.block
                    case 'in_transfer':
                        in_transfer = b.block
        ok = True
        for (swap, pay_to) in peer_swap_blocks:
            pay_to_msg = StonfiV2PayTo(pay_to.get_body())
            if pay_to_msg.exit_code != 0xc64370e5:
                ok = False
        in_transfer_data = {}
        sender = None
        if isinstance(in_transfer, JettonTransferBlock):
            sender = in_transfer.data['sender']
            in_transfer_data = {
                'asset': Asset(is_ton=in_transfer.data['asset'].is_ton,
                               jetton_address=in_transfer.data['asset'].jetton_address),
                'amount': Amount(in_transfer.data['amount']),
                'source': in_transfer.data['sender'],
                'source_jetton_wallet': in_transfer.data['sender_wallet'],
                'destination': in_transfer.data['receiver'],
                'destination_jetton_wallet': in_transfer.data['receiver_wallet']
            }
        else:
            message = in_transfer.event_nodes[0].message
            amount = message.value
            if message.opcode == PTonTransfer.opcode:
                amount = PTonTransfer(in_transfer.get_body()).ton_amount
            sender = AccountId(message.source)
            in_transfer_data = {
                'asset': Asset(is_ton=True, jetton_address=None),
                'amount': Amount(amount),
                'source': AccountId(message.source),
                'source_jetton_wallet': None,
                'destination': AccountId(block.event_nodes[0].message.source),
                'destination_jetton_wallet': AccountId(message.destination)
            }

        out_transfer_data = {}
        additional_blocks_to_include = []
        pton_transfer = next((x for x in out_transfer.next_blocks if isinstance(x, CallContractBlock)
                              and x.opcode == PTonTransfer.opcode), None)
        if pton_transfer is None and out_transfer.data['has_internal_transfer']:
            out_transfer_data = {
                'asset': Asset(is_ton=out_transfer.data['asset'].is_ton,
                               jetton_address=out_transfer.data['asset'].jetton_address),
                'amount': Amount(out_transfer.data['amount']),
                'source': out_transfer.data['sender'],
                'source_jetton_wallet': out_transfer.data['sender_wallet'],
                'destination': out_transfer.data['receiver'],
                'destination_jetton_wallet': out_transfer.data['receiver_wallet']
            }
        else:
            additional_blocks_to_include.append(pton_transfer)
            amount = PTonTransfer(pton_transfer.get_body()).ton_amount
            out_transfer_data = {
                'asset': Asset(is_ton=True, jetton_address=None),
                'amount': Amount(amount),
                'source': out_transfer.data['sender'],
                'source_jetton_wallet': out_transfer.data['sender_wallet'],
                'destination': AccountId(pton_transfer.get_message().destination),
                'destination_jetton_wallet': None,
            }
        new_block = JettonSwapBlock({
            'dex': 'stonfi_v2',
            'source_asset': in_transfer_data['asset'],
            'destination_asset': out_transfer_data['asset'],
            'sender': sender,
            'dex_incoming_transfer': in_transfer_data,
            'dex_outgoing_transfer': out_transfer_data,
            'referral_amount': None,
            'referral_address': None,
            'peer_swaps': []
        })
        new_block.merge_blocks([block] + other_blocks + additional_blocks_to_include)
        new_block.failed = not ok
        if not ok:
            target_asset = await self._get_target_asset_from_notification(block.previous_block.event_nodes[0].message)
            if target_asset is not None:
                new_block.data['destination_asset'] = target_asset
        return [new_block]


class DedustSwapBlockMatcher(BlockMatcher):
    def __init__(self):
        ton_swap_parent_matchers = ContractMatcher(opcode=DedustSwapExternal.opcode,
                                                   parent_matcher=ContractMatcher(opcode=DedustSwap.opcode,
                                                                                  optional=True))

        super().__init__(optional=False, child_matcher=None,
                         parent_matcher=ContractMatcher(opcode=DedustSwapExternal.opcode,
                                                        child_matcher=OrMatcher([
                                                            DedustPeerBlockMatcher(),
                                                            child_sequence_matcher([
                                                                ContractMatcher(opcode=DedustPayoutFromPool.opcode),
                                                                OrMatcher([
                                                                    BlockTypeMatcher(block_type='jetton_transfer'),
                                                                    ContractMatcher(opcode=DedustPayout.opcode)
                                                                ])])]),
                                                        parent_matcher=OrMatcher([BlockTypeMatcher(
                                                            block_type='jetton_transfer'),
                                                            ContractMatcher(opcode=DedustSwap.opcode)])))

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == DedustSwapNotification.opcode

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        new_block = JettonSwapBlock({})
        include = [block]
        for b in other_blocks:
            if isinstance(b, SingleLevelWrapper):
                include.extend(b.children_blocks)
            else:
                include.append(b)
        include.extend(other_blocks)
        new_block.merge_blocks(include)

        messages = find_messages(new_block.children_blocks, DedustSwapNotification)
        messages.sort(key=lambda x: x[0].min_lt)
        peer_swaps = []
        for _, message in messages:
            data = {
                'in': {
                    'amount': Amount(message.amount_in),
                    'asset': Asset(is_ton=message.asset_in.is_ton,
                                   jetton_address=message.asset_in.jetton_address),
                },
                'out': {
                    'amount': Amount(message.amount_out),
                    'asset': Asset(is_ton=message.asset_out.is_ton,
                                   jetton_address=message.asset_out.jetton_address),
                }
            }
            peer_swaps.append(data)
        sender = AccountId(messages[0][1].sender_address)
        sender_jetton_transfer_blocks = [x for x in new_block.children_blocks if isinstance(x, JettonTransferBlock)
                                     and x.min_lt <= block.min_lt and x.data['sender'] == sender]
        sender_wallet = None
        dex_incoming_jetton_wallet = None
        dex_incoming_wallet = None
        if len(sender_jetton_transfer_blocks) > 0:
            dex_incoming_jetton_wallet = sender_jetton_transfer_blocks[0].data['receiver_wallet']
            dex_incoming_wallet = sender_jetton_transfer_blocks[0].data['receiver']
            sender_wallet = sender_jetton_transfer_blocks[0].data['sender_wallet']
        else:
            swap_requests = find_call_contracts(other_blocks, DedustSwap.opcode)
            if len(swap_requests) > 0:
                dex_incoming_wallet = AccountId(swap_requests[0].get_message().destination)


        receiver_jetton_transfer_blocks = [x for x in new_block.children_blocks if isinstance(x, JettonTransferBlock)
                                        and x.min_lt >= block.min_lt and x.data['receiver'] == sender]
        receiver = sender
        receiver_wallet = None
        dex_outgoing_jetton_wallet = None
        dex_outgoing_wallet = None
        if len(receiver_jetton_transfer_blocks) > 0:
            receiver_wallet = receiver_jetton_transfer_blocks[0].data['receiver_wallet']
            dex_outgoing_wallet = receiver_jetton_transfer_blocks[0].data['sender']
            dex_outgoing_jetton_wallet = receiver_jetton_transfer_blocks[0].data['sender_wallet']
        else:
            payouts = find_call_contracts(other_blocks, DedustPayout.opcode)
            if len(payouts) > 0:
                dex_outgoing_wallet = AccountId(payouts[0].get_message().source)
                receiver = AccountId(payouts[0].get_message().destination)

        new_block.data = {
            'dex': 'dedust',
            'sender': sender,
            'dex_incoming_transfer': {
                'asset': peer_swaps[0]['in']['asset'],
                'amount': peer_swaps[0]['in']['amount'],
                'source': sender,
                'source_jetton_wallet': sender_wallet,
                'destination': dex_incoming_wallet,
                'destination_jetton_wallet': dex_incoming_jetton_wallet,
            },
            'dex_outgoing_transfer': {
                'asset': peer_swaps[-1]['out']['asset'],
                'amount': peer_swaps[-1]['out']['amount'],
                'source': dex_outgoing_wallet,
                'source_jetton_wallet': dex_outgoing_jetton_wallet,
                'destination': receiver,
                'destination_jetton_wallet': receiver_wallet,
            },
            'in': peer_swaps[0]['in'],
            'out': peer_swaps[-1]['out'],
            'peer_swaps': peer_swaps if len(peer_swaps) > 1 else []
        }
        return [new_block]
