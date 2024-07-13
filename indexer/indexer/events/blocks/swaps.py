from __future__ import annotations

from indexer.events.blocks.messages import DedustPayout, DedustPayoutFromPool, DedustSwapPeer, DedustSwapExternal, \
    DedustSwap
from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, child_sequence_matcher, ContractMatcher, \
    BlockTypeMatcher, OrMatcher
from indexer.events.blocks.core import Block, SingleLevelWrapper
from indexer.events.blocks.jettons import JettonTransferBlock
from indexer.events.blocks.messages import StonfiSwapMessage, StonfiPaymentRequest, DedustSwapNotification
from indexer.events.blocks.utils import AccountId, Asset, Amount
from indexer.events.blocks.utils.block_utils import find_call_contracts, find_messages
from indexer.core.database import JettonWallet


class JettonSwapBlock(Block):
    def __init__(self, data):
        super().__init__('jetton_swap', [], data)

    def __repr__(self):
        return f"jetton_swap {self.data}"


async def _get_block_data(other_blocks):
    swap_call_block = next(x for x in other_blocks if isinstance(x, CallContractBlock) and
                           x.opcode == StonfiSwapMessage.opcode)
    swap_message = StonfiSwapMessage(swap_call_block.get_body())
    payment_requests_messages = [StonfiPaymentRequest(x.get_body()) for x in
                                 find_call_contracts(other_blocks, StonfiPaymentRequest.opcode)]
    target_payment_request = next(x for x in payment_requests_messages if x.owner == swap_message.from_user_address)
    if target_payment_request.amount0_out > 0:
        out_amt = Amount(target_payment_request.amount0_out)
        out_addr = target_payment_request.token0_out
    else:
        out_amt = Amount(target_payment_request.amount1_out)
        out_addr = target_payment_request.token1_out
    out_wallet = await context.extra_data_repository.get().get_jetton_wallet(out_addr.to_str(False).upper())
    in_wallet = await context.extra_data_repository.get().get_jetton_wallet(
        swap_message.token_wallet.to_str(False).upper())
    out_jetton = AccountId(out_wallet.jetton) if out_wallet is not None else None
    in_jetton = AccountId(in_wallet.jetton) if in_wallet is not None else None

    return {
        'dex': 'stonfi',
        'sender': AccountId(swap_message.from_user_address),
        'in': {
            'amount': Amount(swap_message.amount),
            'asset': Asset(is_ton=in_jetton is None, jetton_address=in_jetton),
        },
        'out': {
            'amount': out_amt,
            'asset': Asset(is_ton=out_jetton is None, jetton_address=out_jetton),
        }
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
        data = await _get_block_data(other_blocks)
        new_bubble = JettonSwapBlock(data)
        include = [block]
        include.extend(other_blocks)
        new_bubble.merge_blocks(include)
        return [new_bubble]


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
        if len(sender_jetton_transfer_blocks) > 0:
            sender_wallet = sender_jetton_transfer_blocks[0].data['sender_wallet']

        receiver_jetton_transfer_blocks = [x for x in new_block.children_blocks if isinstance(x, JettonTransferBlock)
                                        and x.min_lt >= block.min_lt and x.data['receiver'] == sender]
        receiver_wallet = None
        if len(receiver_jetton_transfer_blocks) > 0:
            receiver_wallet = receiver_jetton_transfer_blocks[0].data['receiver_wallet']
        new_block.data = {
            'dex': 'dedust',
            'sender_wallet': sender_wallet,
            'receiver_wallet': receiver_wallet,
            'sender': sender,
            'in': peer_swaps[0]['in'],
            'out': peer_swaps[-1]['out'],
            'peer_swaps': peer_swaps if len(peer_swaps) > 1 else []
        }
        return [new_block]
