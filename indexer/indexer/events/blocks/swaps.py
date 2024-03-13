from __future__ import annotations

from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, child_sequence_matcher, ContractMatcher, BlockTypeMatcher, OrMatcher
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
                                 find_call_contracts(other_blocks, 0xf93bb43f)]
    target_payment_request = next(x for x in payment_requests_messages if x.owner == swap_message.from_user_address)
    if target_payment_request.amount0_out > 0:
        out_amt = Amount(target_payment_request.amount0_out)
        out_addr = target_payment_request.token0_out
    else:
        out_amt = Amount(target_payment_request.amount1_out)
        out_addr = target_payment_request.token1_out

    out_wallet = await context.session.get().get(JettonWallet, out_addr.to_str(False).upper())
    in_wallet = await context.session.get().get(JettonWallet, swap_message.token_wallet.to_str(False).upper())
    # out_wallet = await context.session.get().query(JettonWallet).filter(
    #     JettonWallet.address == out_addr.to_str(False).upper()).first()
    # in_wallet = await context.session.get().query(JettonWallet).filter(
    #     JettonWallet.address == swap_call_block.get_message().message.source).first()
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
                                            ContractMatcher(opcode=0xad4eb6f5,
                                                            child_matcher=OrMatcher([
                                                                ContractMatcher(opcode=0x474f86cf),
                                                                BlockTypeMatcher(block_type='jetton_transfer')
                                                            ]),
                                                            optional=True)])

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == 0x72aca8aa

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        wrapper = SingleLevelWrapper()
        wrapper.wrap([block] + other_blocks)
        return [wrapper]


class DedustSwapBlockMatcher(BlockMatcher):
    def __init__(self):
        ton_swap_parent_matchers = ContractMatcher(opcode=0x61ee542d,
                                                   parent_matcher=ContractMatcher(opcode=0xea06185d, optional=True))

        super().__init__(optional=False, child_matcher=None,
                         parent_matcher=ContractMatcher(opcode=0x61ee542d,
                                                        child_matcher=OrMatcher([
                                                            DedustPeerBlockMatcher(),
                                                            child_sequence_matcher([
                                                                ContractMatcher(opcode=0xad4eb6f5),  # TODO: remove magic number
                                                                OrMatcher([
                                                                    BlockTypeMatcher(block_type='jetton_transfer'),
                                                                    ContractMatcher(opcode=0x474f86cf)  # TODO: remove magic number
                                                                ])])]),
                                                        parent_matcher=OrMatcher([ton_swap_parent_matchers,
                                                                                  BlockTypeMatcher(
                                                                                      block_type='jetton_transfer')])))

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
                    'amount': Amount(message.amount_in),
                    'asset': Asset(is_ton=message.asset_out.is_ton,
                                   jetton_address=message.asset_out.jetton_address),
                }
            }
            peer_swaps.append(data)

        new_block.data = {
            'dex': 'dedust',
            'sender': AccountId(messages[0][1].sender_address),
            'in': peer_swaps[0]['in'],
            'out': peer_swaps[-1]['out'],
            'peer_swaps': peer_swaps if len(peer_swaps) > 1 else []
        }
        return [new_block]
