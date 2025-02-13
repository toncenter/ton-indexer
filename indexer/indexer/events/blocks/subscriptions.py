from __future__ import annotations

from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, ContractMatcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.messages.subscriptions import SubscriptionPaymentRequestResponse, SubscriptionPayment, \
    SubscriptionPaymentRequest, WalletPluginDestruct
from indexer.events.blocks.utils import AccountId, Amount
from indexer.events.blocks.utils.block_utils import find_call_contract


class SubscriptionBlock(Block):
    def __init__(self, data):
        super().__init__('subscribe', [], data)

    def __repr__(self):
        return f"SUBSCRIPTION {self.event_nodes[0].message.transaction.hash}"


class UnsubscribeBlock(Block):
    def __init__(self, data):
        super().__init__('unsubscribe', [], data)

    def __repr__(self):
        return f"UNSUBSCRIBE {self.event_nodes[0].message.transaction.hash}"


class SubscriptionBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=ContractMatcher(opcode=SubscriptionPayment.opcode),
                         parent_matcher=ContractMatcher(opcode=SubscriptionPaymentRequest.opcode, optional=True))

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == SubscriptionPaymentRequestResponse.opcode

    async def build_block(self, block: Block | CallContractBlock, other_blocks: list[Block]) -> list[Block]:
        new_block = SubscriptionBlock({})
        subscriber = AccountId(block.get_message().source)
        subscription = AccountId(block.get_message().destination)
        amount = Amount(block.get_message().value)
        failed = False
        subscription_payment = find_call_contract(other_blocks, SubscriptionPayment.opcode)
        beneficiary = AccountId(subscription_payment.get_message().destination)

        payment_request = find_call_contract(other_blocks, SubscriptionPaymentRequest.opcode)
        if payment_request is not None:
            payment_request_data = SubscriptionPaymentRequest(payment_request.get_body())
            amount = Amount(payment_request_data.grams)
            failed = payment_request.failed
        new_block.data = {
            'subscriber': subscriber,
            'subscription': subscription,
            'beneficiary': beneficiary,
            'amount': amount
        }
        new_block.failed = failed
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


class UnsubscribeBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=ContractMatcher(opcode=WalletPluginDestruct.opcode, optional=True))

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == WalletPluginDestruct.opcode

    async def build_block(self, block: Block | CallContractBlock, other_blocks: list[Block]) -> list[Block]:
        new_block = UnsubscribeBlock({})
        data = {
            'subscriber': AccountId(block.get_message().source),
            'subscription': AccountId(block.get_message().destination),
            'beneficiary': None
        }
        response = find_call_contract(other_blocks, WalletPluginDestruct.opcode)
        if response is not None:
            data['beneficiary'] = AccountId(response.get_message().destination)
        new_block.data = data
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]
