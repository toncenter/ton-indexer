from __future__ import annotations

from pytoniq_core import Slice

from indexer.events.blocks.core import Block, AccountValueFlow
from indexer.events.blocks.utils import AccountId, Amount
from indexer.events.blocks.utils.tree_utils import EventNode
from indexer.core.database import TransactionMessage


def _fill_flow_from_node(flow: AccountValueFlow, node: EventNode):
    if node.message.message.value is not None:
        assert node.message.direction == "in"
        flow.add_ton(AccountId(node.message.message.destination), node.message.message.value)
        flow.add_ton(AccountId(node.message.message.source), -node.message.message.value)
        flow.add_fees(AccountId(node.message.message.destination), node.message.transaction.total_fees)
    elif node.message.direction == "in":
        flow.add_fees(AccountId(node.message.message.destination), node.message.transaction.total_fees)

    for msg in node.message.transaction.messages:
        if msg.message.fwd_fee is not None and msg.direction == "out":
            flow.add_fees(AccountId(msg.message.source), msg.message.fwd_fee)


class TonTransferBlock(Block):
    value: int

    def __init__(self, node: EventNode):
        super().__init__('ton_transfer', [node], {
            'source': AccountId(node.message.message.source) if node.message.message.source is not None else None,
            'destination': AccountId(
                node.message.message.destination) if node.message.message.destination is not None else None,
            'value': Amount(node.message.message.value),
        })
        self.failed = node.failed
        self.value = node.message.message.value
        _fill_flow_from_node(self.value_flow, node)


class ContractDeploy(Block):
    def __init__(self, node: EventNode):
        super().__init__('contract_deploy', [], node.message.transaction.account)
        self.failed = node.failed


class CallContractBlock(Block):
    opcode: int

    def __init__(self, node: EventNode):
        super().__init__('call_contract', [node], {
            'opcode': node.get_opcode(),
            'source': AccountId(node.message.message.source) if node.message.message.source is not None else None,
            'destination': AccountId(
                node.message.message.destination) if node.message.message.destination is not None else None,
            'value': Amount(node.message.message.value),
        })
        self.failed = node.failed
        self.is_external = node.message.message.source is None
        self.opcode = node.get_opcode()
        _fill_flow_from_node(self.value_flow, node)

    def get_body(self) -> Slice:
        return Slice.one_from_boc(self.event_nodes[0].message.message.message_content.body)

    def get_message(self) -> TransactionMessage:
        return self.event_nodes[0].message

    def __repr__(self):
        return f"!{self.btype}:={hex(self.opcode)}"
