from __future__ import annotations

import base64

from pytoniq_core import Slice

from indexer.core.database import Message
from indexer.events.blocks.messages import TonTransferMessage
from indexer.events.blocks.core import Block, AccountValueFlow
from indexer.events.blocks.utils import AccountId, Amount
from indexer.events.blocks.utils.tree_utils import EventNode


def _fill_flow_from_node(flow: AccountValueFlow, node: EventNode):
    if node.message.value is not None:
        assert node.message.direction == "in"
        flow.add_ton(AccountId(node.message.destination), node.message.value)
        flow.add_ton(AccountId(node.message.source), -node.message.value)
        flow.add_fees(AccountId(node.message.destination), node.message.transaction.total_fees)
    elif node.message.direction == "in":
        flow.add_fees(AccountId(node.message.destination), node.message.transaction.total_fees)

    for msg in node.message.transaction.messages:
        if msg.fwd_fee is not None and msg.direction == "out":
            flow.add_fees(AccountId(msg.source), msg.fwd_fee)


class TonTransferBlock(Block):
    value: int
    comment: str | None
    encrypted: bool

    def __init__(self, node: EventNode):
        msg = TonTransferMessage(Slice.one_from_boc(node.message.message_content.body))
        self.encrypted = msg.encrypted
        self.comment_encoded = False
        if msg.comment is not None:
            if self.encrypted:
                self.comment_encoded = True
                self.comment = str(base64.b64encode(msg.comment), encoding='utf-8')
            else:
                try:
                    self.comment = str(msg.comment, encoding='utf-8')
                except Exception:
                    self.comment_encoded = True
                    self.comment = str(base64.b64encode(msg.comment), encoding='utf-8')
        else:
            self.comment = None

        super().__init__('ton_transfer', [node], {
            'source': AccountId(node.message.source) if node.message.source is not None else None,
            'destination': AccountId(
                node.message.destination) if node.message.destination is not None else None,
            'value': Amount(node.message.value),
            'comment': self.comment,
            'encrypted': self.encrypted,
        })
        self.failed = node.failed
        self.value = node.message.value

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
            'source': AccountId(node.message.source) if node.message.source is not None else None,
            'destination': AccountId(
                node.message.destination) if node.message.destination is not None else None,
            'value': Amount(node.message.value),
        })
        self.failed = node.failed
        self.is_external = node.message.source is None
        self.opcode = node.get_opcode()
        _fill_flow_from_node(self.value_flow, node)

    def get_body(self) -> Slice:
        return Slice.one_from_boc(self.event_nodes[0].message.message_content.body)

    def get_message(self) -> Message:
        return self.event_nodes[0].message

    def __repr__(self):
        return f"!{self.btype}:={hex(self.opcode)}"
