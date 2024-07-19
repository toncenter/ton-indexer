from __future__ import annotations
from typing import TypeVar, Type

from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.core import Block, AccountFlow, AccountValueFlow
from indexer.events.blocks.utils import EventNode

T = TypeVar('T')


def find_call_contracts(blocks: list[Block], opcode: int | set) -> list[CallContractBlock]:
    if isinstance(opcode, int):
        return [b for b in blocks if isinstance(b, CallContractBlock) and b.opcode == opcode]
    else:
        return [b for b in blocks if isinstance(b, CallContractBlock) and b.opcode in opcode]


def find_call_contract(blocks: list[Block], opcode: int) -> CallContractBlock | None:
    for b in blocks:
        if isinstance(b, CallContractBlock) and b.opcode == opcode:
            return b
    return None


def find_messages(blocks: list[Block], message_class: Type[T]) -> list[tuple[Block, T]]:
    return [(b, message_class(b.get_body())) for b in find_call_contracts(blocks, message_class.opcode)]


def merge_flows(blocks: list[Block]) -> AccountValueFlow:
    flow = AccountValueFlow()
    for block in blocks:
        flow.merge(block.value_flow)
    return flow
