from __future__ import annotations

import base64
from collections import defaultdict
from collections.abc import Iterable
from typing import Callable

from indexer.core.database import Transaction, Message, Trace, TraceEdge


class NoMessageBodyException(Exception):
    def __init__(self, message):
        super().__init__(message)


class EventNode:
    def __init__(self, message: Message, children: list['EventNode'], is_tick_tock: bool = False,
                 tick_tock_tx: Transaction = None):
        self.message = message
        self.is_tick_tock = is_tick_tock
        self.tick_tock_tx = tick_tock_tx
        self.parent = None
        self.children = children
        self.handled = False
        if not is_tick_tock:
            self.emulated = message.transaction.emulated
            self.failed = message.transaction.aborted
        else:
            self.message = Message(msg_hash=tick_tock_tx.hash, direction='in', transaction=tick_tock_tx)
            self.emulated = tick_tock_tx.emulated
            self.failed = tick_tock_tx.aborted
        if not is_tick_tock and message.message_content is None:
            raise NoMessageBodyException(
                "Message content is None for " + message.msg_hash + " - tx: " + message.tx_hash)

    def get_type(self):
        if self.message.destination is None:
            return 'notification'
        elif self.message.source is None:
            return 'external'
        else:
            return 'internal'

    def get_opcode(self):
        if self.message.opcode is not None:
            return self.message.opcode & 0xFFFFFFFF
        else:
            return None
        # return self.message.message.opcode & 0xFFFFFFFF

    def set_parent(self, parent: 'EventNode'):
        self.parent = parent

    def add_child(self, child: 'EventNode'):
        self.children.append(child)
        child.set_parent(self)

    def get_tx_hash(self):
        if self.is_tick_tock and self.tick_tock_tx is not None:
            return self.tick_tock_tx.hash
        elif self.message is not None:
            return self.message.tx_hash
        else:
            return None


def to_tree(txs: list[Transaction]):
    txs = sorted(txs, key=lambda tx: tx.lt, reverse=True)
    msg_tx = {}
    nodes = {}
    def create_node(tx: Transaction) -> EventNode:
        """Helper function to create an EventNode from a transaction hash."""
        message = next((m for m in tx.messages if m.direction == "in"), None)

        if message is None and tx.descr == "tick_tock":
            return EventNode(None, [], is_tick_tock=True, tick_tock_tx=tx)
        msg_tx[message.msg_hash] = tx.hash
        return EventNode(message, [])

    for tx in txs:
        if tx.hash not in nodes:
            nodes[tx.hash] = create_node(tx)
            for m in tx.messages:
                if m.direction == "out":
                    if m.destination is None:
                        nodes[tx.hash].add_child(EventNode(m, []))
                    else:
                        assert m.msg_hash in msg_tx
                        nodes[tx.hash].add_child(nodes[msg_tx[m.msg_hash]])
    root = nodes[txs[-1].hash]
    while root.parent is not None:
        root = root.parent
    return root



def not_handled_nodes() -> Callable[[EventNode, int], bool]:
    return lambda node, depth: not node.handled


def with_opcode(opcodes: set[int]) -> Callable[[EventNode, int], bool]:
    return lambda node, depth: node.get_opcode() in opcodes
