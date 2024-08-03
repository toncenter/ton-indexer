import base64
from collections import defaultdict
from collections.abc import Iterable
from typing import Callable

from indexer.core.database import Transaction, Message, Trace


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


def to_tree(txs: list[Transaction], trace: Trace):
    """
    Constructs a tree representation of transactions (txs) related to an event, based on connections
    between transactions. Each node is message initiated transaction
    """

    tx_map = {tx.hash: tx for tx in txs}  # Convert to dictionary for faster lookup
    conn_map = defaultdict(list)
    for edge in trace.edges:
        if edge.left_tx is None:
            continue
        if edge.right_tx is None:
            continue
        conn_map[edge.left_tx].append(edge.right_tx)

    def create_node(tx_hash: str) -> EventNode:
        """Helper function to create an EventNode from a transaction hash."""
        tx = tx_map[tx_hash]
        message = next((m for m in tx.messages if m.direction == "in"), None)

        if message is None and tx.descr == "tick_tock":
            return EventNode(None, [], is_tick_tock=True, tick_tock_tx=tx)
        return EventNode(message, [])

    node_map = {}
    root = None  # Initialize root node
    if len(conn_map.keys()) == 0:
        root = create_node(txs[0].hash)

    for left_tx_hash in conn_map:
        if left_tx_hash not in node_map:
            node_map[left_tx_hash] = create_node(left_tx_hash)
            if root is None:  # Set the first node as the potential root
                root = node_map[left_tx_hash]

        used_messages = set()
        for right_tx_hash in conn_map[left_tx_hash]:
            if right_tx_hash not in node_map:
                node_map[right_tx_hash] = create_node(right_tx_hash)

            node_map[left_tx_hash].add_child(node_map[right_tx_hash])
            used_messages.add(node_map[right_tx_hash].message.msg_hash)

            if right_tx_hash not in conn_map:
                for m in tx_map[right_tx_hash].messages:
                    if m.direction == "out":
                        node_map[right_tx_hash].add_child(EventNode(m, []))

        # Add outgoing messages that haven't been used yet
        for m in tx_map[left_tx_hash].messages:
            if m.direction == "out" and m.msg_hash not in used_messages:
                node_map[left_tx_hash].add_child(EventNode(m, []))

    # Find root of the tree
    while root.parent is not None:
        root = root.parent
    return root


def not_handled_nodes() -> Callable[[EventNode, int], bool]:
    return lambda node, depth: not node.handled


def with_opcode(opcodes: set[int]) -> Callable[[EventNode, int], bool]:
    return lambda node, depth: node.get_opcode() in opcodes
