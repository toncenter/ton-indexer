import base64
from collections import defaultdict
from collections.abc import Iterable
from typing import Callable

from indexer.core.database import TransactionMessage, Transaction, Event


class EventNode:
    def __init__(self, message: TransactionMessage, children: list['EventNode']):
        self.message = message
        self.parent = None
        self.children = children
        self.handled = False
        self.emulated = message.transaction.emulated
        self.failed = message.transaction.description["aborted"]
        if message.message.message_content is None:
            raise Exception(
                "Message content is None for " + message.message_hash + " - tx: " + message.transaction_hash)

    def get_opcode(self):
        if self.message.message.opcode is not None:
            return self.message.message.opcode & 0xFFFFFFFF
        else:
            return None
        # return self.message.message.opcode & 0xFFFFFFFF

    def set_parent(self, parent: 'EventNode'):
        self.parent = parent

    def add_child(self, child: 'EventNode'):
        self.children.append(child)
        child.set_parent(self)

    def mark_handled(self):
        print("Marking as handled: ", self.message.transaction_hash,
              base64.b64decode(self.message.transaction_hash).hex())
        self.handled = True

    def find_children_bfs(self, opcode, only_not_handled=False, max_depth=999) -> Iterable['EventNode']:
        # bfs search for opcode
        queue = list([(c, 0) for c in self.children])
        while len(queue) > 0:
            node, depth = queue.pop(0)
            if node.get_opcode() == opcode and (not only_not_handled or not node.handled):
                yield node
            if depth + 1 <= max_depth:
                queue.extend([(c, depth + 1) for c in node.children_blocks])

    def find_children(self,
                      node_filter: Callable[['EventNode', int], bool] = None,
                      max_depth=-1,
                      stop_on_filter_unmatch: bool = False) -> Iterable['EventNode']:
        queue = list([(c, 0) for c in self.children])
        while len(queue) > 0:
            node, depth = queue.pop(0)
            filter_matched = node_filter is None or node_filter(node, depth)
            if filter_matched:
                yield node
            should_extend_queue = (depth + 1 <= max_depth or max_depth < 0)
            if stop_on_filter_unmatch:
                should_extend_queue = should_extend_queue and filter_matched
            if should_extend_queue:
                queue.extend([(c, depth + 1) for c in node.children_blocks])

    def get_previous_nodes(self, opcodes) -> list['EventNode']:
        """Get all nodes from root to current node, until message with matching opcode is found(included in list)"""
        # TODO: better name
        path = []
        node = self
        while node is not None:
            path.append(node)
            if node.get_opcode() in opcodes:
                return path
            node = node.parent
        return []

    def get_ancestor(self, opcode) -> 'EventNode':
        node = self
        while node is not None:
            if node.get_opcode() == opcode:
                return node
            node = node.parent
        return None


def to_tree(txs: list[Transaction], event: Event):
    """
    Constructs a tree representation of transactions (txs) related to an event, based on connections
    between transactions. Each node is message initiated transaction
    """

    tx_map = {tx.hash: tx for tx in txs}  # Convert to dictionary for faster lookup
    conn_map = defaultdict(list)
    for edge in event.edges:
        conn_map[edge.left_tx_hash].append(edge.right_tx_hash)

    def create_node(tx_hash: str) -> EventNode:
        """Helper function to create an EventNode from a transaction hash."""
        tx = tx_map[tx_hash]
        message = next(m for m in tx.messages if m.direction == "in")
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
            used_messages.add(node_map[right_tx_hash].message.message_hash)

            if right_tx_hash not in conn_map:
                for m in tx_map[right_tx_hash].messages:
                    if m.direction == "out":
                        node_map[right_tx_hash].add_child(EventNode(m, []))

        # Add outgoing messages that haven't been used yet
        for m in tx_map[left_tx_hash].messages:
            if m.direction == "out" and m.message_hash not in used_messages:
                node_map[left_tx_hash].add_child(EventNode(m, []))

    # Find root of the tree
    while root.parent is not None:
        root = root.parent
    return root


def not_handled_nodes() -> Callable[[EventNode, int], bool]:
    return lambda node, depth: not node.handled


def with_opcode(opcodes: set[int]) -> Callable[[EventNode, int], bool]:
    return lambda node, depth: node.get_opcode() in opcodes
