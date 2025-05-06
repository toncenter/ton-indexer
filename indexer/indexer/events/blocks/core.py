from __future__ import annotations

from typing import Callable, Iterable

from pytoniq_core import Slice

from indexer.core.database import Message
from indexer.events.blocks.utils import AccountId
from indexer.events.blocks.utils.tree_utils import EventNode


def _get_direction_for_block(block: Block) -> int:
    if len(block.event_nodes) == 1:
        return 1 if block.event_nodes[0].message.direction == "out" else 0
    return 2


def _ensure_earliest_common_block(blocks: list[Block]) -> Block | None:
    """Ensures that all blocks have a common block in their previous blocks, and returns it."""
    # find block with min min_lt
    blocks = sorted(blocks, key=lambda b: (b.min_lt_without_initiating_tx, _get_direction_for_block(b)))
    earliest_node_in_block = blocks[0]
    connected = [earliest_node_in_block]
    for block in blocks:
        if block in connected:
            continue
        else:
            if block.previous_block is not None:
                if block.previous_block in connected:
                    connected.append(block)
                    continue
                else:
                    return None
    return earliest_node_in_block


class AccountFlow:
    ton: int
    fees: int
    jettons: dict[AccountId, int]

    def __init__(self):
        self.ton = 0
        self.fees = 0
        self.jettons = {}

    def merge(self, other: AccountFlow):
        self.ton += other.ton
        self.fees += other.fees
        for jetton, amount in other.jettons.items():
            if jetton not in self.jettons:
                self.jettons[jetton] = 0
            self.jettons[jetton] += amount

    def to_dict(self):
        return {
            'ton': str(self.ton),
            'fees': str(self.fees),
            'jettons': {str(jetton): str(amount) for jetton, amount in self.jettons.items()}
        }


class AccountValueFlow:
    flow: dict[AccountId, AccountFlow]

    def __init__(self):
        self.flow = {}

    def to_dict(self):
        return {
            'flow': {str(account): flow.to_dict() for account, flow in self.flow.items()}
        }

    def add_ton(self, account: AccountId, amount: int):
        if account not in self.flow:
            self.flow[account] = AccountFlow()
        self.flow[account].ton += amount

    def add_jetton(self, account: AccountId, jetton: AccountId, amount: int):
        if account not in self.flow:
            self.flow[account] = AccountFlow()
        if jetton not in self.flow[account].jettons:
            self.flow[account].jettons[jetton] = 0
        self.flow[account].jettons[jetton] += amount

    def add_fees(self, account: AccountId, amount: int):
        if account not in self.flow:
            self.flow[account] = AccountFlow()
        self.flow[account].fees += amount

    def merge(self, other: AccountValueFlow):
        for account, flow in other.flow.items():
            if account not in self.flow:
                self.flow[account] = AccountFlow()
            self.flow[account].merge(flow)


class Block:
    event_nodes: list[EventNode]
    children_blocks: list[Block]
    next_blocks: list[Block]
    contract_deployments: set[AccountId]
    initiating_event_node: EventNode | None
    failed: bool
    previous_block: Block
    parent: Block
    data: any
    min_lt: int
    min_utime: int
    max_utime: int
    max_lt: int
    min_lt_without_initiating_tx: int
    type: str
    value_flow: AccountValueFlow
    transient: bool
    is_ghost_block: bool # Ghost block is a block that represents an intended but not started operation

    def __init__(self, type: str, nodes: list[EventNode], v=None):
        self.failed = False
        self.broken = False
        self.transient = False
        self.event_nodes = nodes
        self.children_blocks = []
        self.next_blocks = []
        self.previous_block = None
        self.parent = None
        self.btype = type
        self.data = v
        self.contract_deployments = set()
        self.initiating_event_node = None
        self.value_flow = AccountValueFlow()
        self.is_ghost_block = False
        if len(nodes) == 1 and nodes[0].ghost_node:
            self.is_ghost_block = True
        if len(nodes) != 0:
            self.min_lt = nodes[0].get_lt()
            self.max_lt = nodes[0].message.transaction.lt
            self.min_utime = nodes[0].get_utime()
            self.max_utime = nodes[0].message.transaction.now
            self.min_lt_without_initiating_tx = nodes[0].message.transaction.lt
            self._find_contract_deployments()
        if len(nodes) == 1:
            parent = nodes[0].parent
            self.initiating_event_node = parent

        else:
            self.min_lt = 0
            self.max_lt = 0
            self.min_utime = 0
            self.max_utime = 0
            self.min_lt_without_initiating_tx = 0

    def calculate_min_max_lt(self):
        self.min_lt = min(n.get_lt() for n in self.event_nodes)
        self.min_lt_without_initiating_tx = self.min_lt
        self.min_utime = min(n.get_utime() for n in self.event_nodes)

        self.max_lt = max(n.message.transaction.lt for n in self.event_nodes)
        self.max_utime = max(n.message.transaction.now for n in self.event_nodes)

    def iter_prev(self, predicate: Callable[[Block], bool]) -> Iterable[Block]:
        """Iterates over all previous blocks that match predicate, starting from the closest one."""
        r = self.previous_block
        while r is not None:
            if predicate(r):
                yield r
                r = r.previous_block
            else:
                return
        return

    def any_parent(self, predicate: Callable[[Block], bool]) -> bool:
        """Checks if any of the previous blocks matches predicate."""
        r = self.previous_block
        while r is not None:
            if filter(r):
                return True
            else:
                r = r.previous_block
        return False

    def merge_blocks(self, blocks: list[Block]):
        # blocks_to_merge = []
        # for block in blocks:
        #     if isinstance(block, SingleLevelWrapper):
        #         blocks_to_merge.extend(block.children_blocks)
        #     else:
        #         blocks_to_merge.append(block)
        """Merges all blocks into one. Preserves structure"""
        blocks_to_merge = [b for b in set(blocks) if b.transient is False]
        earliest_block = _ensure_earliest_common_block(blocks_to_merge)
        if earliest_block is None:
            raise "Earliest common block not found"
        for block in blocks_to_merge:
            block.parent = self
            self.event_nodes.extend(block.event_nodes)
            self.children_blocks.append(block)
            self.value_flow.merge(block.value_flow)
        for block in earliest_block.find_next(lambda b, d: b in blocks_to_merge, stop_on_filter_unmatch=True,
                                              yield_on_unmatch=True):
            self.next_blocks.append(block)
            self.contract_deployments = self.contract_deployments.union(block.contract_deployments)
            block.previous_block = self
        self.previous_block = earliest_block.previous_block
        if earliest_block.previous_block is not None:
            earliest_block.previous_block.compact_connections()
        self.initiating_event_node = earliest_block.initiating_event_node
        self.calculate_min_max_lt()
        for node in self.event_nodes:
            if node.ghost_node:
                self.is_ghost_block = True
                break

    def find_next(self,
                  node_filter: Callable[['Block', int], bool] = None,
                  max_depth=-1,
                  stop_on_filter_unmatch: bool = False,
                  yield_on_unmatch: bool = False) -> Iterable['Block']:
        """Iterates over all next blocks that match predicate, starting from the closest one."""
        queue = list([(c, 0) for c in self.next_blocks])
        while len(queue) > 0:
            node, depth = queue.pop(0)
            filter_matched = node_filter is None or node_filter(node, depth)
            if filter_matched and not yield_on_unmatch:
                yield node
            elif not filter_matched and yield_on_unmatch:
                yield node
            should_extend_queue = (depth + 1 <= max_depth or max_depth < 0)
            if stop_on_filter_unmatch:
                should_extend_queue = should_extend_queue and filter_matched
            if should_extend_queue:
                queue.extend([(c, depth + 1) for c in node.next_blocks])

    def get_body(self) -> Slice:
        return Slice.one_from_boc(self.event_nodes[0].message.message_content.body)

    def get_message(self) -> Message:
        return self.event_nodes[0].message

    def connect(self, other: 'Block'):
        self.next_blocks.append(other)
        other.previous_block = self

    def insert_between(self, next_blocks: ['Block'], new_block: 'Block'):
        assert all(n in self.next_blocks for n in next_blocks)
        for child in self.children_blocks:
            for next_block in next_blocks:
                if next_block in child.next_blocks:
                    child.next_blocks.remove(next_block)
                    child.next_blocks.append(new_block)
        self.next_blocks = [n for n in self.next_blocks if n not in next_blocks]
        for next_block in next_blocks:
            for child in next_block.children_blocks:
                if child.previous_block == next_block:
                    child.previous_block = new_block
        self.connect(new_block)
        for next_block in next_blocks:
            new_block.connect(next_block)

    def topmost_parent(self):
        if self.parent is None:
            return self
        else:
            return self.parent.topmost_parent()

    def compact_connections(self):
        self.next_blocks = list(
            set(n.topmost_parent() for n in self.next_blocks if n not in self.children_blocks and n != self))

    def set_prev(self, prev: 'Block'):
        self.previous_block = prev

    def __repr__(self):
        return f"!{self.btype}:={self.data}"

    def bfs_iter(self, include_self=True):
        if include_self:
            queue = [self]
        else:
            queue = [n for n in self.next_blocks]
        while len(queue) > 0:
            cur = queue.pop(0)
            yield cur
            queue.extend(cur.next_blocks)

    def dict(self):
        return {
            "btype": self.btype,
            "nodes": [n.data for n in self.event_nodes],
            "children": [c.dict() for c in self.children_blocks],
            "next": [n.dict() for n in self.next_blocks],
            "value": self.data
        }

    def calculate_progress(self):
        total = 0
        total_emulated = 0
        for node in self.event_nodes:
            if node.message.destination is not None and node.message.source is not None:
                total += 1
                if node.message.transaction.emulated:
                    total_emulated += 1
        return (1.0 - total_emulated / total) if total != 0 else 0

    def _find_contract_deployments(self):
        for node in self.event_nodes:
            if node.message.transaction.orig_status != 'active' and node.message.transaction.end_status == 'active':
                self.contract_deployments.add(AccountId(node.message.transaction.account))

class SingleLevelWrapper(Block):
    def __init__(self):
        super().__init__('single_wrapper', [], None)

    def wrap(self, blocks: list[Block]):
        block_queue = blocks.copy()
        nodes = []
        while len(block_queue) > 0:
            block = block_queue.pop(0)
            if isinstance(block, SingleLevelWrapper):
                block_queue.extend(block.children_blocks)
                continue
            self.children_blocks.append(block)
            for next_block in block.next_blocks:
                if next_block not in blocks:
                    self.next_blocks.append(next_block)
            nodes.extend(block.event_nodes)
            if block.previous_block not in blocks:
                self.previous_block = block.previous_block
        self.event_nodes = list(set(nodes))

        self.compact_connections()
        self.calculate_min_max_lt()

class EmptyBlock(Block):
    def __init__(self):
        super().__init__('empty', [], None)
