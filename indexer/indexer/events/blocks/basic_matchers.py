from __future__ import annotations

import logging

from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.core import Block
from indexer.events.blocks.messages import ExcessMessage

logger = logging.getLogger(__name__)


class BlockMatcher:

    def __init__(self, child_matcher: BlockMatcher | None = None,
                 parent_matcher: BlockMatcher | None = None,
                 optional=False,
                 children_matchers=None,
                 include_excess=True):
        self.child_matcher = child_matcher
        self.children_matchers = children_matchers
        self.parent_matcher = parent_matcher
        self.optional = optional
        self.include_excess = include_excess

    def test_self(self, block: Block):
        return True

    async def try_build(self, block: Block) -> list[Block] | None:
        child_matched = True
        parent_matched = True
        self_matched = self.test_self(block)
        if not self_matched:
            return None
        blocks = []
        child_matched = await self.process_child_matcher(block, blocks, child_matched)
        parent_matched = await self.process_parent_matcher(block, blocks, parent_matched)
        if self_matched and parent_matched and child_matched:
            try:
                r = await self.build_block(block, blocks)
                if self.include_excess:
                    for next_block in block.next_blocks:
                        if isinstance(next_block, CallContractBlock) and next_block.opcode == ExcessMessage.opcode:
                            r.append(next_block)
                return r
            except Exception as e:
                logger.error(f"Error while building block {block} with matcher {self.__class__.__name__}: {block.event_nodes[0].message.tx_hash}")
                logger.exception(e, exc_info=True)
                return None
        else:
            return None

    async def process_parent_matcher(self, block, blocks, parent_matched):
        if self.parent_matcher is not None:
            matcher_parent_blocks = await self.parent_matcher.try_build(block.previous_block)
            if matcher_parent_blocks is not None:
                parent_matched = True
                blocks.extend(matcher_parent_blocks)
            else:
                parent_matched = self.parent_matcher.optional
        return parent_matched

    async def process_child_matcher(self, block, blocks, child_matched):
        if self.child_matcher is not None:
            for child in block.next_blocks:
                r = await self.child_matcher.try_build(child)
                if r is not None:
                    blocks.extend(r)
            child_matched = self.child_matcher.optional or len(blocks) > 0
        if self.children_matchers is not None:
            r = await self.process_children_matchers(block, blocks, child_matched)
            if r is not None:
                blocks.extend(r)
            else:
                child_matched = False
        return child_matched

    async def process_children_matchers(self, block, blocks, child):
        next_blocks = block.next_blocks.copy()
        remaining_matchers = self.children_matchers.copy()
        blocks = []
        while len(remaining_matchers) > 0:
            matcher = remaining_matchers[0]
            matched = False

            for next_block in next_blocks:
                res = await matcher.try_build(next_block)
                if res is not None:
                    blocks.extend(res)
                    remaining_matchers.pop(0)
                    next_blocks.remove(next_block)
                    matched = True
                    break
            if not matched:
                if matcher.optional:
                    remaining_matchers.pop(0)
                else:
                    return None

        if len(remaining_matchers) == 0:
            return blocks
        if all(m.optional for m in remaining_matchers):
            return blocks
        else:
            return None

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        return [block] + other_blocks


class OrMatcher(BlockMatcher):
    def __init__(self, matchers: list[BlockMatcher], optional=False):
        super().__init__(child_matcher=None, parent_matcher=None, optional=optional)
        self.matchers = matchers

    def test_self(self, block: Block):
        return any(m.test_self(block) for m in self.matchers)

    async def try_build(self, block: Block) -> list[Block] | None:
        for m in self.matchers:
            res = await m.try_build(block)
            if res is not None:
                return res
        return None


class TonTransferMatcher(BlockMatcher):

    def __init__(self):
        super().__init__(child_matcher=None, parent_matcher=None)

    def test_self(self, block: Block):
        return block.btype == "ton_transfer"


class ContractMatcher(BlockMatcher):
    def __init__(self, opcode,
                 child_matcher=None,
                 parent_matcher=None,
                 optional=False,
                 children_matchers=None,
                 include_excess=True):
        super().__init__(child_matcher, parent_matcher, optional, children_matchers, include_excess)
        self.opcode = opcode

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == self.opcode


class BlockTypeMatcher(BlockMatcher):
    def __init__(self, block_type, child_matcher=None, parent_matcher=None, optional=False):
        super().__init__(child_matcher, parent_matcher, optional)
        self.block_type = block_type

    def test_self(self, block: Block):
        return block.btype == self.block_type


def child_sequence_matcher(matchers: list[BlockMatcher]) -> BlockMatcher | None:
    if len(matchers) == 0:
        return None
    if len(matchers) == 1:
        return matchers[0]

    root_matcher = matchers[0]
    current_matcher = matchers[0]
    for matcher in matchers[1:]:
        current_matcher.child_matcher = matcher
        current_matcher = matcher
    return root_matcher
