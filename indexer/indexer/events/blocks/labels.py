from __future__ import annotations

from indexer.events.blocks.basic_matchers import BlockMatcher
from indexer.events.blocks.core import Block


class LabelBlock(Block):
    label: str
    block: Block

    def __init__(self, label: str, block: Block):
        self.label = label
        self.block = block
        super().__init__('label', [], None)
        self.transient = True

        self.event_nodes = block.event_nodes
        self.min_lt = block.min_lt
        self.min_utime = block.min_utime
        self.max_lt = block.max_lt
        self.max_utime = block.max_utime
        self.failed = block.failed

    def __repr__(self):
        return f"!{self.btype}:={self.label} ({self.block})"


class LabeledBlockConstructorMatcher(BlockMatcher):
    def __init__(self, block_type: str, test_self_func, **kwargs):
        self.block_type = block_type
        self.test_self_func = test_self_func
        super().__init__(**kwargs)

    def test_self(self, block: Block):
        return self.test_self_func(block)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        new_block = LabelBlock(self.block_type, block)
        return [new_block, block] + other_blocks


def labeled(label: str, matcher: BlockMatcher) -> BlockMatcher:
    return LabeledBlockConstructorMatcher(label,
                                          lambda x: matcher.test_self(x),
                                          child_matcher=matcher.child_matcher,
                                          children_matchers=matcher.children_matchers,
                                          parent_matcher=matcher.parent_matcher,
                                          optional=matcher.optional,
                                          include_excess=matcher.include_excess
                                          )
