import logging
from dataclasses import dataclass
from typing import Optional, List

from indexer.core.database import Action, Trace
from indexer.events.blocks.utils.block_tree_serializer import serialize_blocks
from indexer.events.event_processing import (
    process_event_async_with_postprocessing,
    try_classify_unknown_trace,
    try_classify_basic_actions
)

logger = logging.getLogger(__name__)

@dataclass
class TraceProcessingResult:
    trace_id: str
    state: str  # 'ok', 'broken', 'failed'
    actions: List[Action]
    exception: Optional[Exception] = None


class TraceProcessor:
    def __init__(self,
                 use_unknown_fallback: bool = True):
        self.use_unknown_fallback = use_unknown_fallback

    async def process_trace(self, trace: Trace) -> TraceProcessingResult:
        # Early return for tick-tock transactions
        if self._is_tick_tock_transaction(trace):
            return TraceProcessingResult(
                trace_id=trace.trace_id,
                state='ok',
                actions=[]
            )

        try:
            # Core processing pipeline (identical across all modes)
            blocks = await process_event_async_with_postprocessing(trace)
            actions, state = serialize_blocks(blocks, trace.trace_id, trace)

            # Unknown trace fallback
            if len(actions) == 0 and len(trace.transactions) > 0 and self.use_unknown_fallback:
                actions = await try_classify_unknown_trace(trace)

            return TraceProcessingResult(
                trace_id=trace.trace_id,
                state=state,
                actions=actions
            )

        except Exception as e:
            logger.error("Marking trace as failed " + trace.trace_id + " - " + str(e))
            logger.exception(e, exc_info=True)

            # Try to create unknown action as fallback
            try:
                fallback_actions = await try_classify_basic_actions(trace)
            except Exception as ex:
                logger.error(f"Failed to classify basic actions for trace {trace.trace_id}: {ex}")
                logger.exception(ex)
                fallback_actions = []

            return TraceProcessingResult(
                trace_id=trace.trace_id,
                state='failed',
                actions=fallback_actions,
                exception=e
            )

    def _is_tick_tock_transaction(self, trace: Trace) -> bool:
        """Check if this is a tick-tock transaction"""
        return (len(trace.transactions) == 1 and
                trace.transactions[0].descr == 'tick_tock')
