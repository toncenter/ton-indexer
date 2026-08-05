import logging

from sqlalchemy import select

from event_classifier import ClassifierTask
from indexer.core.database import SyncSessionMaker, Message, Trace

from indexer.events.blocks.messages.staking import (
    HipoBurnTokens,
    HipoDepositCoins,
    HipoMintTokens,
    HipoProxyReserveTokens,
    HipoProxySaveCoins,
    HipoProxyTokensBurned,
    HipoProxyTokensMinted,
)

logger = logging.getLogger(__name__)


def _normalize_opcodes(opcodes: list[int]) -> list[int]:
    return [o if o <= 0x7FFFFFFF else o - 2**32 for o in opcodes]


def schedule_hipo_actions_reclassification():
    logger.info("Starting Hipo actions reclassification")

    # Every Hipo staking trace contains at least one of these, and each of them is a
    # protocol-internal op that no other contract uses:
    #   deposit_coins          - head of both stake flows
    #   proxy_save_coins       - deferred stake
    #   proxy_tokens_minted    - instant stake and the round-end deposit completion
    #   proxy_reserve_tokens   - head of both unstake flows
    #   proxy_tokens_burned    - instant unstake and the round-end withdrawal completion
    #   mint_tokens/burn_tokens- round-end settlement of a bill
    opcodes = [
        HipoDepositCoins.opcode,          # 0x3d3761a6
        HipoProxySaveCoins.opcode,        # 0x47daa10f
        HipoProxyTokensMinted.opcode,     # 0x5be57626
        HipoProxyReserveTokens.opcode,    # 0x688b0213
        HipoProxyTokensBurned.opcode,     # 0x4476fde0
        HipoMintTokens.opcode,            # 0x42684479
        HipoBurnTokens.opcode,            # 0x7cffe1ee
    ]
    logger.debug(f"Processing {len(opcodes)} opcodes")
    normalized_opcodes = _normalize_opcodes(opcodes)

    with SyncSessionMaker() as session:
        query = (
            select(Message.trace_id)
            .filter(Message.opcode.in_(normalized_opcodes))
            .filter(Message.direction == 'in')
        )
        trace_ids = session.execute(query).all()

    trace_ids = list(set([tid[0] for tid in trace_ids]))
    logger.info(f"Found {len(trace_ids)} unique trace_ids to process")

    BATCH_SIZE = 10000

    with SyncSessionMaker() as session:
        for i in range(0, len(trace_ids), BATCH_SIZE):
            batch = trace_ids[i:i + BATCH_SIZE]
            batch_end = min(i + BATCH_SIZE, len(trace_ids))
            query = session.query(Trace.trace_id, Trace.mc_seqno_end).filter(Trace.trace_id.in_(batch))
            batch_results = query.all()
            tasks = [ClassifierTask(trace_id=t[0], mc_seqno=t[1]) for t in batch_results]
            session.add_all(tasks)
            session.commit()
            logger.info(f"Processed {batch_end} of {len(trace_ids)} trace_ids")

    logger.info(f"Processed {len(trace_ids)} trace_ids in batches of {BATCH_SIZE}")


if __name__ == '__main__':
    schedule_hipo_actions_reclassification()
