import logging

from sqlalchemy import select

from event_classifier import ClassifierTask
from indexer.core.database import SyncSessionMaker, Message, Trace

from indexer.events.blocks.messages.layerzero import (
    EndpointEndpointSend,
    ChannelChannelSend,
    LayerZeroEventMsgBody,
    LayerzeroLzReceivePrepare,
    LayerzeroLzReceiveExecute,
    UlnUlnCommitPacket,
    EndpointEndpointCommitPacket,
    DvnVerify,
    UlnconnectionUlnConnectionVerify,
)

logger = logging.getLogger(__name__)


def _normalize_opcodes(opcodes: list[int]) -> list[int]:
    return [o if o <= 0x7FFFFFFF else o - 2**32 for o in opcodes]


def schedule_layerzero_actions_reclassification():
    logger.info("Starting LayerZero actions reclassification")

    opcodes = [
        # layerzero_send, layerzero_send_tokens
        EndpointEndpointSend.opcode,
        ChannelChannelSend.opcode,
        # layerzero_receive
        LayerzeroLzReceivePrepare.opcode,
        LayerzeroLzReceiveExecute.opcode,
        # layerzero_commit_packet
        UlnUlnCommitPacket.opcode,
        EndpointEndpointCommitPacket.opcode,
        # layerzero_dvn_verify
        DvnVerify.opcode,
        UlnconnectionUlnConnectionVerify.opcode,
        # just all layerzero actions
        LayerZeroEventMsgBody.opcode,
    ]

    normalized_opcodes = _normalize_opcodes(opcodes)
    logger.debug(f"processing {len(opcodes)} opcodes for reclassification")

    with SyncSessionMaker() as session:
        query = (
            select(Message.trace_id)
            .filter(Message.opcode.in_(normalized_opcodes))
            .filter(Message.direction == 'in')
        )
        trace_ids = session.execute(query).all()

    trace_ids = list(set([tid[0] for tid in trace_ids]))
    logger.info(f"found {len(trace_ids)} unique trace_ids to process")

    BATCH_SIZE = 10000
    with SyncSessionMaker() as session:
        for i in range(0, len(trace_ids), BATCH_SIZE):
            batch = trace_ids[i : i + BATCH_SIZE]
            batch_end = min(i + BATCH_SIZE, len(trace_ids))
            q = session.query(Trace.trace_id, Trace.mc_seqno_end).filter(Trace.trace_id.in_(batch))
            batch_results = q.all()
            tasks = [ClassifierTask(trace_id=t[0], mc_seqno=t[1]) for t in batch_results]
            session.add_all(tasks)
            session.commit()
            logger.info(f"processed {batch_end} of {len(trace_ids)} trace_ids")

    logger.info("LayerZero reclassification scheduling complete")


if __name__ == '__main__':
    schedule_layerzero_actions_reclassification()
