import logging

from sqlalchemy import select

from event_classifier import ClassifierTask
from indexer.core.database import SyncSessionMaker, Message, Trace

from indexer.events.blocks.messages.cocoon import (
    CocoonPayoutPayload,
    CocoonLastPayoutPayload,
    CocoonRegisterProxy,
    CocoonUnregisterProxy,
    CocoonExtProxyPayoutRequest,
    CocoonExtProxyIncreaseStake,
    CocoonExtClientTopUp,
    CocoonOwnerClientRegister,
    CocoonOwnerClientChangeSecretHash,
    CocoonOwnerClientWithdraw,
    CocoonOwnerClientRequestRefund,
    CocoonChargePayload,
    CocoonGrantRefundPayload,
)

logger = logging.getLogger(__name__)


def _normalize_opcodes(opcodes: list[int]) -> list[int]:
    return [o if o <= 0x7FFFFFFF else o - 2**32 for o in opcodes]


def schedule_cocoon_actions_reclassification():
    logger.info("Starting COCOON actions reclassification")

    opcodes = [
        # cocoon_worker_payout
        CocoonPayoutPayload.opcode,           # 0xa040ad28
        CocoonLastPayoutPayload.opcode,       # 0xf5f26a36
        # cocoon_proxy_payout
        CocoonExtProxyPayoutRequest.opcode,   # 0x7610e6eb
        # cocoon_proxy_charge
        CocoonChargePayload.opcode,           # 0xbb63ff93
        # cocoon_client_top_up
        CocoonExtClientTopUp.opcode,          # 0xf172e6c2
        # cocoon_register_proxy
        CocoonRegisterProxy.opcode,           # 0x927c7cb5
        # cocoon_unregister_proxy
        CocoonUnregisterProxy.opcode,         # 0x6d49eaf2
        # cocoon_client_register
        CocoonOwnerClientRegister.opcode,     # 0xc45f9f3b
        # cocoon_client_change_secret_hash
        CocoonOwnerClientChangeSecretHash.opcode,  # 0xa9357034
        # cocoon_client_request_refund
        CocoonOwnerClientRequestRefund.opcode,     # 0xfafa6cc1
        # cocoon_grant_refund
        CocoonGrantRefundPayload.opcode,      # 0xefd711e1
        # cocoon_client_increase_stake
        CocoonExtProxyIncreaseStake.opcode,   # 0xda068e78
        # cocoon_client_withdraw
        CocoonOwnerClientWithdraw.opcode,     # 0x9713f187
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

    logger.info("COCOON reclassification scheduling complete")


if __name__ == '__main__':
    schedule_cocoon_actions_reclassification()
