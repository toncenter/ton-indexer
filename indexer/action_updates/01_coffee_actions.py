import logging

from sqlalchemy import select

from event_classifier import ClassifierTask
from indexer.core.database import SyncSessionMaker, Message, Trace

logger = logging.getLogger(__name__)

def schedule_coffee_actions_reclassification():
    logger.info("Starting coffee actions reclassification")
    opcodes = [
        0xF9471134,
        0xCB03BFAF,
        0xB30C7310,
        0xC0FFEE23,
        0xC0FFEE25,
        0xC0FFEE06,
        0x6BC79E7E,
        0xC0FFEE20,
        0xC0FFEE04,
        0xC0FFEE12,
        0xC0FFEE05,
    ]
    logger.debug(f"Processing {len(opcodes)} opcodes")
    normalized_opcodes = [ o if o <= 2147483647 else o - 2**32 for o in opcodes ]
    with SyncSessionMaker() as session:
        query = select(Message.trace_id) \
            .filter(Message.opcode.in_(normalized_opcodes)) \
            .filter(Message.direction == 'in')
        trace_ids = session.execute(query).all()
    
    trace_ids = list(set([tid[0] for tid in trace_ids]))
    logger.info(f"Found {len(trace_ids)} unique trace_ids to process")
    
    BATCH_SIZE = 10000
    all_results = []

    with SyncSessionMaker() as session:
        for i in range(0, len(trace_ids), BATCH_SIZE):
            batch = trace_ids[i:i + BATCH_SIZE]
            batch_end = min(i + BATCH_SIZE, len(trace_ids))
            query = session.query(Trace.trace_id, Trace.mc_seqno_end).filter(Trace.trace_id.in_(batch))
            batch_results = query.all()
            all_results.extend(batch_results)
            tasks = [ClassifierTask(trace_id=t[0], mc_seqno=t[1]) for t in batch_results]
            session.add_all(tasks)
            session.commit()
            logger.info(f"Processed {batch_end} of {len(trace_ids)} trace_ids")

    logger.info(f"Processed {len(trace_ids)} trace_ids in batches of {BATCH_SIZE}")
        

if __name__ == '__main__':
    schedule_coffee_actions_reclassification()