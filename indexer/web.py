import logging
import os, sys

from fastapi import FastAPI
from fastapi import Query

from indexer.database import Block, Transaction, Message, get_session, MASTERCHAIN_INDEX, MASTERCHAIN_SHARD
from dataclasses import asdict, is_dataclass
from sqlalchemy import and_
from sqlalchemy.orm import joinedload


logging.basicConfig(format='%(asctime)s %(module)-15s %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)


description = ''
app = FastAPI(
    title="TONdex",
    description=description,
    version='0.1.0',
    docs_url='/',
    # responses={
    #     422: {'description': 'Validation Error'},
    #     504: {'description': 'Lite Server Timeout'}
    # },
    # root_path='/api/v2',
)


@app.on_event("startup")
def startup():
    logger.info('Service started successfully')

@app.get('/getTransactionsByMasterchainSeqno')
def getTransactionsByMasterchainSeqno(
    seqno: int = Query(..., description="Masterchain seqno"),
    return_message_bodies: bool = Query(False, description="Return message bodies")
    ):
    Session = get_session()
    
    txs = []
    with Session(expire_on_commit=True) as session:
        block = session.query(Block).filter(and_(Block.workchain == MASTERCHAIN_INDEX, Block.shard == MASTERCHAIN_SHARD, Block.seqno == seqno)).first()
        block_ids = [block.block_id] + [x.block_id for x in block.shards]
        if return_message_bodies:
            txs = session.query(Transaction) \
                        .filter(Transaction.block_id.in_(block_ids)) \
                        .options(joinedload(Transaction.in_msg).joinedload(Message.content)) \
                        .options(joinedload(Transaction.out_msgs).joinedload(Message.content)) \
                        .all()
        else:
            txs = session.query(Transaction) \
                        .filter(Transaction.block_id.in_(block_ids)) \
                        .options(joinedload(Transaction.in_msg)) \
                        .options(joinedload(Transaction.out_msgs)) \
                        .all()

    return [tx.asdict() for tx in txs]
