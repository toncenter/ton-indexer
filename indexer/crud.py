from typing import Optional

from sqlalchemy import and_
from sqlalchemy.orm import joinedload, Session

from indexer.database import *
from dataclasses import asdict
from loguru import logger

# find functions
def find_object(session, cls, raw, key):
    fltr = [getattr(cls, k) == raw.get(k, None) for k in key]
    fltr = and_(*fltr)
    return session.query(cls).filter(fltr).first()


def find_or_create(session, cls, raw, key, **build_kwargs):
    return find_object(session, cls, raw, key) or cls.build(raw, **build_kwargs)

def get_existing_seqnos_from_list(session, seqnos):
    seqno_filters = [Block.seqno == seqno for seqno in seqnos]
    seqno_filters = or_(*seqno_filters)
    existing_seqnos = session.query(Block.seqno).\
                              filter(Block.workchain == MASTERCHAIN_INDEX).\
                              filter(Block.shard == MASTERCHAIN_SHARD).\
                              filter(seqno_filters).\
                              all()
    return [x[0] for x in existing_seqnos]

def get_existing_seqnos_between_interval(session, min_seqno, max_seqno):
    """
    Returns set of tuples of existing seqnos: {(19891542,), (19891541,), (19891540,)}
    """
    seqnos_already_in_db = session.query(Block.seqno).\
                                   filter(Block.workchain==MASTERCHAIN_INDEX).\
                                   filter(Block.shard == MASTERCHAIN_SHARD).\
                                   filter(Block.seqno >= min_seqno).\
                                   filter(Block.seqno <= max_seqno).\
                                   all()
    
    return set(seqnos_already_in_db)

def insert_block_data(session, block: Block, block_header_raw, block_transactions):
    # block header
    block_header = BlockHeader.build(block_header_raw, block=block)
    session.add(block_header)
    
    # block transactions
    txs = []
    msgs = []
    for tx_raw, tx_details_raw in block_transactions:
        tx = Transaction.build(tx_raw, tx_details_raw, block=block)
        session.add(tx)
        
        # messages
        if 'in_msg' in tx_details_raw:
            in_msg_raw = deepcopy(tx_details_raw['in_msg'])
            
            in_msg = find_or_create(session, 
                                    Message, 
                                    in_msg_raw, 
                                    ['source', 'destination', 'created_lt', 'body_hash', 'value', 'in_tx_id'])
            in_msg.in_tx = tx
            session.add(in_msg)
            
            in_msg_content = MessageContent.build(in_msg_raw, msg=in_msg)
            session.add(in_msg_content)

        for out_msg_raw in tx_details_raw['out_msgs']:
            out_msg = find_or_create(session, 
                                     Message, 
                                     out_msg_raw, 
                                     ['source', 'destination', 'created_lt', 'body_hash', 'value', 'out_tx_id'])
            out_msg.out_tx = tx
            session.add(out_msg)
            
            out_msg_content = MessageContent.build(out_msg_raw, msg=out_msg)
            session.add(out_msg_content)
    return block_header, txs, msgs


def insert_by_seqno(session, blocks_raw, headers_raw, transactions_raw):
    master_block = None
    for block_raw, header_raw, txs_raw in zip(blocks_raw, headers_raw, transactions_raw):
        block = None
        if master_block is not None:
            block = find_object(session, Block, block_raw, ['workchain', 'shard', 'seqno'])
        
        # building new block
        if block is None:
            block = Block.build(block_raw)
            session.add(block)
            insert_block_data(session, block, header_raw, txs_raw)
        else:
            logger.info(f'Found existsing block: {block}')
        
        # add shards
        if master_block is None:
            master_block = block
        else:
            master_block.shards.append(block)

def get_transactions_by_seqno(session, masterchain_seqno, include_msg_bodies):
    block = session.query(Block).filter(and_(Block.workchain == MASTERCHAIN_INDEX, Block.shard == MASTERCHAIN_SHARD, Block.seqno == masterchain_seqno)).first()
    if block is None:
        raise Exception(f"Block ({MASTERCHAIN_INDEX}, {MASTERCHAIN_SHARD}, {masterchain_seqno}) not found in DB")
    block_ids = [block.block_id] + [x.block_id for x in block.shards]
    if include_msg_bodies:
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
    return txs

def get_transactions_by_address(session: Session, account: str, start_utime: Optional[int], end_utime: Optional[int], limit: int, offset: int, sort: str):
    query = session.query(Transaction).filter(Transaction.account == account)
    if start_utime is not None:
        query = query.filter(Transaction.utime >= start_utime)
    if end_utime is not None:
        query = query.filter(Transaction.utime <= end_utime)
    
    if sort == 'asc':
        query = query.order_by(Transaction.utime.asc())
    elif sort == 'desc':
        query = query.order_by(Transaction.utime.desc())

    query = query.limit(limit)
    query = query.offset(offset)

    return query.all()

