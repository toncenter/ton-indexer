from typing import Optional
from collections import defaultdict

from sqlalchemy import and_
from sqlalchemy.orm import joinedload, Session, contains_eager
from sqlalchemy.future import select
from sqlalchemy import update


from indexer.database import *
from dataclasses import asdict
from loguru import logger

class DataNotFound(Exception):
    pass

class BlockNotFound(DataNotFound):
    def __init__(self, workchain, shard, seqno):
        self.workchain = workchain
        self.shard = shard
        self.seqno = seqno

    def __str__(self):
        return f"Block ({self.workchain}, {self.shard}, {self.seqno}) not found in DB"

class TransactionNotFound(DataNotFound):
    def __init__(self, lt, hash):
        self.lt = lt
        self.hash = hash

    def __str__(self):
        return f"Transaction ({self.lt}, {self.hash}) not found in DB"

async def get_existing_seqnos_from_list(session, seqnos):
    seqno_filters = [Block.seqno == seqno for seqno in seqnos]
    seqno_filters = or_(*seqno_filters)
    existing_seqnos = await session.execute(select(Block.seqno).\
                              filter(Block.workchain == MASTERCHAIN_INDEX).\
                              filter(Block.shard == MASTERCHAIN_SHARD).\
                              filter(seqno_filters))
    existing_seqnos = existing_seqnos.all()
    return [x[0] for x in existing_seqnos]

async def get_existing_seqnos_between_interval(session, min_seqno, max_seqno):
    """
    Returns set of tuples of existing seqnos: {(19891542,), (19891541,), (19891540,)}
    """
    seqnos_already_in_db = await session.execute(select(Block.seqno).\
                                   filter(Block.workchain==MASTERCHAIN_INDEX).\
                                   filter(Block.shard == MASTERCHAIN_SHARD).\
                                   filter(Block.seqno >= min_seqno).\
                                   filter(Block.seqno <= max_seqno))
    seqnos_already_in_db = seqnos_already_in_db.all()
    return set(seqnos_already_in_db)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

async def insert_by_seqno_core(session, blocks_raw, headers_raw, transactions_raw):
    meta = Base.metadata
    block_t = meta.tables[Block.__tablename__]
    block_headers_t = meta.tables[BlockHeader.__tablename__]
    transaction_t = meta.tables[Transaction.__tablename__]
    message_t = meta.tables[Message.__tablename__]
    message_content_t = meta.tables[MessageContent.__tablename__]

    async with engine.begin() as conn:
        mc_block_id = None
        shard_headers = []
        in_msgs_by_hash = defaultdict(list)
        out_msgs_by_hash = defaultdict(list)
        msg_contents_by_hash = {}
        for block_raw, header_raw, txs_raw in zip(blocks_raw, headers_raw, transactions_raw):
            s_block = Block.raw_block_to_dict(block_raw)
            s_block['masterchain_block_id'] = mc_block_id

            res = await conn.execute(block_t.insert(), [s_block])
            block_id = res.inserted_primary_key[0]
            if mc_block_id is None:
                mc_block_id = block_id

            s_header = BlockHeader.raw_header_to_dict(header_raw)
            s_header['block_id'] = block_id
            shard_headers.append(s_header)

            for tx_raw, tx_details_raw in txs_raw:
                tx = Transaction.raw_transaction_to_dict(tx_raw, tx_details_raw)
                tx['block_id'] = block_id
                res = await conn.execute(transaction_t.insert(), [tx])

                if 'in_msg' in tx_details_raw:
                    in_msg_raw = tx_details_raw['in_msg']
                    in_msg = Message.raw_msg_to_dict(in_msg_raw)
                    in_msg['in_tx_id'] = res.inserted_primary_key[0]
                    in_msg['out_tx_id'] = None
                    in_msgs_by_hash[in_msg['hash']].append(in_msg)
                    msg_contents_by_hash[in_msg['hash']] = MessageContent.raw_msg_to_content_dict(in_msg_raw)
                for out_msg_raw in tx_details_raw['out_msgs']:
                    out_msg = Message.raw_msg_to_dict(out_msg_raw)
                    out_msg['out_tx_id'] = res.inserted_primary_key[0]
                    out_msg['in_tx_id'] = None
                    out_msgs_by_hash[out_msg['hash']].append(out_msg)
                    msg_contents_by_hash[out_msg['hash']] = MessageContent.raw_msg_to_content_dict(out_msg_raw)

        await conn.execute(block_headers_t.insert(), shard_headers)

        for in_msg_hash, in_msgs_list in in_msgs_by_hash.items():
            if in_msg_hash in out_msgs_by_hash:
                assert len(in_msgs_list) == 1, "Multiple inbound messages match outbound message"
                in_msg = in_msgs_list[0]
                assert len(out_msgs_by_hash[in_msg_hash]) == 1, "Multiple outbound messages match inbound message"
                out_msg = out_msgs_by_hash[in_msg_hash][0]
                in_msg['out_tx_id'] = out_msg['out_tx_id']
                out_msgs_by_hash.pop(in_msg_hash)
        
        existing_in_msgs = []
        for chunk in chunks(list(in_msgs_by_hash.keys()), 10000):
            q = select(message_t.c.hash).where(message_t.c.hash.in_(chunk) & message_t.c.in_tx_id.is_(None))
            r = await conn.execute(q)
            existing_in_msgs += r.all()
        for e_in_msg in existing_in_msgs:
            hash = e_in_msg['hash']
            assert len(in_msgs_by_hash[hash]) == 1
            in_tx_id = in_msgs_by_hash[hash][0]['in_tx_id']
            q = update(message_t).where(message_t.c.hash == hash).values(in_tx_id=in_tx_id)
            await conn.execute(q)
            in_msgs_by_hash.pop(hash)

        existing_out_msgs = []
        for chunk in chunks(list(out_msgs_by_hash.keys()), 10000):
            q = select(message_t.c.hash).where(message_t.c.hash.in_(chunk) & message_t.c.out_tx_id.is_(None))
            r = await conn.execute(q)
            existing_out_msgs += r.all()
        for e_out_msg in existing_out_msgs:
            hash = e_out_msg['hash']
            assert len(out_msgs_by_hash[hash]) == 1
            out_tx_id = out_msgs_by_hash[hash][0]['out_tx_id']
            q = update(message_t).where(message_t.c.hash == hash).values(out_tx_id=out_tx_id)
            await conn.execute(q)
            out_msgs_by_hash.pop(hash)

        msgs_to_insert = list(out_msgs_by_hash.values()) + list(in_msgs_by_hash.values())
        msgs_to_insert = [item for sublist in msgs_to_insert for item in sublist] # flatten
        if len(msgs_to_insert):
            msg_ids = []
            for chunk in chunks(msgs_to_insert, 1000):
                msg_ids += (await conn.execute(message_t.insert().returning(message_t.c.msg_id).values(chunk))).all()

            contents = []
            for i, msg_id_tuple in enumerate(msg_ids):
                content = msg_contents_by_hash[msgs_to_insert[i]['hash']].copy() # copy is necessary because there might be duplicates, but msg_id differ
                content['msg_id'] = msg_id_tuple[0]
                contents.append(content)

            for chunk in chunks(contents, 3000):
                await conn.execute(message_content_t.insert(), chunk)

def get_transactions_by_masterchain_seqno(session, masterchain_seqno: int, include_msg_body: bool):
    block = session.query(Block).filter(and_(Block.workchain == MASTERCHAIN_INDEX, Block.shard == MASTERCHAIN_SHARD, Block.seqno == masterchain_seqno)).first()
    if block is None:
        raise BlockNotFound(MASTERCHAIN_INDEX, MASTERCHAIN_SHARD, masterchain_seqno)
    block_ids = [block.block_id] + [x.block_id for x in block.shards]
    query = session.query(Transaction) \
            .filter(Transaction.block_id.in_(block_ids))

    if include_msg_body:
        query = query.options(joinedload(Transaction.in_msg).joinedload(Message.content)) \
                     .options(joinedload(Transaction.out_msgs).joinedload(Message.content))
    else:
        query = query.options(joinedload(Transaction.in_msg)) \
                     .options(joinedload(Transaction.out_msgs))

    return query.all()

def get_transactions_by_address(session: Session, account: str, start_utime: Optional[int], end_utime: Optional[int], limit: int, offset: int, sort: str, include_msg_body: bool):
    query = session.query(Transaction).filter(Transaction.account == account)
    if start_utime is not None:
        query = query.filter(Transaction.utime >= start_utime)
    if end_utime is not None:
        query = query.filter(Transaction.utime <= end_utime)

    if include_msg_body:
        query = query.options(joinedload(Transaction.in_msg).joinedload(Message.content)) \
                     .options(joinedload(Transaction.out_msgs).joinedload(Message.content))
    else:
        query = query.options(joinedload(Transaction.in_msg)) \
                     .options(joinedload(Transaction.out_msgs))
    
    if sort == 'asc':
        query = query.order_by(Transaction.utime.asc(), Transaction.lt.asc())
    elif sort == 'desc':
        query = query.order_by(Transaction.utime.desc(), Transaction.lt.desc())

    query = query.limit(limit)
    query = query.offset(offset)

    return query.all()

def get_transactions_in_block(session: Session, workchain: int, shard: int, seqno: int, include_msg_body: bool):
    block = session.query(Block).filter(and_(Block.workchain == workchain, Block.shard == shard, Block.seqno == seqno)).first()

    if block is None:
        raise BlockNotFound(workchain, shard, seqno)

    query = session.query(Transaction) \
            .filter(Transaction.block_id == block.block_id)

    if include_msg_body:
        query = query.options(joinedload(Transaction.in_msg).joinedload(Message.content)) \
                     .options(joinedload(Transaction.out_msgs).joinedload(Message.content))
    else:
        query = query.options(joinedload(Transaction.in_msg)) \
                     .options(joinedload(Transaction.out_msgs))
    
    return query.all()

def get_chain_last_transactions(session: Session, workchain: Optional[int], start_utime: Optional[int], end_utime: Optional[int], limit: int, offset: int, include_msg_body: bool):
    query = session.query(Transaction)

    if workchain is not None:
        query = query.join(Transaction.block).options(contains_eager(Transaction.block)).filter(Block.workchain == workchain)

    if start_utime is not None:
        query = query.filter(Transaction.utime >= start_utime)
    if end_utime is not None:
        query = query.filter(Transaction.utime <= end_utime)

    if include_msg_body:
        query = query.options(joinedload(Transaction.in_msg).joinedload(Message.content)) \
                     .options(joinedload(Transaction.out_msgs).joinedload(Message.content))
    else:
        query = query.options(joinedload(Transaction.in_msg)) \
                     .options(joinedload(Transaction.out_msgs))

    query = query.order_by(Transaction.utime.desc(), Transaction.lt.desc())

    query = query.limit(limit)
    query = query.offset(offset)

    return query.all()
    
def get_in_message_by_transaction(session: Session, tx_lt: int, tx_hash: int, include_msg_body: bool):
    tx = session.query(Transaction).filter(Transaction.lt == tx_lt).filter(Transaction.hash == tx_hash).first()
    if tx is None:
        raise TransactionNotFound(tx_lt, tx_hash)

    return session.query(Message).filter(Message.in_tx_id == tx.tx_id).first()

def get_out_messages_by_transaction(session: Session, tx_lt: int, tx_hash: int, include_msg_body: bool):
    tx = session.query(Transaction).filter(Transaction.lt == tx_lt).filter(Transaction.hash == tx_hash).first()
    if tx is None:
        raise TransactionNotFound(tx_lt, tx_hash)

    return session.query(Message).filter(Message.out_tx_id == tx.tx_id).all()

def get_messages_by_hash(session: Session, msg_hash: str, include_msg_body: bool):
    query = session.query(Message).filter(Message.hash == msg_hash)
    if include_msg_body:
        query = query.options(joinedload(Message.content))
    query = query.limit(500)
    return query.all()

def get_transactions_by_hash(session: Session, tx_hash: str, include_msg_body: bool):
    query = session.query(Transaction).filter(Transaction.hash == tx_hash)
    if include_msg_body:
        query = query.options(joinedload(Transaction.in_msg).joinedload(Message.content)) \
                     .options(joinedload(Transaction.out_msgs).joinedload(Message.content))
    else:
        query = query.options(joinedload(Transaction.in_msg)) \
                     .options(joinedload(Transaction.out_msgs))
    query = query.limit(500)
    return query.all()

def get_transactions_by_in_message_hash(session: Session, msg_hash: str, include_msg_body: bool):
    query = session.query(Transaction).join(Transaction.in_msg).options(contains_eager(Transaction.in_msg))
    query = query.filter(Message.hash == msg_hash)
    if include_msg_body:
        query = query.options(joinedload(Transaction.in_msg).joinedload(Message.content)) \
                     .options(joinedload(Transaction.out_msgs).joinedload(Message.content))
    else:
        query = query.options(joinedload(Transaction.in_msg)) \
                     .options(joinedload(Transaction.out_msgs))
    query = query.order_by(Transaction.utime.desc()).limit(500)
    return query.all()

def get_blocks_by_unix_time(session: Session, start_utime: Optional[int], end_utime: Optional[int], workchain: Optional[int], shard: Optional[int], limit: int, offset: int, sort: str):
    query = session.query(BlockHeader).join(BlockHeader.block).options(contains_eager(BlockHeader.block))
    if start_utime is not None:
        query = query.filter(BlockHeader.gen_utime >= start_utime)
    if end_utime is not None:
        query = query.filter(BlockHeader.gen_utime <= end_utime)

    if workchain is not None:
        query = query.filter(Block.workchain == workchain)

    if shard is not None:
        query = query.filter(Block.shard == shard)

    if sort == 'asc':
        query = query.order_by(BlockHeader.gen_utime.asc())
    elif sort == 'desc':
        query = query.order_by(BlockHeader.gen_utime.desc())

    query = query.limit(limit)
    query = query.offset(offset)

    return query.all()

def get_block_by_transaction(session: Session, tx_hash: str):
    query = session.query(Transaction).filter(Transaction.hash == tx_hash) \
        .join(Transaction.block).join(Block.block_header) \
        .options(contains_eager(Transaction.block, Block.block_header, BlockHeader.block))
        
    tx = query.first()
    if tx is None:
        raise TransactionNotFound(None, tx_hash)
    return tx.block.block_header

def lookup_masterchain_block(session: Session, workchain: int, shard: int, seqno: int):
    block = session.query(Block).filter(and_(Block.workchain == workchain, Block.shard == shard, Block.seqno == seqno)).first()
    if block is None:
        raise BlockNotFound(workchain, shard, seqno)
    mc_block_id = block.masterchain_block_id
    query = session.query(BlockHeader).join(BlockHeader.block).options(contains_eager(BlockHeader.block)).filter(Block.block_id == mc_block_id)
    
    return query.first()

def get_active_accounts_count_in_period(session: Session, start_utime: int, end_utime: int):
    query = session.query(Transaction.account) \
                   .filter(Transaction.utime >= start_utime) \
                   .filter(Transaction.utime <= end_utime) \
                   .distinct()

    return query.count()
