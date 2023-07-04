import logging

from typing import Optional

from sqlalchemy import and_, or_
from sqlalchemy.orm import joinedload, Session, Query, contains_eager
from indexer.core.database import (
    Block,
    Transaction,
    TransactionMessage,
    Message,
    MASTERCHAIN_INDEX,
    MASTERCHAIN_SHARD
)
from indexer.core.exceptions import (
    BlockNotFound,
    TransactionNotFound,
)


logger = logging.getLogger(__name__)


# Common
def limit_query(query: Query, 
                limit: Optional[int], 
                offset: Optional[int]):
    if limit is not None:
        query = query.limit(limit)
    if offset is not None:
        query = query.offset(offset)
    return query


# Blocks
def get_blocks_by_unix_time(session: Session, 
                            start_utime: Optional[int], 
                            end_utime: Optional[int], 
                            workchain: Optional[int], 
                            shard: Optional[int], 
                            limit: int, 
                            offset: int, 
                            sort: str):
    query = session.query(Block)

    if start_utime is not None:
        query = query.filter(Block.gen_utime >= start_utime)
    if end_utime is not None:
        query = query.filter(Block.gen_utime <= end_utime)

    if workchain is not None:
        query = query.filter(Block.workchain == workchain)
    if shard is not None:
        query = query.filter(Block.shard == shard)

    if sort == 'asc':
        query = query.order_by(Block.gen_utime.asc())
    elif sort == 'desc':
        query = query.order_by(Block.gen_utime.desc())

    query = limit_query(query, limit, offset)
    return query.all()


# def get_workchain_last_block(session: Session,
#                              workchain: int,
#                              shard: int):
#     query = session.query(Block) \
#                    .filter(Block.workchain == workchain) \
#                    .filter(Block.shard == shard)
#     block = query.first()
#     if not block:
#         raise BlockNotFound(workchain=workchain, 
#                             shard=shard,
#                             seqno='latest')
#     return block


def get_masterchain_block_shards(session: Session,
                                 seqno: int,
                                 include_mc_block: bool=False):
    mc_block_fltr = and_(Block.workchain == MASTERCHAIN_INDEX, 
                         Block.shard == MASTERCHAIN_SHARD, 
                         Block.seqno == seqno)
    shards_fltr = and_(Block.mc_block_workchain == MASTERCHAIN_INDEX, 
                       Block.mc_block_shard == MASTERCHAIN_SHARD,
                       Block.mc_block_seqno == seqno)
    fltr = or_(mc_block_fltr, shards_fltr) if include_mc_block else shards_fltr
    query = session.query(Block).filter(fltr)
    query = query.order_by(Block.workchain, Block.shard, Block.seqno)
    return query.all()


def get_blocks(session: Session,
               workchain: Optional[int]=None,
               shard: Optional[int]=None,
               seqno: Optional[int]=None,
               root_hash: Optional[str]=None,
               file_hash: Optional[str]=None,
               from_gen_utime: Optional[int]=None,
               to_gen_utime: Optional[int]=None,
               from_start_lt: Optional[int]=None,
               to_start_lt: Optional[int]=None,
               include_mc_block: bool=False,
               sort_gen_utime: Optional[str]=None,
               sort_seqno: Optional[str]=None,
               limit: Optional[int]=None,
               offset: Optional[int]=None):
    query = session.query(Block)

    if workchain is not None:
        query = query.filter(Block.workchain == workchain)
    if shard is not None:
        query = query.filter(Block.shard == shard)
    if seqno is not None:
        query = query.filter(Block.seqno == seqno)

    if root_hash is not None:
        query = query.filter(Block.root_hash == root_hash)
    if file_hash is not None:
        query = query.filter(Block.file_hash == file_hash)

    if from_gen_utime is not None:
        query = query.filter(Block.gen_utime == from_gen_utime)
    if to_gen_utime is not None:
        query = query.filter(Block.gen_utime == to_gen_utime)

    if from_start_lt is not None:
        query = query.filter(Block.start_lt == from_start_lt)
    if to_start_lt is not None:
        query = query.filter(Block.start_lt == to_start_lt)

    if sort_gen_utime == 'asc':
        query = query.order_by(Block.gen_utime.asc())
    elif sort_gen_utime == 'desc':
        query = query.order_by(Block.gen_utime.desc())

    if sort_seqno == 'asc':
        query = query.order_by(Block.seqno.asc())
    elif sort_seqno == 'desc':
        query = query.order_by(Block.seqno.desc())


    if include_mc_block:
        query = query.options(joinedload(Block.masterchain_block))

    query = limit_query(query, limit, offset)
    return query.all()


# Transaction utils
def augment_transaction_query(query: Query, 
                              include_msg_body: bool, 
                              include_block: bool,
                              include_account_state: bool):
    if include_block:
        query = query.options(joinedload(Transaction.block))
    
    msg_join = joinedload(Transaction.messages).joinedload(TransactionMessage.message)
    if include_msg_body:
        msg_join = msg_join.joinedload(Message.message_content)
    query = query.options(msg_join)

    if include_account_state:
        query = query.options(joinedload(Transaction.account_state_after)) \
                     .options(joinedload(Transaction.account_state_before))
    return query


def sort_transaction_query_by_lt(query: Query, sort: str):
    if sort == 'asc':
        query = query.order_by(Transaction.lt.asc())
    elif sort == 'desc':
        query = query.order_by(Transaction.lt.desc())
    elif sort is None or sort == 'none':
        pass
    else:
        raise ValueError(f'Unknown sort type: {sort}')
    return query


def query_transactions_by_utime(query: Query, 
                                start_utime: Optional[int],
                                end_utime: Optional[int]):
    if start_utime is not None:
        query = query.filter(Transaction.now >= start_utime)
    if end_utime is not None:
        query = query.filter(Transaction.now >= end_utime)
    return query


def query_transactions_by_lt(query: Query, 
                             start_lt: Optional[int],
                             end_lt: Optional[int]):
    if start_lt is not None:
        query = query.filter(Transaction.lt >= start_lt)
    if end_lt is not None:
        query = query.filter(Transaction.lt >= end_lt)
    return query


# Transactions
def get_chain_last_transactions(session: Session, 
                                workchain: Optional[int], 
                                start_utime: Optional[int], 
                                end_utime: Optional[int], 
                                limit: int, 
                                offset: int, 
                                include_msg_body: bool=False, 
                                include_block: bool=False,
                                include_account_state: bool=False,):
    query = session.query(Transaction)
    if workchain is not None:
        query = query.filter(Transaction.block_workchain == workchain)
    
    query = query_transactions_by_utime(query, start_utime, end_utime)
    query = augment_transaction_query(query, include_msg_body, include_block, include_account_state)
    query = query.order_by(Transaction.lt.desc())  # TODO: index
    query = limit_query(query, limit, offset)
    return query.all()


def get_block_transactions(session: Session,
                           workchain: int,
                           shard: int,
                           seqno: int,
                           include_msg_body: bool=False, 
                           include_block: bool=False,
                           include_account_state: bool=False,
                           limit: Optional[int]=None,
                           offset: Optional[int]=None,):
    query = session.query(Transaction)
    query = query.filter(Transaction.block_workchain == workchain) \
                 .filter(Transaction.block_shard == shard) \
                 .filter(Transaction.block_seqno == seqno)  # TODO: index
    query = augment_transaction_query(query, include_msg_body, include_block, include_account_state)
    query = limit_query(query, limit, offset)
    return query.all()


def get_transactions_by_masterchain_seqno(session: Session, 
                                          masterchain_seqno: int, 
                                          include_msg_body: bool=True, 
                                          include_block: bool=False,
                                          include_account_state: bool=True,
                                          limit: Optional[int]=None,
                                          offset: Optional[int]=None,
                                          sort: Optional[str]=None):
    mc_block = session.query(Block).filter(Block.workchain == MASTERCHAIN_INDEX) \
                                .filter(Block.shard == MASTERCHAIN_SHARD) \
                                .filter(Block.seqno == masterchain_seqno) \
                                .first()
    if not mc_block:
        raise BlockNotFound(workchain=MASTERCHAIN_INDEX, 
                            shard=MASTERCHAIN_SHARD, 
                            seqno=masterchain_seqno)
    shards = session.query(Block).filter(Block.mc_block_seqno == masterchain_seqno).all()
    blocks = [mc_block] + shards

    fltr = or_(*[and_(Transaction.block_workchain == b.workchain, Transaction.block_shard == b.shard, Transaction.block_seqno == b.seqno)
                 for b in blocks])
    
    query = session.query(Transaction).filter(fltr)
    query = augment_transaction_query(query, include_msg_body, include_block, include_account_state)
    query = sort_transaction_query_by_lt(query, sort)
    query = limit_query(query, limit, offset)
    
    txs = query.all()
    return txs


def get_transactions(session: Session,
                     workchain: Optional[int]=None,
                     shard: Optional[int]=None,
                     seqno: Optional[int]=None,
                     account: Optional[str]=None,
                     hash: Optional[str]=None,
                     start_lt: Optional[str]=None,
                     end_lt: Optional[str]=None,
                     start_utime: Optional[str]=None,
                     end_utime: Optional[str]=None,
                     include_msg_body: bool=True, 
                     include_block: bool=False,
                     include_account_state: bool=True,
                     limit: Optional[int]=None,
                     offset: Optional[int]=None,
                     sort: Optional[str]=None):
    query = session.query(Transaction)

    if workchain is not None:
        query = query.filter(Transaction.block_workchain == workchain)  # TODO: index
    if shard is not None:
        query = query.filter(Transaction.block_shard == shard)  # TODO: index
    if seqno is not None:
        query = query.filter(Transaction.block_seqno == seqno)  # TODO: index

    if account is not None:
        query = query.filter(Transaction.account == account.upper())  # TODO: index

    if hash is not None:
        query = query.filter(Transaction.hash == hash)  # TODO: index

    query = query_transactions_by_lt(query, start_lt, end_lt)
    query = query_transactions_by_utime(query, start_utime, end_utime)
    query = sort_transaction_query_by_lt(query, sort)
    query = augment_transaction_query(query, include_msg_body, include_block, include_account_state)
    query = limit_query(query, limit, offset)
    return query.all()


def get_adjacent_transactions(session: Session,
                              hash: str,
                              direction: Optional[str]=None,
                              include_msg_body: bool=False, 
                              include_block: bool=False,
                              include_account_state: bool=False,
                              limit: Optional[int]=None,
                              offset: Optional[int]=None,
                              sort: Optional[str]=None):
    query = session.query(TransactionMessage)
    query = query.filter(TransactionMessage.transaction_hash == hash)
    if direction:
        query = query.filter(TransactionMessage.direction == direction)
    tx_msgs = query.all()
    if len(tx_msgs) < 1:
        raise TransactionNotFound(adjacent_tx_hash=hash, direction=direction)
    query = session.query(TransactionMessage)
    fltr = []
    inv_direction = {'in': 'out', 'out': 'in'}
    for row in tx_msgs:
        loc = and_(TransactionMessage.message_hash == row.message_hash, 
                   TransactionMessage.direction == inv_direction[row.direction])
        fltr.append(loc)
    fltr = or_(*fltr)
    query = query.filter(fltr)
    txs = query.all()
    if len(txs) < 1:
        raise TransactionNotFound(adjacent_tx_hash=hash, direction=direction)
    
    fltr = or_(*[Transaction.hash == tx.transaction_hash for tx in txs])
    query = session.query(Transaction).filter(fltr)
    query = sort_transaction_query_by_lt(query, sort)
    query = augment_transaction_query(query, include_msg_body, include_block, include_account_state)
    query = limit_query(query, limit, offset)
    return query.all()


# Message utils
def augment_message_query(query: Query,
                          include_msg_body: bool):
    if include_msg_body:
        query = query.options(joinedload(Message.message_content)) \
                     .options(joinedload(Message.init_state))
    return query


# Messages
def get_messages(session: Session,
                 hash: Optional[str]=None,
                 source: Optional[str]=None,
                 destination: Optional[str]=None,
                 body_hash: Optional[str]=None,
                 include_msg_body: bool=False,
                 limit: Optional[int]=None,
                 offset: Optional[int]=None):
    query = session.query(Message)

    if hash is not None:
        query = query.filter(Message.hash == hash)  # TODO: index
    if source is not None:
        query = query.filter(Message.source == source)  # TODO: index
    if destination is not None:
        query = query.filter(Message.destination == destination)  # TODO: index
    if body_hash is not None:
        query = query.filter(Message.body_hash == body_hash)  # TODO: index

    query = augment_message_query(query, include_msg_body)
    query = limit_query(query, limit, offset)
    return query.all()


# DEPRECATED
def get_transactions_by_in_message_hash(session: Session,
                                        msg_hash: str,
                                        include_msg_body: bool=False,
                                        include_block: bool=False,
                                        include_account_state: bool=False):
    query = session.query(Transaction).join(Transaction.messages)
    query = augment_transaction_query(query, 
                                      include_msg_body, 
                                      include_block,
                                      include_account_state)
    query = query.filter(TransactionMessage.direction == 'in') \
                 .filter(TransactionMessage.message_hash == msg_hash)
    logger.info(f'query: {query}')
    return query.all()


def get_transactions_by_message(session: Session,
                                direction: Optional[str],
                                source: Optional[str]=None,
                                destination: Optional[str]=None,
                                created_lt: Optional[int]=None,
                                hash: Optional[str]=None,
                                include_msg_body: bool=False,
                                include_block: bool=False,
                                include_account_state: bool=False,
                                limit: Optional[int]=None,
                                offset: Optional[int]=None,
                                sort: Optional[str]=None):
    query = session.query(Transaction).join(Transaction.messages).join(TransactionMessage.message)
    if direction is not None:
        if not direction == 'in' and not direction == 'out':
            raise ValueError(f'Unknown direction: {direction}')
        query = query.filter(TransactionMessage.direction == direction)
    if source is not None:
        query = query.filter(Message.source == source)
    if destination is not None:
        query = query.filter(Message.destination == destination)
    if created_lt is not None:
        query = query.filter(Message.created_lt == created_lt)
    if hash is not None:
        query = query.filter(Message.hash == hash)

    query = augment_transaction_query(query, 
                                      include_msg_body, 
                                      include_block,
                                      include_account_state)
    query = limit_query(query, limit, offset)
    query = sort_transaction_query_by_lt(query, sort)
    return query.all()
