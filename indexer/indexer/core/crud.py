import logging

from typing import Optional, List

from sqlalchemy import and_, or_
from sqlalchemy.orm import joinedload, Session, Query, contains_eager, aliased
from indexer.core.database import (
    Block,
    Transaction,
    TransactionMessage,
    Message,
    NFTCollection,
    NFTItem,
    NFTTransfer,
    JettonMaster,
    JettonWallet,
    JettonTransfer,
    JettonBurn,
    Event,
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
        query = query.filter(Block.gen_utime >= from_gen_utime)
    if to_gen_utime is not None:
        query = query.filter(Block.gen_utime <= to_gen_utime)

    if from_start_lt is not None:
        query = query.filter(Block.start_lt >= from_start_lt)
    if to_start_lt is not None:
        query = query.filter(Block.start_lt <= to_start_lt)

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
                              include_account_state: bool,
                              include_trace: bool=False):
    if include_block:
        query = query.options(joinedload(Transaction.block))

    if include_trace:
        event_query = joinedload(Transaction.event)
        query = query.options(event_query.joinedload(Event.edges))
        query = query.options(event_query.joinedload(Event.transactions).joinedload(Transaction.messages))
    
    msg_join = joinedload(Transaction.messages).joinedload(TransactionMessage.message)
    if include_msg_body:
        msg_join_1 = msg_join.joinedload(Message.message_content)
        msg_join_2 = msg_join.joinedload(Message.init_state)
        query = query.options(msg_join_1).options(msg_join_2)

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
        query = query.filter(Transaction.now <= end_utime)
    return query


def query_transactions_by_lt(query: Query, 
                             start_lt: Optional[int],
                             end_lt: Optional[int]):
    if start_lt is not None:
        query = query.filter(Transaction.lt >= start_lt)
    if end_lt is not None:
        query = query.filter(Transaction.lt <= end_lt)
    return query


# Transactions
def get_transactions_by_masterchain_seqno(session: Session, 
                                          masterchain_seqno: int, 
                                          include_msg_body: bool=True, 
                                          include_block: bool=False,
                                          include_account_state: bool=True,
                                          include_trace: int=0,
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
    query = augment_transaction_query(query, include_msg_body, include_block, include_account_state, include_trace)
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
                     lt: Optional[str]=None,
                     start_lt: Optional[str]=None,
                     end_lt: Optional[str]=None,
                     start_utime: Optional[str]=None,
                     end_utime: Optional[str]=None,
                     include_msg_body: bool=True, 
                     include_block: bool=False,
                     include_account_state: bool=True,
                     include_trace: bool=False,
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
        query = query.filter(Transaction.account == account)  # TODO: index

    if hash is not None:
        query = query.filter(Transaction.hash == hash)  # TODO: index

    if lt is not None:
        query = query.filter(Transaction.lt == lt)  # TODO: index

    query = query_transactions_by_lt(query, start_lt, end_lt)
    query = query_transactions_by_utime(query, start_utime, end_utime)
    query = sort_transaction_query_by_lt(query, sort)
    query = augment_transaction_query(query, include_msg_body, include_block, include_account_state, include_trace)
    query = limit_query(query, limit, offset)
    return query.all()


def get_adjacent_transactions(session: Session,
                              hash: str,
                              lt: Optional[str]=None,
                              direction: Optional[str]=None,
                              include_msg_body: bool=True, 
                              include_block: bool=False,
                              include_account_state: bool=True,
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


def get_traces(session: Session,
               event_ids: Optional[List[int]]=None,
               tx_hashes: Optional[List[str]]=None):
    if not event_ids and not tx_hashes:
        raise RuntimeError('event_ids or tx_hashes are required')
    if tx_hashes:
        if event_ids is not None:
            raise RuntimeError('event_ids should be None when using tx_hashes')
        subquery = session.query(Transaction.hash, Transaction.event_id) \
                          .filter(Transaction.hash.in_(tx_hashes))
        event_ids = dict(subquery.all())
        event_ids = [event_ids.get(x) for x in tx_hashes]        
    
    query = session.query(Event)
    query = query.filter(Event.id.in_([x for x in event_ids if x is not None]))
    query = query.options(joinedload(Event.edges))
    
    tx_join = joinedload(Event.transactions)
    
    msg_join = tx_join.joinedload(Transaction.messages).joinedload(TransactionMessage.message)
    msg_join_1 = msg_join.joinedload(Message.message_content)
    msg_join_2 = msg_join.joinedload(Message.init_state)
    query = query.options(msg_join_1).options(msg_join_2)

    query = query.options(tx_join.joinedload(Transaction.account_state_after)) \
                 .options(tx_join.joinedload(Transaction.account_state_before))
    raw_traces = query.all()
    
    result = {}
    # build trees
    for raw in raw_traces:
        head_hash = None
        nodes = {}
        txs = {tx.hash: tx for tx in raw.transactions}
        for edge in raw.edges:
            left = {'id': raw.id, 'transaction': txs[edge.left_tx_hash], 'children': []} if edge.left_tx_hash not in nodes else nodes[edge.left_tx_hash]
            right = {'id': raw.id, 'transaction': txs[edge.right_tx_hash], 'children': []} if edge.right_tx_hash not in nodes else nodes[edge.right_tx_hash]
            left['children'].append(right)
            nodes[edge.left_tx_hash] = left
            nodes[edge.right_tx_hash] = right

            if head_hash is None or head_hash == edge.right_tx_hash:
                head_hash = edge.left_tx_hash
        result[raw.id] = nodes[head_hash]
    return [result.get(x) for x in event_ids]


def get_transaction_trace(session: Session, 
                          hash: str,
                          include_msg_body: bool=True, 
                          include_block: bool=False,
                          include_account_state: bool=True,
                          sort: Optional[str]=None):
    TM1 = aliased(TransactionMessage)
    TM2 = aliased(TransactionMessage)

    # find transaction trace
    tx_hashes = {hash}
    new_tx_hashes = {hash}
    edges = set()
    while new_tx_hashes:
        query = session.query(TM1.transaction_hash.label('tx1'), TM2.transaction_hash.label('tx2'), TM1.direction.label('dir')) \
                        .join(TM2, TM1.message_hash == TM2.message_hash) \
                        .filter(TM1.transaction_hash.in_(new_tx_hashes)) \
                        .filter(TM1.transaction_hash != TM2.transaction_hash)
        res = query.all()
        for a, b, dir in res:
            edge = (a, b) if dir == 'out' else (b, a)
            edges.add(edge)
        new_tx_hashes = {x[1] for x in res} - tx_hashes
        tx_hashes = tx_hashes | new_tx_hashes
    
    # query transactions
    query = session.query(Transaction)
    query = query.filter(Transaction.hash.in_(tx_hashes))

    query = sort_transaction_query_by_lt(query, sort)
    query = augment_transaction_query(query, include_msg_body, include_block, include_account_state)
    transactions = query.all()
    transactions = {x.hash: x for x in transactions}

    # build tree
    nodes = {}
    for a, b in edges:
        left = {'transaction': transactions[a], 'children': []} if a not in nodes else nodes[a]
        right = {'transaction': transactions[b], 'children': []} if b not in nodes else nodes[b]
        left['children'].append(right)
        nodes[a] = left
        nodes[b] = right

    root_hash = list(tx_hashes - {x[1] for x in edges})
    assert len(root_hash) == 1, 'multiple roots!?'
    root_hash = root_hash[0]
    return nodes[root_hash]


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
                 include_msg_body: bool=True,
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


# nfts
def get_nft_collections(session: Session,
                        address: Optional[str]=None,
                        owner_address: Optional[str]=None,
                        limit: Optional[int]=None,
                        offset: Optional[int]=None):
    query = session.query(NFTCollection)
    if address is not None:
        query = query.filter(NFTCollection.address == address)  # TODO: index
    if owner_address is not None:
        query = query.filter(NFTCollection.owner_address == owner_address)  # TODO: index
    query = limit_query(query, limit, offset)
    return query.all()


def get_nft_items(session: Session,
                  address: Optional[str]=None,
                  index: Optional[int]=None,
                  collection_address: Optional[str]=None,
                  owner_address: Optional[str]=None,
                  limit: Optional[int]=None,
                  offset: Optional[int]=None,):
    query = session.query(NFTItem)
    if address is not None:
        query = query.filter(NFTItem.address == address)  # TODO: index
    if index is not None:
        query = query.filter(NFTItem.index == index)  # TODO: index
    if collection_address is not None:
        query = query.filter(NFTItem.collection_address == collection_address)  # TODO: index
    if owner_address is not None:
        query = query.filter(NFTItem.owner_address == owner_address)  # TODO: index
    query = limit_query(query, limit, offset)
    query = query.options(joinedload(NFTItem.collection))
    return query.all()


def get_nft_transfers(session: Session,
                      nft_item: Optional[str]=None,
                      nft_collection: Optional[str]=None,
                      account: Optional[str]=None,
                      direction: Optional[str]=None,
                      start_lt: Optional[str]=None,
                      end_lt: Optional[str]=None,
                      start_utime: Optional[str]=None,
                      end_utime: Optional[str]=None,
                      limit: Optional[int]=None,
                      offset: Optional[int]=None,
                      sort: Optional[str]=None):
    query = session.query(NFTTransfer).join(NFTTransfer.transaction).join(NFTTransfer.nft_item)
    if nft_item is not None:
        query = query.filter(NFTTransfer.nft_item_address == nft_item)
    if nft_collection is not None:
        query = query.filter(NFTItem.collection_address == nft_collection)
    if account is not None:
        if direction == 'in':
            fltr = NFTTransfer.new_owner == account
        elif direction == 'out':
            fltr = NFTTransfer.old_owner == account
        elif direction is None:
            fltr = or_(NFTTransfer.new_owner == account,
                       NFTTransfer.old_owner == account)
        else:
            raise ValueError(f"Unknown nft transfer direction :'{direction}'")
        query = query.filter(fltr)
    query = query_transactions_by_lt(query, start_lt, end_lt)
    query = query_transactions_by_utime(query, start_utime, end_utime)
    query = sort_transaction_query_by_lt(query, sort)
    query = limit_query(query, limit, offset)
    query = query.options(joinedload(NFTTransfer.nft_item))
    query = query.options(joinedload(NFTTransfer.transaction))
    return query.all()


def get_account_nft_collections(session: Session,
                                address: str,
                                limit: Optional[int]=None,
                                offset: Optional[int]=None,):
    query = session.query(NFTCollection).join(NFTCollection.items)
    query = query.filter(NFTItem.owner_address == address)
    query = limit_query(query, limit, offset)
    query = query.distinct()
    return query.all()


# jettons
def get_jetton_masters(session: Session,
                       address: Optional[str]=None,
                       admin_address: Optional[str]=None,
                       limit: Optional[int]=None,
                       offset: Optional[int]=None):
    query = session.query(JettonMaster)
    if address is not None:
        query = query.filter(JettonMaster.address == address)
    if admin_address is not None:
        query = query.filter(JettonMaster.admin_address == admin_address)
    query = limit_query(query, limit, offset)
    return query.all()


def get_jetton_wallets(session: Session,
                       address: Optional[str]=None,
                       owner_address: Optional[str]=None,
                       jetton_address: Optional[str]=None,
                       limit: Optional[int]=None,
                       offset: Optional[int]=None):
    query = session.query(JettonWallet)
    if address is not None:
        query = query.filter(JettonWallet.address == address)
    if owner_address is not None:
        query = query.filter(JettonWallet.owner == owner_address)
    if jetton_address is not None:
        query = query.filter(JettonWallet.jetton == jetton_address)    
    query = limit_query(query, limit, offset)
    return query.all()


def get_jetton_transfers(session: Session,
                         account: Optional[str]=None,
                         direction: Optional[str]=None,
                         jetton_account: Optional[str]=None,
                         jetton_master: Optional[str]=None,
                         start_lt: Optional[str]=None,
                         end_lt: Optional[str]=None,
                         start_utime: Optional[str]=None,
                         end_utime: Optional[str]=None,
                         limit: Optional[int]=None,
                         offset: Optional[int]=None,
                         sort: Optional[str]=None):
    query = session.query(JettonTransfer) \
                   .join(JettonTransfer.jetton_wallet) \
                   .join(JettonTransfer.transaction)
    if account is not None:
        if direction == 'in':
            fltr = JettonTransfer.destination == account
        elif direction == 'out':
            fltr = JettonTransfer.source == account
        elif direction is None:
            fltr = or_(JettonTransfer.source == account,
                       JettonTransfer.destination == account)
        else:
            raise ValueError(f"Unknown nft transfer direction :'{direction}'")
        query = query.filter(fltr)
    if jetton_account is not None:
        query = query.filter(JettonTransfer.jetton_wallet_address == jetton_account)
    if jetton_master is not None:
        query = query.filter(JettonWallet.jetton == jetton_master)
    
    query = query_transactions_by_lt(query, start_lt, end_lt)
    query = query_transactions_by_utime(query, start_utime, end_utime)
    query = sort_transaction_query_by_lt(query, sort)
    query = limit_query(query, limit, offset)
    query = query.options(joinedload(JettonTransfer.jetton_wallet))
    query = query.options(joinedload(JettonTransfer.transaction))
    return query.all()


def get_jetton_burns(session: Session,
                     account: Optional[str]=None,
                     jetton_account: Optional[str]=None,
                     jetton_master: Optional[str]=None,
                     start_lt: Optional[str]=None,
                     end_lt: Optional[str]=None,
                     start_utime: Optional[str]=None,
                     end_utime: Optional[str]=None,
                     limit: Optional[int]=None,
                     offset: Optional[int]=None,
                     sort: Optional[str]=None):
    query = session.query(JettonBurn) \
                   .join(JettonBurn.jetton_wallet) \
                   .join(JettonBurn.transaction)
    if account is not None:
        query = query.filter(JettonBurn.owner == account)
    if jetton_account is not None:
        query = query.filter(JettonBurn.jetton_wallet_address == jetton_account)
    if jetton_master is not None:
        query = query.filter(JettonWallet.jetton == jetton_master)
    
    query = query_transactions_by_lt(query, start_lt, end_lt)
    query = query_transactions_by_utime(query, start_utime, end_utime)
    query = sort_transaction_query_by_lt(query, sort)
    query = limit_query(query, limit, offset)
    query = query.options(joinedload(JettonBurn.jetton_wallet))
    query = query.options(joinedload(JettonBurn.transaction))
    return query.all()


# DEPRECATED
def get_transactions_by_in_message_hash(session: Session,
                                        msg_hash: str,
                                        include_msg_body: bool=True,
                                        include_block: bool=True,
                                        include_account_state: bool=True):
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
                                direction: Optional[str]=None,
                                source: Optional[str]=None,
                                destination: Optional[str]=None,
                                created_lt: Optional[int]=None,
                                hash: Optional[str]=None,
                                include_msg_body: bool=True,
                                include_block: bool=True,
                                include_account_state: bool=True,
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
    query = sort_transaction_query_by_lt(query, sort)
    query = limit_query(query, limit, offset)
    return query.all()
