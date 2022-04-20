from copy import deepcopy
from time import sleep
from typing import List, Optional

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database

from sqlalchemy import Column, String, Integer, BigInteger, Boolean, Index
from sqlalchemy import ForeignKey, UniqueConstraint, Table
from sqlalchemy import and_, or_, ColumnDefault
from sqlalchemy.orm import relationship, backref
from dataclasses import dataclass, asdict

from config import settings as S
from loguru import logger

MASTERCHAIN_INDEX = -1
MASTERCHAIN_SHARD = -9223372036854775808

with open(S.postgres.password_file, 'r') as f:
    db_password = f.read()

# init database
def get_engine(database):
    engine = create_engine('postgresql://{user}:{db_password}@{host}:{port}/{dbname}'.format(host=S.postgres.host,
                                                                                             port=S.postgres.port,
                                                                                             user=S.postgres.user,
                                                                                             db_password=db_password,
                                                                                             dbname=database))
    return engine

engine = get_engine(S.postgres.dbname)

# database
Base = declarative_base()

def delete_database():
    if database_exists(engine.url):
        logger.info('Drop database')
        drop_database(engine.url)


def init_database(create=False):
    while not database_exists(engine.url):
        if create:
            logger.info('Creating database')
            create_database(engine.url)
            Base.metadata.create_all(engine)
        sleep(0.5)
        

def get_session():
    Session = sessionmaker(bind=engine)
    return Session


shards_association = Table('shards', 
                           Base.metadata,
                           Column('masterchain_block_id', 
                                  Integer, 
                                  ForeignKey('blocks.block_id'), 
                                  primary_key=True),
                           Column('shard_block_id', 
                                  Integer, 
                                  ForeignKey('blocks.block_id'), 
                                  primary_key=True))


@dataclass(init=False)
class Block(Base):
    __tablename__ = 'blocks'
    block_id: int = Column(Integer, autoincrement=True, primary_key=True)
    
    workchain: int = Column(Integer)
    shard: int = Column(BigInteger)
    seqno: int = Column(Integer)
    root_hash: str = Column(String(44))
    file_hash: str = Column(String(44))
    
    __table_args__ = (Index('blocks_index_1', 'workchain', 'shard', 'seqno'), 
                      UniqueConstraint('workchain', 'shard', 'seqno'))
    
    shards = relationship("Block", 
                          shards_association,
                          primaryjoin=(block_id == shards_association.c.masterchain_block_id),
                          secondaryjoin=(block_id == shards_association.c.shard_block_id))
    masterchain_blocks = relationship("Block", 
                                      shards_association,
                                      primaryjoin=(block_id == shards_association.c.shard_block_id),
                                      secondaryjoin=(block_id == shards_association.c.masterchain_block_id),
                                      viewonly=True)
    
    
    @classmethod
    def build(cls, raw):
        return Block(workchain=raw['workchain'],
                     shard=raw['shard'],
                     seqno=raw['seqno'],
                     root_hash=raw['root_hash'],
                     file_hash=raw['file_hash'])


@dataclass(init=False)
class BlockHeader(Base):
    __tablename__ = 'block_headers'
    
    block_id: int = Column(Integer, ForeignKey('blocks.block_id'), primary_key=True)
    global_id: int = Column(Integer)
    version: int = Column(Integer)
    flags: int = Column(Integer)
    after_merge: bool = Column(Boolean)
    after_split: bool = Column(Boolean)
    before_split: bool = Column(Boolean)
    want_merge: bool = Column(Boolean)
    validator_list_hash_short: int = Column(Integer)
    catchain_seqno: int = Column(Integer)
    min_ref_mc_seqno: int = Column(Integer)
    is_key_block: bool = Column(Boolean)
    prev_key_block_seqno: int = Column(Integer)
    start_lt: int = Column(BigInteger)
    end_lt: int = Column(BigInteger)
    gen_utime: int = Column(BigInteger)
    vert_seqno: int = Column(Integer)
    
    block = relationship("Block", backref=backref("block_header", uselist=False))
    
    __table_args__ = (Index('block_headers_index_1', 'catchain_seqno'), 
                      Index('block_headers_index_2', 'min_ref_mc_seqno'),
                      Index('block_headers_index_3', 'prev_key_block_seqno'),
                      Index('block_headers_index_4', 'start_lt', 'end_lt'),
                      Index('block_headers_index_5', 'is_key_block'),
                      Index('block_headers_index_6', 'gen_utime')
                     )
    
    @classmethod
    def build(cls, raw, block):
        return BlockHeader(block=block,
                           global_id=raw['global_id'],
                           version=raw['version'],
                           flags=raw['flags'],
                           after_merge=raw['after_merge'],
                           after_split=raw['after_split'],
                           before_split=raw['before_split'],
                           want_merge=raw['want_merge'],
                           validator_list_hash_short=raw['validator_list_hash_short'],
                           catchain_seqno=raw['catchain_seqno'],
                           min_ref_mc_seqno=raw['min_ref_mc_seqno'],
                           is_key_block=raw['is_key_block'],
                           prev_key_block_seqno=raw['prev_key_block_seqno'],
                           start_lt=int(raw['start_lt']),
                           end_lt=int(raw['end_lt']),
                           gen_utime=int(raw['gen_utime']),
                           vert_seqno=raw['vert_seqno'])


@dataclass(init=False)
class Transaction(Base):
    __tablename__ = 'transactions'
    
    tx_id: int = Column(BigInteger, autoincrement=True, primary_key=True)
    account: str = Column(String)
    lt: int = Column(BigInteger)
    hash: str = Column(String(44))
    
    utime: int = Column(BigInteger)
    fee: int = Column(BigInteger)
    storage_fee: int = Column(BigInteger)
    other_fee: int = Column(BigInteger)
    
    block_id = Column(Integer, ForeignKey("blocks.block_id"))
    block = relationship("Block", backref="transactions")
    
    __table_args__ = (Index('transactions_index_1', 'account'),
                      Index('transactions_index_2', 'utime'), 
                      Index('transactions_index_3', 'hash'),
                      Index('transactions_index_4', 'lt'),
                      Index('transactions_index_5', 'account', 'utime')
                     )

    @classmethod
    def build(cls, raw, raw_detail, block):
        return Transaction(block=block,
                           account=raw['account'],
                           lt=raw['lt'],
                           hash=raw['hash'],
                           utime=raw_detail['utime'],
                           fee=int(raw_detail['fee']),
                           storage_fee=int(raw_detail['storage_fee']),
                           other_fee=int(raw_detail['other_fee']))

    def asdict(self):
        res = asdict(self)
        try:
            res['in_msg'] = [msg.asdict() for msg in self.in_msg]
        except: pass
        try:
            res['out_msgs'] = [msg.asdict() for msg in self.out_msgs]
        except: pass
        return res


@dataclass(init=False)
class Message(Base):
    __tablename__ = 'messages'
    msg_id: int = Column(BigInteger, primary_key=True)
    source: str = Column(String)
    destination: str = Column(String)
    value: int = Column(BigInteger)
    fwd_fee: int = Column(BigInteger)
    ihr_fee: int = Column(BigInteger)
    created_lt: int = Column(BigInteger)
    body_hash: str = Column(String(44))
    
    out_tx_id = Column(BigInteger, ForeignKey("transactions.tx_id"))
    out_tx = relationship("Transaction", backref="out_msgs", foreign_keys=[out_tx_id])

    in_tx_id = Column(BigInteger, ForeignKey("transactions.tx_id"))
    in_tx = relationship("Transaction", backref="in_msg", uselist=False, foreign_keys=[in_tx_id])

    __table_args__ = (Index('messages_index_1', 'source'),
                      Index('messages_index_2', 'destination'),
                      Index('messages_index_3', 'created_lt'),
                      Index('messages_index_4', 'body_hash'),
                      Index('messages_index_5', 'source', 'destination', 'created_lt'),
                     )
    
    @classmethod
    def build(cls, raw):
        return Message(source=raw['source'],
                       destination=raw['destination'],
                       value=int(raw['value']),
                       fwd_fee=int(raw['fwd_fee']),
                       ihr_fee=int(raw['ihr_fee']),
                       created_lt=raw['created_lt'],
                       body_hash=raw['body_hash'])

    def asdict(self):
        res = asdict(self)
        try:
            res['body'] = self.content.body
        except: pass
        return res


@dataclass(init=False)
class MessageContent(Base):
    __tablename__ = 'message_contents'
    
    msg_id: int = Column(BigInteger, ForeignKey("messages.msg_id"), primary_key=True)
    body: str = Column(String)
        
    msg = relationship("Message", backref=backref("content", cascade="save-update, merge, "
                                                  "delete, delete-orphan", uselist=False))
    
    @classmethod
    def build(cls, raw, msg):
        return MessageContent(msg=msg,
                              body=raw['msg_data'].get('body'))


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
