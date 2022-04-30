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

@dataclass(init=False)
class Block(Base):
    __tablename__ = 'blocks'
    block_id: int = Column(Integer, autoincrement=True, primary_key=True)
    
    workchain: int = Column(Integer)
    shard: int = Column(BigInteger)
    seqno: int = Column(Integer)
    root_hash: str = Column(String(44))
    file_hash: str = Column(String(44))
    masterchain_block_id = Column(Integer, ForeignKey('blocks.block_id'))

    shards = relationship("Block",
        backref=backref('masterchain_block', remote_side=[block_id])
    )

    __table_args__ = (Index('blocks_index_1', 'workchain', 'shard', 'seqno'),
                      Index('blocks_index_2', 'masterchain_block_id'),
                      UniqueConstraint('workchain', 'shard', 'seqno'))
    
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

    in_msg = relationship("Message", uselist=False, back_populates="in_tx", foreign_keys="Message.in_tx_id")
    out_msgs = relationship("Message", back_populates="out_tx", foreign_keys="Message.out_tx_id")
    
    __table_args__ = (Index('transactions_index_1', 'account'),
                      Index('transactions_index_2', 'utime'), 
                      Index('transactions_index_3', 'hash'),
                      Index('transactions_index_4', 'lt'),
                      Index('transactions_index_5', 'account', 'utime'),
                      Index('transactions_index_6', 'block_id')
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
    # out_tx = relationship("Transaction", backref="out_msgs", foreign_keys=[out_tx_id])
    out_tx = relationship("Transaction", back_populates="out_msgs", foreign_keys=[out_tx_id])

    in_tx_id = Column(BigInteger, ForeignKey("transactions.tx_id"))
    # in_tx = relationship("Transaction", backref="in_msg", uselist=False, foreign_keys=[in_tx_id])
    in_tx = relationship("Transaction", back_populates="in_msg", uselist=False, foreign_keys=[in_tx_id])

    __table_args__ = (Index('messages_index_1', 'source'),
                      Index('messages_index_2', 'destination'),
                      Index('messages_index_3', 'created_lt'),
                      Index('messages_index_4', 'body_hash'),
                      Index('messages_index_5', 'source', 'destination', 'created_lt'),
                      Index('messages_index_6', 'in_tx_id'),
                      Index('messages_index_7', 'out_tx_id'),
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

