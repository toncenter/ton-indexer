import codecs
import asyncio
from copy import deepcopy
from time import sleep
from typing import List, Optional

from pytonlib.utils.tlb import parse_transaction
from pytonlib.utils.address import detect_address
from tvm_valuetypes.cell import deserialize_boc

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database

from sqlalchemy import Column, String, Integer, BigInteger, Boolean, Index, Enum
from sqlalchemy import ForeignKey, UniqueConstraint, Table, exc
from sqlalchemy import and_, or_, ColumnDefault
from sqlalchemy.orm import relationship, backref
from dataclasses import dataclass, asdict

from sqlalchemy.dialects.postgresql import ARRAY

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

import asyncpg

from config import settings as S
from loguru import logger

MASTERCHAIN_INDEX = -1
MASTERCHAIN_SHARD = -9223372036854775808

with open(S.postgres.password_file, 'r') as f:
    db_password = f.read()

# init database
def get_engine(database):
    engine = create_async_engine('postgresql+asyncpg://{user}:{db_password}@{host}:{port}/{dbname}'.format(host=S.postgres.host,
                                                                                             port=S.postgres.port,
                                                                                             user=S.postgres.user,
                                                                                             db_password=db_password,
                                                                                             dbname=database), pool_size=20, max_overflow=10, echo=False)
    return engine

engine = get_engine(S.postgres.dbname)

SessionMaker = sessionmaker(bind=engine, class_=AsyncSession)

# database
Base = declarative_base()

utils_url = str(engine.url).replace('+asyncpg', '')

def init_database(create=False):
    while not database_exists(utils_url):
        if create:
            logger.info('Creating database')
            create_database(utils_url)

            async def create_tables():
                async with engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
            asyncio.run(create_tables())
        sleep(0.5)


# from sqlalchemy import event
# from sqlalchemy.engine import Engine
# import time
# import logging
# logger1 = logging.getLogger("myapp.sqltime")
# logger1.setLevel(logging.DEBUG)
# @event.listens_for(Engine, "before_cursor_execute")
# def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
#     conn.info.setdefault("query_start_time", []).append(time.time())
#     # logger1.debug(f"Start Query: {statement}")


# @event.listens_for(Engine, "after_cursor_execute")
# def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
#     total = time.time() - conn.info["query_start_time"].pop(-1)
#     logger1.debug(f"Query Complete: {statement}! Total Time: {total}")

@dataclass(init=False)
class Block(Base):
    __tablename__ = 'blocks'
    block_id: int = Column(Integer, autoincrement=True, primary_key=True)
    
    workchain: int = Column(Integer, nullable=False)
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
    def raw_block_to_dict(cls, raw_block):
        return {'workchain': raw_block['workchain'],
                'shard': int(raw_block['shard']),
                'seqno': raw_block['seqno'],
                'root_hash': raw_block['root_hash'],
                'file_hash': raw_block['file_hash'] }


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
    def raw_header_to_dict(cls, raw_header):
        return {
            'global_id': raw_header['global_id'],
            'version': raw_header['version'],
            'flags': raw_header.get('flags', 0),
            'after_merge': raw_header['after_merge'],
            'after_split': raw_header['after_split'],
            'before_split': raw_header['before_split'],
            'want_merge': raw_header['want_merge'],
            'validator_list_hash_short': raw_header['validator_list_hash_short'],
            'catchain_seqno': raw_header['catchain_seqno'],
            'min_ref_mc_seqno': raw_header['min_ref_mc_seqno'],
            'is_key_block': raw_header['is_key_block'],
            'prev_key_block_seqno': raw_header['prev_key_block_seqno'],
            'start_lt': int(raw_header['start_lt']),
            'end_lt': int(raw_header['end_lt']),
            'gen_utime': int(raw_header['gen_utime']),
            'vert_seqno': raw_header.get('vert_seqno', 0)
        }


@dataclass(init=False)
class Transaction(Base):
    __tablename__ = 'transactions'
    
    tx_id: int = Column(BigInteger, autoincrement=True, primary_key=True)
    account: str = Column(String)
    account_code_hash: str = Column(String)
    # account_code_hash_rel = relationship("CodeHashInterfaces")
    account_code_hash_rel = relationship('CodeHashInterfaces', foreign_keys=[account_code_hash],
                                primaryjoin='CodeHashInterfaces.code_hash == Transaction.account_code_hash')

    lt: int = Column(BigInteger)
    hash: str = Column(String(44))
    
    balance: int = Column(BigInteger)
    utime: int = Column(BigInteger)
    fee: int = Column(BigInteger)
    storage_fee: int = Column(BigInteger)
    other_fee: int = Column(BigInteger)
    transaction_type = Column(Enum('trans_storage', 'trans_ord', 'trans_tick_tock', \
        'trans_split_prepare', 'trans_split_install', 'trans_merge_prepare', 'trans_merge_install', name='trans_type'))
    compute_exit_code: int = Column(Integer)
    compute_gas_used: int = Column(Integer)
    compute_gas_limit: int = Column(Integer)
    compute_gas_credit: int = Column(Integer)
    compute_gas_fees: int = Column(BigInteger)
    compute_vm_steps: int = Column(Integer)
    compute_skip_reason: str = Column(Enum('cskip_no_state', 'cskip_bad_state', 'cskip_no_gas', name='compute_skip_reason_type'))
    action_result_code: int = Column(Integer)
    action_total_fwd_fees: int = Column(BigInteger)
    action_total_action_fees: int = Column(BigInteger)

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
    def raw_transaction_to_dict(cls, raw, raw_detail):
        try:
            parsed_tx = parse_transaction(raw_detail['data'])
        except:
            logger.error(f"Error parsing transaction data {raw_detail['data']}")
            raise
        
        def safe_get(dict_val, keys):
            res = dict_val
            for key in keys:
                res = res.get(key) if res else None
            return res

        transaction_type = safe_get(parsed_tx, ['description', 'type'])
        compute_exit_code = safe_get(parsed_tx, ['description', 'compute_ph', 'exit_code'])
        compute_gas_used = safe_get(parsed_tx, ['description', 'compute_ph', 'gas_used'])
        compute_gas_limit = safe_get(parsed_tx, ['description', 'compute_ph', 'gas_limit'])
        compute_gas_credit = safe_get(parsed_tx, ['description', 'compute_ph', 'gas_credit'])
        compute_gas_fees = safe_get(parsed_tx, ['description', 'compute_ph', 'gas_fees'])
        compute_vm_steps = safe_get(parsed_tx, ['description', 'compute_ph', 'vm_steps'])
        compute_skip_reason = safe_get(parsed_tx, ['description', 'compute_ph', 'reason'])
        action_result_code = safe_get(parsed_tx, ['description', 'action', 'result_code'])
        action_total_fwd_fees = safe_get(parsed_tx, ['description', 'action', 'total_fwd_fees'])
        action_total_action_fees = safe_get(parsed_tx, ['description', 'action', 'total_action_fees'])
        return {
            'account': raw['account'],
            'account_code_hash': raw_detail['code_hash'],
            'lt': int(raw['lt']),
            'hash': raw['hash'],
            'balance': int(raw_detail['balance']),
            'utime': raw_detail['utime'],
            'fee': int(raw_detail['fee']),
            'storage_fee': int(raw_detail['storage_fee']),
            'other_fee': int(raw_detail['other_fee']),
            'transaction_type': transaction_type,
            'compute_exit_code': compute_exit_code,
            'compute_gas_used': compute_gas_used,
            'compute_gas_limit': compute_gas_limit,
            'compute_gas_credit': compute_gas_credit,
            'compute_gas_fees': compute_gas_fees,
            'compute_vm_steps': compute_vm_steps,
            'compute_skip_reason': compute_skip_reason,
            'action_result_code': action_result_code,
            'action_total_fwd_fees': action_total_fwd_fees,
            'action_total_action_fees': action_total_action_fees
        }

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
    hash: str = Column(String(44))
    body_hash: str = Column(String(44))
    op: int = Column(Integer)
    comment: str = Column(String)
    ihr_disabled: bool = Column(Boolean)
    bounce: bool = Column(Boolean)
    bounced: bool = Column(Boolean)
    has_init_state: bool = Column(Boolean)
    import_fee: int = Column(BigInteger)
    
    out_tx_id = Column(BigInteger, ForeignKey("transactions.tx_id"))
    # out_tx = relationship("Transaction", backref="out_msgs", foreign_keys=[out_tx_id])
    out_tx = relationship("Transaction", back_populates="out_msgs", foreign_keys=[out_tx_id])

    in_tx_id = Column(BigInteger, ForeignKey("transactions.tx_id"))
    # in_tx = relationship("Transaction", backref="in_msg", uselist=False, foreign_keys=[in_tx_id])
    in_tx = relationship("Transaction", back_populates="in_msg", uselist=False, foreign_keys=[in_tx_id])

    __table_args__ = (Index('messages_index_1', 'source'),
                      Index('messages_index_2', 'destination'),
                      Index('messages_index_3', 'created_lt'),
                      Index('messages_index_4', 'hash'),
                      Index('messages_index_5', 'body_hash'),
                      Index('messages_index_6', 'source', 'destination', 'created_lt'),
                      Index('messages_index_7', 'in_tx_id'),
                      Index('messages_index_8', 'out_tx_id'),
                     )
    
    @classmethod
    def raw_msg_to_dict(cls, raw):
        op = None
        comment = None
        msg_body = raw['msg_data']['body']
        try:
            msg_cell_boc = codecs.decode(codecs.encode(msg_body, 'utf8'), 'base64')
            message_cell = deserialize_boc(msg_cell_boc)
            if len(message_cell.data.data) >= 32:
                op = int.from_bytes(message_cell.data.data[:32].tobytes(), 'big', signed=True)
                if op == 0:
                    comment = codecs.decode(message_cell.data.data[32:], 'utf8')
                    while len(message_cell.refs) > 0:
                        message_cell = message_cell.refs[0]
                        comment += codecs.decode(message_cell.data.data, 'utf8')
                    comment = comment.replace('\x00', '')
        except BaseException as e:
            comment = None
            logger.error(f"Error parsing message comment and op: {e}, msg body: {msg_body}")
        
        source = detect_address(raw['source'])["raw_form"] if len(raw['source']) else ""
        destination = detect_address(raw['destination'])["raw_form"] if len(raw['destination']) else ""
        return {
            'source': source,
            'destination': destination,
            'value': int(raw['value']),
            'fwd_fee': int(raw['fwd_fee']),
            'ihr_fee': int(raw['ihr_fee']),
            'created_lt': int(raw['created_lt']),
            'hash': raw['hash'],
            'body_hash': raw['body_hash'],
            'op': op,
            'comment': comment,
            'ihr_disabled': raw['ihr_disabled'] if raw['ihr_disabled'] != -1 else None,
            'bounce': int(raw['bounce']) if int(raw['bounce']) != -1 else None,
            'bounced': int(raw['bounced']) if int(raw['bounced']) != -1 else None,
            'has_init_state': int(raw['has_init_state']),
            'import_fee': int(raw['import_fee']) if int(raw['import_fee']) != -1 else None,
        }


@dataclass(init=False)
class MessageContent(Base):
    __tablename__ = 'message_contents'
    
    msg_id: int = Column(BigInteger, ForeignKey("messages.msg_id"), primary_key=True)
    body: str = Column(String)
        
    msg = relationship("Message", backref=backref("content", cascade="save-update, merge, "
                                                  "delete, delete-orphan", uselist=False))

    @classmethod
    def raw_msg_to_content_dict(cls, raw_msg):
        return {
            'body': raw_msg['msg_data'].get('body')
        }

class CodeHashInterfaces(Base):
    __tablename__ = 'code_hash'

    code_hash = Column(String, primary_key=True)
    interfaces = Column(ARRAY(Enum('nft_item', 
                                   'nft_editable', 
                                   'nft_collection', 
                                   'nft_royalty',
                                   'jetton_wallet', 
                                   'jetton_master',
                                   'domain',
                                   'subscription',
                                   'auction',
                                   name='interface_name')))

