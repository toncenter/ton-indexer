import codecs
import asyncio
from os import environ
import decimal
from copy import deepcopy
from time import sleep
from typing import List, Optional
from datetime import datetime

from pytonlib.utils.tlb import parse_transaction
from tvm_valuetypes.cell import deserialize_boc

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database

from sqlalchemy import Column, String, Integer, BigInteger, Boolean, Index, Enum, Numeric, LargeBinary
from sqlalchemy import ForeignKey, UniqueConstraint, Table
from sqlalchemy import and_, or_, ColumnDefault
from sqlalchemy.orm import relationship, backref
from dataclasses import dataclass, asdict

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select


from config import settings as S
from loguru import logger

MASTERCHAIN_INDEX = -1
MASTERCHAIN_SHARD = -9223372036854775808

try:
    with open(S.postgres.password_file, 'r') as f:
        db_password = f.read()
except:
    logger.info("pg password file not found, using PGPASSWORD env var")
    db_password = environ["PGPASSWORD"]

# init database
def get_engine(database):
    if "PGCONNECTION_URL" in environ:
        connection_url = environ["PGCONNECTION_URL"]
    else:
        connection_url = 'postgresql+asyncpg://{user}:{db_password}@{host}:{port}/{dbname}'.format(host=S.postgres.host,
                                                                                  port=S.postgres.port,
                                                                                  user=S.postgres.user,
                                                                                  db_password=db_password,
                                                                                  dbname=database)
    engine = create_async_engine(connection_url, pool_size=20, max_overflow=10, echo=False)
    return engine

engine = get_engine(S.postgres.dbname)

SessionMaker = sessionmaker(bind=engine, class_=AsyncSession)

# database
Base = declarative_base()

utils_url = str(engine.url).replace('+asyncpg', '')

def delete_database():
    if database_exists(utils_url):
        logger.info('Drop database')
        drop_database(utils_url)


async def check_database_inited(url):
    if not database_exists(url):
        return False
    return True


async def init_database(create=False):
    logger.info(f"Create db ${utils_url}")
    logger.info(database_exists(utils_url))
    while not await check_database_inited(utils_url):
        logger.info("Create db")
        if create:
            logger.info('Creating database')
            create_database(utils_url)
        asyncio.sleep(0.5)

    if create:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    logger.info("DB ready")


def cell_b64(cell):
    return codecs.encode(cell.hash(), "base64").decode().strip()

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
    lt: int = Column(BigInteger)
    hash: str = Column(String(44))
    
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
    created_time: int = Column(BigInteger)
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
        except NotImplementedError as e:
            logger.error(f"Error parsing transaction data {raw_detail['data']}: {e}")
            return None
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
            'lt': int(raw['lt']),
            'hash': raw['hash'],
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
            'action_total_action_fees': action_total_action_fees,
            'created_time': int(datetime.today().timestamp())
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
    import_fee: int = Column(BigInteger)
    created_time: int = Column(BigInteger)
    
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

        return {
            'source': raw['source'],
            'destination': raw['destination'],
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
            'import_fee': int(raw['import_fee']) if int(raw['import_fee']) != -1 else None,
            'created_time': int(datetime.today().timestamp())
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

@dataclass(init=False)
class Code(Base):
    __tablename__ = 'code'

    hash: str = Column(String, primary_key=True)
    code: str = Column(String)

@dataclass(init=False)
class AccountState(Base):
    __tablename__ = 'account_state'

    state_id: int = Column(BigInteger, primary_key=True)
    address: str = Column(String)
    check_time: int = Column(BigInteger)
    last_tx_lt: int = Column(BigInteger)
    last_tx_hash: str = Column(String)
    balance: int = Column(BigInteger)

    code_hash: str = Column(String)
    data: str = Column(String)

    __table_args__ = (Index('account_state_index_1', 'address', 'last_tx_lt'),
                      UniqueConstraint('address', 'last_tx_lt'))

    @classmethod
    def raw_account_info_to_content_dict(cls, raw, address):
        code_cell = None
        try:
            if len(raw['code']) > 0:
                code_cell_boc = codecs.decode(codecs.encode(raw['code'], 'utf8'), 'base64')
                code_cell = deserialize_boc(code_cell_boc)
        except NotImplementedError:
            logger.error(f"NotImplementedError for {address}")

        return {
            'address': address,
            'check_time': int(datetime.today().timestamp()),
            'last_tx_lt': int(raw['last_transaction_id']['lt']) if 'last_transaction_id' in raw else None,
            'last_tx_hash': raw['last_transaction_id']['hash'] if 'last_transaction_id' in raw else None,
            'balance': int(raw['balance']),
            'code_hash': cell_b64(code_cell) if code_cell is not None else None,
            'data':  raw['data']
        }


@dataclass(init=False)
class KnownAccounts(Base):
    __tablename__ = 'accounts'

    address: str = Column(String, primary_key=True)
    last_check_time: int = Column(BigInteger)

    __table_args__ = (Index('known_accounts_index_1', 'last_check_time'),)

    @classmethod
    def from_address(cls, address):
        return {
            'address': address,
            'last_check_time': None
        }


@dataclass(init=False)
class ParseOutbox(Base):
    __tablename__ = 'parse_outbox'

    PARSE_TYPE_MESSAGE = 1
    PARSE_TYPE_ACCOUNT = 2

    outbox_id: int = Column(BigInteger, primary_key=True)
    added_time: int = Column(BigInteger)
    entity_type = Column(BigInteger)
    entity_id: int = Column(BigInteger)
    attempts: int = Column(BigInteger)

    __table_args__ = (Index('parse_outbox_index_1', 'added_time'),
                      UniqueConstraint('entity_type', 'entity_id')
                      )
    @classmethod
    def generate(cls, entity_type, entity_id, added_time, attempts=0):
        return {
            'entity_type': entity_type,
            'entity_id': entity_id,
            'added_time': added_time,
            'attempts': attempts
        }


"""
Models for parsed data
"""
@dataclass(init=False)
class JettonTransfer(Base):
    __tablename__ = 'jetton_transfers'

    id: int = Column(BigInteger, primary_key=True)
    msg_id: int = Column(BigInteger, ForeignKey('messages.msg_id'))
    successful: bool = Column(Boolean)
    originated_msg_id: int = Column(BigInteger, ForeignKey('messages.msg_id'))
    query_id: str = Column(String)
    amount: decimal.Decimal = Column(Numeric(scale=0)) # in some cases Jetton amount is larger than pgsql int8 (bigint)
    source_owner: str = Column(String)
    destination_owner: str = Column(String)
    source_wallet: str = Column(String)
    response_destination: str = Column(String)
    custom_payload: str = Column(LargeBinary)
    forward_ton_amount: int = Column(BigInteger)
    forward_payload: str = Column(LargeBinary)
    sub_op: int = Column(BigInteger)


    __table_args__ = (Index('jetton_transfer_index_1', 'source_owner'),
                      Index('jetton_transfer_index_2', 'destination_owner'),
                      Index('jetton_transfer_index_3', 'query_id'),
                      UniqueConstraint('msg_id')
                      )

"""
op::internal_transfer without preceding op::transfer message, it is typically used for minting
"""
@dataclass(init=False)
class JettonMint(Base):
    __tablename__ = 'jetton_mint'

    id: int = Column(BigInteger, primary_key=True)
    msg_id: int = Column(BigInteger, ForeignKey('messages.msg_id'))
    successful: bool = Column(Boolean)
    originated_msg_id: int = Column(BigInteger, ForeignKey('messages.msg_id'))
    query_id: str = Column(String)
    amount: decimal.Decimal = Column(Numeric(scale=0))
    minter: str = Column(String) # sender of internal_transfer
    from_address: str = Column(String) # equals to minter?
    wallet: str = Column(String)
    response_destination: str = Column(String)
    forward_ton_amount: int = Column(BigInteger)
    forward_payload: str = Column(LargeBinary)
    sub_op: int = Column(BigInteger)


    __table_args__ = (Index('jetton_mint_index_1', 'minter'),
                      Index('jetton_mint_index_2', 'wallet'),
                      UniqueConstraint('msg_id')
                      )


"""
op::burn request
"""
@dataclass(init=False)
class JettonBurn(Base):
    __tablename__ = 'jetton_burn'

    id: int = Column(BigInteger, primary_key=True)
    msg_id: int = Column(BigInteger, ForeignKey('messages.msg_id'))
    successful: bool = Column(Boolean)
    originated_msg_id: int = Column(BigInteger, ForeignKey('messages.msg_id'))
    query_id: str = Column(String)
    amount: decimal.Decimal = Column(Numeric(scale=0))
    owner: str = Column(String) # jettons owner
    wallet: str = Column(String)
    response_destination: str = Column(String)
    custom_payload: str = Column(LargeBinary)

    __table_args__ = (Index('jetton_burn_index_1', 'owner'),
                      Index('jetton_burn_index_2', 'wallet'),
                      UniqueConstraint('msg_id')
                      )


@dataclass(init=False)
class JettonWallet(Base):
    __tablename__ = 'jetton_wallets'

    id: int = Column(BigInteger, primary_key=True)
    state_id: int = Column(BigInteger, ForeignKey('account_state.state_id'))
    address: str = Column(String)
    owner: str = Column(String)
    jetton_master: str = Column(String)
    balance: decimal.Decimal = Column(Numeric(scale=0)) # NOTE: it is not actual balance, just balance associated with state_id


    __table_args__ = (Index('jetton_wallet_index_1', 'owner'),
                      Index('jetton_wallet_index_2', 'jetton_master'),
                      UniqueConstraint('address')
                      )

@dataclass(init=False)
class JettonMaster(Base):
    __tablename__ = 'jetton_master'

    id: int = Column(BigInteger, primary_key=True)
    state_id: int = Column(BigInteger, ForeignKey('account_state.state_id'))
    address: str = Column(String)
    total_supply: decimal.Decimal = Column(Numeric(scale=0))
    mintable: str = Column(String)
    admin_address: str = Column(String)
    jetton_wallet_code: str = Column(String)
    symbol: str = Column(String)
    name: str = Column(String)
    image: str = Column(String)
    image_data: str = Column(String)
    decimals: int = Column(BigInteger)
    metadata_url: str = Column(String)
    description: str = Column(String)


    __table_args__ = (UniqueConstraint('address'), )

