import asyncio
import logging
from time import sleep
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists

from sqlalchemy import Column, String, Integer, BigInteger, Boolean, Index, Enum, Numeric
from sqlalchemy.schema import ForeignKeyConstraint
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import JSONB

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.future import select


from indexer.core.settings import Settings


logger = logging.getLogger(__name__)

MASTERCHAIN_INDEX = -1
MASTERCHAIN_SHARD = -9223372036854775808

settings = Settings()


# async engine
def get_engine(settings: Settings):
    logger.critical(settings.pg_dsn)
    engine = create_async_engine(settings.pg_dsn, 
                                 pool_size=128, 
                                 max_overflow=0, 
                                 pool_timeout=5,
                                 pool_pre_ping=True, # using pessimistic approach about closed connections problem: https://docs.sqlalchemy.org/en/14/core/pooling.html#disconnect-handling-pessimistic
                                 echo=False,
                                 connect_args={'server_settings': {'statement_timeout': '3000'}}
                                 )
    return engine
engine = get_engine(settings)
SessionMaker = sessionmaker(bind=engine, class_=AsyncSession)

# # async engine
# def get_sync_engine(settings: Settings):
#     pg_dsn = settings.pg_dsn.replace('+asyncpg', '')
#     logger.critical(pg_dsn)
#     engine = create_engine(pg_dsn, 
#                            pool_size=128, 
#                            max_overflow=0, 
#                            pool_timeout=5,
#                            echo=False)
#     return engine
# sync_engine = get_sync_engine(settings)
# SyncSessionMaker = sessionmaker(bind=sync_engine)

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


# types
AccountStatus = Enum('uninit', 'frozen', 'active', 'nonexist', name='account_status')



# classes
class ShardBlock(Base):
    __tablename__ = 'shard_state'
    mc_seqno: int = Column(Integer, primary_key=True)
    workchain: int = Column(Integer, primary_key=True)
    shard: int = Column(BigInteger, primary_key=True)
    seqno: int = Column(Integer, primary_key=True)

    block = relationship(
        "Block",
        primaryjoin="and_(ShardBlock.workchain == foreign(Block.workchain), ShardBlock.shard == foreign(Block.shard), ShardBlock.seqno == foreign(Block.seqno))",
        uselist=False
    )


class Block(Base):
    __tablename__ = 'blocks'
    __table_args__ = (
        ForeignKeyConstraint(
            ["mc_block_workchain", "mc_block_shard", "mc_block_seqno"],
            ["blocks.workchain", "blocks.shard", "blocks.seqno"]
        ),
    )

    workchain: int = Column(Integer, primary_key=True)
    shard: int = Column(BigInteger, primary_key=True)
    seqno: int = Column(Integer, primary_key=True)
    root_hash: str = Column(String(44))
    file_hash: str = Column(String(44))

    mc_block_workchain: int = Column(Integer, nullable=True)
    mc_block_shard: str = Column(BigInteger, nullable=True)
    mc_block_seqno: int = Column(Integer, nullable=True)

    masterchain_block = relationship("Block", 
                                     remote_side=[workchain, shard, seqno], 
                                     backref='shard_blocks')

    global_id: int = Column(Integer)
    version: int = Column(Integer)
    after_merge: bool = Column(Boolean)
    before_split: bool = Column(Boolean)
    after_split: bool = Column(Boolean)
    want_merge: bool = Column(Boolean)
    want_split: bool = Column(Boolean)
    key_block: bool = Column(Boolean)
    vert_seqno_incr: bool = Column(Boolean)
    flags: int = Column(Integer)
    gen_utime: int = Column(BigInteger)
    start_lt: int = Column(BigInteger)
    end_lt: int = Column(BigInteger)
    validator_list_hash_short: int = Column(Integer)
    gen_catchain_seqno: int = Column(Integer)
    min_ref_mc_seqno: int = Column(Integer)
    prev_key_block_seqno: int = Column(Integer)
    vert_seqno: int = Column(Integer)
    master_ref_seqno: int = Column(Integer, nullable=True)
    rand_seed: str = Column(String(44))
    created_by: str = Column(String)

    tx_count: int = Column(Integer)
    prev_blocks: List[Any] = Column(JSONB)

    transactions = relationship("Transaction", back_populates="block")


class Event(Base):
    __tablename__ = 'events'
    id: int = Column(BigInteger, primary_key=True)
    meta: Dict[str, Any] = Column(JSONB)
    
    # transactions: List["EventTransaction"] = relationship("EventTransaction", back_populates="event")
    transactions: List["Transaction"] = relationship("Transaction", 
                                                     foreign_keys=[id],
                                                     primaryjoin='Event.id == Transaction.event_id',
                                                     uselist=True,
                                                     viewonly=True)
    edges: List["EventEdge"] = relationship("EventEdge", back_populates="event")


# class EventTransaction(Base):
#     __tablename__ = 'event_transactions'
#     event_id: int = Column(BigInteger, ForeignKey("events.id"), primary_key=True)
#     tx_hash: str = Column(String, ForeignKey("transactions.hash"), primary_key=True)

#     event: Event = relationship("Event", back_populates="transactions")
#     transactions: List["Transaction"] = relationship("Transaction", back_populates="event")


class EventEdge(Base):
    __tablename__ = 'event_graph'
    event_id: int = Column(BigInteger, ForeignKey("events.id"), primary_key=True)
    left_tx_hash: str = Column(String, primary_key=True)
    right_tx_hash: str = Column(String, primary_key=True)

    event: "Event" = relationship("Event", back_populates="edges")


class Transaction(Base):
    __tablename__ = 'transactions'
    __table_args__ = (
        ForeignKeyConstraint(
            ["block_workchain", "block_shard", "block_seqno"],
            ["blocks.workchain", "blocks.shard", "blocks.seqno"]
        ),
    )

    block_workchain = Column(Integer)
    block_shard = Column(BigInteger)
    block_seqno = Column(Integer)

    mc_block_seqno: int = Column(Integer, nullable=True)

    block = relationship("Block", back_populates="transactions")

    account: str = Column(String)
    hash: str = Column(String, primary_key=True)
    lt: int = Column(BigInteger)
    prev_trans_hash = Column(String)
    prev_trans_lt = Column(BigInteger)
    now: int = Column(Integer)

    orig_status = Column(AccountStatus)
    end_status = Column(AccountStatus)

    total_fees = Column(BigInteger)

    account_state_hash_before = Column(String)
    account_state_hash_after = Column(String)

    event_id: Optional[int] = Column(BigInteger)
    account_state_before = relationship("AccountState", 
                                        foreign_keys=[account_state_hash_before],
                                        primaryjoin="AccountState.hash == Transaction.account_state_hash_before", 
                                        viewonly=True)
    account_state_after = relationship("AccountState", 
                                       foreign_keys=[account_state_hash_after],
                                       primaryjoin="AccountState.hash == Transaction.account_state_hash_after", 
                                       viewonly=True)
    account_state_latest = relationship("LatestAccountState", 
                                       foreign_keys=[account],
                                       primaryjoin="LatestAccountState.account == Transaction.account",
                                       lazy='selectin',
                                       viewonly=True)
    description = Column(JSONB)
    
    messages: List["TransactionMessage"] = relationship("TransactionMessage", back_populates="transaction")
    event: Optional["Event"] = relationship("Event", 
                                  foreign_keys=[event_id],
                                  primaryjoin="Transaction.event_id == Event.id",
                                  viewonly=True)
    # event: Event = relationship("EventTransaction", back_populates="transactions")


class AccountState(Base):
    __tablename__ = 'account_states'

    hash = Column(String, primary_key=True)
    account = Column(String)
    balance = Column(BigInteger)
    account_status = Column(AccountStatus)
    frozen_hash = Column(String)
    code_hash = Column(String)
    data_hash = Column(String)


class Message(Base):
    __tablename__ = 'messages'
    hash: str = Column(String(44), primary_key=True)
    source: str = Column(String)
    destination: str = Column(String)
    value: int = Column(BigInteger)
    fwd_fee: int = Column(BigInteger)
    ihr_fee: int = Column(BigInteger)
    created_lt: int = Column(BigInteger)
    created_at: int = Column(BigInteger)
    opcode: int = Column(Integer)
    ihr_disabled: bool = Column(Boolean)
    bounce: bool = Column(Boolean)
    bounced: bool = Column(Boolean)
    import_fee: int = Column(BigInteger)
    body_hash: str = Column(String(44))
    init_state_hash: Optional[str] = Column(String(44), nullable=True)

    transactions = relationship("TransactionMessage", 
                                foreign_keys=[hash],
                                primaryjoin="TransactionMessage.message_hash == Message.hash", 
                                uselist=True,
                                viewonly=True)
    message_content = relationship("MessageContent", 
                                   foreign_keys=[body_hash],
                                   primaryjoin="Message.body_hash == MessageContent.hash",
                                   viewonly=True)
    init_state = relationship("MessageContent", 
                              foreign_keys=[init_state_hash],
                              primaryjoin="Message.init_state_hash == MessageContent.hash", 
                              viewonly=True)
    
    source_account_state = relationship("LatestAccountState", 
                              foreign_keys=[source],
                              primaryjoin="Message.source == LatestAccountState.account", 
                              lazy='selectin',
                              viewonly=True)

    destination_account_state = relationship("LatestAccountState", 
                              foreign_keys=[destination],
                              primaryjoin="Message.destination == LatestAccountState.account", 
                              lazy='selectin',
                              viewonly=True)


class TransactionMessage(Base):
    __tablename__ = 'transaction_messages'
    transaction_hash: str = Column(String(44), ForeignKey('transactions.hash'), primary_key=True)
    message_hash: str = Column(String(44), primary_key=True)
    direction: str = Column(Enum('in', 'out', name="direction"), primary_key=True)

    transaction: "Transaction" = relationship("Transaction", back_populates="messages")
    # message = relationship("Message", back_populates="transactions")
    message: "Message" = relationship("Message", foreign_keys=[message_hash],
                                      primaryjoin="TransactionMessage.message_hash == Message.hash", 
                                      viewonly=True)


class MessageContent(Base):
    __tablename__ = 'message_contents'
    
    hash: str = Column(String(44), primary_key=True)
    body: str = Column(String)

    # message = relationship("Message", back_populates="message_content")


class JettonWallet(Base):
    __tablename__ = 'jetton_wallets'
    address = Column(String, primary_key=True)
    balance: int = Column(Numeric)
    owner = Column(String)
    jetton = Column(String)
    last_transaction_lt = Column(BigInteger)
    code_hash = Column(String)
    data_hash = Column(String)

    transfers: List["JettonTransfer"] = relationship("JettonTransfer",
                                                     foreign_keys=[address],
                                                     primaryjoin="JettonWallet.address == JettonTransfer.jetton_wallet_address",
                                                     viewonly=True)
    burns: List["JettonBurn"] = relationship("JettonBurn",
                                             foreign_keys=[address],
                                             primaryjoin="JettonWallet.address == JettonBurn.jetton_wallet_address",
                                             viewonly=True)
    
    jetton_master: "JettonMaster" = relationship("JettonMaster",
                                                 foreign_keys=[jetton],
                                                 primaryjoin="JettonWallet.jetton == JettonMaster.address")


class JettonMaster(Base):
    __tablename__ = 'jetton_masters'
    address = Column(String, primary_key=True)
    total_supply: int = Column(Numeric)
    mintable: bool = Column(Boolean)
    admin_address = Column(String, nullable=True)
    jetton_content = Column(JSONB, nullable=True)
    jetton_wallet_code_hash = Column(String)
    code_hash = Column(String)
    data_hash = Column(String)
    last_transaction_lt = Column(BigInteger)
    code_boc = Column(String)
    data_boc = Column(String)


class JettonTransfer(Base):
    __tablename__ = 'jetton_transfers'
    transaction_hash = Column(String, ForeignKey("transactions.hash"), primary_key=True)
    query_id: int = Column(Numeric)
    amount: int = Column(Numeric)
    source = Column(String)
    destination = Column(String)
    jetton_wallet_address = Column(String)
    response_destination = Column(String)
    custom_payload = Column(String)
    forward_ton_amount: int = Column(Numeric)
    forward_payload = Column(String)

    transaction: Transaction = relationship("Transaction")
    jetton_wallet: JettonWallet = relationship("JettonWallet",
                                               foreign_keys=[jetton_wallet_address],
                                               primaryjoin="JettonWallet.address == JettonTransfer.jetton_wallet_address")


class JettonBurn(Base):
    __tablename__ = 'jetton_burns'
    transaction_hash = Column(String, ForeignKey("transactions.hash"), primary_key=True)
    query_id: int = Column(Numeric)
    owner: str = Column(String)
    jetton_wallet_address: str = Column(String)
    amount: int = Column(Numeric)
    response_destination = Column(String)
    custom_payload = Column(String)

    transaction: Transaction = relationship("Transaction")
    jetton_wallet: JettonWallet = relationship("JettonWallet",
                                               foreign_keys=[jetton_wallet_address],
                                               primaryjoin="JettonWallet.address == JettonBurn.jetton_wallet_address")


class NFTCollection(Base):
    __tablename__ = 'nft_collections'
    address = Column(String, primary_key=True)
    next_item_index: int = Column(Numeric)
    owner_address = Column(String)
    collection_content = Column(JSONB)
    data_hash = Column(String)
    code_hash = Column(String)
    last_transaction_lt = Column(BigInteger)
    code_boc = Column(String)
    data_boc = Column(String)

    items: List["NFTItem"] = relationship('NFTItem',
                                          foreign_keys=[address],
                                          primaryjoin="NFTCollection.address == NFTItem.collection_address",)


class NFTItem(Base):
    __tablename__ = 'nft_items'
    address = Column(String, primary_key=True)
    init: bool = Column(Boolean)
    index: int = Column(Numeric)
    collection_address = Column(String)  # TODO: index
    owner_address = Column(String)  # TODO: index
    content = Column(JSONB)
    last_transaction_lt = Column(BigInteger)
    code_hash = Column(String)
    data_hash = Column(String)

    collection: Optional[NFTCollection] = relationship('NFTCollection', 
                                                       foreign_keys=[collection_address],
                                                       primaryjoin="NFTCollection.address == NFTItem.collection_address",)
    
    transfers: List["NFTTransfer"] = relationship('NFTTransfer',
                                                  foreign_keys=[address],
                                                  primaryjoin="NFTItem.address == NFTTransfer.nft_item_address",)


class NFTTransfer(Base):
    __tablename__ = 'nft_transfers'
    transaction_hash = Column(String, ForeignKey("transactions.hash"), primary_key=True)
    query_id: int = Column(Numeric)
    nft_item_address = Column(String)  # TODO: index
    old_owner = Column(String)  # TODO: index
    new_owner = Column(String)  # TODO: index
    response_destination = Column(String)
    custom_payload = Column(String)
    forward_amount: int = Column(Numeric)
    forward_payload = Column(String)

    transaction: Transaction = relationship("Transaction")
    nft_item: NFTItem = relationship("NFTItem",
                                     foreign_keys=[nft_item_address],
                                     primaryjoin="NFTItem.address == NFTTransfer.nft_item_address",)

class LatestAccountState(Base):
    __tablename__ = 'latest_account_states'
    account = Column(String, primary_key=True)
    hash = Column(String)
    code_hash = Column(String)
    data_hash = Column(String)
    frozen_hash = Column(String)
    account_status = Column(String)
    timestamp = Column(Integer)
    last_trans_lt = Column(BigInteger)
    balance: int = Column(Numeric)

# Indexes
# Index("blocks_index_1", Block.workchain, Block.shard, Block.seqno)
Index("blocks_index_2", Block.gen_utime)
Index("blocks_index_3", Block.mc_block_workchain, Block.mc_block_shard, Block.mc_block_seqno)
Index("blocks_index_4", Block.seqno, postgresql_where=(Block.workchain == -1))
Index("blocks_index_5", Block.start_lt)

Index("transactions_index_1", Transaction.block_workchain, Transaction.block_shard, Transaction.block_seqno)
Index("transactions_index_2", Transaction.account, Transaction.lt)
Index("transactions_index_2a", Transaction.account, Transaction.now)
Index("transactions_index_3", Transaction.now, Transaction.hash)
Index("transactions_index_4", Transaction.lt, Transaction.hash)
Index("transactions_index_6", Transaction.event_id)
Index("transactions_index_8", Transaction.mc_block_seqno)

# Index('account_states_index_1', AccountState.hash)
# Index('account_states_index_2', AccountState.code_hash)

# Index("messages_index_1", Message.hash)
Index("messages_index_2", Message.source)
Index("messages_index_3", Message.destination)
Index("messages_index_4", Message.created_lt)
# Index("messages_index_5", Message.created_at)
# Index("messages_index_6", Message.body_hash)
# Index("messages_index_7", Message.init_state_hash)

# Index("transaction_messages_index_1", TransactionMessage.transaction_hash)
Index("transaction_messages_index_2", TransactionMessage.message_hash)

# Index("message_contents_index_1", MessageContent.hash)

# Index("jetton_wallets_index_1", JettonWallet.address)
Index("jetton_wallets_index_2", JettonWallet.owner)
Index("jetton_wallets_index_3", JettonWallet.jetton)
Index("jetton_wallets_index_4", JettonWallet.jetton, JettonWallet.balance)
# Index("jetton_wallets_index_4", JettonWallet.code_hash)

# Index("jetton_masters_index_1", JettonMaster.address)
Index("jetton_masters_index_2", JettonMaster.admin_address)
# Index("jetton_masters_index_3", JettonMaster.code_hash)

# Index("jetton_transfers_index_1", JettonTransfer.transaction_hash)
Index("jetton_transfers_index_2", JettonTransfer.source)
Index("jetton_transfers_index_3", JettonTransfer.destination)
Index("jetton_transfers_index_4", JettonTransfer.jetton_wallet_address)
# Index("jetton_transfers_index_5", JettonTransfer.response_destination)

# Index("jetton_burns_index_1", JettonBurn.transaction_hash)
Index("jetton_burns_index_2", JettonBurn.owner)
Index("jetton_burns_index_3", JettonBurn.jetton_wallet_address)

# Index("nft_collections_index_1", NFTCollection.address)
Index("nft_collections_index_2", NFTCollection.owner_address)
# Index("nft_collections_index_3", NFTCollection.code_hash)

# Index("nft_items_index_1", NFTItem.address)
Index("nft_items_index_2", NFTItem.collection_address)
Index("nft_items_index_3", NFTItem.owner_address)
Index("nft_items_index_4", NFTItem.collection_address, NFTItem.index)

# Index("nft_transfers_index_1", NFTTransfer.transaction_hash)
Index("nft_transfers_index_2", NFTTransfer.nft_item_address)
Index("nft_transfers_index_3", NFTTransfer.old_owner)
Index("nft_transfers_index_4", NFTTransfer.new_owner)


# # event indexes
# Index("event_transaction_index_1", EventTransaction.tx_hash)
Index("event_detector__transaction_index_1", Transaction.lt.asc(), postgresql_where=(Transaction.event_id.is_(None)))
