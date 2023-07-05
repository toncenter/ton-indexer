import asyncio
import logging
from time import sleep
from typing import Optional, List

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
from sqlalchemy.future import select


from indexer.core.settings import Settings


logger = logging.getLogger(__name__)

MASTERCHAIN_INDEX = -1
MASTERCHAIN_SHARD = -9223372036854775808

settings = Settings()


# init database
def get_engine(settings: Settings):
    logger.critical(settings.pg_dsn)
    engine = create_async_engine(settings.pg_dsn, 
                                 pool_size=128, 
                                 max_overflow=24, 
                                 pool_timeout=128,
                                 echo=False)
    return engine


engine = get_engine(settings)
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


# types
AccountStatus = Enum('uninit', 'frozen', 'active', 'nonexist', name='account_status')



# classes
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

    transactions = relationship("Transaction", back_populates="block")


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

    block = relationship("Block", back_populates="transactions")

    account = Column(String)
    hash = Column(String, primary_key=True)
    lt = Column(BigInteger)
    prev_trans_hash = Column(String)
    prev_trans_lt = Column(BigInteger)
    now = Column(Integer)

    orig_status = Column(AccountStatus)
    end_status = Column(AccountStatus)

    total_fees = Column(BigInteger)

    account_state_hash_before = Column(String)
    account_state_hash_after = Column(String)

    account_state_before = relationship("AccountState", 
                                        foreign_keys=[account_state_hash_before],
                                        primaryjoin="AccountState.hash == Transaction.account_state_hash_before", 
                                        viewonly=True)
    account_state_after = relationship("AccountState", 
                                       foreign_keys=[account_state_hash_after],
                                       primaryjoin="AccountState.hash == Transaction.account_state_hash_after", 
                                       viewonly=True)

    description = Column(JSONB)
    
    messages = relationship("TransactionMessage", back_populates="transaction")


class AccountState(Base):
    __tablename__ = 'account_states'

    hash = Column(String, primary_key=True)
    account = Column(String)
    balance = Column(BigInteger)
    account_status = Column(Enum('uninit', 'frozen', 'active', name='account_status_type'))
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
    body_hash: str = Column(String(44), ForeignKey("message_contents.hash"))
    init_state_hash: Optional[str] = Column(String(44), nullable=True)

    transactions = relationship("TransactionMessage", back_populates="message")
    message_content = relationship("MessageContent", back_populates="message")
    init_state = relationship("MessageContent", 
                              foreign_keys=[init_state_hash],
                              primaryjoin="Message.init_state_hash == MessageContent.hash", 
                              viewonly=True)


class TransactionMessage(Base):
    __tablename__ = 'transaction_messages'
    transaction_hash = Column(String(44), ForeignKey('transactions.hash'), primary_key=True)
    message_hash = Column(String(44), ForeignKey('messages.hash'), primary_key=True)
    direction = Column(Enum('in', 'out', name="direction"), primary_key=True)

    transaction = relationship("Transaction", back_populates="messages")
    message = relationship("Message", back_populates="transactions")


class MessageContent(Base):
    __tablename__ = 'message_contents'
    
    hash: str = Column(String(44), primary_key=True)
    body: str = Column(String)

    message = relationship("Message", back_populates="message_content")


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
                                                     primaryjoin="JettonWallet.address == JettonTransfer.jetton_wallet_address")
    burns: List["JettonBurn"] = relationship("JettonBurn",
                                             foreign_keys=[address],
                                             primaryjoin="JettonWallet.address == JettonBurn.jetton_wallet_address")
    
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
