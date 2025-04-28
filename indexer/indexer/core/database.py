from __future__ import annotations

import asyncio
import logging
from time import sleep
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, CompositeType

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
                                 max_overflow=24,
                                 pool_timeout=128,
                                 echo=False)
    return engine
engine = get_engine(settings)
SessionMaker = sessionmaker(bind=engine, class_=AsyncSession)

# # async engine
def get_sync_engine(settings: Settings):
    pg_dsn = settings.pg_dsn.replace('+asyncpg', '')
    logger.critical(pg_dsn)
    engine = create_engine(pg_dsn,
                           pool_size=128,
                           max_overflow=0,
                           pool_timeout=5,
                           echo=False)
    return engine
sync_engine = get_sync_engine(settings)
SyncSessionMaker = sessionmaker(bind=sync_engine)

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


def convert_numerics_to_strings(data, exclusions):
    """
    Recursively converts numeric values to strings in a dictionary, excluding specific top-level keys.

    :param data: The dictionary to process.
    :param exclusions: A set of top-level keys to exclude from processing.
    :return: The processed dictionary with numeric values converted to strings.
    """

    def convert(value):
        # Helper function to recursively process values
        if isinstance(value, dict):
            return {k: convert(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [convert(item) for item in value]
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
        return value

    # Process the dictionary, applying exclusions
    result = {}
    for key, value in data.items():
        if key in exclusions:
            result[key] = value  # Exclude these keys
        else:
            result[key] = convert(value)

    return result

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


class Trace(Base):
    __tablename__ = 'traces'
    trace_id = Column(String(44), primary_key=True)
    external_hash: str = Column(String)
    mc_seqno_start: int = Column(Integer)
    mc_seqno_end: int = Column(Integer)
    start_lt: int = Column(BigInteger)
    start_utime: int = Column(Integer)
    end_lt: int = Column(BigInteger)
    end_utime: int = Column(Integer)
    state = Column(Enum('complete', 'pending', 'broken', name='trace_state'))
    pending_edges_: int = Column(BigInteger)
    edges_: int = Column(BigInteger)
    nodes_: int = Column(BigInteger)
    classification_state = Column(Enum('unclassified', 'failed', 'ok', 'broken', name='trace_classification_state'))

    # edges: List[TraceEdge] = relationship("TraceEdge", back_populates="trace", uselist=True, viewonly=True)
    transactions: List["Transaction"] = relationship("Transaction",
                                                     foreign_keys=[trace_id],
                                                     primaryjoin='Trace.trace_id == Transaction.trace_id',
                                                     uselist=True,
                                                     viewonly=True)


class TraceEdge(Base):
    __tablename__ = 'trace_edges'
    trace_id: str = Column(String(44), ForeignKey("traces.trace_id"), primary_key=True)
    msg_hash: str = Column(String(44), primary_key=True)
    left_tx: str = Column(String)
    right_tx: str = Column(String)
    incomplete: bool = Column(Boolean)
    broken: bool = Column(Boolean)

    # trace: "Trace" = relationship("Trace", back_populates="edges", viewonly=True)

class ActionAccount(Base):
    __tablename__ = 'action_accounts'
    action_id: str = Column(String, primary_key=True)
    trace_id: str = Column(String, primary_key=True)
    account: str = Column(String(70), primary_key=True)
    trace_end_lt: int = Column(Numeric)
    action_end_lt: int = Column(Numeric)
    trace_end_utime: int = Column(Numeric)
    action_end_utime: int = Column(Numeric)

class Action(Base):
    __tablename__ = 'actions'

    trace_id: str = Column(String(44), nullable=False, primary_key=True)
    action_id: str = Column(String, nullable=False, primary_key=True)
    type: str = Column(String())
    tx_hashes: list[str] = Column(ARRAY(String()))
    value: int | None = Column(Numeric)
    amount: int | None = Column(Numeric)
    start_lt: int | None = Column(BigInteger)
    end_lt: int | None = Column(BigInteger)
    start_utime: int | None = Column(BigInteger)
    end_utime: int | None = Column(BigInteger)
    source: str | None = Column(String(70))
    source_secondary: str | None = Column(String(70))
    destination: str | None = Column(String(70))
    destination_secondary: str | None = Column(String(70))
    asset: str | None = Column(String(70))
    asset_secondary: str | None = Column(String(70))
    asset2: str | None = Column(String(70))
    asset2_secondary: str | None = Column(String(70))
    opcode: int | None = Column(BigInteger)
    success: bool = Column(Boolean)
    ton_transfer_data = Column(CompositeType("ton_transfer_details", [
        Column("content", String),
        Column("encrypted", Boolean)
    ]))
    jetton_transfer_data = Column(CompositeType("jetton_transfer_details", [
        Column("response_destination", String),
        Column("forward_amount", Numeric),
        Column("query_id", Numeric),
        Column("custom_payload", String),
        Column("forward_payload", String),
        Column("comment", String),
        Column("is_encrypted_comment", Boolean)
    ]))
    nft_transfer_data = Column(CompositeType("nft_transfer_details", [
        Column("is_purchase", Boolean),
        Column("price", Numeric),
        Column("query_id", Numeric),
        Column("custom_payload", String),
        Column("forward_payload", String),
        Column("forward_amount", Numeric),
        Column("response_destination", String),
        Column("nft_item_index", Numeric),
    ]))
    jetton_swap_data = Column(CompositeType("jetton_swap_details", [
        Column("dex", String),
        Column("sender", String),
        Column("dex_incoming_transfer", CompositeType("dex_transfer_details",[
            Column("amount", Numeric),
            Column("asset", String),
            Column("source", String),
            Column("destination", String),
            Column("source_jetton_wallet", String),
            Column("destination_jetton_wallet", String)
        ])),
        Column("dex_outgoing_transfer", CompositeType("dex_transfer_details", [
            Column("amount", Numeric),
            Column("asset", String),
            Column("source", String),
            Column("destination", String),
            Column("source_jetton_wallet", String),
            Column("destination_jetton_wallet", String)
        ])),
        Column("peer_swaps", ARRAY(CompositeType("peer_swap_details", [
            Column("asset_in", String),
            Column("amount_in", Numeric),
            Column("asset_out", String),
            Column("amount_out", Numeric),
        ])))]))
    change_dns_record_data = Column(CompositeType("change_dns_record_details", [
        Column("key", String),
        Column("value_schema", String),
        Column("value", String),
        Column("flags", Integer)
    ]))
    nft_mint_data = Column(CompositeType("nft_mint_details", [
        Column("nft_item_index", Numeric)]))
    evaa_supply_data = Column(CompositeType("evaa_supply_details", [
        Column("sender_jetton_wallet", String),
        Column("recipient_jetton_wallet", String),
        Column("master_jetton_wallet", String),
        Column("master", String),
        Column("asset_id", String),
        Column("is_ton", Boolean)
    ]))
    evaa_withdraw_data = Column(CompositeType("evaa_withdraw_details", [
        Column("sender_jetton_wallet", String),
        Column("recipient_jetton_wallet", String),
        Column("master_jetton_wallet", String),
        Column("master", String),
        Column("fail_reason", String),
        Column("asset_id", String)
    ]))
    evaa_liquidate_data = Column(CompositeType("evaa_liquidate_details", [
        Column("fail_reason", String),
        Column("debt_amount", Numeric),
        Column("asset_id", String)

    ]))
    dex_deposit_liquidity_data = Column(CompositeType("dex_deposit_liquidity_details", [
        Column("dex", String),
        Column("amount1", Numeric),
        Column("amount2", Numeric),
        Column("asset1", String),
        Column("asset2", String),
        Column('user_jetton_wallet_1', String),
        Column('user_jetton_wallet_2', String),
        Column("lp_tokens_minted", Numeric),
    ]))
    dex_withdraw_liquidity_data = Column(CompositeType("dex_withdraw_liquidity_details", [
        Column("dex", String),
        Column("amount1", Numeric),
        Column("amount2", Numeric),
        Column('asset1_out', String),
        Column('asset2_out', String),
        Column('user_jetton_wallet_1', String),
        Column('user_jetton_wallet_2', String),
        Column('dex_jetton_wallet_1', String),
        Column('dex_jetton_wallet_2', String),
        Column("lp_tokens_burnt", Numeric),
        Column('dex_wallet_1', String),
        Column('dex_wallet_2', String)
    ]))
    jvault_claim_data = Column(CompositeType("jvault_claim_details", [
        Column("claimed_jettons", ARRAY(String())),
        Column("claimed_amounts", ARRAY(Numeric()))
    ]))
    jvault_stake_data = Column(CompositeType("jvault_stake_details", [
        Column("period", Numeric),
        Column("minted_stake_jettons", Numeric),
        Column("stake_wallet", String)
    ]))
    multisig_create_order_data = Column(CompositeType("multisig_create_order_details", [
        Column("query_id", Numeric),
        Column("order_seqno", Numeric),
        Column("is_created_by_signer", Boolean),
        Column("is_signed_by_creator", Boolean),
        Column("creator_index", Numeric),
        Column("expiration_date", Numeric),
        Column("order_boc", String),
    ]))
    multisig_approve_data = Column(CompositeType("multisig_approve_details", [
        Column("signer_index", Numeric),
        Column("exit_code", Numeric),
    ]))
    multisig_execute_data = Column(CompositeType("multisig_execute_details", [
        Column("query_id", Numeric),
        Column("order_seqno", Numeric),
        Column("expiration_date", Numeric),
        Column("approvals_num", Numeric),
        Column("signers_hash", String),
        Column("order_boc", String),
    ]))
    vesting_send_message_data = Column(CompositeType("vesting_send_message_details", [
        Column("query_id", Numeric),
        Column("message_boc", String),
    ]))
    vesting_add_whitelist_data = Column(CompositeType("vesting_add_whitelist_details", [
        Column("query_id", Numeric),
        Column("accounts_added", ARRAY(String()))
    ]))
    staking_data = Column(CompositeType("staking_details", [
        Column("provider", String),
        Column("ts_nft", String),
        Column("tokens_burnt", Numeric),
        Column("tokens_minted", Numeric),
    ]))
    trace_end_lt: int = Column(Numeric)
    trace_end_utime: int = Column(Numeric)
    trace_external_hash: str = Column(String)
    mc_seqno_end: int = Column(Numeric)
    trace_mc_seqno_end: int = Column(Numeric)
    value_extra_currencies: dict = Column(JSONB)
    parent_action_id: str = Column(String)
    ancestor_type: list[str] = Column(ARRAY(String), default=[])

    _accounts: list[str]

    def __repr__(self):
        full_repr = ""
        for key, value in self.__dict__.items():
            if key.startswith("_"):
                continue
            full_repr += f"{key}={value}, "
        return full_repr

    def get_action_accounts(self):
        accounts = []
        for account in self._accounts:
            accounts.append(ActionAccount(action_id=self.action_id,
                                          trace_id=self.trace_id,
                                          account=account,
                                          action_end_lt=self.end_lt,
                                          trace_end_lt=self.trace_end_lt,
                                          trace_end_utime=self.trace_end_utime,
                                          action_end_utime=self.end_utime))
        return accounts


    def to_dict(self):
        r = self.__dict__.copy()
        r.pop('_sa_instance_state')

        return convert_numerics_to_strings(r, {'start_lt', 'end_lt', 'start_utime', 'end_utime', 'opcode'})

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
    mc_block_seqno = Column(Integer)
    trace_id = Column(String(44))

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

    descr = Column(Enum('ord', 'storage', 'tick_tock', 'split_prepare',
                        'split_install', 'merge_prepare', 'merge_install', name='descr_type'))
    aborted: bool = Column(Boolean)
    destroyed: bool = Column(Boolean)
    credit_first: bool = Column(Boolean)
    is_tock: bool = Column(Boolean)
    installed: bool = Column(Boolean)
    storage_fees_collected: int = Column(BigInteger)
    storage_fees_due: int = Column(BigInteger)
    storage_status_change = Column(Enum('unchanged', 'frozen', 'deleted', name='status_change_type'))
    credit_due_fees_collected: int = Column(BigInteger)
    credit: int = Column(BigInteger)
    compute_skipped: bool = Column(Boolean)
    skipped_reason = Column(Enum('no_state', 'bad_state', 'no_gas', 'suspended', name='skipped_reason_type'))
    compute_success: bool = Column(Boolean)
    compute_msg_state_used: bool = Column(Boolean)
    compute_account_activated: bool = Column(Boolean)
    compute_gas_fees: int = Column(BigInteger)
    compute_gas_used: int = Column(BigInteger)
    compute_gas_limit: int = Column(BigInteger)
    compute_gas_credit: int = Column(BigInteger)
    compute_mode: int = Column(Integer)
    compute_exit_code: int = Column(Integer)
    compute_exit_arg: int = Column(Integer)
    compute_vm_steps: int = Column(BigInteger)
    compute_vm_init_state_hash: str = Column(String)
    compute_vm_final_state_hash: str = Column(String)
    action_success: bool = Column(Boolean)
    action_valid: bool = Column(Boolean)
    action_no_funds: bool = Column(Boolean)
    action_status_change = Column(Enum('unchanged', 'frozen', 'deleted', name='status_change_type'))
    action_total_fwd_fees: int = Column(BigInteger)
    action_total_action_fees: int = Column(BigInteger)
    action_result_code: int = Column(Integer)
    action_result_arg: int = Column(Integer)
    action_tot_actions: int = Column(Integer)
    action_spec_actions: int = Column(Integer)
    action_skipped_actions: int = Column(Integer)
    action_msgs_created: int = Column(Integer)
    action_action_list_hash: str = Column(String)
    action_tot_msg_size_cells: int = Column(BigInteger)
    action_tot_msg_size_bits: int = Column(BigInteger)
    bounce = Column(Enum('negfunds', 'nofunds', 'ok', name='bounce_type'))
    bounce_msg_size_cells: int = Column(BigInteger)
    bounce_msg_size_bits: int = Column(BigInteger)
    bounce_req_fwd_fees: int = Column(BigInteger)
    bounce_msg_fees: int = Column(BigInteger)
    bounce_fwd_fees: int = Column(BigInteger)
    split_info_cur_shard_pfx_len: int = Column(Integer)
    split_info_acc_split_depth: int = Column(Integer)
    split_info_this_addr: str = Column(String)
    split_info_sibling_addr: str = Column(String)

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
    messages: List[Message] = relationship("Message", back_populates="transaction", viewonly=True)
    trace: Optional[Trace] = relationship("Trace", foreign_keys=[trace_id], primaryjoin="Transaction.trace_id == Trace.trace_id", viewonly=True)
    emulated: bool = False

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
    msg_hash: str = Column(String(44), primary_key=True)
    tx_hash: str = Column(String(44), ForeignKey("transactions.hash"), primary_key=True)
    tx_lt: int = Column(BigInteger, primary_key=True)
    direction = Column(Enum('out', 'in', name='msg_direction'), primary_key=True)
    trace_id: str = Column(String(44))
    source: str = Column(String)
    destination: str = Column(String)
    value: int = Column(BigInteger)
    fwd_fee: int = Column(BigInteger)
    ihr_fee: int = Column(BigInteger)
    created_lt: int = Column(BigInteger)
    created_at: int = Column(BigInteger)
    opcode: int = Column(BigInteger)
    ihr_disabled: bool = Column(Boolean)
    bounce: bool = Column(Boolean)
    bounced: bool = Column(Boolean)
    import_fee: int = Column(BigInteger)
    body_hash: str = Column(String(44))
    init_state_hash: Optional[str] = Column(String(44), nullable=True)
    value_extra_currencies: dict = Column(JSONB, nullable=True)

    transaction = relationship("Transaction",
                               viewonly=True,
                               back_populates="messages",
                               foreign_keys=[tx_hash],
                               primaryjoin="Message.tx_hash == Transaction.hash")

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

    def __repr__(self):
        opcode = self.opcode
        if opcode is not None:
            if opcode > 0:
                opcode = hex(opcode)
            else:
                opcode = hex(opcode & 0xffffffff)

        return f"Message({self.direction}, {self.msg_hash}, {opcode})"


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
    trace_id = Column(String(44))

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


class NftSale(Base):
    __tablename__  = 'getgems_nft_sales'

    address = Column(String, primary_key=True)
    is_complete = Column(Boolean)
    created_at = Column(BigInteger)
    marketplace_address = Column(String)
    nft_address = Column(String)
    nft_owner_address = Column(String)
    full_price = Column(Numeric)


class NftAuction(Base):
    __tablename__ = 'getgems_nft_auctions'

    address = Column(String, primary_key=True)
    nft_addr = Column(String)
    nft_owner = Column(String)



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
    data_boc: str = Column(String)

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

# Index("transaction_messages_index_1", TransactionMessage.transaction_hash, postgresql_using='btree', postgresql_concurrently=False)
# Index("message_contents_index_1", MessageContent.hash, postgresql_using='btree', postgresql_concurrently=False)

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
