from enum import Enum
from typing import List, Optional, Literal, Union, Any, Dict
from pydantic import BaseModel, Field

from indexer.core.utils import b64_to_hex, address_to_raw, int_to_hex

import logging
logger = logging.getLogger(__name__)



def hash_type(value):
    return b64_to_hex(value).upper() if value else None

def address_type(value):
    return address_to_raw(value).upper() if value else None

def shard_type(value):
    return int_to_hex(value, length=64, signed=True).upper() if value else None


class BlockReference(BaseModel):
    workchain: int
    shard: str
    seqno: int


class Block(BaseModel):
    workchain: int
    shard: str
    seqno: int
    root_hash: str
    file_hash: str

    global_id: int
    version: int
    after_merge: bool
    before_split: bool
    after_split: bool
    want_split: bool
    key_block: bool
    vert_seqno_incr: bool
    flags: int

    gen_utime: str
    start_lt: str
    end_lt: str

    validator_list_hash_short: int
    gen_catchain_seqno: int
    min_ref_mc_seqno: int
    prev_key_block_seqno: int
    vert_seqno: int
    master_ref_seqno: Optional[int]
    rand_seed: str
    created_by: str

    masterchain_block_ref: Optional[BlockReference]


    @classmethod
    def from_orm(cls, obj):
        return Block(workchain=obj.workchain,
                     shard=shard_type(obj.shard),
                     seqno=obj.seqno,
                     root_hash=hash_type(obj.root_hash),
                     file_hash=hash_type(obj.file_hash),
                     global_id=obj.global_id,
                     version=obj.version,
                     after_merge=obj.after_merge,
                     before_split=obj.before_split,
                     after_split=obj.after_split,
                     want_split=obj.want_split,
                     key_block=obj.key_block,
                     vert_seqno_incr=obj.vert_seqno_incr,
                     flags=obj.flags,
                     gen_utime=obj.gen_utime,
                     start_lt=obj.start_lt,
                     end_lt=obj.end_lt,
                     validator_list_hash_short=obj.validator_list_hash_short,
                     gen_catchain_seqno=obj.gen_catchain_seqno,
                     min_ref_mc_seqno=obj.min_ref_mc_seqno,
                     prev_key_block_seqno=obj.prev_key_block_seqno,
                     vert_seqno=obj.vert_seqno,
                     master_ref_seqno=obj.master_ref_seqno,
                     rand_seed=hash_type(obj.rand_seed),
                     created_by=hash_type(obj.created_by),
                     masterchain_block_ref=BlockReference(workchain=obj.mc_block_workchain, 
                                                          shard=shard_type(obj.mc_block_shard), 
                                                          seqno=obj.mc_block_seqno) 
                                           if obj.mc_block_seqno is not None else None)


class AccountStatus(str, Enum):
    uninit = 'uninit'
    frozen = 'frozen'
    active = 'active'
    nonexist = 'nonexist'


class MessageContent(BaseModel):
    hash: str
    body: str

    @classmethod
    def from_orm(cls, obj):
        return MessageContent(hash=hash_type(obj.hash),
                              body=obj.body)


class Message(BaseModel):
    hash: str
    source: str
    destination: str
    value: Optional[str]
    fwd_fee: Optional[str]
    ihr_fee: Optional[str]
    created_lt: Optional[str]
    created_at: Optional[str]
    opcode: Optional[int]
    ihr_disabled: Optional[bool]
    bounce: Optional[bool]
    bounced: Optional[bool]
    import_fee: Optional[str]
    body_hash: str
    init_state_hash: Optional[str]

    message_content: Optional[MessageContent]
    init_state: Optional[MessageContent]

    @classmethod
    def from_orm(cls, obj):
        return Message(hash=hash_type(obj.hash),
                       source=address_type(obj.source) or 'addr_none',
                       destination=address_type(obj.destination) or 'addr_none',
                       value=obj.value,
                       fwd_fee=obj.fwd_fee,
                       ihr_fee=obj.ihr_fee,
                       created_lt=obj.created_lt,
                       created_at=obj.created_at,
                       opcode=obj.opcode,
                       ihr_disabled=obj.ihr_disabled,
                       bounce=obj.bounce,
                       bounced=obj.bounced,
                       import_fee=obj.import_fee,
                       body_hash=hash_type(obj.body_hash),
                       init_state_hash=hash_type(obj.init_state_hash),
                       message_content=MessageContent.from_orm(obj.message_content) if obj.message_content else None,
                       init_state=MessageContent.from_orm(obj.init_state) if obj.init_state else None)


class AccountState(BaseModel):
    hash: str
    account: str
    balance: str
    account_status: AccountStatus
    frozen_hash: Optional[str]
    code_hash: str
    data_hash: str

    @classmethod
    def from_orm(cls, obj):
        return AccountState(hash=hash_type(obj.hash),
                            account=address_type(obj.account),
                            balance=obj.balance,
                            account_status=AccountStatus(obj.account_status),
                            frozen_hash=hash_type(obj.frozen_hash),
                            code_hash=hash_type(obj.code_hash),
                            data_hash=hash_type(obj.data_hash))


class Transaction(BaseModel):
    account: str
    hash: str
    lt: str

    now: int

    orig_status: AccountStatus
    end_status: AccountStatus

    total_fees: str

    account_state_hash_before: str
    account_state_hash_after: str

    prev_trans_hash: str
    prev_trans_lt: str

    description: Any

    block_ref: Optional[BlockReference]
    in_msg: Optional[Message]
    out_msgs: List[Message]

    account_state_before: Optional[AccountState]
    account_state_after: Optional[AccountState]

    @classmethod
    def from_orm(cls, obj):
        in_msg = None
        out_msgs = []
        for tx_msg in obj.messages:
            msg = Message.from_orm(tx_msg.message)
            if tx_msg.direction == 'in':
                in_msg = msg
            else:
                out_msgs.append(msg)
        return Transaction(account=address_type(obj.account),
                           hash=hash_type(obj.hash),
                           lt=obj.lt,
                           now=obj.now,
                           orig_status=AccountStatus(obj.orig_status),
                           end_status=AccountStatus(obj.end_status),
                           total_fees=obj.total_fees,
                           account_state_hash_after=hash_type(obj.account_state_hash_after),
                           account_state_hash_before=hash_type(obj.account_state_hash_before),
                           prev_trans_hash=hash_type(obj.prev_trans_hash),
                           prev_trans_lt=obj.prev_trans_lt,
                           description=obj.description,
                           block_ref=BlockReference(workchain=obj.block_workchain, 
                                                    shard=shard_type(obj.block_shard), 
                                                    seqno=obj.block_seqno),
                           in_msg=in_msg,
                           out_msgs=out_msgs,
                           account_state_before=AccountState.from_orm(obj.account_state_before) if obj.account_state_before else None,
                           account_state_after=AccountState.from_orm(obj.account_state_after) if obj.account_state_after else None,)
