from enum import Enum
from typing import List, Optional, Literal, Union, Any, Dict
from pydantic import BaseModel, Field

import logging
logger = logging.getLogger(__name__)


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

    gen_utime: int
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
                     shard=obj.shard,
                     seqno=obj.seqno,
                     root_hash=obj.root_hash,
                     file_hash=obj.file_hash,
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
                     rand_seed=obj.rand_seed,
                     created_by=obj.created_by,
                     masterchain_block_ref=BlockReference(workchain=obj.mc_block_workchain, 
                                                          shard=obj.mc_block_shard, 
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
        return MessageContent(hash=obj.hash,
                              body=obj.body)


class Message(BaseModel):
    hash: str
    source: str
    destination: str
    value: Optional[int]
    fwd_fee: Optional[int]
    ihr_fee: Optional[int]
    created_lt: Optional[int]
    created_at: Optional[int]
    opcode: Optional[int]
    ihr_disabled: Optional[bool]
    bounce: Optional[bool]
    import_fee: Optional[int]
    body_hash: str
    init_state_hash: Optional[str]

    message_content: Optional[MessageContent]

    @classmethod
    def from_orm(cls, obj, include_msg_body=False):
        return Message(hash=obj.hash,
                       source=obj.source or 'addr_none',
                       destination=obj.destination or 'addr_none',
                       value=obj.value,
                       fwd_fee=obj.fwd_fee,
                       ihr_fee=obj.ihr_fee,
                       created_lt=obj.created_lt,
                       created_at=obj.created_at,
                       opcode=obj.opcode,
                       ihr_disabled=obj.ihr_disabled,
                       bounce=obj.bounce,
                       import_fee=obj.import_fee,
                       body_hash=obj.body_hash,
                       init_state_hash=obj.init_state_hash,
                       message_content=MessageContent.from_orm(obj.message_content) 
                                       if include_msg_body else None)


class AccountState(BaseModel):
    hash: str
    account: str
    balance: int
    account_status: AccountStatus
    frozen_hash: str
    code_hash: str
    data_hash: str

    @classmethod
    def from_orm(cls, obj):
        return AccountState(hash=obj.hash,
                            account=obj.account,
                            balance=obj.balance,
                            account_status=AccountStatus(obj.account_status),
                            frozen_hash=obj.frozen_hash,
                            code_hash=obj.code_hash,
                            data_hash=obj.data_hash)


class Transaction(BaseModel):
    account: str
    hash: str
    lt: int

    now: int

    orig_status: AccountStatus
    end_status: AccountStatus

    total_fees: int

    account_state_hash_before: str
    account_state_hash_after: str

    description: Any

    block_ref: Optional[BlockReference]
    in_msg: Optional[Message]
    out_msgs: List[Message]

    @classmethod
    def from_orm(cls, obj, include_msg_body=False):
        in_msg = None
        out_msgs = []
        for tx_msg in obj.messages:
            msg = Message.from_orm(tx_msg.message, include_msg_body=include_msg_body)
            if tx_msg.direction == 'in':
                in_msg = msg
            else:
                out_msgs.append(msg)
        return Transaction(account=obj.account,
                           hash=obj.hash,
                           lt=obj.lt,
                           now=obj.now,
                           orig_status=AccountStatus(obj.orig_status),
                           end_status=AccountStatus(obj.end_status),
                           total_fees=obj.total_fees,
                           account_state_hash_after=obj.account_state_hash_after,
                           account_state_hash_before=obj.account_state_hash_before,
                           description=obj.description,
                           block_ref=BlockReference(workchain=obj.block_workchain, 
                                                    shard=obj.block_shard, 
                                                    seqno=obj.block_seqno),
                           in_msg=in_msg,
                           out_msgs=out_msgs)
