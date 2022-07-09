from typing import List, Optional

from pydantic import BaseModel, Field

class Block(BaseModel):
    workchain: int
    shard: str
    seqno: int
    root_hash: str
    file_hash: str
    gen_utime: int
    start_lt: int
    end_lt: int

    @classmethod
    def block_from_orm_block_header(cls, obj):
        return Block(
            workchain=obj.block.workchain,
            shard=str(obj.block.shard),
            seqno=obj.block.seqno,
            root_hash=obj.block.root_hash,
            file_hash=obj.block.file_hash,
            gen_utime=obj.gen_utime,
            start_lt=obj.start_lt,
            end_lt=obj.end_lt
        )

class Message(BaseModel):
    source: str
    destination: str
    value: int
    fwd_fee: int
    ihr_fee: int
    created_lt: int
    op: Optional[int]
    comment: Optional[str]
    body_hash: str
    body: Optional[str]

    @classmethod
    def message_from_orm(cls, obj, include_msg_bodies):
        return Message(
            source=obj.source,
            destination=obj.destination,
            value=obj.value,
            fwd_fee=obj.fwd_fee,
            ihr_fee=obj.ihr_fee,
            created_lt=obj.created_lt,
            op=obj.op,
            comment=obj.comment,
            body_hash=obj.body_hash,
            body=obj.content.body if include_msg_bodies else None
        )

class Transaction(BaseModel):
    account: str
    lt: int
    hash: str
    utime: int
    fee: int
    storage_fee: int
    other_fee: int
    transaction_type: str
    compute_exit_code: Optional[int]
    compute_gas_used: Optional[int]
    compute_gas_limit: Optional[int]
    compute_gas_credit: Optional[int]
    compute_gas_fees: Optional[int]
    compute_vm_steps: Optional[int]
    action_result_code: Optional[int]
    action_total_fwd_fees: Optional[int]
    action_total_action_fees: Optional[int]
    in_msg: Optional[Message]
    out_msgs: List[Message] = []

    @classmethod
    def transaction_from_orm(cls, obj, include_msg_bodies):
        if obj.in_msg is not None:
            in_msg = Message.message_from_orm(obj.in_msg, include_msg_bodies)
        else:
            in_msg = None
        out_msgs = [Message.message_from_orm(out_msg, include_msg_bodies) for out_msg in obj.out_msgs]
        return Transaction(
            account=obj.account,
            lt=obj.lt,
            hash=obj.hash,
            utime=obj.utime,
            fee=obj.fee,
            storage_fee=obj.storage_fee,
            other_fee=obj.other_fee,
            transaction_type=obj.transaction_type,
            compute_exit_code=obj.compute_exit_code,
            compute_gas_used=obj.compute_gas_used,
            compute_gas_limit=obj.compute_gas_limit,
            compute_gas_credit=obj.compute_gas_credit,
            compute_gas_fees=obj.compute_gas_fees,
            compute_vm_steps=obj.compute_vm_steps,
            action_result_code=obj.action_result_code,
            action_total_fwd_fees=obj.action_total_fwd_fees,
            action_total_action_fees=obj.action_total_action_fees,
            in_msg=in_msg,
            out_msgs=out_msgs
        )

class CountResponse(BaseModel):
    count: int
