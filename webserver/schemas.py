from typing import List, Optional

from pydantic import BaseModel, Field

class Message(BaseModel):
    source: str
    destination: str
    value: int
    fwd_fee: int
    ihr_fee: int
    created_lt: int
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
            in_msg=in_msg,
            out_msgs=out_msgs
        )
