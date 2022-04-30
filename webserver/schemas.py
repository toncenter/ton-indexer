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

    class Config:
        orm_mode = True

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

    class Config:
        orm_mode = True

