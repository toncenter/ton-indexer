import json
from typing import List, Optional, Literal, Union

from pydantic import BaseModel, Field
from loguru import logger

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


SmcInterface = Literal["nft_item", "nft_collection", "nft_editable", "jetton_wallet", "jetton_master"]

from pytonlib.utils import tlb

class MsgAddressInt(BaseModel):
    type: Literal["addr_std", "addr_var"]
    workchain_id: int
    address: str

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.MsgAddressInt): 
        return MsgAddressInt(type=tlb_obj.type,
                             workchain_id=tlb_obj.workchain_id,
                             address=tlb_obj.address)

class MsgAddressExt(BaseModel):
    type: Literal["addr_none", "addr_extern"]

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.MsgAddressExt): 
        return MsgAddressExt(type=tlb_obj.type)

class MsgAddress:
    @classmethod
    def from_tlb(cls, tlb_obj: Union[tlb.MsgAddressInt, tlb.MsgAddressExt]) -> Union[MsgAddressInt, MsgAddressExt]: 
        if type(tlb_obj) == tlb.MsgAddressInt:
            return MsgAddressInt.from_tlb(tlb_obj)
        elif type(tlb_obj) == tlb.MsgAddressExt:
            return MsgAddressExt.from_tlb(tlb_obj)
        raise RuntimeError(f"Unexpected object of type {type(tlb_obj)}")

class InternalMsgBody(BaseModel):
    pass

class NftTransfer(InternalMsgBody):
    type: Literal["nft_transfer"] = "nft_transfer"
    query_id: int
    new_owner: Union[MsgAddressInt, MsgAddressExt]
    response_destination: Union[MsgAddressInt, MsgAddressExt]
    # custom_payload: str
    forward_amount: int
    # forward_payload: str

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.NftTransferMessage):
        return NftTransfer(query_id=tlb_obj.query_id, 
                           new_owner=MsgAddress.from_tlb(tlb_obj.new_owner),
                           response_destination=MsgAddress.from_tlb(tlb_obj.response_destination),
                           forward_amount=tlb_obj.forward_amount)

class NftOwnershipAssigned(InternalMsgBody):
    type: Literal["nft_ownership_assigned"] = "nft_ownership_assigned"
    query_id: int
    prev_owner: Union[MsgAddressInt, MsgAddressExt]
    # forward_payload: str

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.NftOwnershipAssignedMessage):
        return NftOwnershipAssigned(query_id=tlb_obj.query_id, 
                                    prev_owner=MsgAddress.from_tlb(tlb_obj.prev_owner))

class NftExcesses(InternalMsgBody):
    type: Literal["nft_excesses"] = "nft_excesses"
    query_id: int

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.NftExcessesMessage):
        return NftExcesses(query_id=tlb_obj.query_id)

class NftGetStaticData(InternalMsgBody):
    type: Literal["nft_get_static_data"] = "nft_get_static_data"
    query_id: int

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.NftGetStaticDataMessage):
        return NftGetStaticData(query_id=tlb_obj.query_id)

class NftReportStaticData(InternalMsgBody):
    type: Literal["nft_report_static_data"] = "nft_report_static_data"
    query_id: int
    index: int
    collection: Union[MsgAddressInt, MsgAddressExt]

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.NftGetStaticDataMessage):
        return NftReportStaticData(query_id=tlb_obj.query_id,
                                   index=tlb_obj.index,
                                   collection=MsgAddress.from_tlb(tlb_obj.collection))

class JettonTransfer(InternalMsgBody):
    type: Literal["jetton_transfer"] = "jetton_transfer"
    query_id: int
    amount: int
    destination: Union[MsgAddressInt, MsgAddressExt]
    response_destination: Union[MsgAddressInt, MsgAddressExt]

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.JettonTransferMessage):
        return JettonTransfer(query_id=tlb_obj.query_id,
                              amount=tlb_obj.amount,
                              destination=MsgAddress.from_tlb(tlb_obj.destination),
                              response_destination=MsgAddress.from_tlb(tlb_obj.response_destination))

class JettonTransferNotification(InternalMsgBody):
    type: Literal["jetton_transfer_notification"] = "jetton_transfer_notification"
    query_id: int
    amount: int
    sender: Union[MsgAddressInt, MsgAddressExt]

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.JettonTransferNotificationMessage):
        return JettonTransferNotification(query_id=tlb_obj.query_id,
                                          amount=tlb_obj.amount,
                                          sender=MsgAddress.from_tlb(tlb_obj.sender))

class JettonExcesses(InternalMsgBody):
    type: Literal["jetton_excesses"] = "jetton_excesses"
    query_id: int

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.JettonExcessesMessage):
        return JettonExcesses(query_id=tlb_obj.query_id)

class JettonBurn(InternalMsgBody):
    type: Literal["jetton_burn"] = "jetton_burn"
    query_id: int
    amount: int
    response_destination: Union[MsgAddressInt, MsgAddressExt]

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.JettonBurnMessage):
        return JettonBurn(query_id=tlb_obj.query_id,
                          amount=tlb_obj.amount,
                          response_destination=MsgAddress.from_tlb(tlb_obj.response_destination))

class JettonInternalTransfer(InternalMsgBody):
    type: Literal["jetton_internal_transfer"] = "jetton_internal_transfer"
    query_id: int
    amount: int
    from_: Union[MsgAddressInt, MsgAddressExt]
    response_address: Union[MsgAddressInt, MsgAddressExt]
    forward_ton_amount: int

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.JettonInternalTransferMessage):
        return JettonInternalTransfer(query_id=tlb_obj.query_id,
                                      amount=tlb_obj.amount,
                                      from_=MsgAddress.from_tlb(tlb_obj.from_),
                                      response_address=MsgAddress.from_tlb(tlb_obj.response_address),
                                      forward_ton_amount=tlb_obj.forward_ton_amount)

class JettonBurnNotification(InternalMsgBody):
    type: Literal["jetton_burn_notification"] = "jetton_burn_notification"
    query_id: int
    amount: int
    sender: Union[MsgAddressInt, MsgAddressExt]
    response_destination: Union[MsgAddressInt, MsgAddressExt]

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.JettonBurnNotificationMessage):
        return JettonBurnNotification(query_id=tlb_obj.query_id,
                                      amount=tlb_obj.amount,
                                      sender=MsgAddress.from_tlb(tlb_obj.sender),
                                      response_destination=MsgAddress.from_tlb(tlb_obj.response_destination))

class TextComment(InternalMsgBody):
    type: Literal["text_comment"] = "text_comment"
    comment: str

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.TextCommentMessage):
        return TextComment(comment=tlb_obj.text_comment)

class BinaryComment(InternalMsgBody):
    type: Literal["binary_comment"] = "binary_comment"
    hex_comment: str

    @classmethod
    def from_tlb(cls, tlb_obj: tlb.BinaryCommentMessage): 
        return BinaryComment(hex_comment=tlb_obj.hex_comment)

class Comment():
    @classmethod
    def from_tlb(cls, tlb_obj: Union[tlb.TextCommentMessage, tlb.BinaryCommentMessage]) -> Union[TextComment, BinaryComment]:
        if type(tlb_obj) is tlb.TextCommentMessage:
            return TextComment.from_tlb(tlb_obj)
        if type(tlb_obj) is tlb.BinaryCommentMessage:
            return BinaryComment.from_tlb(tlb_obj)
        raise RuntimeError(f"Unexpected object of type {type(tlb_obj)}")

class RawBody(InternalMsgBody):
    type: Literal["raw"] = "raw"
    boc: str

def get_msg_body_annotation(body, op, source_interfaces, dest_interfaces):
    def int32_twos_complement(hexstr):
        value = int(hexstr, 16)
        if value & (1 << 31):
            value -= 1 << 32
        return value

    result = None
    if 'nft_item' in dest_interfaces:
        if op == int32_twos_complement('0x5fcc3d14'):
            obj = tlb.boc_to_object(body, tlb.NftTransferMessage)
            result = NftTransfer.from_tlb(obj)
        elif op == int32_twos_complement('0x2fcb26a2'):
            obj = tlb.boc_to_object(body, tlb.NftGetStaticDataMessage)
            result = NftGetStaticData.from_tlb(obj)
    if 'nft_item' in source_interfaces:
        if op == int32_twos_complement('0x05138d91'):
            obj = tlb.boc_to_object(body, tlb.NftOwnershipAssignedMessage)
            result = NftOwnershipAssigned.from_tlb(obj)
        elif op == int32_twos_complement('0xd53276db'):
            obj = tlb.boc_to_object(body, tlb.NftExcessesMessage)
            result = NftExcesses.from_tlb(obj)
        elif op == int32_twos_complement('0x8b771735'):
            obj = tlb.boc_to_object(body, tlb.NftReportStaticDataMessage)
            result = NftReportStaticData.from_tlb(obj)
    if 'jetton_wallet' in dest_interfaces:
        if op == int32_twos_complement('0x0f8a7ea5'):
            obj = tlb.boc_to_object(body, tlb.JettonTransferMessage)
            result = JettonTransfer.from_tlb(obj)
        elif op == int32_twos_complement('0x595f07bc'):
            obj = tlb.boc_to_object(body, tlb.JettonBurnMessage)
            result = JettonBurn.from_tlb(obj)
        elif op == int32_twos_complement('0x178d4519'):
            obj = tlb.boc_to_object(body, tlb.JettonInternalTransferMessage)
            result = JettonInternalTransfer.from_tlb(obj)
    if 'jetton_wallet' in source_interfaces:
        if op == int32_twos_complement('0xd53276db'):
            obj = tlb.boc_to_object(body, tlb.JettonExcessesMessage)
            result = JettonExcesses.from_tlb(obj)
        elif op == int32_twos_complement('0x7362d09c'):
            obj = tlb.boc_to_object(body, tlb.JettonTransferNotificationMessage)
            result = JettonTransferNotification.from_tlb(obj)
    if 'jetton_master' in dest_interfaces:
        if op == int32_twos_complement('0x7bdd97de'):
            obj = tlb.boc_to_object(body, tlb.JettonBurnNotificationMessage)
            result = JettonBurnNotification.from_tlb(obj)

    if op == 0:
        obj = tlb.boc_to_object(body, tlb.CommentMessage)
        result = Comment.from_tlb(obj)

    if result is None:
        result = RawBody(boc=body)

    logger.info(f"MSG BODY: {result}")
    return result

class Message(BaseModel):
    source: str
    destination: str
    value: int
    fwd_fee: int
    ihr_fee: int
    created_lt: int
    op: Optional[int]
    hash: str
    body_hash: str
    body: Union[NftTransfer, NftOwnershipAssigned, NftExcesses, TextComment, NftGetStaticData, NftReportStaticData,
                JettonTransfer, JettonBurn, JettonBurnNotification, JettonInternalTransfer, JettonTransferNotification, JettonExcesses,
                TextComment, BinaryComment, RawBody, None] = Field(..., discriminator='type')

    @classmethod
    def message_from_orm(cls, obj, body_model):
        logger.info(f'm_f_o {obj} ----- {body_model}')
        return Message(
            source=obj.source,
            destination=obj.destination,
            value=obj.value,
            fwd_fee=obj.fwd_fee,
            ihr_fee=obj.ihr_fee,
            created_lt=obj.created_lt,
            op=obj.op,
            hash=obj.hash,
            body_hash=obj.body_hash,
            body=body_model
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
    compute_skip_reason: Optional[str]
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
            body_model = None
            if include_msg_bodies:
                body = obj.in_msg.content.body
                op = obj.in_msg.op
                source_interfaces = obj.in_msg.out_tx.account_code_hash_rel.interfaces if obj.in_msg.out_tx else []
                dest_interfaces = obj.account_code_hash_rel.interfaces
                body_model = get_msg_body_annotation(body, op, source_interfaces, dest_interfaces)
            
            in_msg = Message.message_from_orm(obj.in_msg, body_model)
        else:
            in_msg = None
        out_msgs = []
        for out_msg in obj.out_msgs:
            body_model = None
            if include_msg_bodies:
                body = out_msg.content.body
                op = out_msg.op
                source_interfaces = obj.account_code_hash_rel.interfaces
                dest_interfaces = out_msg.in_tx.account_code_hash_rel.interfaces if out_msg.in_tx else []
                body_model = get_msg_body_annotation(body, op, source_interfaces, dest_interfaces)
            
            out_msgs.append(Message.message_from_orm(out_msg, body_model))

        return Transaction(
            account=obj.account,
            lt=obj.lt,
            hash=obj.hash,
            utime=obj.utime,
            fee=obj.fee,
            storage_fee=obj.storage_fee,
            other_fee=obj.other_fee,
            transaction_type=obj.transaction_type,
            compute_skip_reason=obj.compute_skip_reason,
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
