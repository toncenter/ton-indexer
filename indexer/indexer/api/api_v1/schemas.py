from enum import Enum
from typing import List, Optional, Literal, Union, Any, Dict
from pydantic import BaseModel, ConfigDict, Field

from indexer.core.utils import b64_to_hex, address_to_raw, address_to_friendly, int_to_hex

from pytonlib.utils import tlb

import logging
logger = logging.getLogger(__name__)



def hash_type(value):
    return b64_to_hex(value).upper() if value else None

def address_type(value):
    return address_to_raw(value).upper() if value and value != 'addr_none' else None

def is_wallet(code_hash):
    wallets_code_hashes = {
        'oM/CxIruFqJx8s/AtzgtgXVs7LEBfQd/qqs7tgL2how=',    # wallet_v1_r1 
        '1JAvzJ+tdGmPqONTIgpo2g3PcuMryy657gQhfBfTBiw=',    # wallet_v1_r2
        'WHzHie/xyE9G7DeX5F/ICaFP9a4k8eDHpqmcydyQYf8=',    # wallet_v1_r3
        'XJpeaMEI4YchoHxC+ZVr+zmtd+xtYktgxXbsiO7mUyk=',    # wallet_v2_r1
        '/pUw0yQ4Uwg+8u8LTCkIwKv2+hwx6iQ6rKpb+MfXU/E=',    # wallet_v2_r2
        'thBBpYp5gLlG6PueGY48kE0keZ/6NldOpCUcQaVm9YE=',    # wallet_v3_r1
        'hNr6RJ+Ypph3ibojI1gHK8D3bcRSQAKl0JGLmnXS1Zk=',    # wallet_v3_r2
        'ZN1UgFUixb6KnbWc6gEFzPDQh4bKeb64y3nogKjXMi0=',    # wallet_v4_r1
        '/rX/aCDi/w2Ug+fg1iyBfYRniftK5YDIeIZtlZ2r1cA='     # wallet_v4_r2
    }
    return code_hash in wallets_code_hashes

def address_type_friendly(address_raw, latest_account_state):
    """
    As per address update proposal https://github.com/ton-blockchain/TEPs/pull/123 
    we use non-bounceable user-friendly format for nonexist/uninit account and wallets
    and bounceable for others.
    """
    bounceable = True
    if latest_account_state is None:
        # We consider this as destroyed account (nonexist)
        bounceable = False
    elif latest_account_state.account_status == 'uninit':
        bounceable = False
    elif is_wallet(latest_account_state.code_hash):
        bounceable = False
    return address_to_friendly(address_raw, bounceable) if address_raw and address_raw != 'addr_none' else None

def shard_type(value):
    return int_to_hex(value, length=64, signed=True).upper() if value else None

class BlockReference(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    workchain: int
    shard: str
    seqno: int


class Block(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

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
    
    tx_count: Optional[int]

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
                     tx_count=obj.tx_count,
                     masterchain_block_ref=BlockReference(workchain=obj.mc_block_workchain, 
                                                          shard=shard_type(obj.mc_block_shard), 
                                                          seqno=obj.mc_block_seqno) 
                                           if obj.mc_block_seqno is not None else None)


class AccountStatus(str, Enum):
    uninit = 'uninit'
    frozen = 'frozen'
    active = 'active'
    nonexist = 'nonexist'

    @classmethod
    def from_ton_http_api(cls, value):
        if value == 'uninitialized':
            return cls.uninit
        if value == 'active':
            return cls.active
        if value == 'frozen':
            return cls.frozen
        # ton-http-api returns 'uninitialized' for both uninit and nonexist accounts
        raise ValueError(f'Unexpected account status: {value}')

class InternalMsgBody(BaseModel):
    pass

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

class Comment:
    @classmethod
    def from_tlb(cls, tlb_obj: Union[tlb.TextCommentMessage, tlb.BinaryCommentMessage]) -> Union[TextComment, BinaryComment]:
        if type(tlb_obj) is tlb.TextCommentMessage:
            return TextComment.from_tlb(tlb_obj)
        if type(tlb_obj) is tlb.BinaryCommentMessage:
            return BinaryComment.from_tlb(tlb_obj)
        raise RuntimeError(f"Unexpected object of type {type(tlb_obj)}")
    
def decode_msg_body(body, op):
    try:
        if op == '0x00000000':
            obj = tlb.boc_to_object(body, tlb.CommentMessage)
            return Comment.from_tlb(obj)
    except BaseException as e:
        logger.error(f"Error parsing msg with op {op}: {e}")
    return None

class MessageContent(BaseModel):
    hash: str
    body: str
    decoded: Union[TextComment, BinaryComment, None]  = Field(..., discriminator='type')

    @classmethod
    def from_orm(cls, obj, op):
        return MessageContent(hash=hash_type(obj.hash),
                              body=obj.body,
                              decoded=decode_msg_body(obj.body, op))
    
class MessageInitState(BaseModel):
    hash: str
    body: str

    @classmethod
    def from_orm(cls, obj):
        return MessageInitState(hash=hash_type(obj.hash),
                                body=obj.body)

class Message(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    hash: str
    source: Optional[str]
    source_friendly: Optional[str]
    destination: Optional[str]
    destination_friendly: Optional[str]
    value: Optional[str]
    fwd_fee: Optional[str]
    ihr_fee: Optional[str]
    created_lt: Optional[str]
    created_at: Optional[str]
    opcode: Optional[str]
    ihr_disabled: Optional[bool]
    bounce: Optional[bool]
    bounced: Optional[bool]
    import_fee: Optional[str]
    # body_hash: str
    # init_state_hash: Optional[str]

    message_content: Optional[MessageContent]
    init_state: Optional[MessageInitState]

    @classmethod
    def from_orm(cls, obj):
        op = f'0x{(obj.opcode & 0xffffffff):08x}' if obj.opcode is not None else None
        return Message(hash=hash_type(obj.hash),
                       source=address_type(obj.source),
                       source_friendly=address_type_friendly(obj.source, obj.source_account_state),
                       destination=address_type(obj.destination),
                       destination_friendly=address_type_friendly(obj.destination, obj.destination_account_state),
                       value=obj.value,
                       fwd_fee=obj.fwd_fee,
                       ihr_fee=obj.ihr_fee,
                       created_lt=obj.created_lt,
                       created_at=obj.created_at,
                       opcode=op,
                       ihr_disabled=obj.ihr_disabled,
                       bounce=obj.bounce,
                       bounced=obj.bounced,
                       import_fee=obj.import_fee,
                    #    body_hash=hash_type(obj.body_hash),
                    #    init_state_hash=hash_type(obj.init_state_hash),
                       message_content=MessageContent.from_orm(obj.message_content, op) if obj.message_content else None,
                       init_state=MessageInitState.from_orm(obj.init_state) if obj.init_state else None)


class AccountState(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    hash: str
    account: str
    balance: str
    account_status: AccountStatus
    frozen_hash: Optional[str]
    code_hash: Optional[str]
    data_hash: Optional[str]

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
    model_config = ConfigDict(coerce_numbers_to_str=True)

    account: str
    account_friendly: str
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

    trace_id: Optional[str]

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
                           account_friendly=address_type_friendly(obj.account, obj.account_state_latest),
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
                           account_state_after=AccountState.from_orm(obj.account_state_after) if obj.account_state_after else None,
                           trace_id=str(obj.event_id) if obj.event_id is not None else None,)


class TransactionTrace(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    id: str
    transaction: Transaction
    children: List["TransactionTrace"]

    @classmethod
    def from_orm(cls, obj):
        id = str(obj.get('id', 0))
        transaction = Transaction.from_orm(obj['transaction'])
        children = [TransactionTrace.from_orm(x) for x in obj['children']]
        return TransactionTrace(id=id, transaction=transaction, children=children)


class NFTCollection(BaseModel):
    address: str
    owner_address: Optional[str]
    last_transaction_lt: str
    next_item_index: str
    collection_content: Any
    
    code_hash: str
    data_hash: str
    
    @classmethod
    def from_orm(cls, obj):
        return NFTCollection(address=address_type(obj.address),
                             owner_address=address_type(obj.owner_address),
                             last_transaction_lt=str(obj.last_transaction_lt),
                             next_item_index=str(int(obj.next_item_index)),
                             collection_content=obj.collection_content,
                             code_hash=hash_type(obj.code_hash),
                             data_hash=hash_type(obj.data_hash),)


class NFTItem(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    address: str
    collection_address: Optional[str]
    owner_address: Optional[str]
    init: bool
    index: str
    last_transaction_lt: str
    code_hash: str
    data_hash: str
    content: Any

    collection: Optional[NFTCollection]

    @classmethod
    def from_orm(cls, obj):
        return NFTItem(address=address_type(obj.address),
                       collection_address=address_type(obj.collection_address),
                       owner_address=address_type(obj.owner_address),
                       init=obj.init,
                       index=int(obj.index),
                       last_transaction_lt=obj.last_transaction_lt,
                       code_hash=hash_type(obj.code_hash),
                       data_hash=hash_type(obj.data_hash),
                       content=obj.content,
                       collection=NFTCollection.from_orm(obj.collection) if obj.collection else None)


class NFTTransfer(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    query_id: str
    nft_address: str
    transaction_hash: str
    transaction_lt: str
    transaction_now: int
    old_owner: str
    new_owner: str
    response_destination: Optional[str]
    custom_payload: Optional[str]
    forward_amount: str
    forward_payload: Optional[str]

    # transaction: Optional[Transaction]
    # nft_item: Optional[NFTItem]

    @classmethod
    def from_orm(cls, obj):
        return NFTTransfer(query_id=int(obj.query_id),
                           nft_address=address_type(obj.nft_item_address),
                           transaction_hash=hash_type(obj.transaction_hash),
                           transaction_lt=obj.transaction.lt,  # TODO: maybe fix
                           transaction_now=obj.transaction.now,  # TODO: maybe fix
                           old_owner=address_type(obj.old_owner),
                           new_owner=address_type(obj.new_owner),
                           response_destination=address_type(obj.response_destination),
                           custom_payload=obj.custom_payload,
                           forward_amount=int(obj.forward_amount),
                           forward_payload=obj.forward_payload,)


class JettonMaster(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    address: str
    total_supply: str
    mintable: bool
    admin_address: Optional[str]
    last_transaction_lt: str
    jetton_wallet_code_hash: str
    jetton_content: Any
    code_hash: str
    data_hash: str

    @classmethod
    def from_orm(cls, obj):
        return JettonMaster(address=address_type(obj.address),
                            total_supply=int(obj.total_supply),
                            mintable=obj.mintable,
                            admin_address=address_type(obj.admin_address),
                            last_transaction_lt=obj.last_transaction_lt,
                            jetton_wallet_code_hash=obj.jetton_wallet_code_hash,
                            jetton_content=obj.jetton_content,
                            code_hash=hash_type(obj.code_hash),
                            data_hash=hash_type(obj.data_hash),)


class JettonWallet(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    address: str
    balance: str
    owner: str
    jetton: str
    last_transaction_lt: str
    code_hash: str
    data_hash: str
    
    @classmethod
    def from_orm(cls, obj):
        return JettonWallet(address=address_type(obj.address),
                            balance=int(obj.balance),
                            owner=address_type(obj.owner),
                            jetton=address_type(obj.jetton),
                            last_transaction_lt=obj.last_transaction_lt,
                            code_hash=hash_type(obj.code_hash),
                            data_hash=hash_type(obj.data_hash),)


class JettonTransfer(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    query_id: str
    source: str
    destination: str
    amount: str
    source_wallet: str
    jetton_master: str
    transaction_hash: str
    transaction_lt: str
    transaction_now: int
    response_destination: Optional[str]
    custom_payload: Optional[str]
    forward_ton_amount: Optional[str]
    forward_payload: Optional[str]

    @classmethod
    def from_orm(cls, obj):
        return JettonTransfer(query_id=int(obj.query_id),
                              source=address_type(obj.source),
                              destination=address_type(obj.destination),
                              amount=int(obj.amount),
                              source_wallet=address_type(obj.jetton_wallet_address),
                              jetton_master=address_type(obj.jetton_wallet.jetton),
                              transaction_hash=hash_type(obj.transaction_hash),
                              transaction_lt=obj.transaction.lt,  # TODO: maybe fix
                              transaction_now=obj.transaction.now,  # TODO: maybe fix
                              response_destination=address_type(obj.response_destination),
                              custom_payload=obj.custom_payload,
                              forward_ton_amount=int(obj.forward_ton_amount) if obj.forward_ton_amount else None,
                              forward_payload=obj.forward_payload,)


class JettonBurn(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    query_id: str
    owner: str
    jetton_master: str
    transaction_hash: str
    transaction_lt: str
    transaction_now: int
    response_destination: Optional[str]
    custom_payload: Optional[str]
    
    @classmethod
    def from_orm(cls, obj):
        return JettonBurn(query_id=int(obj.query_id),
                          owner=address_type(obj.owner),
                          jetton_master=address_type(obj.jetton_wallet.jetton),
                          transaction_hash=hash_type(obj.transaction_hash),
                          transaction_lt=obj.transaction.lt,  # TODO: maybe fix
                          transaction_now=obj.transaction.now,  # TODO: maybe fix
                          response_destination=address_type(obj.response_destination),
                          custom_payload=obj.custom_payload,)

class MasterchainInfo(BaseModel):
    first: Block
    last: Block

class AccountBalance(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    account: str
    balance: str

    @classmethod
    def from_orm(cls, obj):
        return AccountBalance(account=address_type_friendly(obj.account, obj),
                              balance=obj.balance)

class LatestAccountState(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    account_state_hash: str
    last_trans_lt: str
    last_trans_timestamp: int
    balance: str
    account_status: AccountStatus
    frozen_hash: Optional[str]
    code_hash: Optional[str]
    data_hash: Optional[str]

    @classmethod
    def from_orm(cls, obj):
        return LatestAccountState(account_state_hash=hash_type(obj.hash), 
                                  last_trans_lt=obj.last_trans_lt,
                                  last_trans_timestamp=obj.timestamp,
                                  balance=obj.balance,
                                  account_status=AccountStatus(obj.account_status),
                                  frozen_hash=hash_type(obj.frozen_hash),
                                  code_hash=hash_type(obj.code_hash),
                                  data_hash=hash_type(obj.data_hash))

class ExternalMessage(BaseModel):
    """
    Message in base64 boc serialized format.
    """
    boc: str = Field(examples=["te6ccgECBQEAARUAAkWIAWTtae+KgtbrX26Bep8JSq8lFLfGOoyGR/xwdjfvpvEaHg"])

class SentMessage(BaseModel):
    message_hash: str = Field(description="Hash of sent message in hex format", examples=["383E348617141E35BC25ED9CD0EDEC2A4EAF6413948BF1FB7F865CEFE8C2CD44"])

    @classmethod
    def from_ton_http_api(cls, obj):
        return SentMessage(message_hash=hash_type(obj['hash']))

class GetMethodParameterType(Enum):
    cell = "cell"
    slice = "slice"
    num = "num"
    list = "list"
    tuple = "tuple"
    unsupported_type = "unsupported_type"

class GetMethodParameter(BaseModel):
    type: GetMethodParameterType
    value: Union[List['GetMethodParameter'], str, None]

    @classmethod
    def from_ton_http_api(cls, obj):
        if obj[0] == 'cell':
            return GetMethodParameter(type=GetMethodParameterType.cell, value=obj[1]['bytes'])
        elif obj[0] == 'slice':
            return GetMethodParameter(type=GetMethodParameterType.slice, value=obj[1]['bytes'])
        elif obj[0] == 'num':
            return GetMethodParameter(type=GetMethodParameterType.num, value=obj[1])
        elif obj[0] == 'list':
            return GetMethodParameter(type=GetMethodParameterType.list, value=[GetMethodParameter.from_ton_http_api(x) for x in obj[1]['elements']])
        elif obj[0] == 'tuple':
            return GetMethodParameter(type=GetMethodParameterType.tuple, value=[GetMethodParameter.from_ton_http_api(x) for x in obj[1]['elements']])
        
        return GetMethodParameter(type=GetMethodParameterType.unsupported_type, value=None)

class RunGetMethodRequest(BaseModel):
    address: str
    method: str
    stack: List[GetMethodParameter]

    def to_ton_http_api(self) -> dict:
        ton_http_api_stack = []
        for p in self.stack:
            if p.type == GetMethodParameterType.num:
                ton_http_api_stack.append(['num', p.value])
            elif p.type == GetMethodParameterType.cell:
                ton_http_api_stack.append(['tvm.Cell', p.value])
            elif p.type == GetMethodParameterType.slice:
                ton_http_api_stack.append(['tvm.Slice', p.value])
            else:
                raise Exception(f"Unsupported stack parameter type: {p.type}")
        return {
            'address': self.address,
            'method': self.method,
            'stack': ton_http_api_stack
        }

class RunGetMethodResponse(BaseModel):
    gas_used: int
    exit_code: int
    stack: List[GetMethodParameter]

    @classmethod
    def from_ton_http_api(cls, obj):
        return RunGetMethodResponse(gas_used=obj['gas_used'],
                                    exit_code=obj['exit_code'],
                                    stack=[GetMethodParameter.from_ton_http_api(x) for x in obj['stack']])

class EstimateFeeRequest(BaseModel):
    address: str
    body: str
    init_code: Optional[str] = None
    init_data: Optional[str] = None
    ignore_chksig: bool = True

    def to_ton_http_api(self) -> dict:
        return {
            'address': self.address,
            'body': self.body,
            'init_code': self.init_code if self.init_code else '',
            'init_data': self.init_data if self.init_data else '',
            'ignore_chksig': self.ignore_chksig
        }

class Fee(BaseModel):
    in_fwd_fee: int
    storage_fee: int
    gas_fee: int
    fwd_fee: int

    @classmethod
    def from_ton_http_api(cls, obj):
        return Fee(in_fwd_fee=obj['in_fwd_fee'],
                   storage_fee=obj['storage_fee'],
                   gas_fee=obj['gas_fee'],
                   fwd_fee=obj['fwd_fee'])

class EstimateFeeResponse(BaseModel):
    source_fees: Fee
    destination_fees: List[Fee]

    @classmethod
    def from_ton_http_api(cls, obj):
        return EstimateFeeResponse(source_fees=Fee.from_ton_http_api(obj['source_fees']),
                                   destination_fees=[Fee.from_ton_http_api(x) for x in obj['destination_fees']])

class Account(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    balance: str
    code: Optional[str]
    data: Optional[str]
    last_transaction_lt: Optional[str]
    last_transaction_hash: Optional[str]
    frozen_hash: Optional[str]
    status: AccountStatus

    @classmethod
    def from_ton_http_api(cls, obj):
        null_hash = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA='
        return Account(balance=obj['balance'],
                       code=obj['code'] if len(obj['code']) > 0 else None,
                       data=obj['data'] if len(obj['data']) > 0 else None,
                       last_transaction_lt=obj['last_transaction_id']['lt'] if obj['last_transaction_id']['lt'] != '0' else None,
                       last_transaction_hash=hash_type(obj['last_transaction_id']['hash']) if obj['last_transaction_id']['hash'] != null_hash else None,
                       frozen_hash=obj['frozen_hash'] if len(obj['frozen_hash']) > 0 else None,
                       status=AccountStatus.from_ton_http_api(obj['state']))

class WalletInfo(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    balance: str
    wallet_type: Optional[str]
    seqno: Optional[int]
    wallet_id: Optional[int]
    last_transaction_lt: Optional[str]
    last_transaction_hash: Optional[str]
    status: AccountStatus
    
    @classmethod
    def from_ton_http_api(cls, obj):
        null_hash = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA='
        return WalletInfo(balance=obj['balance'],
                          wallet_type=obj.get('wallet_type'),
                          seqno=obj.get('seqno'),
                          wallet_id=obj.get('wallet_id'),
                          last_transaction_lt=obj['last_transaction_id']['lt'] if obj['last_transaction_id']['lt'] != '0' else None,
                          last_transaction_hash=hash_type(obj['last_transaction_id']['hash']) if obj['last_transaction_id']['hash'] != null_hash else None,
                          status=AccountStatus.from_ton_http_api(obj['account_state']))
