from __future__ import annotations

from pytoniq_core import Cell, Slice


class MultisigNewOrder:
    # creator -> multisig
    # new_order#f718510f query_id:uint64
    #                order_seqno:uint256
    #                signer:(## 1)
    #                index:uint8
    #                expiration_date:uint48
    #                order:^Order = InternalMsgBody;
    opcode = 0xF718510F

    query_id: int
    order_seqno: int
    is_signer: bool
    singer_index: int
    expiration_date: int
    order: Cell

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.order_seqno = slice.load_uint(256)
        self.is_signer = slice.load_bool()
        self.singer_index = slice.load_uint(8)
        self.expiration_date = slice.load_uint(48)
        self.order = slice.load_ref()


class MultisigInitOrder:
    # multisig -> order
    # init#9c73fba2 query_id:uint64
    #           threshold:uint8
    #           signers:^(Hashmap 8 MsgAddressInt)
    #           expiration_date:uint48
    #           order:^Order
    #           approve_on_init:(## 1)
    #           signer_index:approve_on_init?uint8 = InternalMsgBody;
    opcode = 0x9C73FBA2

    query_id: int
    threshold: int
    signers: dict
    expiration_date: int
    order: Cell
    approve_on_init: bool
    signer_index: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.threshold = slice.load_uint(8)
        self.signers = (
            slice.load_dict(8, lambda x: x.load_uint(8), lambda x: x.load_address())
            or {}
        )
        self.expiration_date = slice.load_uint(48)
        self.order = slice.load_ref()
        self.approve_on_init = slice.load_bool()
        if self.approve_on_init:
            self.signer_index = slice.load_uint(8)
        else:
            self.signer_index = -1


class MultisigApprove:
    # signer -> order
    # approve#a762230f query_id:uint64 signer_index:uint8 = InternalMsgBody;
    # comment_approve#00000000617070726f7665 = InternalMsgBody;
    opcode = 0xA762230F
    comment = "approve"

    query_id: int
    signer_index: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.signer_index = slice.load_uint(8)


class MultisigApproveAccepted:
    # order -> signer
    # approve_accepted#82609bf6 query_id:uint64 = InternalMsgBody;
    opcode = 0x82609BF6


class MultisigApproveRejected:
    # order -> signer
    # approve_rejected#afaf283e query_id:uint64 exit_code:uint32 = InternalMsgBody;
    opcode = 0xAFAF283E

    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        slice.load_uint(64)  # query_id
        self.exit_code = slice.load_uint(32)

class  MultisigExecute:
    # order -> multisig
    opcode = 0x75097f5d
    query_id: int
    order_seqno: int
    expiration_date: int
    approvals_num: int
    signers_hash: bytes
    order: Cell
    # execute#75097f5d query_id:uint64 
    #                  order_seqno:uint256
    #                  expiration_date:uint48
    #                  approvals_num:uint8
    #                  signers_hash:bits256
    #                  order:^Order = InternalMsgBody;
    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.order_seqno = slice.load_uint(256)
        self.expiration_date = slice.load_uint(48)
        self.approvals_num = slice.load_uint(8)
        self.signers_hash = slice.load_bytes(32)
        self.order = slice.load_ref()
