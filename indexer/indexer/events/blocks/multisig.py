from __future__ import annotations

import base64
from dataclasses import dataclass

from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    ContractMatcher,
    OrMatcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.messages.multisig import (
    MultisigApprove,
    MultisigApproveAccepted,
    MultisigApproveRejected,
    MultisigExecute,
    MultisigInitOrder,
    MultisigNewOrder,
)
from indexer.events.blocks.utils import AccountId
from indexer.events.blocks.utils.block_utils import get_labeled
from indexer.events import context


@dataclass
class MultisigCreateOrderData:
    query_id: int
    multisig: AccountId
    order_seqno: int
    created_by: AccountId
    is_created_by_signer: bool
    creator_index: int
    order_contract_address: AccountId
    creator_approved: bool
    expiration_date: int
    order_boc_str: str
    signers: list[str]


class MultisigCreateOrderBlock(Block):
    data: MultisigCreateOrderData

    def __init__(self, data: MultisigCreateOrderData):
        super().__init__("multisig_create_order", [], data)

    def __repr__(self):
        return f"multisig_create_order {self.data}"


@dataclass
class MultisigApproveData:
    signer: AccountId
    order: AccountId
    success: bool
    signer_index: int
    exit_code: int
    signers: list[str]


class MultisigApproveBlock(Block):
    data: MultisigApproveData

    def __init__(self, data):
        super().__init__("multisig_approve", [], data)

    def __repr__(self):
        return f"multisig_approve {self.data}"


@dataclass
class MultisigExecuteData:
    query_id: int
    multisig: AccountId
    success: bool
    order_contract_address: AccountId
    order_seqno: int
    expiration_date: int
    approvals_num: int
    signers_hash_str: str
    order_boc_str: str
    signers: list[str]


class MultisigExecuteBlock(Block):
    data: MultisigExecuteData

    def __init__(self, data):
        super().__init__("multisig_execute", [], data)

    def __repr__(self):
        return f"multisig_execute {self.data}"


class MultisigCreateOrderBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            child_matcher=labeled(
                "init_order",
                ContractMatcher(opcode=MultisigInitOrder.opcode, optional=False),
            )
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == MultisigNewOrder.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        included = [block]
        init_msg = block.get_message()
        init_data = MultisigNewOrder(block.get_body())

        deploy_block = get_labeled("init_order", other_blocks, CallContractBlock)
        assert deploy_block is not None  # well, it can't be None
        included.append(deploy_block)
        deploy_msg = deploy_block.get_message()
        deploy_data = MultisigInitOrder(deploy_block.get_body())

        order = await context.interface_repository.get().get_multisig_order(deploy_block.get_message().destination)
        if order is None:
            return []
        new_block = MultisigCreateOrderBlock(
            data=MultisigCreateOrderData(
                query_id=init_data.query_id,
                multisig=AccountId(init_msg.destination),
                order_seqno=init_data.order_seqno,
                created_by=AccountId(init_msg.source),
                is_created_by_signer=init_data.is_signer,
                creator_index=init_data.singer_index,
                order_contract_address=AccountId(deploy_msg.destination),
                creator_approved=deploy_data.approve_on_init,
                expiration_date=init_data.expiration_date,
                order_boc_str=base64.b64encode(deploy_data.order.to_boc()).decode(
                    "utf-8"
                ),
                signers=order.signers,
            )
        )
        new_block.merge_blocks(included)
        return [new_block]


class MultisigApproveBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            child_matcher=OrMatcher(
                [
                    labeled(
                        "accepted",
                        ContractMatcher(
                            opcode=MultisigApproveAccepted.opcode, optional=False
                        ),
                    ),
                    labeled(
                        "rejected",
                        ContractMatcher(
                            opcode=MultisigApproveRejected.opcode, optional=False
                        ),
                    ),
                ]
            )
        )

    def test_self(self, block: Block):

        return (
            isinstance(block, CallContractBlock)
            and block.opcode == MultisigApprove.opcode
        ) or (
            isinstance(block, TonTransferBlock)
            and block.comment == MultisigApprove.comment
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        included = [block]
        accepted = get_labeled("accepted", other_blocks, CallContractBlock)
        exit_code = 0  # no exit code if accepted. so just set to 0
        rejected = None
        if accepted is None:
            rejected = get_labeled("rejected", other_blocks, CallContractBlock)
            # must be either accepted or rejected
            if rejected is None:
                return []
            exit_code = MultisigApproveRejected(rejected.get_body()).exit_code
            included.append(rejected)
        else:
            included.append(accepted)

        signer_index = -1
        try:
            # one can vote either by comment or by opcode
            # if by opcode, we can get the signer index
            # otherwise, we can't, tho leave it as -1
            thru_opcode_data = MultisigApprove(block.get_body())
            signer_index = thru_opcode_data.signer_index
        except:
            pass

        msg = block.get_message()
        order_address = msg.destination

        order = await context.interface_repository.get().get_multisig_order(order_address)
        if order is None:
            return []

        new_block = MultisigApproveBlock(
            data=MultisigApproveData(
                signer=AccountId(msg.source),
                order=AccountId(msg.destination),
                success=(accepted is not None),
                signer_index=signer_index,
                exit_code=exit_code,
                signers=order.signers or [],
            )
        )
        new_block.merge_blocks(included)
        return [new_block]


class MultisigExecuteBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__()

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == MultisigExecute.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        execute_data = MultisigExecute(block.get_body())

        msg = block.get_message()
        order_address = msg.source
        order = await context.interface_repository.get().get_multisig_order(order_address)
        if order is None:
            return []

        new_block = MultisigExecuteBlock(
            data=MultisigExecuteData(
                query_id=execute_data.query_id,
                multisig=AccountId(msg.destination),
                success=(not block.failed),
                order_contract_address=AccountId(msg.source),
                order_seqno=execute_data.order_seqno,
                expiration_date=execute_data.expiration_date,
                approvals_num=execute_data.approvals_num,
                signers_hash_str=base64.b64encode(execute_data.signers_hash).decode(
                    "utf-8"
                ),
                order_boc_str=base64.b64encode(execute_data.order.to_boc()).decode(
                    "utf-8"
                ),
                signers=order.signers or [],
            )
        )
        new_block.merge_blocks([block])
        return [new_block]
