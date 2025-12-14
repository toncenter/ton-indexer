from dataclasses import dataclass
from loguru import logger

from indexer.events.blocks.basic_blocks import Block, CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, ContractMatcher
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.utils.block_utils import find_call_contract, get_labeled
from indexer.events.blocks.utils.ton_utils import AccountId, Amount
from indexer.events.blocks.messages.cocoon import (
    CocoonLastPayoutPayload,
    CocoonPayout,
    CocoonPayoutPayload,
    CocoonReturnExcessesBack,
    CocoonWorkerProxyRequest,
)


# === Worker Payout ===


@dataclass
class CocoonWorkerPayoutData:
    """
    worker payout action data
    """

    proxy_contract: AccountId  # who initiated payout (proxy)
    worker_contract: AccountId  # worker contract receiving payout request
    worker_owner: AccountId  # final recipient of payout
    payout_type: str  # "regular" or "last"
    query_id: int
    new_tokens: int  # number of new tokens from payload
    expected_address: AccountId | None  # expected address from payload
    payout_amount: Amount  # actual payout amount sent to worker owner
    worker_state: int  # worker state from WorkerProxyRequest
    worker_tokens: int  # worker tokens from WorkerProxyRequest


class CocoonWorkerPayoutBlock(Block):
    data: CocoonWorkerPayoutData

    def __init__(self, data: CocoonWorkerPayoutData):
        super().__init__("cocoon_worker_payout", [], data)

    def __repr__(self):
        return f"cocoon_worker_payout worker={self.data.worker_contract.address} owner={self.data.worker_owner.address} amount={self.data.payout_amount} type={self.data.payout_type}"


class CocoonWorkerPayoutMatcher(BlockMatcher):
    """
    matches worker payout flow:
    external_in → PayoutPayload/LastPayoutPayload (to Worker)
      → WorkerProxyRequest (Worker → Proxy)
        → Payout (Worker → Worker Owner) ✅
        → ReturnExcessesBack (Worker → Proxy)
    """

    def __init__(self):
        # target: payout to worker owner
        payout_to_owner = ContractMatcher(
            opcode=CocoonPayout.opcode,  # 0xc59a7cd3
            optional=False,
        )

        # excesses back to proxy
        excesses = ContractMatcher(
            opcode=CocoonReturnExcessesBack.opcode,  # 0x2565934c
            optional=True,
        )

        # worker reports back to proxy
        worker_request = ContractMatcher(
            opcode=CocoonWorkerProxyRequest.opcode,  # 0x4d725d2c
            optional=False,
            children_matchers=[
                labeled("payout", payout_to_owner),
                labeled("excesses", excesses),
            ],
        )

        super().__init__(
            optional=False,
            child_matcher=worker_request,
        )

    def test_self(self, block: Block):
        """check if block is starting message: PayoutPayload or LastPayoutPayload"""
        return isinstance(block, CallContractBlock) and block.opcode in [
            CocoonPayoutPayload.opcode,  # 0xa040ad28
            CocoonLastPayoutPayload.opcode,  # 0xf5f26a36
        ]

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        """build CocoonWorkerPayoutBlock from matched blocks"""
        # starting block: PayoutPayload or LastPayoutPayload
        start_block = block
        start_msg = start_block.get_message()

        # determine payout type
        payout_type = (
            "last"
            if start_block.opcode == CocoonLastPayoutPayload.opcode
            else "regular"
        )

        # parse payload
        try:
            if payout_type == "last":
                payload_msg = CocoonLastPayoutPayload(start_block.get_body())
            else:
                payload_msg = CocoonPayoutPayload(start_block.get_body())

            query_id = payload_msg.query_id
            new_tokens = payload_msg.new_tokens
            expected_address = (
                AccountId(payload_msg.expected_my_address)
                if payload_msg.expected_my_address
                else None
            )
        except Exception as e:
            logger.error(
                f"failed to parse payout payload in trace {start_msg.trace_id}: {e}",
                exc_info=True,
            )
            return []

        # find WorkerProxyRequest
        worker_request_block = find_call_contract(
            start_block.next_blocks, CocoonWorkerProxyRequest.opcode
        )
        if not worker_request_block:
            logger.warning(
                f"WorkerProxyRequest not found in cocoon worker payout trace {start_msg.trace_id}"
            )
            return []

        # parse WorkerProxyRequest
        try:
            worker_request_msg = CocoonWorkerProxyRequest(
                worker_request_block.get_body()
            )
            worker_owner = AccountId(worker_request_msg.owner_address)
            worker_state = worker_request_msg.state
            worker_tokens = worker_request_msg.tokens
        except Exception as e:
            logger.error(
                f"failed to parse WorkerProxyRequest in trace {start_msg.trace_id}: {e}",
                exc_info=True,
            )
            return []

        # find Payout block
        payout_block = get_labeled("payout", other_blocks, CallContractBlock)
        if not payout_block:
            logger.warning(
                f"Payout block not found in cocoon worker payout trace {start_msg.trace_id}"
            )
            return []

        # extract payout amount from message value
        payout_msg = payout_block.get_message()
        payout_amount = Amount(payout_msg.value)

        # determine addresses
        proxy_contract = AccountId(start_msg.source)
        worker_contract = AccountId(start_msg.destination)

        # create data object
        data = CocoonWorkerPayoutData(
            proxy_contract=proxy_contract,
            worker_contract=worker_contract,
            worker_owner=worker_owner,
            payout_type=payout_type,
            query_id=query_id,
            new_tokens=new_tokens,
            expected_address=expected_address,
            payout_amount=payout_amount,
            worker_state=worker_state,
            worker_tokens=worker_tokens,
        )

        # create block
        new_block = CocoonWorkerPayoutBlock(data)

        # merge all blocks
        blocks_to_merge = [start_block] + other_blocks
        if worker_request_block and worker_request_block not in blocks_to_merge:
            blocks_to_merge.append(worker_request_block)
        if payout_block and payout_block not in blocks_to_merge:
            blocks_to_merge.append(payout_block)

        new_block.merge_blocks(list(set(blocks_to_merge)))

        # check success (if payout transaction was successful)
        new_block.failed = payout_msg.transaction.aborted

        return [new_block]
