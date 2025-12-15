from dataclasses import dataclass
from loguru import logger

from indexer.events.blocks.basic_blocks import Block, CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, ContractMatcher
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.utils.block_utils import find_call_contract, get_labeled
from indexer.events.blocks.utils.ton_utils import AccountId, Amount
from indexer.events.blocks.messages.cocoon import (
    CocoonChargePayload,
    CocoonClientProxyRequest,
    CocoonExtClientTopUp,
    CocoonExtProxyPayoutRequest,
    CocoonGrantRefundPayload,
    CocoonLastPayoutPayload,
    CocoonOwnerClientChangeSecretHash,
    CocoonOwnerClientRegister,
    CocoonOwnerClientRequestRefund,
    CocoonOwnerWalletSendMessage,
    CocoonPayout,
    CocoonPayoutPayload,
    CocoonRegisterProxy,
    CocoonReturnExcessesBack,
    CocoonUnregisterProxy,
    CocoonWorkerProxyRequest,
)

from pytoniq_core import ExternalAddress, Address

# log this module to file at debug level
logger.add("output.log", level="DEBUG")

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
    expected_address: str | None  # expected address from payload
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
            if isinstance(start_block, CallContractBlock) and start_block.opcode == CocoonLastPayoutPayload.opcode
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
            expected_address = payload_msg.expected_my_address
            if isinstance(expected_address, ExternalAddress):
                expected_address = f"{expected_address.len};{hex(expected_address.external_address)[2:]}" # type: ignore
            elif isinstance(expected_address, Address):
                expected_address = expected_address.to_str(False).upper()
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


# === Proxy Payout ===


@dataclass
class CocoonProxyPayoutData:
    """proxy payout action data"""

    proxy_contract: AccountId  # proxy initiating payout
    proxy_owner: AccountId  # destination of payout
    excesses_recipient: AccountId  # where to send excesses
    query_id: int


class CocoonProxyPayoutBlock(Block):
    data: CocoonProxyPayoutData

    def __init__(self, data: CocoonProxyPayoutData):
        super().__init__("cocoon_proxy_payout", [], data)

    def __repr__(self):
        return f"cocoon_proxy_payout proxy={self.data.proxy_contract.address} owner={self.data.proxy_owner.address}"


class CocoonProxyPayoutMatcher(BlockMatcher):
    """
    matches proxy payout flow:
    external_in → ExtProxyPayoutRequest (to Proxy)
      → Payout (Proxy → Proxy Owner) ✅
      → ReturnExcessesBack (Proxy → excesses_recipient)
    """

    def __init__(self):
        payout_to_owner = ContractMatcher(
            opcode=CocoonPayout.opcode, optional=False
        )
        excesses = ContractMatcher(
            opcode=CocoonReturnExcessesBack.opcode, optional=False
        )
        super().__init__(
            optional=False, 
            children_matchers=[
                labeled("payout", payout_to_owner),
                labeled("excesses", excesses),
            ]
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == CocoonExtProxyPayoutRequest.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        start_block = block
        start_msg = start_block.get_message()

        # parse ExtProxyPayoutRequest
        try:
            request_msg = CocoonExtProxyPayoutRequest(start_block.get_body())
            query_id = request_msg.query_id
            excesses_recipient = AccountId(request_msg.send_excesses_to)
        except Exception as e:
            logger.error(
                f"failed to parse ExtProxyPayoutRequest in trace {start_msg.trace_id}: {e}",
                exc_info=True,
            )
            return []

        # find Payout block
        payout_block = get_labeled("payout", other_blocks, CallContractBlock)
        if not payout_block:
            logger.warning(
                f"Payout block not found in cocoon proxy payout trace {start_msg.trace_id}"
            )
            return []

        payout_msg = payout_block.get_message()
        proxy_contract = AccountId(start_msg.destination)
        proxy_owner = AccountId(payout_msg.destination)

        data = CocoonProxyPayoutData(
            proxy_contract=proxy_contract,
            proxy_owner=proxy_owner,
            excesses_recipient=excesses_recipient,
            query_id=query_id,
        )

        new_block = CocoonProxyPayoutBlock(data)
        new_block.merge_blocks([start_block] + other_blocks)
        new_block.failed = payout_msg.transaction.aborted

        return [new_block]


# === Proxy Charge ===


@dataclass
class CocoonProxyChargeData:
    """proxy charge action data"""

    proxy_contract: AccountId  # proxy doing the charge
    client_contract: AccountId  # client being charged
    query_id: int
    new_tokens_used: int  # tokens consumed
    expected_address: str | None  # expected address from payload


class CocoonProxyChargeBlock(Block):
    data: CocoonProxyChargeData

    def __init__(self, data: CocoonProxyChargeData):
        super().__init__("cocoon_proxy_charge", [], data)

    def __repr__(self):
        return f"cocoon_proxy_charge proxy={self.data.proxy_contract.address} client={self.data.client_contract.address}"


class CocoonProxyChargeMatcher(BlockMatcher):
    """
    matches proxy charge flow:
    external_in → ChargePayload (to Proxy)
      → ReturnExcessesBack (Proxy → excesses)
      → ClientProxyRequest (Proxy → Client) [usually aborted]
    """

    def __init__(self):
        excesses = ContractMatcher(
            opcode=CocoonReturnExcessesBack.opcode, optional=False
        )
        client_request = ContractMatcher(
            opcode=CocoonClientProxyRequest.opcode, optional=False
        )

        super().__init__(optional=False, children_matchers=[
                labeled("excesses", excesses),
                labeled("client_request", client_request),
            ])

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == CocoonChargePayload.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        start_block = block
        start_msg = start_block.get_message()

        # parse ChargePayload
        try:
            charge_msg = CocoonChargePayload(start_block.get_body())
            query_id = charge_msg.query_id
            new_tokens_used = charge_msg.new_tokens_used
            expected_address = charge_msg.expected_my_address
            if isinstance(expected_address, ExternalAddress):
                expected_address = f"{expected_address.len};{hex(expected_address.external_address)[2:]}"  # type: ignore
            elif isinstance(expected_address, Address):
                expected_address = expected_address.to_str(False).upper()
        except Exception as e:
            logger.error(
                f"failed to parse ChargePayload in trace {start_msg.trace_id}: {e}",
                exc_info=True,
            )
            return []

        # find ClientProxyRequest
        client_request_block = get_labeled("client_request", other_blocks, CallContractBlock)
        if not client_request_block:
            logger.warning(
                f"ClientProxyRequest not found in cocoon proxy charge trace {start_msg.trace_id}"
            )
            return []

        client_msg = client_request_block.get_message()
        proxy_contract = AccountId(start_msg.destination)
        client_contract = AccountId(client_msg.destination)

        data = CocoonProxyChargeData(
            proxy_contract=proxy_contract,
            client_contract=client_contract,
            query_id=query_id,
            new_tokens_used=new_tokens_used,
            expected_address=expected_address,
        )

        new_block = CocoonProxyChargeBlock(data)
        new_block.merge_blocks([start_block] + other_blocks)
        new_block.failed = start_msg.transaction.aborted

        return [new_block]


# === Client Top Up ===


@dataclass
class CocoonClientTopUpData:
    """client top up action data"""

    client_contract: AccountId  # client being topped up
    proxy_contract: AccountId  # proxy handling the client
    sender: AccountId  # who sent the top-up
    query_id: int
    top_up_amount: Amount  # amount added to balance


class CocoonClientTopUpBlock(Block):
    data: CocoonClientTopUpData

    def __init__(self, data: CocoonClientTopUpData):
        super().__init__("cocoon_client_top_up", [], data)

    def __repr__(self):
        return f"cocoon_client_top_up client={self.data.client_contract.address} amount={self.data.top_up_amount}"


class CocoonClientTopUpMatcher(BlockMatcher):
    """
    matches client top up flow:
    external_in → ExtClientTopUp (to Client)
      → ClientProxyRequest (Client → Proxy)
        → ReturnExcessesBack (Proxy → excesses)
    """

    def __init__(self):
        excesses = ContractMatcher(
            opcode=CocoonReturnExcessesBack.opcode, optional=False
        )

        proxy_request = ContractMatcher(
            opcode=CocoonClientProxyRequest.opcode,
            optional=False,
            children_matchers=[labeled("excesses", excesses)],
        )

        super().__init__(optional=False, child_matcher=labeled("proxy_request", proxy_request))

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == CocoonExtClientTopUp.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        start_block = block
        start_msg = start_block.get_message()

        # parse ExtClientTopUp
        try:
            top_up_msg = CocoonExtClientTopUp(start_block.get_body())
            query_id = top_up_msg.query_id
            top_up_amount = Amount(top_up_msg.top_up_amount)
            sender = AccountId(top_up_msg.send_excesses_to)
        except Exception as e:
            logger.error(
                f"failed to parse ExtClientTopUp in trace {start_msg.trace_id}: {e}",
                exc_info=True,
            )
            return []

        # find ClientProxyRequest
        proxy_request_block = get_labeled("proxy_request", other_blocks, CallContractBlock)
        if not proxy_request_block:
            logger.warning(
                f"ClientProxyRequest not found in cocoon client top up trace {start_msg.trace_id}"
            )
            return []

        proxy_msg = proxy_request_block.get_message()
        client_contract = AccountId(start_msg.destination)
        proxy_contract = AccountId(proxy_msg.destination)

        data = CocoonClientTopUpData(
            client_contract=client_contract,
            proxy_contract=proxy_contract,
            sender=sender,
            query_id=query_id,
            top_up_amount=top_up_amount,
        )

        new_block = CocoonClientTopUpBlock(data)
        new_block.merge_blocks([start_block] + other_blocks)
        new_block.failed = start_msg.transaction.aborted

        return [new_block]


# === Register Proxy ===


@dataclass
class CocoonRegisterProxyData:
    """register proxy action data"""

    root_contract: AccountId  # root doing registration
    query_id: int


class CocoonRegisterProxyBlock(Block):
    data: CocoonRegisterProxyData

    def __init__(self, data: CocoonRegisterProxyData):
        super().__init__("cocoon_register_proxy", [], data)

    def __repr__(self):
        return f"cocoon_register_proxy root={self.data.root_contract.address}"


class CocoonRegisterProxyMatcher(BlockMatcher):
    """
    matches register proxy flow:
    external_in → RegisterProxy (to Root)
      → ReturnExcessesBack (Root → sender)
    """
 
    def __init__(self):
        excesses = ContractMatcher(
            opcode=CocoonReturnExcessesBack.opcode, optional=False
        )
        super().__init__(optional=False, child_matcher=labeled("excesses", excesses))

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == CocoonRegisterProxy.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        start_block = block
        start_msg = start_block.get_message()

        # parse RegisterProxy
        try:
            register_msg = CocoonRegisterProxy(start_block.get_body())
            query_id = register_msg.query_id
        except Exception as e:
            logger.error(
                f"failed to parse RegisterProxy in trace {start_msg.trace_id}: {e}",
                exc_info=True,
            )
            return []

        root_contract = AccountId(start_msg.destination)

        data = CocoonRegisterProxyData(
            root_contract=root_contract,
            query_id=query_id,
        )

        new_block = CocoonRegisterProxyBlock(data)
        new_block.merge_blocks([start_block] + other_blocks)
        new_block.failed = start_msg.transaction.aborted

        return [new_block]


# === Unregister Proxy ===


@dataclass
class CocoonUnregisterProxyData:
    """unregister proxy action data"""

    root_contract: AccountId  # root doing unregistration
    query_id: int
    seqno: int  # sequence number


class CocoonUnregisterProxyBlock(Block):
    data: CocoonUnregisterProxyData

    def __init__(self, data: CocoonUnregisterProxyData):
        super().__init__("cocoon_unregister_proxy", [], data)

    def __repr__(self):
        return f"cocoon_unregister_proxy root={self.data.root_contract.address}"


class CocoonUnregisterProxyMatcher(BlockMatcher):
    """
    matches unregister proxy flow:
    external_in → UnregisterProxy (to Root)
      → ReturnExcessesBack (Root → sender)
    """

    def __init__(self):
        excesses = ContractMatcher(
            opcode=CocoonReturnExcessesBack.opcode, optional=False
        )
        super().__init__(optional=False, child_matcher=labeled("excesses", excesses))

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == CocoonUnregisterProxy.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        start_block = block
        start_msg = start_block.get_message()

        # parse UnregisterProxy
        try:
            unregister_msg = CocoonUnregisterProxy(start_block.get_body())
            query_id = unregister_msg.query_id
            seqno = unregister_msg.seqno
        except Exception as e:
            logger.error(
                f"failed to parse UnregisterProxy in trace {start_msg.trace_id}: {e}",
                exc_info=True,
            )
            return []

        root_contract = AccountId(start_msg.destination)

        data = CocoonUnregisterProxyData(
            root_contract=root_contract,
            query_id=query_id,
            seqno=seqno,
        )

        new_block = CocoonUnregisterProxyBlock(data)
        new_block.merge_blocks([start_block] + other_blocks)
        new_block.failed = start_msg.transaction.aborted

        return [new_block]


# === Client Register ===


@dataclass
class CocoonClientRegisterData:
    """client register action data"""

    client_contract: AccountId  # client being registered
    owner: AccountId  # owner registering
    query_id: int
    nonce: int  # registration nonce


class CocoonClientRegisterBlock(Block):
    data: CocoonClientRegisterData

    def __init__(self, data: CocoonClientRegisterData):
        super().__init__("cocoon_client_register", [], data)

    def __repr__(self):
        return f"cocoon_client_register client={self.data.client_contract.address} owner={self.data.owner.address}"


class CocoonClientRegisterMatcher(BlockMatcher):
    """
    matches client register flow:
    external_in → OwnerClientRegister (to Client)
      → ReturnExcessesBack (Client → send_excesses_to)
      → ClientProxyRequest (Client → Proxy) [usually aborted]
    """

    def __init__(self):
        excesses = ContractMatcher(
            opcode=CocoonReturnExcessesBack.opcode, optional=False
        )
        client_request = ContractMatcher(
            opcode=CocoonClientProxyRequest.opcode, optional=False
        )
        super().__init__(optional=False, children_matchers=[
                labeled("excesses", excesses),
                labeled("client_request", client_request),
            ])

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == CocoonOwnerClientRegister.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        start_block = block
        start_msg = start_block.get_message()

        # parse OwnerClientRegister
        try:
            register_msg = CocoonOwnerClientRegister(start_block.get_body())
            query_id = register_msg.query_id
            nonce = register_msg.nonce
            owner = AccountId(register_msg.send_excesses_to)
        except Exception as e:
            logger.error(
                f"failed to parse OwnerClientRegister in trace {start_msg.trace_id}: {e}",
                exc_info=True,
            )
            return []

        client_contract = AccountId(start_msg.destination)

        data = CocoonClientRegisterData(
            client_contract=client_contract,
            owner=owner,
            query_id=query_id,
            nonce=nonce,
        )

        new_block = CocoonClientRegisterBlock(data)
        new_block.merge_blocks([start_block] + other_blocks)
        new_block.failed = start_msg.transaction.aborted

        return [new_block]


# === Client Change Secret Hash ===


@dataclass
class CocoonClientChangeSecretHashData:
    """client change secret hash action data"""

    client_contract: AccountId  # client changing hash
    owner: AccountId  # owner
    query_id: int
    new_secret_hash: int  # new hash (uint256)


class CocoonClientChangeSecretHashBlock(Block):
    data: CocoonClientChangeSecretHashData

    def __init__(self, data: CocoonClientChangeSecretHashData):
        super().__init__("cocoon_client_change_secret_hash", [], data)

    def __repr__(self):
        return f"cocoon_client_change_secret_hash client={self.data.client_contract.address}"


class CocoonClientChangeSecretHashMatcher(BlockMatcher):
    """
    matches client change secret hash flow:
    external_in → OwnerClientChangeSecretHash (to Client)
      → ReturnExcessesBack (Client → send_excesses_to)
      → ClientProxyRequest (Client → Proxy) [usually aborted]
    """

    def __init__(self):
        excesses = ContractMatcher(
            opcode=CocoonReturnExcessesBack.opcode, optional=False
        )
        client_request = ContractMatcher(
            opcode=CocoonClientProxyRequest.opcode, optional=False
        )
        super().__init__(optional=False, children_matchers=[
                labeled("excesses", excesses),
                labeled("client_request", client_request),
            ])

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == CocoonOwnerClientChangeSecretHash.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        start_block = block
        start_msg = start_block.get_message()

        # parse OwnerClientChangeSecretHash
        try:
            change_msg = CocoonOwnerClientChangeSecretHash(start_block.get_body())
            query_id = change_msg.query_id
            new_secret_hash = change_msg.new_secret_hash
            owner = AccountId(change_msg.send_excesses_to)
        except Exception as e:
            logger.error(
                f"failed to parse OwnerClientChangeSecretHash in trace {start_msg.trace_id}: {e}",
                exc_info=True,
            )
            return []

        client_contract = AccountId(start_msg.destination)

        data = CocoonClientChangeSecretHashData(
            client_contract=client_contract,
            owner=owner,
            query_id=query_id,
            new_secret_hash=new_secret_hash,
        )

        new_block = CocoonClientChangeSecretHashBlock(data)
        new_block.merge_blocks([start_block] + other_blocks)
        new_block.failed = start_msg.transaction.aborted

        return [new_block]


# === Client Request Refund ===


@dataclass
class CocoonClientRequestRefundData:
    """client request refund action data"""

    client_contract: AccountId  # client requesting refund
    owner: AccountId  # owner
    query_id: int
    via_wallet: bool  # true if sent via OwnerWalletSendMessage


class CocoonClientRequestRefundBlock(Block):
    data: CocoonClientRequestRefundData

    def __init__(self, data: CocoonClientRequestRefundData):
        super().__init__("cocoon_client_request_refund", [], data)

    def __repr__(self):
        return f"cocoon_client_request_refund client={self.data.client_contract.address}"


class CocoonClientRequestRefundMatcher(BlockMatcher):
    """
    matches client request refund flow:
    external_in → OwnerClientRequestRefund (to Client)
      → ReturnExcessesBack (Client → send_excesses_to)
      → ClientProxyRequest (Client → Proxy) [usually aborted]
    
    or via wallet:
    external_in → OwnerWalletSendMessage (to Wallet)
      → OwnerClientRequestRefund (to Client)
        → ReturnExcessesBack
        → ClientProxyRequest
    """

    def __init__(self):
        excesses = ContractMatcher(
            opcode=CocoonReturnExcessesBack.opcode, optional=False
        )
        client_request = ContractMatcher(
            opcode=CocoonClientProxyRequest.opcode, optional=False
        )

        # optional wrapper via wallet
        wallet_wrapper = ContractMatcher(
            opcode=CocoonOwnerWalletSendMessage.opcode,
            optional=True
        )

        super().__init__(optional=False,
                         parent_matcher=labeled("wallet_wrapper", wallet_wrapper),
                         children_matchers=[
                            labeled("excesses", excesses),
                            labeled("client_request", client_request)
                        ])

    def test_self(self, block: Block):
        # can start with either wallet message or direct refund request
        return isinstance(block, CallContractBlock) and block.opcode in [
            CocoonOwnerWalletSendMessage.opcode,
            CocoonOwnerClientRequestRefund.opcode,
        ]

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        start_block = block
        start_msg = start_block.get_message()

        via_wallet = get_labeled("wallet_wrapper", other_blocks, CallContractBlock) is not None

        refund_block = start_block

        # parse OwnerClientRequestRefund
        try:
            refund_msg = CocoonOwnerClientRequestRefund(refund_block.get_body())
            query_id = refund_msg.query_id
            owner = AccountId(refund_msg.send_excesses_to)
        except Exception as e:
            logger.error(
                f"failed to parse OwnerClientRequestRefund in trace {start_msg.trace_id}: {e}",
                exc_info=True,
            )
            return []

        client_contract = AccountId(refund_block.get_message().destination)

        data = CocoonClientRequestRefundData(
            client_contract=client_contract,
            owner=owner,
            query_id=query_id,
            via_wallet=via_wallet,
        )

        new_block = CocoonClientRequestRefundBlock(data)
        new_block.merge_blocks([start_block] + other_blocks)
        new_block.failed = start_msg.transaction.aborted

        return [new_block]


# === Grant Refund ===


@dataclass
class CocoonGrantRefundData:
    """grant refund action data"""

    proxy_contract: AccountId  # proxy granting refund
    client_contract: AccountId  # client receiving refund
    query_id: int
    new_tokens_used: int  # tokens used/charged
    expected_address: str | None  # expected address


class CocoonGrantRefundBlock(Block):
    data: CocoonGrantRefundData

    def __init__(self, data: CocoonGrantRefundData):
        super().__init__("cocoon_grant_refund", [], data)

    def __repr__(self):
        return f"cocoon_grant_refund proxy={self.data.proxy_contract.address} client={self.data.client_contract.address}"


class CocoonGrantRefundMatcher(BlockMatcher):
    """
    matches grant refund flow:
    external_in → GrantRefundPayload (to Proxy)
      → ClientProxyRequest (Proxy → Client) [aborted with 1014]
    """

    def __init__(self):
        client_request = ContractMatcher(
            opcode=CocoonClientProxyRequest.opcode, optional=False
        )
        super().__init__(optional=False, child_matcher=labeled("client_request", client_request))

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == CocoonGrantRefundPayload.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        start_block = block
        start_msg = start_block.get_message()

        # parse GrantRefundPayload
        try:
            grant_msg = CocoonGrantRefundPayload(start_block.get_body())
            query_id = grant_msg.query_id
            new_tokens_used = grant_msg.new_tokens_used
            expected_address = grant_msg.expected_my_address
            if isinstance(expected_address, ExternalAddress):
                expected_address = f"{expected_address.len};{hex(expected_address.external_address)[2:]}"  # type: ignore
            elif isinstance(expected_address, Address):
                expected_address = expected_address.to_str(False).upper()
        except Exception as e:
            logger.error(
                f"failed to parse GrantRefundPayload in trace {start_msg.trace_id}: {e}",
                exc_info=True,
            )
            return []

        # find ClientProxyRequest
        client_request_block = get_labeled("client_request", other_blocks, CallContractBlock)
        if not client_request_block:
            logger.warning(
                f"ClientProxyRequest not found in cocoon grant refund trace {start_msg.trace_id}"
            )
            return []

        client_msg = client_request_block.get_message()
        proxy_contract = AccountId(start_msg.destination)
        client_contract = AccountId(client_msg.destination)

        data = CocoonGrantRefundData(
            proxy_contract=proxy_contract,
            client_contract=client_contract,
            query_id=query_id,
            new_tokens_used=new_tokens_used,
            expected_address=expected_address,
        )

        new_block = CocoonGrantRefundBlock(data)
        new_block.merge_blocks([start_block] + other_blocks)
        new_block.failed = start_msg.transaction.aborted

        return [new_block]
