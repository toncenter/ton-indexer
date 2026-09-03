from __future__ import annotations

import base64
from dataclasses import dataclass

from pytoniq_core import Slice

from indexer.events import context

from indexer.events.blocks.labels import labeled
from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, OrMatcher, ContractMatcher
from indexer.events.blocks.core import Block, EmptyBlock
from indexer.events.blocks.messages import NftOwnershipAssigned, ExcessMessage
from indexer.events.blocks.messages.nft import NftDiscovery, NftReportStaticData, NftTransfer, TeleitemBidInfo, AuctionFillUp
from indexer.events.blocks.utils import AccountId, Amount
from indexer.events.blocks.utils.block_utils import find_call_contracts, get_labeled
from indexer.events.blocks.utils.block_utils import find_messages


class NftMintBlock(Block):
    def __init__(self, data: dict):
        super().__init__('nft_mint', [], data)


class NftTransferBlock(Block):
    def __init__(self):
        super().__init__('nft_transfer', [],  None)

    def get_nft_collection(self):
        if self.data is not None and 'nft' in self.data and 'collection' in self.data['nft']:
            return self.data['nft']['collection']['address']
        return None


@dataclass
class NftDiscoveryBlockData:
    sender: AccountId
    nft: AccountId
    result_collection: AccountId
    result_index: int
    query_id: int

class NftDiscoveryBlock(Block):
    data: NftDiscoveryBlockData

    def __init__(self, data: NftDiscoveryBlockData):
        super().__init__("nft_discovery", [], data)

    def __repr__(self):
        return f"nft_discovery {self.data}"


async def _get_nft_data(nft_address: AccountId):
    data = {
        "address": nft_address,
        "index": None,
        "collection": None,
        "exists": False
    }
    nft = await context.interface_repository.get().get_nft_item(nft_address.as_str())
    if nft is not None:
        data['index'] = nft.index
        data['exists'] = True
        if "uri" in nft.content and "https://nft.fragment.com" in nft.content["uri"]:
            tokens = nft.content["uri"].split("/")
            data["name"] = tokens[-1][:-5]
            data["type"] = tokens[-2]
        else:
            data['meta'] = nft.content
        if nft.collection_address is not None:
            data['collection'] = {
                'address': AccountId(nft.collection_address),
            }
    return data


async def _try_get_nft_purchase_data(prev_block: Block, owner: str) -> dict | None:
    event_node = prev_block.event_nodes[0]
    if isinstance(prev_block, TonTransferBlock) and event_node.message.source.upper() == owner.upper():
        nft_sale = await context.interface_repository.get().get_nft_sale(event_node.message.transaction.account)
        if nft_sale is not None:
            return {
                'marketplace_address': nft_sale.marketplace_address,
                'nft_address': nft_sale.nft_address,
                'block': prev_block,
                'price': nft_sale.full_price,
                'real_prev_owner': nft_sale.nft_owner_address,
            }

    nft_auction = await context.interface_repository.get().get_nft_auction(event_node.message.transaction.account)
    if nft_auction is not None:
        return {
            'marketplace_address': nft_auction.mp_addr,
            'nft_address': nft_auction.nft_addr,
            'block': prev_block,
            'price': nft_auction.last_bid,
            'real_prev_owner': nft_auction.nft_owner,
        }

    return None


def _purchase_parent_to_consume(prev_block: 'Block | None') -> 'Block | None':
    """Legacy purchase-branch parent consumption: the funding parent (buyer
    payment / sale-contract call) is absorbed into the nft_transfer unless it is
    a finish/stop sale-contract ton_transfer (that shape rides the getgems proxy
    surgery instead). Shared by the legacy wrapper and the mch shaper."""
    if isinstance(prev_block, TonTransferBlock):
        return prev_block if prev_block.comment not in ['finish', 'stop'] else None
    if isinstance(prev_block, CallContractBlock) and prev_block.get_message().source is None:
        return prev_block
    return None


async def build_nft_transfer_core(
        block: Block,
        ownership_message: 'NftOwnershipAssigned | None',
        funding_parent: 'Block | None') -> tuple['NftTransferBlock | None', list[Block]]:
    """Shared field build for a base NFT transfer (plain transfer + getgems
    purchase). Pure over the anchor `block` (the NftTransfer call) and the
    optional ownership notification and funding parent supplied by the caller.

    Returns `(new_block, extra)` — the produced NftTransferBlock (data set, NOT
    merged) plus the funding parent to consume in the purchase branch (`[]`
    otherwise), or `(None, [])` to reject when the NFT item is unknown. Merging
    is the caller's job (legacy wrapper / mch synthesized wrapper + shaper),
    mirroring build_telegram_nft_purchase_core.
    """
    new_block = NftTransferBlock()
    data = dict()
    data['is_purchase'] = False
    nft_transfer_message = NftTransfer(
        Slice.one_from_boc(block.event_nodes[0].message.message_content.body))
    if ownership_message is not None:
        data['prev_owner'] = AccountId(ownership_message.prev_owner)
    else:
        data['prev_owner'] = AccountId(block.event_nodes[0].message.source)
    data['query_id'] = nft_transfer_message.query_id
    data['forward_amount'] = Amount(nft_transfer_message.forward_amount)
    if nft_transfer_message.response_destination:
        data['response_destination'] = AccountId(nft_transfer_message.response_destination)
    else:
        data['response_destination'] = None
    data['custom_payload'] = base64.b64encode(nft_transfer_message.custom_payload).decode('utf-8') if (
            nft_transfer_message.custom_payload is not None) else None
    data['forward_payload'] = base64.b64encode(nft_transfer_message.forward_payload).decode('utf-8') if (
            nft_transfer_message.forward_payload is not None) else None
    data['new_owner'] = AccountId(nft_transfer_message.new_owner)
    data['nft'] = await _get_nft_data(AccountId(block.event_nodes[0].message.transaction.account))
    if not data['nft']['exists']:
        return None, []
    extra: list[Block] = []
    if funding_parent is not None:
        nft_purchase_data = await _try_get_nft_purchase_data(
            funding_parent, nft_transfer_message.new_owner.to_str(False))
        if nft_purchase_data is not None and AccountId(nft_purchase_data['nft_address']) == data['nft']['address']:
            real_owner = AccountId(nft_purchase_data['real_prev_owner'])
            if real_owner != data['new_owner']:
                data['is_purchase'] = True
                data['marketplace'] = 'getgems'
                data['marketplace_address'] = AccountId(nft_purchase_data['marketplace_address'])
                data['price'] = Amount(nft_purchase_data['price'])
                data['real_prev_owner'] = AccountId(nft_purchase_data['real_prev_owner'])
                parent = _purchase_parent_to_consume(funding_parent)
                if parent is not None:
                    extra.append(parent)
    new_block.data = data
    new_block.failed = block.failed
    return new_block, extra


class NftTransferBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=OrMatcher([
            ContractMatcher(opcode=NftOwnershipAssigned.opcode, optional=True),
            ContractMatcher(opcode=ExcessMessage.opcode, optional=True)
        ], optional=True), parent_matcher=None)

    def test_self(self, block: Block):
        if isinstance(block, CallContractBlock) and block.opcode == NftTransfer.opcode:
            return True

    async def build_block(self, block: Block, other_blocks: list['Block']):
        ownership_assigned_message = find_messages(other_blocks, NftOwnershipAssigned)
        ownership_message = ownership_assigned_message[0][1] if len(ownership_assigned_message) > 0 else None
        new_block, extra = await build_nft_transfer_core(
            block, ownership_message, block.previous_block)
        if new_block is None:
            return []
        new_block.merge_blocks([block] + extra + other_blocks)
        return [new_block]

@dataclass()
class NftPurchaseData:
    nft_address: AccountId
    collection_address: AccountId | None
    nft_index: int
    prev_owner: AccountId | None
    new_owner: AccountId
    query_id: int
    forward_amount: Amount | None
    response_destination: AccountId | None
    custom_payload: str | None
    forward_payload: str | None
    payout_amount: Amount | None
    payout_comment_encrypted: bool | None
    payout_comment_encoded: bool | None
    payout_comment: str | None
    price: Amount | None
    real_prev_owner: AccountId | None
    marketplace: str | None
    marketplace_address: AccountId | None

class NftPurchaseBlock(Block):
    data: NftPurchaseData
    def __init__(self, data: NftPurchaseData):
        super().__init__("nft_purchase", [], data)

def build_getgems_nft_purchase_core(
        block: Block, ton_transfer: 'TonTransferBlock | None') -> NftPurchaseBlock | None:
    """Shared field build for a getgems NFT purchase.

    Pure function of the anchor `block` (the already-produced getgems
    `nft_transfer` block) and the `ton_transfer` payout to the seller (the
    ton_transfer whose destination is `real_prev_owner`). Returns the produced
    NftPurchaseBlock with data set but NOT merged, or None to reject — mirroring
    build_telegram_nft_purchase_core. The legacy matcher derives `ton_transfer`
    by scanning candidate siblings/children; the mch builder gets it from a
    pattern capture. Consumption/merge and the finish/stop proxy insertion are
    the caller's job (legacy wrapper / mch synthesized wrapper + shaper).
    """
    if block.data.get('real_prev_owner') is None:
        return None
    if ton_transfer is None:  # Ton transfer to seller not found
        return None
    data = NftPurchaseData(
        nft_address=block.data['nft']['address'],
        collection_address=block.data['nft']['collection']['address'] if block.data['nft']['collection'] else None,
        nft_index=block.data['nft']['index'],
        prev_owner=block.data['prev_owner'],
        new_owner=block.data['new_owner'],
        query_id=block.data['query_id'],
        forward_amount=block.data['forward_amount'],
        response_destination=block.data['response_destination'],
        custom_payload=block.data['custom_payload'],
        forward_payload=block.data['forward_payload'],
        payout_amount=Amount(ton_transfer.value),
        payout_comment_encrypted=ton_transfer.encrypted,
        payout_comment_encoded=ton_transfer.comment_encoded,
        payout_comment=ton_transfer.comment,
        price=block.data['price'],
        real_prev_owner=block.data['real_prev_owner'],
        marketplace=block.data['marketplace'],
        marketplace_address=block.data['marketplace_address'],
    )
    return NftPurchaseBlock(data)


class GetgemsNftPurchaseBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__()

    def test_self(self, block: Block) -> bool:
        return (block.btype == 'nft_transfer' and block.data['is_purchase'] == True
                and block.data.get('marketplace') == 'getgems')

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        if block.data.get('real_prev_owner') is None:
            return []

        include = [block]
        candidates = block.next_blocks
        need_proxy = False
        if isinstance(block.previous_block, TonTransferBlock) and block.previous_block.comment in ['finish', 'stop']:
            candidates = block.previous_block.next_blocks
            need_proxy = True

        # Find ton transfer to seller
        ton_transfer: TonTransferBlock|None = None
        for n in candidates:
            if n.btype == 'ton_transfer' and n.get_message().destination == block.data['real_prev_owner']:
                ton_transfer = n
                include.append(n)
                break

        new_block = build_getgems_nft_purchase_core(block, ton_transfer)
        if new_block is None:
            return []

        if need_proxy:
            proxy = EmptyBlock()
            block.previous_block.insert_between([block, ton_transfer], proxy)
            include.append(proxy)

        new_block.merge_blocks(include)
        return [new_block]


class NftDiscoveryBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            optional=False,
            child_matcher=labeled("report",
                            ContractMatcher(
                                opcode=NftReportStaticData.opcode,
                                optional=False,
                            )
                    ),
            )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == NftDiscovery.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        include = [block]

        sender = block.get_message().source
        nft = block.get_message().destination

        report_block = get_labeled("report", other_blocks, CallContractBlock)
        if not report_block:
            return []
        report_data = NftReportStaticData(report_block.get_body())

        data = NftDiscoveryBlockData(
            sender=AccountId(sender),
            nft=AccountId(nft),
            result_collection=AccountId(report_data.collection),
            result_index=report_data.index,
            query_id=report_data.query_id,
        )
        include.append(report_block)

        new_block = NftDiscoveryBlock(data)
        new_block.merge_blocks(include)

        return [new_block]




async def build_telegram_nft_purchase_core(
        block: CallContractBlock,
        prev_block: Block | None,
        payouts: list[Block]) -> tuple[NftTransferBlock | None, list[Block]]:
    """Shared field build for a telegram (fragment) NFT purchase.

    Pure function of the anchor `block` (the NftOwnershipAssigned call), its
    `prev_block` (the payment ton_transfer / external call that funded the
    purchase, or None) and the AuctionFillUp `payouts` found under that parent.
    The legacy TelegramNftPurchaseBlockMatcher derives prev_block/payouts by
    walking the tree; the mch builder gets them from pattern captures.

    Returns `(new_block, extra_include)` — the produced NftTransferBlock (data
    set, NOT merged) plus the blocks beyond the anchor that this build absorbs
    (sorted payouts + the payment), or `(None, [])` to reject. Merging is the
    caller's job (legacy wrapper / mch synthesized wrapper), mirroring
    build_jetton_transfer_core.
    """
    new_block = NftTransferBlock()
    data = dict()
    data['is_purchase'] = False
    message = block.get_message()
    nft_ownership_message = NftOwnershipAssigned(Slice.one_from_boc(message.message_content.body))
    data['new_owner'] = AccountId(message.destination)
    prev_owner = AccountId(nft_ownership_message.prev_owner) if nft_ownership_message.prev_owner is not None else None
    data['prev_owner'] = prev_owner
    data['query_id'] = nft_ownership_message.query_id
    data['forward_amount'] = None
    data['response_destination'] = None
    data['custom_payload'] = None
    data['forward_payload'] = None
    data['nft'] = await _get_nft_data(AccountId(block.get_message().source))
    if not data['nft']['exists']:
        return None, []
    extra: list[Block] = []
    payload = nft_ownership_message.nft_payload
    if payload is not None:
        data['forward_payload'] = base64.b64encode(payload.raw).decode('utf-8')
    if payload is not None and isinstance(payload.value, TeleitemBidInfo):
        data['is_purchase'] = True
        data['price'] = Amount(payload.value.bid)
        data['marketplace'] = 'fragment'
        data['real_prev_owner'] = None
        is_mint = False
        if isinstance(prev_block, CallContractBlock) and prev_block.opcode == 0x299a3e15: # telemint (currently not supported)
            is_mint = True
        elif prev_block is not None and prev_block.btype == 'nft_mint':
            # btype, not isinstance(NftMintBlock): specs/nft_mint.mch is declarative,
            # so under the mch engines the mint parent is a generic Block carrying
            # that btype. NftMintBlock.btype is 'nft_mint', so legacy is unchanged.
            is_mint = True
        if is_mint:
            data['is_purchase'] = False
        if (isinstance(prev_block, TonTransferBlock) or
                (isinstance(prev_block, CallContractBlock) and prev_block.get_message().source is None)):
            payouts = list(payouts)
            payouts.sort(key=lambda p: p.get_message().created_lt)
            # Sending fee is always first fill up message for teleitems
            if len(payouts) > 1:
                data['royalty_amount'] = Amount(payouts[0].get_message().value)
                data['payout_amount'] = Amount(payouts[1].get_message().value)
                data['royalty_address'] = AccountId(payouts[0].get_message().destination)
                data['payout_address'] = AccountId(payouts[1].get_message().destination)
            elif len(payouts) == 1:
                data['payout_address'] = AccountId(payouts[0].get_message().destination)
                data['payout_amount'] = Amount(payouts[0].get_message().value)
            extra.extend(payouts)
            extra.append(prev_block)

    new_block.data = data
    return new_block, extra


class TelegramNftPurchaseBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=None,
                         parent_matcher=None)

    def test_self(self, block: Block):
        if isinstance(block, CallContractBlock) and block.opcode == NftOwnershipAssigned.opcode:
            return True

    async def build_block(self, block: Block, other_blocks: list['Block']):
        assert isinstance(block, CallContractBlock)
        prev_block = block.previous_block
        payouts: list[Block] = []
        if (isinstance(prev_block, TonTransferBlock) or
                (isinstance(prev_block, CallContractBlock) and prev_block.get_message().source is None)):
            payouts = find_call_contracts(prev_block.next_blocks, AuctionFillUp.opcode)
        new_block, extra = await build_telegram_nft_purchase_core(block, prev_block, payouts)
        if new_block is None:
            return []
        new_block.merge_blocks([block] + extra + other_blocks)
        return [new_block]


class NftMintBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=None,
                         parent_matcher=None)

    def test_self(self, block: Block):
        return len(block.contract_deployments) == 1

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        address = next(iter(block.contract_deployments)).as_str()
        nft_item = await context.interface_repository.get().get_nft_item(address)
        if nft_item is None:
            return []
        source = block.event_nodes[0].message.source
        data = {
            "source": AccountId(source) if source else None,
            "address": AccountId(address),
            "index": nft_item.index,
            "opcode": block.event_nodes[0].get_opcode(),
            "collection": AccountId(nft_item.collection_address) if nft_item.collection_address else None,
        }
        new_block = NftMintBlock(data)
        new_block.merge_blocks([block])
        return [new_block]
