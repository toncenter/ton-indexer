from __future__ import annotations

from dataclasses import dataclass

from indexer.events import context
from indexer.events.blocks.basic_blocks import Block, TonTransferBlock, CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, GenericMatcher, BlockTypeMatcher
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.nft import NftTransferBlock, NftPurchaseBlock
from indexer.events.blocks.utils import AccountId, Amount
from indexer.events.blocks.utils.block_utils import get_labeled


class AuctionBid(Block):
    def __init__(self, data):
        super().__init__('auction_bid', [], data)

    def __repr__(self):
        return f"Auction bid {self.event_nodes[0].message.transaction.hash}"

def _is_teleitem(data: dict):
    if 'content' not in data:
        return False
    if 'uri' in data['content'] and 'https://nft.fragment.com' in data['content']['uri']:
        return True
    return False


class AuctionBidMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=None, include_excess=False)

    def test_self(self, block: Block):
        return isinstance(block, TonTransferBlock)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        interfaces = await context.interface_repository.get().get_interfaces(block.event_nodes[0].message.destination)
        if interfaces is None:
            return []

        if len(block.contract_deployments) > 0:
            return []

        if block.comment in ['cancel', 'finish', 'stop']:
            return []

        bid_block = AuctionBid({})

        if 'NftAuction' in interfaces:
            nft_address = interfaces['NftAuction']['nft_addr']
            nft_item = await context.interface_repository.get().get_nft_item(nft_address)
            data = {
                'amount': Amount(block.event_nodes[0].message.value),
                'bidder': AccountId(block.event_nodes[0].message.source),
                'auction': AccountId(block.event_nodes[0].message.destination),
                'nft_address': AccountId(nft_address),
                'nft_item_index': None,
                'nft_collection': None,
                'auction_type': 'getgems'
            }
            if nft_item:
                data['nft_item_index'] = nft_item.index
                data['nft_collection'] = AccountId(nft_item.collection_address)
            bid_block.data = data
        elif 'NftItem' in interfaces and _is_teleitem(interfaces['NftItem']):
            nft_data = interfaces['NftItem']
            bid_block.data = {
                'amount': Amount(block.event_nodes[0].message.value),
                'bidder': AccountId(block.event_nodes[0].message.source),
                'auction': AccountId(block.event_nodes[0].message.destination),
                'nft_address': AccountId(block.event_nodes[0].message.destination),
                'nft_collection': AccountId(nft_data['collection_address']) if nft_data['collection_address'] is not None else None,
                'nft_item_index': nft_data['index'] if nft_data['index'] is not None else None,
                'auction_type': 'fragment'
            }
        else:
            return []
        bid_block.merge_blocks([block])
        return [block]

@dataclass
class AuctionOutbidData:
    auction_address: AccountId
    nft: AccountId
    nft_collection: AccountId
    bidder: AccountId
    new_bidder: AccountId
    amount: Amount
    comment: str|None
    auction_type: str

class AuctionOutbidBlock(Block):
    data: AuctionOutbidData
    def __init__(self, data):
        super().__init__('auction_outbid', [], data)

    def __repr__(self):
        return f"Auction outbid {self.event_nodes[0].message.transaction.hash}"

class AuctionOutbidMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=None, include_excess=False)

    def test_self(self, block: Block) -> bool:
        return isinstance(block, AuctionBid)

    def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        data: AuctionOutbidData = None
        include: list[Block] = []
        if block.data['auction_type'] == 'getgems':
            outbid_transfer: TonTransferBlock = None
            for b in block.next_blocks:
                if (isinstance(b, TonTransferBlock) and block.data['auction'] == b.get_message().source
                        and "Your bid has been outbid by another user" in b.comment):
                    if outbid_transfer is not None: # To avoid false positives only one ton transfer allowed
                        return []
                    outbid_transfer = b

            if outbid_transfer is None:
                return []

            include = [outbid_transfer]

            data = AuctionOutbidData(
                auction_address=block.data['auction'],
                nft=block.data['nft_address'],
                nft_collection=block.data['nft_collection'],
                bidder=AccountId(outbid_transfer.get_message().destination),
                new_bidder=block.data['bidder'],
                amount=Amount(outbid_transfer.value),
                comment=outbid_transfer.comment,
                auction_type='getgems'
            )
        elif block.data['auction_type'] == 'fragment':
            outbid_transfer: CallContractBlock = None
            for b in block.next_blocks:
                if (isinstance(b, CallContractBlock) and block.data['auction'] == b.get_message().source
                        and b.opcode == 0x557cea20):
                    if outbid_transfer is not None:  # To avoid false positives only one ton transfer allowed
                        return []
                    outbid_transfer = b
            if outbid_transfer is None:
                return []
            include = [outbid_transfer]

            data = AuctionOutbidData(
                auction_address=block.data['auction'],
                nft=block.data['nft_address'],
                nft_collection=block.data['nft_collection'],
                bidder=AccountId(outbid_transfer.get_message().destination),
                new_bidder=block.data['bidder'],
                amount=Amount(outbid_transfer.get_message().value),
                comment=None,
                auction_type='fragment'
            )
        new_block = AuctionOutbidBlock(data)
        new_block.merge_blocks(include)
        return [new_block]

@dataclass
class NftPutOnSaleBlockData:
    nft_address: AccountId
    nft_index: int
    nft_collection: AccountId
    owner: AccountId
    listing_address: AccountId
    sale_address: AccountId
    full_price: Amount
    marketplace_address: AccountId
    marketplace: str
    marketplace_fee_address: AccountId
    marketplace_fee: Amount
    royalty_address: AccountId
    royalty_amount: Amount

class NftPutOnSaleBlock(Block):
    def __init__(self, data: NftPutOnSaleBlockData):
        super().__init__('nft_put_on_sale', [], data)

@dataclass
class NftPutOnAuctionBlockData:
    nft_address: AccountId
    nft_index: int
    nft_collection: AccountId
    owner: AccountId
    listing_address: AccountId
    auction_address: AccountId
    marketplace_address: AccountId
    marketplace: str
    mp_fee_address: AccountId
    mp_fee_factor: Amount
    mp_fee_base: Amount
    royalty_fee_addr: AccountId
    royalty_fee_base: Amount
    max_bid: Amount
    min_bid: Amount

class NftPutOnAuctionBlock(Block):
    def __init__(self, data: NftPutOnAuctionBlockData):
        super().__init__('nft_put_on_auction', [], data)

class NftPutOnSaleBlockMatcher(BlockMatcher):
    def __init__(self):
        sale_init = GenericMatcher(test_self_func=lambda b:
            b.btype in ['ton_transfer', 'call_contract'] and len(b.contract_deployments) > 0)
        super().__init__(children_matchers=[
            labeled('sale_init', sale_init),
            labeled('transfer_to_sale', BlockTypeMatcher('nft_transfer'))
        ], include_excess=False)

    def test_self(self, block: Block) -> bool:
        return isinstance(block, NftTransferBlock)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        transfer_to_listing: NftTransferBlock = block
        transfer_to_sale: NftTransferBlock = get_labeled('transfer_to_sale', other_blocks, NftTransferBlock)
        sale_init: Block = get_labeled('sale_init', other_blocks, Block)
        if transfer_to_sale.data['nft']['address'] != transfer_to_listing.data['nft']['address']:
            return []
        if transfer_to_sale.data['new_owner'] != AccountId(sale_init.get_message().destination):
            return []

        getgems_sale = await context.interface_repository.get().get_nft_sale(sale_init.get_message().destination)
        if getgems_sale is not None:
            assert transfer_to_sale.data['nft']['address'] == getgems_sale.nft_address
            assert transfer_to_listing.data['prev_owner'] == getgems_sale.nft_owner_address

            full_price = Amount(int(getgems_sale.full_price))
            marketplace = AccountId(getgems_sale.marketplace_address)
            marketplace_fee_address = AccountId(
                getgems_sale.marketplace_fee_address) if getgems_sale.marketplace_fee_address else None
            marketplace_fee = Amount(int(getgems_sale.marketplace_fee)) if getgems_sale.marketplace_fee else None
            royalty_address = AccountId(getgems_sale.royalty_address) if getgems_sale.royalty_address else None
            royalty_amount = Amount(int(getgems_sale.royalty_amount)) if getgems_sale.royalty_amount else None

            new_block = NftPutOnSaleBlock(NftPutOnSaleBlockData(
                nft_address=AccountId(transfer_to_sale.data['nft']['address']),
                nft_index=transfer_to_sale.data['nft']['index'],
                nft_collection=AccountId(transfer_to_sale.data['nft']['collection']['address'] if transfer_to_sale.data['nft']['collection'] is not None else None),
                owner=AccountId(getgems_sale.nft_owner_address),
                listing_address=AccountId(transfer_to_listing.data['new_owner']),
                sale_address=AccountId(sale_init.get_message().destination),
                full_price=full_price,
                marketplace_address=marketplace,
                marketplace='getgems',
                marketplace_fee_address=marketplace_fee_address,
                marketplace_fee=marketplace_fee,
                royalty_address=royalty_address,
                royalty_amount=royalty_amount,
            ))
            new_block.merge_blocks([block] + other_blocks)

            return [new_block]
        else:
            getgems_auction = await context.interface_repository.get().get_nft_auction(sale_init.get_message().destination)
            if getgems_auction is not None:
                assert transfer_to_sale.data['nft']['address'] == getgems_auction.nft_addr
                assert transfer_to_listing.data['prev_owner'] == getgems_auction.nft_owner

                new_block = NftPutOnAuctionBlock(NftPutOnAuctionBlockData(
                    nft_address=AccountId(transfer_to_sale.data['nft']['address']),
                    nft_index=transfer_to_sale.data['nft']['index'],
                    nft_collection=AccountId(
                        transfer_to_sale.data['nft']['collection']['address'] if transfer_to_sale.data['nft'][
                                                                                     'collection'] is not None else None),
                    owner=AccountId(getgems_auction.nft_owner),
                    listing_address=AccountId(transfer_to_listing.data['new_owner']),
                    auction_address=AccountId(sale_init.get_message().destination),
                    marketplace_address=AccountId(getgems_auction.mp_addr),
                    marketplace='getgems',
                    mp_fee_address=AccountId(getgems_auction.mp_fee_addr) if getgems_auction.mp_fee_addr else None,
                    mp_fee_factor=Amount(int(getgems_auction.mp_fee_factor)) if getgems_auction.mp_fee_factor else None,
                    mp_fee_base=Amount(int(getgems_auction.mp_fee_base)) if getgems_auction.mp_fee_base else None,
                    royalty_fee_addr=AccountId(getgems_auction.royalty_fee_addr) if getgems_auction.royalty_fee_addr else None,
                    royalty_fee_base=Amount(int(getgems_auction.royalty_fee_base)) if getgems_auction.royalty_fee_base else None,
                    max_bid=Amount(int(getgems_auction.max_bid)) if getgems_auction.max_bid else None,
                    min_bid=Amount(int(getgems_auction.min_bid)) if getgems_auction.min_bid else None,
                ))
                new_block.merge_blocks([block] + other_blocks)
                return [new_block]
            else:
                return []

@dataclass
class NftCancelTradeData:
    nft_address: AccountId
    nft_collection: AccountId
    owner: AccountId
    trace_contract: AccountId

async def get_cancel_trade_data(block: Block, return_nft: NftTransferBlock, is_sale = True) -> NftCancelTradeData|None:
    contract_address = block.get_message().destination

    if is_sale:
        sale_info = await context.interface_repository.get().get_nft_sale(contract_address)
        if sale_info is None:
            return None
        if return_nft.data['nft']['address'] != sale_info.nft_address:
            return None
    else:
        auction_info = await context.interface_repository.get().get_nft_auction(contract_address)
        if auction_info is None:
            return None
        if return_nft.data['nft']['address'] != auction_info.nft_addr:
            return None


    return NftCancelTradeData(
        nft_address=return_nft.data['nft']['address'],
        nft_collection=return_nft.get_nft_collection(),
        owner=return_nft.data['new_owner'],
        trace_contract=AccountId(contract_address),
    )

class NftCancelSaleBlock(Block):
    data: NftCancelTradeData
    def __init__(self, data: NftCancelTradeData):
        super().__init__('nft_cancel_sale', [], data)

class NftCancelSaleMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            child_matcher=labeled('nft_transfer', BlockTypeMatcher('nft_transfer'))
        )

    def test_self(self, block: Block) -> bool:
        return isinstance(block, CallContractBlock) and block.opcode == 0x3

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        return_nft: NftTransferBlock = get_labeled('nft_transfer', other_blocks, NftTransferBlock)
        data = await get_cancel_trade_data(block, return_nft)
        if data is None:
            return []

        new_block = NftCancelSaleBlock(data)
        new_block.merge_blocks([block])
        return [new_block]

class NftCancelAuctionBlock(Block):
    data: NftCancelTradeData
    def __init__(self, data: NftCancelTradeData):
        super().__init__('nft_cancel_auction', [], data)

class NftFinishAuctionBlock(Block):
    data: NftCancelTradeData
    def __init__(self, data: NftCancelTradeData):
        super().__init__('nft_finish_auction', [], data)

class NftCancelAuctionMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            child_matcher=labeled('nft_transfer', BlockTypeMatcher('nft_transfer'))
        )
    def test_self(self, block: Block) -> bool:
        if isinstance(block, TonTransferBlock) and block.comment in ['cancel', 'finish', 'stop']:
            return True
        elif isinstance(block, CallContractBlock):
            if block.opcode in [0x5616c572, 0x20c9eb18, 0xb95616b6]: #[cancel, finish, stop]
                return True
        return False

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        return_nft: NftTransferBlock = get_labeled('nft_transfer', other_blocks, NftTransferBlock)
        data = await get_cancel_trade_data(block, return_nft, is_sale=False)
        if data is None:
            return []

        is_finish = False
        if isinstance(block, CallContractBlock) and block.opcode in [0xb95616b6, 0x20c9eb18]:
            is_finish = True
        elif isinstance(block, TonTransferBlock) in ['finish', 'stop']:
            is_finish = True
        if is_finish:
            new_block = NftFinishAuctionBlock(data)
        else:
            new_block = NftCancelAuctionBlock(data)
        new_block.merge_blocks([block])
        return [new_block]

class NftFinishAuctionMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            child_matcher=labeled('nft_purchase', BlockTypeMatcher('nft_purchase'))
        )
    def test_self(self, block: Block) -> bool:
        if isinstance(block, TonTransferBlock) and block.comment in ['finish', 'stop']:
            return True
        elif isinstance(block, CallContractBlock):
            if block.opcode in [0xb95616b6, 0x20c9eb18]: #[stop, finish]
                return True
        return False

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        return_nft: NftPurchaseBlock = get_labeled('nft_purchase', other_blocks, NftPurchaseBlock)
        contract_address = block.get_message().destination
        auction_info = await context.interface_repository.get().get_nft_auction(contract_address)
        if auction_info is None:
            return []
        if return_nft.data.nft_address != auction_info.nft_addr:
            return []

        data = NftCancelTradeData(
            nft_address=return_nft.data.nft_address,
            nft_collection=return_nft.data.collection_address,
            owner=return_nft.data.new_owner,
            trace_contract=AccountId(contract_address),
        )
        new_block = NftFinishAuctionBlock(data)
        new_block.merge_blocks([block])
        return [new_block]