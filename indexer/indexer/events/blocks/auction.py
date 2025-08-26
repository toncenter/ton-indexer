from dataclasses import dataclass

from indexer.events import context
from indexer.events.blocks.basic_blocks import Block, TonTransferBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, GenericMatcher, BlockTypeMatcher
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.nft import NftTransferBlock
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
                'nft_collection': None
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
                'nft_item_index': nft_data['index'] if nft_data['index'] is not None else None
            }
        else:
            return []
        bid_block.merge_blocks([block])
        return [block]

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
