from indexer.events.blocks.utils import AccountId, Amount
from indexer.events import context
from indexer.events.blocks.basic_matchers import BlockMatcher
from indexer.events.blocks.basic_blocks import Block, TonTransferBlock


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

