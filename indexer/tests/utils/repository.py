from indexer.events.interface_repository import InterfaceRepository
from tests.utils.trace_deserializer import deserialize_jetton_wallet, deserialize_dedust_pool, deserialize_nft_item


class TestInterfaceRepository(InterfaceRepository):
    def __init__(self, interfaces=None):
        self.interfaces = interfaces or {}

    async def get_jetton_wallet(self, address: str):
        if address in self.interfaces and 'JettonWallet' in self.interfaces[address]:
            return deserialize_jetton_wallet(self.interfaces[address]['JettonWallet'])
        return None

    async def get_nft_item(self, address: str):
        if address in self.interfaces and 'NftItem' in self.interfaces[address]:
            return deserialize_nft_item(self.interfaces[address]['NftItem'])
        return None

    async def get_nft_sale(self, address: str):
        if address in self.interfaces and 'NftSale' in self.interfaces[address]:
            return self.interfaces[address]['NftSale']
        return None

    async def get_nft_auction(self, address: str):
        if address in self.interfaces and 'NftAuction' in self.interfaces[address]:
            return self.interfaces[address]['NftAuction']
        return None

    async def get_dedust_pool(self, address: str):
        if address in self.interfaces and 'DedustPool' in self.interfaces[address]:
            return deserialize_dedust_pool(address, self.interfaces[address]['DedustPool'])
        return None

    async def get_interfaces(self, address: str):
        return self.interfaces.get(address, {})

    async def get_extra_data(self, address: str, request: str):
        if request in self.interfaces.get(address, {}):
            return self.interfaces[address][request]
        return None