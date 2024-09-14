from __future__ import annotations

import abc
import asyncio
from collections import defaultdict
from contextvars import ContextVar

import msgpack
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from indexer.core.database import JettonWallet, NFTItem, NftSale, NftAuction
import redis


class InterfaceRepository(abc.ABC):
    @abc.abstractmethod
    async def get_jetton_wallet(self, address: str) -> JettonWallet | None:
        pass

    @abc.abstractmethod
    async def get_nft_item(self, address: str) -> NFTItem | None:
        pass

    @abc.abstractmethod
    async def get_nft_sale(self, address: str) -> NftSale | None:
        pass

    @abc.abstractmethod
    async def get_nft_auction(self, address: str) -> NftAuction | None:
        pass

    @abc.abstractmethod
    async def get_interfaces(self, addresses: set[str]) -> dict[str, dict]:
        pass


class InMemoryInterfaceRepository(InterfaceRepository):
    def __init__(self, interface_map: dict[str, dict[str, dict]], backoff_repository: InterfaceRepository):
        self.interface_map = interface_map
        self.backoff_repository = backoff_repository

    async def get_jetton_wallet(self, address: str) -> JettonWallet | None:
        if address in self.interface_map:
            interfaces = self.interface_map[address]
            for (interface_type, interface_data) in interfaces.items():
                if interface_type == "JettonWallet":
                    return JettonWallet(
                        balance=interface_data["balance"],
                        address=interface_data["address"],
                        owner=interface_data["owner"],
                        jetton=interface_data["jetton"],
                    )
        elif self.backoff_repository is not None:
            return await self.backoff_repository.get_jetton_wallet(address)
        return None

    async def get_nft_item(self, address: str) -> NFTItem | None:
        if address in self.interface_map:
            interfaces = self.interface_map[address]
            for (interface_type, interface_data) in interfaces.items():
                if interface_type == "NftItem":
                    return NFTItem(
                        address=interface_data["address"],
                        init=interface_data["init"],
                        index=interface_data["index"],
                        collection_address=interface_data["collection_address"],
                        owner_address=interface_data["owner_address"],
                        content=interface_data["content"],
                    )
        elif self.backoff_repository is not None:
            return await self.backoff_repository.get_nft_item(address)
        return None

    async def get_nft_sale(self, address: str) -> NftSale | None:
        if address in self.interface_map:
            interfaces = self.interface_map[address]
            for (interface_type, interface_data) in interfaces.items():
                if interface_type == "NftSale":
                    return NftSale(
                        address=interface_data["address"],
                        is_complete=interface_data["is_complete"],
                        marketplace_address=interface_data["marketplace_address"],
                        nft_address=interface_data["nft_address"],
                        nft_owner_address=interface_data["nft_owner_address"],
                        full_price=interface_data["full_price"],
                    )
        return None

    async def get_nft_auction(self, address: str) -> NftAuction | None:
        return None


class SqlAlchemyInterfaceRepository(InterfaceRepository):
    def __init__(self, session: ContextVar[AsyncSession]):
        self.session = session

    async def get_jetton_wallet(self, address: str) -> JettonWallet | None:
        return await self.session.get().get(JettonWallet, address)

    async def get_nft_item(self, address: str) -> NFTItem | None:
        return await self.session.get().get(NFTItem, address)

    async def get_nft_sale(self, address: str) -> NftSale | None:
        return None

    async def get_nft_auction(self, address: str) -> NftAuction | None:
        return await self.session.get().get(NftAuction, address)


class RedisInterfaceRepository(InterfaceRepository):
    prefix = "I_"  # Prefix for keys in Redis

    def __init__(self, connection: redis.Redis):
        self.connection = connection

    async def put_interfaces(self, interfaces: dict[str, dict[str, dict]]):
        batch_size = 5000
        serialized_interfaces = [(RedisInterfaceRepository.prefix + address, msgpack.packb(data, use_bin_type=True))
                                 for (address, data) in interfaces.items() if len(data.keys()) > 0]
        for i in range(0, len(serialized_interfaces), batch_size):
            pipe = self.connection.pipeline()
            for (key, value) in serialized_interfaces[i:i + batch_size]:
                pipe.set(key, value, ex=300)
            pipe.execute()

    async def get_jetton_wallet(self, address: str) -> JettonWallet | None:
        raw_data = self.connection.get(RedisInterfaceRepository.prefix + address)
        if raw_data is None:
            return None

        interfaces = msgpack.unpackb(raw_data, raw=False)
        interface_data = next((data for (interface_type, data) in interfaces.items() if interface_type == "JettonWallet"), None)
        if interface_data is not None:
            return JettonWallet(
                balance=interface_data["balance"],
                address=interface_data["address"],
                owner=interface_data["owner"],
                jetton=interface_data["jetton"],
            )
        return None

    async def get_nft_item(self, address: str) -> NFTItem | None:
        raw_data = self.connection.get(RedisInterfaceRepository.prefix + address)
        if raw_data is None:
            return None

        interfaces = msgpack.unpackb(raw_data, raw=False)
        interface_data = next((data for (interface_type, data) in interfaces.items() if interface_type == "NftItem"), None)
        if interface_data is not None:
            return NFTItem(
                address=interface_data["address"],
                init=interface_data["init"],
                index=interface_data["index"],
                collection_address=interface_data["collection_address"],
                owner_address=interface_data["owner_address"],
                content=interface_data["content"],
            )
        return None

    async def get_nft_sale(self, address: str) -> NftSale | None:
        raw_data = self.connection.get(RedisInterfaceRepository.prefix + address)
        if raw_data is None:
            return None

        interfaces = msgpack.unpackb(raw_data, raw=False)
        interface_data = next((data for (interface_type, data) in interfaces.items() if interface_type == "NftSale"), None)
        if interface_data is not None:
            return NftSale(
                address=interface_data["address"],
                is_complete=interface_data["is_complete"],
                marketplace_address=interface_data["marketplace_address"],
                nft_address=interface_data["nft_address"],
                nft_owner_address=interface_data["nft_owner_address"],
                full_price=interface_data["full_price"],
            )
        return None

    async def get_interfaces(self, address: str) -> dict[str, dict]:
        result = {}
        raw_data = self.connection.get(RedisInterfaceRepository.prefix + address)
        if raw_data is None:
            return {}

        interfaces = msgpack.unpackb(raw_data, raw=False)
        return interfaces

    async def get_nft_auction(self, address: str) -> NftAuction | None:
        raw_data = self.connection.get(RedisInterfaceRepository.prefix + address)
        if raw_data is None:
            return None

        interfaces = msgpack.unpackb(raw_data, raw=False)
        interface_data = next((data for (interface_type, data) in interfaces.items() if interface_type == "NftAuction"),
                              None)
        if interface_data is not None:
            return NftAuction(
                address=interface_data["address"],
                nft_addr=interface_data["nft_addr"],
                nft_owner=interface_data["nft_owner"],
            )
        return None


class EmulatedTransactionsInterfaceRepository(InterfaceRepository):

    def __init__(self, redis_hash: dict[str, bytes]):
        self.data = redis_hash

    async def get_jetton_wallet(self, address: str) -> JettonWallet | None:
        raw_data = self.data.get(address)
        if raw_data is None:
            return None

        data = msgpack.unpackb(raw_data, raw=False)
        interfaces = data[0]
        for (interface_type, interface_data) in interfaces:
            if interface_type == 0:
                return JettonWallet(
                    balance=interface_data[0],
                    address=interface_data[1],
                    owner=interface_data[2],
                    jetton=interface_data[3],
                )
        return None

    async def get_nft_item(self, address: str) -> NFTItem | None:
        raw_data = self.data.get(address)
        if raw_data is None:
            return None

        data = msgpack.unpackb(raw_data, raw=False)
        interfaces = data[0]
        for (interface_type, interface_data) in interfaces:
            if interface_type == 2:
                return NFTItem(
                    address=interface_data[0],
                    init=interface_data[1],
                    index=interface_data[2],
                    collection_address=interface_data[3],
                    owner_address=interface_data[4],
                    content=interface_data[5],
                )
        return None

    async def get_nft_sale(self, address: str) -> NftSale | None:
        raw_data = self.data.get(address)
        if raw_data is None:
            return None

        data = msgpack.unpackb(raw_data, raw=False)
        interfaces = data[0]
        for (interface_type, interface_data) in interfaces:
            if interface_type == 4:
                return NftSale(
                    address=interface_data[0],
                    is_complete=interface_data[1],
                    marketplace_address=interface_data[3],
                    nft_address=interface_data[4],
                    nft_owner_address=interface_data[5],
                    full_price=interface_data[6],
                )
        return None

    async def get_nft_auction(self, address: str) -> NftAuction | None:
        raw_data = self.data.get(address)
        if raw_data is None:
            return None

        data = msgpack.unpackb(raw_data, raw=False)
        interfaces = data[0]
        for (interface_type, interface_data) in interfaces:
            if interface_type == 5:
                return NftAuction(
                    address=interface_data[0],
                    nft_addr=interface_data[4],
                    nft_owner=interface_data[5],
                )
        return None

    async def get_interfaces(self, address: str) -> dict[str, dict]:
        return {}


async def _gather_data_from_db(
        accounts: set[str],
        session: AsyncSession
) -> tuple[list[JettonWallet], list[NFTItem], list[NftSale], list[NftAuction]]:
    jetton_wallets = []
    nft_items = []
    nft_sales = []
    getgems_auctions = []
    account_list = list(accounts)
    for i in range(0, len(account_list), 5000):
        batch = account_list[i:i + 5000]
        wallets = await session.execute(select(JettonWallet).filter(JettonWallet.address.in_(batch)))
        nft = await session.execute(select(NFTItem).filter(NFTItem.address.in_(batch)))
        sales = await session.execute(select(NftSale).filter(NftSale.address.in_(batch)))
        auctions = await session.execute(select(NftAuction).filter(NftAuction.address.in_(batch)))
        jetton_wallets += list(wallets.scalars().all())
        nft_items += list(nft.scalars().all())
        nft_sales += list(sales.scalars().all())
        getgems_auctions += list(auctions.scalars().all())

    return jetton_wallets, nft_items, nft_sales, getgems_auctions


async def gather_interfaces(accounts: set[str], session: AsyncSession) -> dict[str, dict[str, dict]]:
    result = defaultdict(dict)
    (jetton_wallets, nft_items, nft_sales, nft_auctions) = await _gather_data_from_db(accounts, session)
    for wallet in accounts:
        result[wallet] = {}
    for wallet in jetton_wallets:
        result[wallet.address]["JettonWallet"] = {
            "balance": float(wallet.balance),
            "address": wallet.address,
            "owner": wallet.owner,
            "jetton": wallet.jetton,
        }
    for item in nft_items:
        result[item.address]["NftItem"] = {
            "address": item.address,
            "init": item.init,
            "index": float(item.index),
            "collection_address": item.collection_address,
            "owner_address": item.owner_address,
            "content": item.content,
        }
    for sale in nft_sales:
        result[sale.address]["NftSale"] = {
            "address": sale.address,
            "is_complete": sale.is_complete,
            "marketplace_address": sale.marketplace_address,
            "nft_address": sale.nft_address,
            "nft_owner_address": sale.nft_owner_address,
            "full_price": float(sale.full_price),
        }
    for auction in nft_auctions:
        result[auction.address]["NftAuction"] = {
            "address": auction.address,
            "nft_addr": auction.nft_addr,
            "nft_owner": auction.nft_owner
        }
    return result
