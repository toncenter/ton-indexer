from __future__ import annotations

import abc
from collections import defaultdict
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional, Dict, Any

import msgpack
import redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from indexer.core.database import JettonWallet, NFTItem, NftSale, NftAuction, LatestAccountState
from indexer.events import context

NOMINATOR_POOL_CODE_HASH = "mj7BS8CY9rRAZMMFIiyuooAPF92oXuaoGYpwle3hDc8="

@dataclass
class DedustPool:
    address: str
    assets: dict

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
    async def get_interfaces(self, address: str) -> dict[str, dict]:
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
        if address in context.dedust_pools.get():
            interfaces['dedust_pool'] = context.dedust_pools.get()[address]
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

    async def get_dedust_pool(self, address: str) -> DedustPool | None:
        if address in context.dedust_pools.get():
            return DedustPool(address=address, assets=context.dedust_pools.get()[address]['assets'])
        return None


class EmulatedTransactionsInterfaceRepository(InterfaceRepository):

    def __init__(self, redis_hash: dict[str, bytes]):
        self.data = redis_hash

    async def get_jetton_wallet(self, address: str) -> JettonWallet | None:
        raw_data = self.data.get(address)
        if raw_data is None:
            return None

        data = msgpack.unpackb(raw_data, raw=False)
        if 'interfaces' not in data:
            return None
        interfaces = data['interfaces']
        for (interface_type, interface_data) in interfaces:
            if interface_type == 0:
                return JettonWallet(
                    balance=interface_data['balance'],
                    address=interface_data['address'],
                    owner=interface_data['owner'],
                    jetton=interface_data['jetton'],
                )
        return None

    async def get_nft_item(self, address: str) -> NFTItem | None:
        raw_data = self.data.get(address)
        if raw_data is None:
            return None

        data = msgpack.unpackb(raw_data, raw=False)
        if 'interfaces' not in data:
            return None
        interfaces = data['interfaces']
        for (interface_type, interface_data) in interfaces:
            if interface_type == 2:
                return NFTItem(
                    address=interface_data['address'],
                    init=interface_data['init'],
                    index=interface_data['index'],
                    collection_address=interface_data['collection_address'],
                    owner_address=interface_data['owner_address'],
                    content=interface_data['content'],
                )
        return None

    async def get_nft_sale(self, address: str) -> NftSale | None:
        raw_data = self.data.get(address)
        if raw_data is None:
            return None

        data = msgpack.unpackb(raw_data, raw=False)
        if 'interfaces' not in data:
            return None
        interfaces = data['interfaces']
        for (interface_type, interface_data) in interfaces:
            if interface_type == 4:
                return NftSale(
                    address=interface_data['address'],
                    is_complete=interface_data['is_complete'],
                    marketplace_address=interface_data['marketplace_address'],
                    nft_address=interface_data['nft_address'],
                    nft_owner_address=interface_data['nft_owner_address'],
                    full_price=interface_data['full_price'],
                )
        return None

    async def get_nft_auction(self, address: str) -> NftAuction | None:
        raw_data = self.data.get(address)
        if raw_data is None:
            return None

        data = msgpack.unpackb(raw_data, raw=False)
        if 'interfaces' not in data:
            return None
        interfaces = data['interfaces']
        for (interface_type, interface_data) in interfaces:
            if interface_type == 5:
                return NftAuction(
                    address=interface_data['address'],
                    nft_addr=interface_data['nft_addr'],
                    nft_owner=interface_data['nft_owner'],
                )
        return None

    async def get_interfaces(self, address: str) -> dict[str, dict]:
        return {}

    async def get_dedust_pool(self, address: str) -> DedustPool | None:
        if address in context.dedust_pools.get():
            return DedustPool(address=address, assets=context.dedust_pools.get()[address]['assets'])
        return None


class EmulatedRepositoryWithDbFallback(InterfaceRepository):
    def __init__(self,
                 emulated_repository: InterfaceRepository,
                 db_interfaces: Dict[str, Dict[str, Dict]] = None):
        self.emulated_repository = emulated_repository
        self.db_interfaces = db_interfaces or {}

    async def get_jetton_wallet(self, address: str) -> Optional[JettonWallet]:
        result = await self.emulated_repository.get_jetton_wallet(address)

        if result is None and address in self.db_interfaces:
            if "JettonWallet" in self.db_interfaces[address]:
                data = self.db_interfaces[address]["JettonWallet"]
                result = JettonWallet(
                    balance=data["balance"],
                    address=data["address"],
                    owner=data["owner"],
                    jetton=data["jetton"],
                )

        return result

    async def get_nft_item(self, address: str) -> Optional[NFTItem]:
        result = await self.emulated_repository.get_nft_item(address)

        if result is None and address in self.db_interfaces:
            if "NftItem" in self.db_interfaces[address]:
                data = self.db_interfaces[address]["NftItem"]
                result = NFTItem(
                    address=data["address"],
                    init=data["init"],
                    index=data["index"],
                    collection_address=data["collection_address"],
                    owner_address=data["owner_address"],
                    content=data["content"],
                )

        return result

    async def get_nft_sale(self, address: str) -> Optional[NftSale]:
        result = await self.emulated_repository.get_nft_sale(address)

        if result is None and address in self.db_interfaces:
            if "NftSale" in self.db_interfaces[address]:
                data = self.db_interfaces[address]["NftSale"]
                result = NftSale(
                    address=data["address"],
                    is_complete=data["is_complete"],
                    marketplace_address=data["marketplace_address"],
                    nft_address=data["nft_address"],
                    nft_owner_address=data["nft_owner_address"],
                    full_price=data["full_price"],
                )

        return result

    async def get_nft_auction(self, address: str) -> Optional[NftAuction]:
        result = await self.emulated_repository.get_nft_auction(address)

        if result is None and address in self.db_interfaces:
            if "NftAuction" in self.db_interfaces[address]:
                data = self.db_interfaces[address]["NftAuction"]
                result = NftAuction(
                    address=data["address"],
                    nft_addr=data["nft_addr"],
                    nft_owner=data["nft_owner"],
                )

        return result

    async def get_dedust_pool(self, address: str) -> Optional[DedustPool]:
        result = await self.emulated_repository.get_dedust_pool(address)
        return result

    async def get_interfaces(self, address: str) -> Dict[str, Any]:
        emulated_interfaces = await self.emulated_repository.get_interfaces(address)

        if address in self.db_interfaces:
            result = {**self.db_interfaces[address]}

            for key, value in emulated_interfaces.items():
                result[key] = value
        else:
            result = emulated_interfaces

        return result


async def _gather_data_from_db(
        accounts: set[str],
        session: AsyncSession
) -> tuple[list[JettonWallet], list[NFTItem], list[NftSale], list[NftAuction], list[LatestAccountState]]:
    jetton_wallets = []
    nft_items = []
    nft_sales = []
    getgems_auctions = []
    nominator_pools = []
    account_list = list(accounts)
    for i in range(0, len(account_list), 5000):
        batch = account_list[i:i + 5000]
        auctions = await session.execute(select(NftAuction).filter(NftAuction.address.in_(batch)))
        auctions = list(auctions.scalars().all())
        nft_batch = batch.copy()
        for auction in auctions:
            nft_batch.append(auction.nft_addr)
        wallets = await session.execute(select(JettonWallet).filter(JettonWallet.address.in_(batch)))
        nft = await session.execute(select(NFTItem).filter(NFTItem.address.in_(nft_batch)))
        sales = await session.execute(select(NftSale).filter(NftSale.address.in_(batch)))
        pools = await session.execute(select(LatestAccountState)
                                                .filter(LatestAccountState.account.in_(batch))
                                                .filter(LatestAccountState.code_hash == NOMINATOR_POOL_CODE_HASH))
        jetton_wallets += list(wallets.scalars().all())
        nft_items += list(nft.scalars().all())
        nft_sales += list(sales.scalars().all())
        getgems_auctions += auctions
        nominator_pools += list(pools.scalars().all())

    return jetton_wallets, nft_items, nft_sales, getgems_auctions, nominator_pools


async def gather_interfaces(accounts: set[str], session: AsyncSession) -> dict[str, dict[str, dict]]:
    result = defaultdict(dict)
    (jetton_wallets, nft_items, nft_sales, nft_auctions, nominator_pools) = await _gather_data_from_db(accounts, session)
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
    for account_state in nominator_pools:
        result[account_state.account]["NominatorPool"] = {
            "address": account_state.account,
        }
    return result
