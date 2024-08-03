from __future__ import annotations

import abc
from collections import defaultdict
from contextvars import ContextVar

import msgpack
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from indexer.core.database import JettonWallet, NFTItem


class ExtraDataRepository(abc.ABC):
    @abc.abstractmethod
    async def get_jetton_wallet(self, address: str) -> JettonWallet | None:
        pass

    @abc.abstractmethod
    async def get_nft_item(self, address: str) -> NFTItem | None:
        pass


class InMemoryExtraDataRepository(ExtraDataRepository):
    def __init__(self, interface_map: dict[str, dict[str, dict]], backoff_repository: ExtraDataRepository):
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
                if interface_type == "NFTItem":
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


class SqlAlchemyExtraDataRepository(ExtraDataRepository):
    def __init__(self, session: ContextVar[AsyncSession]):
        self.session = session

    async def get_jetton_wallet(self, address: str) -> JettonWallet | None:
        return await self.session.get().get(JettonWallet, address)

    async def get_nft_item(self, address: str) -> NFTItem | None:
        return await self.session.get().get(NFTItem, address)


class RedisExtraDataRepository(ExtraDataRepository):
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


async def gather_interfaces(accounts: set[str], session: AsyncSession) -> dict[str, dict[str, dict]]:
    result = defaultdict(dict)
    jetton_wallets = await session.execute(select(JettonWallet).filter(JettonWallet.address.in_(accounts)))
    nft_items = await session.execute(select(NFTItem).filter(NFTItem.address.in_(accounts)))
    for wallet in accounts:
        result[wallet] = {}
    for wallet in jetton_wallets.scalars().all():
        result[wallet.address]["JettonWallet"] = {
            "balance": wallet.balance,
            "address": wallet.address,
            "owner": wallet.owner,
            "jetton": wallet.jetton,
        }
    for item in nft_items.scalars().all():
        result[item.address]["NFTItem"] = {
            "address": item.address,
            "init": item.init,
            "index": item.index,
            "collection_address": item.collection_address,
            "owner_address": item.owner_address,
            "content": item.content,
        }
    return result
