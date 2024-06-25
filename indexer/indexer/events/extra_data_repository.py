from __future__ import annotations

import abc
from contextvars import ContextVar

import msgpack
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import JettonWallet, NFTItem


class ExtraDataRepository(abc.ABC):
    @abc.abstractmethod
    async def get_jetton_wallet(self, address: str) -> JettonWallet | None:
        pass

    @abc.abstractmethod
    async def get_nft_item(self, address: str) -> NFTItem | None:
        pass


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
