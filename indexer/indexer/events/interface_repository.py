from __future__ import annotations

import abc
from collections import defaultdict, deque
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional, Dict, Any, Callable

import msgpack
import redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from indexer.core import kvrocks
from indexer.core.database import JettonWallet, NFTItem, NftSale, NftAuction, LatestAccountState, MultisigOrder

NOMINATOR_POOL_CODE_HASH = "mj7BS8CY9rRAZMMFIiyuooAPF92oXuaoGYpwle3hDc8="

@dataclass
class DedustPool:
    address: str
    assets: dict


def _context():
    from indexer.events import context
    return context


def _to_float(value):
    if value is None:
        return None
    return float(value)


def _to_int(value):
    if value is None:
        return None
    return int(value)


def _jetton_wallet_interface(payload: dict) -> dict:
    return {
        "balance": _to_float(payload.get("balance")),
        "address": payload.get("address"),
        "owner": payload.get("owner"),
        "jetton": payload.get("jetton"),
    }


def _nft_item_interface(payload: dict) -> dict:
    return {
        "address": payload.get("address"),
        "init": payload.get("init"),
        "index": _to_float(payload.get("index")),
        "collection_address": payload.get("collection_address"),
        "owner_address": payload.get("owner_address"),
        "content": payload.get("content"),
        "code_hash": payload.get("code_hash"),
    }


def _nft_sale_interface(payload: dict) -> dict:
    return {
        "address": payload.get("address"),
        "is_complete": payload.get("is_complete"),
        "marketplace_address": payload.get("marketplace_address"),
        "nft_address": payload.get("nft_address"),
        "nft_owner_address": payload.get("nft_owner_address"),
        "full_price": _to_float(payload.get("full_price")),
        "marketplace_fee_address": payload.get("marketplace_fee_address"),
        "marketplace_fee": _to_float(payload.get("marketplace_fee")),
        "royalty_address": payload.get("royalty_address"),
        "royalty_amount": _to_float(payload.get("royalty_amount")),
        "code_hash": payload.get("code_hash"),
    }


def _nft_auction_interface(payload: dict) -> dict:
    return {
        "address": payload.get("address"),
        "mp_addr": payload.get("mp_addr"),
        "nft_addr": payload.get("nft_addr"),
        "nft_owner": payload.get("nft_owner"),
        "last_bid": _to_float(payload.get("last_bid")),
        "mp_fee_addr": payload.get("mp_fee_addr"),
        "mp_fee_factor": _to_float(payload.get("mp_fee_factor")),
        "mp_fee_base": _to_float(payload.get("mp_fee_base")),
        "royalty_fee_addr": payload.get("royalty_fee_addr"),
        "royalty_fee_base": _to_float(payload.get("royalty_fee_base")),
        "max_bid": _to_float(payload.get("max_bid")),
        "min_bid": _to_float(payload.get("min_bid")),
        "code_hash": payload.get("code_hash"),
    }


def _multisig_order_interface(payload: dict) -> dict:
    return {
        "address": payload.get("address"),
        "multisig_address": payload.get("multisig_address"),
        "order_seqno": _to_float(payload.get("order_seqno")),
        "threshold": _to_float(payload.get("threshold")),
        "sent_for_execution": payload.get("sent_for_execution"),
        "approvals_mask": _to_float(payload.get("approvals_mask")),
        "approvals_num": _to_float(payload.get("approvals_num")),
        "expiration_date": _to_float(payload.get("expiration_date")),
        "order_boc": payload.get("order_boc"),
        "signers": payload.get("signers"),
        "last_transaction_lt": _to_float(payload.get("last_transaction_lt")),
        "code_hash": payload.get("code_hash"),
        "data_hash": payload.get("data_hash"),
    }


def _dedust_pool_from_payload(payload: dict) -> DedustPool:
    assets = []
    for key in ("asset_1", "asset_2"):
        address = payload.get(key)
        assets.append({
            "is_ton": address is None,
            "address": address,
        })
    return DedustPool(address=payload.get("address"), assets=assets)


def _is_destroyed(payload: dict | None) -> bool:
    return bool(payload and payload.get("destroyed"))


def _jetton_wallet_from_interface(data: dict) -> JettonWallet:
    return JettonWallet(
        balance=data["balance"],
        address=data["address"],
        owner=data["owner"],
        jetton=data["jetton"],
    )


def _nft_item_from_interface(data: dict) -> NFTItem:
    return NFTItem(
        address=data["address"],
        init=data["init"],
        index=data["index"],
        collection_address=data["collection_address"],
        owner_address=data["owner_address"],
        content=data["content"],
        code_hash=data.get("code_hash"),
    )


def _nft_sale_from_interface(data: dict) -> NftSale:
    return NftSale(
        address=data["address"],
        is_complete=data["is_complete"],
        marketplace_address=data["marketplace_address"],
        nft_address=data["nft_address"],
        nft_owner_address=data["nft_owner_address"],
        full_price=data["full_price"],
        marketplace_fee_address=data.get("marketplace_fee_address"),
        marketplace_fee=data.get("marketplace_fee"),
        royalty_address=data.get("royalty_address"),
        royalty_amount=data.get("royalty_amount"),
        code_hash=data.get("code_hash"),
    )


def _nft_auction_from_interface(data: dict) -> NftAuction:
    return NftAuction(
        address=data["address"],
        nft_addr=data["nft_addr"],
        nft_owner=data["nft_owner"],
        last_bid=data["last_bid"],
        mp_addr=data["mp_addr"],
        mp_fee_addr=data.get("mp_fee_addr"),
        mp_fee_factor=data.get("mp_fee_factor"),
        mp_fee_base=data.get("mp_fee_base"),
        royalty_fee_addr=data.get("royalty_fee_addr"),
        royalty_fee_base=data.get("royalty_fee_base"),
        max_bid=data.get("max_bid"),
        min_bid=data.get("min_bid"),
        code_hash=data.get("code_hash"),
    )


def _multisig_order_from_interface(data: dict) -> MultisigOrder:
    return MultisigOrder(
        address=data["address"],
        multisig_address=data["multisig_address"],
        order_seqno=data["order_seqno"],
        threshold=data["threshold"],
        sent_for_execution=data["sent_for_execution"],
        approvals_mask=data["approvals_mask"],
        approvals_num=data["approvals_num"],
        expiration_date=data["expiration_date"],
        order_boc=data["order_boc"],
        signers=data["signers"],
        last_transaction_lt=data["last_transaction_lt"],
        code_hash=data["code_hash"],
        data_hash=data["data_hash"],
    )

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
    async def get_multisig_order(self, address: str) -> MultisigOrder | None:
        pass

    @abc.abstractmethod
    async def get_interfaces(self, address: str) -> dict[str, dict]:
        pass


class KvRocksRepository(InterfaceRepository):
    def __init__(self, interfaces: Dict[str, Dict[str, Dict]] = None):
        self.interfaces = interfaces or {}

    async def _get_prefetched_interface(self, address: str, interface_type: str) -> Optional[dict]:
        address = kvrocks.normalize_address_id(address)
        if address is None:
            return None
        return self.interfaces.get(address, {}).get(interface_type)

    async def _get_payload_interface(self, table: str, address: str, converter: Callable[[dict], dict]) -> Optional[dict]:
        address = kvrocks.normalize_address_id(address)
        if address is None or not kvrocks.is_enabled():
            return None
        payloads = await kvrocks.get_payloads(table, [address], normalize=kvrocks.normalize_address_id)
        payload = payloads.get(address)
        if payload is None or _is_destroyed(payload):
            return None
        return converter(payload)

    async def get_jetton_wallet(self, address: str) -> JettonWallet | None:
        data = await self._get_prefetched_interface(address, "JettonWallet")
        if data is None:
            data = await self._get_payload_interface("jetton_wallets", address, _jetton_wallet_interface)
        return _jetton_wallet_from_interface(data) if data is not None else None

    async def get_nft_item(self, address: str) -> NFTItem | None:
        data = await self._get_prefetched_interface(address, "NftItem")
        if data is None:
            data = await self._get_payload_interface("nft_items", address, _nft_item_interface)
        return _nft_item_from_interface(data) if data is not None else None

    async def get_nft_sale(self, address: str) -> NftSale | None:
        data = await self._get_prefetched_interface(address, "NftSale")
        if data is None:
            data = await self._get_payload_interface("getgems_nft_sales", address, _nft_sale_interface)
        return _nft_sale_from_interface(data) if data is not None else None

    async def get_nft_auction(self, address: str) -> NftAuction | None:
        data = await self._get_prefetched_interface(address, "NftAuction")
        if data is None:
            data = await self._get_payload_interface("getgems_nft_auctions", address, _nft_auction_interface)
        return _nft_auction_from_interface(data) if data is not None else None

    async def get_multisig_order(self, address: str) -> MultisigOrder | None:
        data = await self._get_prefetched_interface(address, "MultisigOrder")
        if data is None:
            data = await self._get_payload_interface("multisig_orders", address, _multisig_order_interface)
        return _multisig_order_from_interface(data) if data is not None else None

    async def get_interfaces(self, address: str) -> dict[str, dict]:
        address = kvrocks.normalize_address_id(address)
        if address is None:
            return {}
        result = {**self.interfaces.get(address, {})}
        if kvrocks.is_enabled():
            missing_result = await gather_interfaces_from_kvrocks({address})
            result.update(missing_result.get(address, {}))
            for item_address, item_interfaces in missing_result.items():
                self.interfaces[item_address] = {**self.interfaces.get(item_address, {}), **item_interfaces}
        ctx = _context()
        if address in ctx.dedust_pools.get():
            result['dedust_pool'] = ctx.dedust_pools.get()[address]
        return result

    async def get_dedust_pool(self, address: str) -> DedustPool | None:
        address = kvrocks.normalize_address_id(address)
        if address is None:
            return None
        ctx = _context()
        if address in ctx.dedust_pools.get():
            return DedustPool(address=address, assets=ctx.dedust_pools.get()[address]['assets'])
        if not kvrocks.is_enabled():
            return None
        payloads = await kvrocks.get_payloads("dex_pools", [address], normalize=kvrocks.normalize_address_id)
        payload = payloads.get(address)
        if payload is None or _is_destroyed(payload):
            return None
        return _dedust_pool_from_payload(payload)

    async def get_extra_data(self, address: str, request: str) -> Any:
        address = kvrocks.normalize_address_id(address)
        if address is None:
            return None
        if address in self.interfaces and request in self.interfaces[address]:
            return self.interfaces[address][request]
        if request != "data_boc" or not kvrocks.is_enabled():
            return None
        payloads = await kvrocks.get_payloads("latest_account_states", [address], normalize=kvrocks.normalize_address_id)
        payload = payloads.get(address)
        if payload is None:
            return None
        result = {
            "account": address,
            "request": request,
            "data_boc": payload.get("data_boc"),
        }
        self.interfaces.setdefault(address, {})[request] = result
        return result


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
                code_hash=interface_data.get("code_hash"),
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
                marketplace_fee_address=interface_data.get("marketplace_fee_address"),
                marketplace_fee=interface_data.get("marketplace_fee"),
                royalty_address=interface_data.get("royalty_address"),
                royalty_amount=interface_data.get("royalty_amount"),
                code_hash=interface_data.get("code_hash"),
            )
        return None

    async def get_interfaces(self, address: str) -> dict[str, dict]:
        result = {}
        raw_data = self.connection.get(RedisInterfaceRepository.prefix + address)
        if raw_data is None:
            return {}

        interfaces = msgpack.unpackb(raw_data, raw=False)
        ctx = _context()
        if address in ctx.dedust_pools.get():
            interfaces['dedust_pool'] = ctx.dedust_pools.get()[address]
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
                last_bid=interface_data["last_bid"],
                mp_addr=interface_data['mp_addr'],
                mp_fee_addr=interface_data.get("mp_fee_addr"),
                mp_fee_factor=interface_data.get("mp_fee_factor"),
                mp_fee_base=interface_data.get("mp_fee_base"),
                royalty_fee_addr=interface_data.get("royalty_fee_addr"),
                royalty_fee_base=interface_data.get("royalty_fee_base"),
                max_bid=interface_data.get("max_bid"),
                min_bid=interface_data.get("min_bid"),
                code_hash=interface_data.get("code_hash"),
            )
        return None

    async def get_multisig_order(self, address: str) -> MultisigOrder | None:
        raw_data = self.connection.get(RedisInterfaceRepository.prefix + address)
        if raw_data is None:
            return None

        interfaces = msgpack.unpackb(raw_data, raw=False)
        interface_data = next((data for (interface_type, data) in interfaces.items() if interface_type == "MultisigOrder"),
                              None)
        if interface_data is not None:
            return MultisigOrder(
                address=interface_data["address"],
                multisig_address=interface_data["multisig_address"],
                order_seqno=interface_data["order_seqno"],
                threshold=interface_data["threshold"],
                sent_for_execution=interface_data["sent_for_execution"],
                approvals_mask=interface_data["approvals_mask"],
                approvals_num=interface_data["approvals_num"],
                expiration_date=interface_data["expiration_date"],
                order_boc=interface_data["order_boc"],
                signers=interface_data["signers"],
                last_transaction_lt=interface_data["last_transaction_lt"],
                code_hash=interface_data["code_hash"],
                data_hash=interface_data["data_hash"],
            )
        return None

    async def get_interfaces(self, address: str) -> dict[str, dict]:
        result = {}
        raw_data = self.connection.get(RedisInterfaceRepository.prefix + address)
        if raw_data is None:
            return {}

        interfaces = msgpack.unpackb(raw_data, raw=False)
        ctx = _context()
        if address in ctx.dedust_pools.get():
            interfaces['dedust_pool'] = ctx.dedust_pools.get()[address]
        return interfaces

    async def get_dedust_pool(self, address: str) -> DedustPool | None:
        ctx = _context()
        if address in ctx.dedust_pools.get():
            return DedustPool(address=address, assets=ctx.dedust_pools.get()[address]['assets'])
        return None

    async def get_extra_data(self, address: str, request: str) -> Any:
        raw_data = self.connection.get(RedisInterfaceRepository.prefix + address)
        if raw_data is None:
            return None
        interfaces = msgpack.unpackb(raw_data, raw=False)
        if request in interfaces:
            return interfaces[request]
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
                    code_hash=interface_data.get('code_hash'),
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
                    marketplace_fee_address=interface_data.get('marketplace_fee_address'),
                    marketplace_fee=interface_data.get('marketplace_fee'),
                    royalty_address=interface_data.get('royalty_address'),
                    royalty_amount=interface_data.get('royalty_amount'),
                    code_hash=interface_data.get('code_hash'),
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
                    mp_addr=interface_data['mp_addr'],
                    mp_fee_addr=interface_data.get('mp_fee_addr'),
                    mp_fee_factor=interface_data.get('mp_fee_factor'),
                    mp_fee_base=interface_data.get('mp_fee_base'),
                    royalty_fee_addr=interface_data.get('royalty_fee_addr'),
                    royalty_fee_base=interface_data.get('royalty_fee_base'),
                    max_bid=interface_data.get('max_bid'),
                    min_bid=interface_data.get('min_bid'),
                    code_hash=interface_data.get('code_hash'),
                )
        return None

    async def get_multisig_order(self, address: str) -> MultisigOrder | None:
        # Emulated transactions don't have multisig orders
        return None

    async def get_interfaces(self, address: str) -> dict[str, dict]:
        return {}

    async def get_dedust_pool(self, address: str) -> DedustPool | None:
        ctx = _context()
        if address in ctx.dedust_pools.get():
            return DedustPool(address=address, assets=ctx.dedust_pools.get()[address]['assets'])
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
                    code_hash=data.get("code_hash"),
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
                    marketplace_fee_address=data.get("marketplace_fee_address"),
                    marketplace_fee=data.get("marketplace_fee"),
                    royalty_address=data.get("royalty_address"),
                    royalty_amount=data.get("royalty_amount"),
                    code_hash=data.get("code_hash"),
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
                    mp_addr=data["mp_addr"],
                    mp_fee_addr=data.get("mp_fee_addr"),
                    mp_fee_factor=data.get("mp_fee_factor"),
                    mp_fee_base=data.get("mp_fee_base"),
                    royalty_fee_addr=data.get("royalty_fee_addr"),
                    royalty_fee_base=data.get("royalty_fee_base"),
                    max_bid=data.get("max_bid"),
                    min_bid=data.get("min_bid"),
                    code_hash=data.get("code_hash"),
                )

        return result

    async def get_multisig_order(self, address: str) -> Optional[MultisigOrder]:
        result = await self.emulated_repository.get_multisig_order(address)

        if result is None and address in self.db_interfaces:
            if "MultisigOrder" in self.db_interfaces[address]:
                data = self.db_interfaces[address]["MultisigOrder"]
                result = MultisigOrder(
                    address=data["address"],
                    multisig_address=data["multisig_address"],
                    order_seqno=data["order_seqno"],
                    threshold=data["threshold"],
                    sent_for_execution=data["sent_for_execution"],
                    approvals_mask=data["approvals_mask"],
                    approvals_num=data["approvals_num"],
                    expiration_date=data["expiration_date"],
                    order_boc=data["order_boc"],
                    signers=data["signers"],
                    last_transaction_lt=data["last_transaction_lt"],
                    code_hash=data["code_hash"],
                    data_hash=data["data_hash"],
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

    async def get_extra_data(self, address: str, request: str) -> Any:
        if address in self.db_interfaces and request in self.db_interfaces[address]:
            data = self.db_interfaces[address][request]
            return data
        return None


async def _process_kvrocks_extra_requests(
        accounts: set[str],
        extra_requests: set['ExtraAccountRequest'] = None,
) -> list[dict]:
    extra = []
    queue = deque(extra_requests or [])
    processed = set()

    queue_iterations = 0
    while queue:
        queue_iterations += 1
        if queue_iterations > 10:
            break

        batch = defaultdict(list)
        batch_processors = {}
        while queue:
            extra_request = queue.popleft()
            account = kvrocks.normalize_address_id(extra_request.account)
            if account is None:
                continue
            request_key = (account, extra_request.request_type)
            if request_key in processed:
                continue
            batch[extra_request.request_type].append(account)
            batch_processors[request_key] = extra_request.callback
            processed.add(request_key)

        for req_type, accounts_batch in batch.items():
            if req_type != "data_boc":
                continue
            payloads = await kvrocks.get_payloads(
                "latest_account_states",
                accounts_batch,
                normalize=kvrocks.normalize_address_id,
            )
            for account in accounts_batch:
                payload = payloads.get(account)
                if payload is None:
                    continue
                processor = batch_processors.get((account, req_type))
                n_extra = {
                    "account": account,
                    "request": req_type,
                    "data_boc": payload.get("data_boc"),
                }
                if processor:
                    new_requests, new_accounts = processor(n_extra)
                    queue.extend(new_requests)
                    accounts.update(
                        normalized for normalized in
                        (kvrocks.normalize_address_id(item) for item in new_accounts)
                        if normalized is not None
                    )
                extra.append(n_extra)
    return extra


async def gather_interfaces_from_kvrocks(
        accounts: set[str],
        extra_requests: set['ExtraAccountRequest'] = None,
) -> dict[str, dict[str, dict]]:
    result = defaultdict(dict)
    normalized_accounts = {
        normalized for normalized in
        (kvrocks.normalize_address_id(account) for account in accounts)
        if normalized is not None
    }
    for account in normalized_accounts:
        result[account] = {}

    if not kvrocks.is_enabled():
        return result

    extra = await _process_kvrocks_extra_requests(normalized_accounts, extra_requests)
    account_list = list(normalized_accounts)
    for i in range(0, len(account_list), 5000):
        batch = account_list[i:i + 5000]
        auctions = await kvrocks.get_payloads("getgems_nft_auctions", batch, normalize=kvrocks.normalize_address_id)

        nft_batch = batch.copy()
        for payload in auctions.values():
            if _is_destroyed(payload):
                continue
            nft_addr = kvrocks.normalize_address_id(payload.get("nft_addr"))
            if nft_addr is not None:
                nft_batch.append(nft_addr)

        wallets = await kvrocks.get_payloads("jetton_wallets", batch, normalize=kvrocks.normalize_address_id)
        nft_items = await kvrocks.get_payloads("nft_items", nft_batch, normalize=kvrocks.normalize_address_id)
        sales = await kvrocks.get_payloads("getgems_nft_sales", batch, normalize=kvrocks.normalize_address_id)
        orders = await kvrocks.get_payloads("multisig_orders", batch, normalize=kvrocks.normalize_address_id)
        states = await kvrocks.get_payloads("latest_account_states", batch, normalize=kvrocks.normalize_address_id)

        for payload in wallets.values():
            if _is_destroyed(payload):
                continue
            address = kvrocks.normalize_address_id(payload.get("address"))
            if address is not None:
                result[address]["JettonWallet"] = _jetton_wallet_interface(payload)
        for payload in nft_items.values():
            if _is_destroyed(payload):
                continue
            address = kvrocks.normalize_address_id(payload.get("address"))
            if address is not None:
                result[address]["NftItem"] = _nft_item_interface(payload)
        for payload in sales.values():
            if _is_destroyed(payload):
                continue
            address = kvrocks.normalize_address_id(payload.get("address"))
            if address is not None:
                result[address]["NftSale"] = _nft_sale_interface(payload)
        for payload in auctions.values():
            if _is_destroyed(payload):
                continue
            address = kvrocks.normalize_address_id(payload.get("address"))
            if address is not None:
                result[address]["NftAuction"] = _nft_auction_interface(payload)
        for payload in orders.values():
            if _is_destroyed(payload):
                continue
            address = kvrocks.normalize_address_id(payload.get("address"))
            if address is not None:
                result[address]["MultisigOrder"] = _multisig_order_interface(payload)
        for account, payload in states.items():
            if payload.get("code_hash") == NOMINATOR_POOL_CODE_HASH:
                result[account]["NominatorPool"] = {"address": account}

    for item in extra:
        result[item["account"]][item["request"]] = item
    return result

async def _gather_data_from_db(
        accounts: set[str],
        session: AsyncSession,
        extra_requests: list[ExtraAccountRequest] = None,
) -> tuple[list[JettonWallet], list[NFTItem], list[NftSale], list[NftAuction], list[LatestAccountState], list[MultisigOrder], list[dict]]:
    jetton_wallets = []
    nft_items = []
    nft_sales = []
    getgems_auctions = []
    nominator_pools = []
    multisig_orders = []
    extra = []
    queue = deque(extra_requests or [])
    processed = set()

    queue_iterations = 0
    while queue:
        queue_iterations += 1
        if queue_iterations > 10:
            break
        batch = defaultdict(list)
        batch_processors = {}

        while queue:
            extra_request = queue.popleft()

            if (extra_request.account, extra_request.request_type) not in processed:
                batch[extra_request.request_type].append(extra_request.account)
                batch_processors[(extra_request.account, extra_request.request_type)] = extra_request.callback
                processed.add((extra_request.account, extra_request.request_type))

        # Execute batch
        for req_type, accounts_batch in batch.items():
            if req_type == 'data_boc':
                results = await session.execute(
                    select(LatestAccountState.account, LatestAccountState.data_boc)
                    .filter(LatestAccountState.account.in_(accounts_batch))
                )

                for account, data_boc in results:
                    processor = batch_processors.get((account, req_type))
                    n_extra = {
                        'account': account,
                        'request': req_type,
                        'data_boc': data_boc
                    }
                    if processor:
                        new_requests, new_accounts = processor(n_extra)
                        queue.extend(new_requests)
                        accounts.update(new_accounts)
                    extra.append(n_extra)


    account_list = list(accounts)
    for i in range(0, len(account_list), 5000):
        batch = account_list[i:i + 5000]
        auctions = await session.execute(
            select(NftAuction)
            .filter(NftAuction.address.in_(batch))
            .filter(NftAuction.destroyed.is_(False))
        )
        auctions = list(auctions.scalars().all())
        nft_batch = batch.copy()
        for auction in auctions:
            nft_batch.append(auction.nft_addr)
        wallets = await session.execute(
            select(JettonWallet)
            .filter(JettonWallet.address.in_(batch))
            .filter(JettonWallet.destroyed.is_(False))
        )
        nft = await session.execute(
            select(NFTItem)
            .filter(NFTItem.address.in_(nft_batch))
            .filter(NFTItem.destroyed.is_(False))
        )
        sales = await session.execute(
            select(NftSale)
            .filter(NftSale.address.in_(batch))
            .filter(NftSale.destroyed.is_(False))
        )
        orders = await session.execute(
            select(MultisigOrder)
            .filter(MultisigOrder.address.in_(batch))
            .filter(MultisigOrder.destroyed.is_(False))
        )
        pools = await session.execute(select(LatestAccountState)
                                                .filter(LatestAccountState.account.in_(batch))
                                                .filter(LatestAccountState.code_hash == NOMINATOR_POOL_CODE_HASH))
        jetton_wallets += list(wallets.scalars().all())
        nft_items += list(nft.scalars().all())
        nft_sales += list(sales.scalars().all())
        getgems_auctions += auctions
        multisig_orders += list(orders.scalars().all())
        nominator_pools += list(pools.scalars().all())


    return jetton_wallets, nft_items, nft_sales, getgems_auctions, nominator_pools, multisig_orders, extra

@dataclass(frozen=True, eq=True)
class ExtraAccountRequest:
    account: str
    request_type: str = 'data_boc'
    # Callback returns (new_requests, accounts_for_interfaces)
    callback: Optional[Callable[[dict], tuple[list['ExtraAccountRequest'], set[str]]]] = None


async def gather_interfaces(accounts: set[str], session: AsyncSession, extra_requests: set[ExtraAccountRequest] = None)\
        -> dict[str, dict[str, dict]]:
    result = defaultdict(dict)
    (jetton_wallets, nft_items, nft_sales, nft_auctions, nominator_pools, multisig_orders, extra) = await _gather_data_from_db(
        accounts, session, extra_requests=extra_requests)
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
            "code_hash": item.code_hash,
        }
    for sale in nft_sales:
        result[sale.address]["NftSale"] = {
            "address": sale.address,
            "is_complete": sale.is_complete,
            "marketplace_address": sale.marketplace_address,
            "nft_address": sale.nft_address,
            "nft_owner_address": sale.nft_owner_address,
            "full_price": float(sale.full_price),
            "marketplace_fee_address": sale.marketplace_fee_address,
            "marketplace_fee": float(sale.marketplace_fee) if sale.marketplace_fee is not None else None,
            "royalty_address": sale.royalty_address,
            "royalty_amount": float(sale.royalty_amount) if sale.royalty_amount is not None else None,
            "code_hash": sale.code_hash,
        }
    for auction in nft_auctions:
        result[auction.address]["NftAuction"] = {
            "address": auction.address,
            "mp_addr": auction.mp_addr,
            "nft_addr": auction.nft_addr,
            "nft_owner": auction.nft_owner,
            "last_bid": float(auction.last_bid),
            "mp_fee_addr": auction.mp_fee_addr,
            "mp_fee_factor": float(auction.mp_fee_factor) if auction.mp_fee_factor is not None else None,
            "mp_fee_base": float(auction.mp_fee_base) if auction.mp_fee_base is not None else None,
            "royalty_fee_addr": auction.royalty_fee_addr,
            "royalty_fee_base": float(auction.royalty_fee_base) if auction.royalty_fee_base is not None else None,
            "max_bid": float(auction.max_bid) if auction.max_bid is not None else None,
            "min_bid": float(auction.min_bid) if auction.min_bid is not None else None,
            "code_hash": auction.code_hash,
        }
    for account_state in nominator_pools:
        result[account_state.account]["NominatorPool"] = {
            "address": account_state.account,
        }
    for order in multisig_orders:
        result[order.address]["MultisigOrder"] = {
            "address": order.address,
            "multisig_address": order.multisig_address,
            "order_seqno": float(order.order_seqno),
            "threshold": float(order.threshold),
            "sent_for_execution": order.sent_for_execution,
            "approvals_mask": float(order.approvals_mask),
            "approvals_num": float(order.approvals_num),
            "expiration_date": float(order.expiration_date),
            "order_boc": order.order_boc,
            "signers": order.signers,
            "last_transaction_lt": float(order.last_transaction_lt),
            "code_hash": order.code_hash,
            "data_hash": order.data_hash,
        }
    for wallet in extra:
        result[wallet['account']][wallet['request']] = wallet
    return result
