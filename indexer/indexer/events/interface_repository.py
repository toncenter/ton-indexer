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

from indexer.core.database import JettonWallet, NFTItem, NftSale, NftAuction, LatestAccountState, MultisigOrder
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
    async def get_multisig_order(self, address: str) -> MultisigOrder | None:
        pass

    @abc.abstractmethod
    async def get_interfaces(self, address: str) -> dict[str, dict]:
        pass


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
                last_bid=interface_data["last_bid"],
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
        if address in context.dedust_pools.get():
            interfaces['dedust_pool'] = context.dedust_pools.get()[address]
        return interfaces

    async def get_dedust_pool(self, address: str) -> DedustPool | None:
        if address in context.dedust_pools.get():
            return DedustPool(address=address, assets=context.dedust_pools.get()[address]['assets'])
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

    async def get_multisig_order(self, address: str) -> MultisigOrder | None:
        # Emulated transactions don't have multisig orders
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
        auctions = await session.execute(select(NftAuction).filter(NftAuction.address.in_(batch)))
        auctions = list(auctions.scalars().all())
        nft_batch = batch.copy()
        for auction in auctions:
            nft_batch.append(auction.nft_addr)
        wallets = await session.execute(select(JettonWallet).filter(JettonWallet.address.in_(batch)))
        nft = await session.execute(select(NFTItem).filter(NFTItem.address.in_(nft_batch)))
        sales = await session.execute(select(NftSale).filter(NftSale.address.in_(batch)))
        orders = await session.execute(select(MultisigOrder).filter(MultisigOrder.address.in_(batch)))
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
            "nft_owner": auction.nft_owner,
            "last_bid": float(auction.last_bid)
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
