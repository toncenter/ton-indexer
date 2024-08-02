import logging
from typing import List, Dict, Optional
from datetime import datetime
from functools import wraps

from fastapi import FastAPI, APIRouter, Depends, Query, Path, Body, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import DBAPIError

from indexer.core.utils import (
    address_to_raw, 
    hash_to_b64,
    hex_to_int
)

from indexer.api.api_v1 import schemas
from indexer.core.database import SessionMaker
from indexer.core.settings import Settings
from indexer.core.database import (
    MASTERCHAIN_INDEX,
    MASTERCHAIN_SHARD,
)
from indexer.core import crud, exceptions

logger = logging.getLogger(__name__)


settings = Settings()
router = APIRouter()


# Dependency
async def get_db():
    async with SessionMaker() as db:
        yield db


INT64_MIN = -2**63
INT64_MAX = 2**63 - 1
UINT64_MAX = 2**64 - 1
INT32_MIN = -2**31
INT32_MAX = 2**31 - 1
UINT32_MAX = 2**32 - 1


def catch_cancelled(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except DBAPIError as ee:
            logger.warning(f'Timeout for request {func.__name__} args: {args}, kwargs: {kwargs} exception: {ee}')
            raise exceptions.TimeoutError()
        except exceptions.DataNotFound as ee:
            raise ee
        except Exception as ee:
            logger.warning(f'Error for request {func.__name__} args: {args}, kwargs: {kwargs} exception: {ee}')
            raise ee
    return wrapper

@router.get("/masterchainInfo", response_model=schemas.MasterchainInfo,
            responses=schemas.ResponseNotFoundMasterchainInfo.get_response_json(), tags=['blocks'])
@catch_cancelled
async def get_masterchain_info(db: AsyncSession = Depends(get_db)):
    """
    Returns first and last blocks information, can be used as a start point for blockchain search process\n
    """
    last = await db.run_sync(crud.get_blocks,
                             workchain=MASTERCHAIN_INDEX,
                             shard=MASTERCHAIN_SHARD,
                             sort='desc',
                             limit=1)
    first = await db.run_sync(crud.get_blocks,
                             workchain=MASTERCHAIN_INDEX,
                             shard=MASTERCHAIN_SHARD,
                             sort='asc',
                             limit=1)
    if len(first) == 0 or len(last) == 0:
        raise exceptions.BlockNotFound(workchain=MASTERCHAIN_INDEX,
                                       shard=MASTERCHAIN_SHARD,
                                       seqno='latest')
    return schemas.MasterchainInfo(first=schemas.Block.from_orm(first[0]), last=schemas.Block.from_orm(last[0]))


def validate_block_idx(workchain, shard, seqno):
    if workchain is None:
        if shard is not None or seqno is not None:
            raise ValueError('workchain id required')
    if shard is None:
        if seqno is not None:
            raise ValueError('shard id required')
    return True


@router.get("/blocks", response_model=schemas.BlockList, tags=['blocks'])
@catch_cancelled
async def get_blocks(
    workchain: Optional[int] = Query(None, description='Block workchain.'),
    shard: Optional[str] = Query(None, description='Block shard id. Must be sent with *workchain*. Example: `8000000000000000`'),
    seqno: Optional[int] = Query(None, description='Block seqno. Must be sent with *workchain* and *shard*.'),
    start_utime: Optional[int] = Query(None, description='Query blocks with generation UTC timestamp **after** given timestamp.'),
    end_utime: Optional[int] = Query(None, description='Query blocks with generation UTC timestamp **before** given timestamp'),
    start_lt: Optional[int] = Query(None, description='Query blocks with `lt >= start_lt`'),
    end_lt: Optional[int] = Query(None, description='Query blocks with `lt <= end_lt`'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    sort: str = Query('desc', description='Sort results by UTC timestamp.', enum=['asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Returns blocks by specified filters.
    """
    validate_block_idx(workchain, shard, seqno)
    shard = hex_to_int(shard)
    res = await db.run_sync(crud.get_blocks,
                            workchain=workchain,
                            shard=shard,
                            seqno=seqno,
                            from_gen_utime=start_utime,
                            to_gen_utime=end_utime,
                            from_start_lt=start_lt,
                            to_start_lt=end_lt,
                            limit=limit,
                            offset=offset,
                            sort=sort,)
    return schemas.BlockList.from_orm(res)


@router.get("/masterchainBlockShardState",
    response_model=schemas.BlockList,
    responses=schemas.ResponseNotFoundMasterchainBlockShardState.get_response_json(),
    tags=['blocks'])
@catch_cancelled
async def get_shards_by_masterchain_block(seqno: int = Query(..., description='Masterchain block seqno'),
    db: AsyncSession = Depends(get_db)):
    """
    Returns one masterchain block (with seqno equal to argument) and some number of shard blocks (with masterchain_block_ref.seqno equal to argument)
    """
    result = await db.run_sync(crud.get_shard_state, mc_seqno=seqno)
    if len(result) == 0:
        raise exceptions.BlockNotFound(workchain=MASTERCHAIN_INDEX,
                                       shard=MASTERCHAIN_SHARD,
                                       seqno=seqno)
    return schemas.BlockList.from_orm(result)


# # NOTE: This method is not reliable in case account was destroyed, it will return it's state before destruction. So for now we comment it out.
# @router.get("/account", response_model=schemas.LatestAccountState)
# @catch_cancelled
# async def get_account(
#     address: str = Query(..., description='Account address. Can be sent in raw or user-friendly format.'),
#     db: AsyncSession = Depends(get_db)):
#     """
#     Returns latest account state by specified address. 
#     """
#     address = address_to_raw(address)
#     res = await db.run_sync(crud.get_latest_account_state_by_address,
#                             address=address)
#     if res is None:
#         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f'Account {address} not found')
#     return schemas.LatestAccountState.from_orm(res)


@router.get("/addressBook", response_model=Dict[str, schemas.AddressBookEntry], tags=['blocks'])
@catch_cancelled
async def get_address_book(
    address: List[str] = Query(..., description="List of addresses in any form. Max list size: 1024"),
    db: AsyncSession = Depends(get_db)):
    """
    Generates and returns a user-friendly address book for a given contract address list.
    """
    if len(address) > 1024:
        raise ValueError(f'Maximum number of addresses is 1024. Got {len(address)}')
    address_list_raw = [address_to_raw(addr) for addr in address]
    result = await db.run_sync(crud.get_latest_account_state,
                               address_list=address_list_raw)
    return {addr: schemas.AddressBookEntry(user_friendly=schemas.address_type_friendly(addr_raw, item))
            for addr, addr_raw, item in zip(address, address_list_raw, result)}

@router.get("/masterchainBlockShards", response_model=schemas.BlockList, tags=['blocks'])
@catch_cancelled
async def get_masterchain_block_shards(
    seqno: int = Query(..., description='Masterchain block seqno'),
    db: AsyncSession = Depends(get_db)):
    """
    Returns all workchain blocks, that appeared after previous masterchain block.

    **Note:** this method is not equivalent with [/api/v2/shards](https://toncenter.com/api/v2/#/blocks/get_shards_shards_get).
    """
    result = await db.run_sync(crud.get_masterchain_block_shards,
                               seqno=seqno)
    return schemas.BlockList.from_orm(result)


@router.get("/transactions", response_model=schemas.TransactionList, tags=['transactions'])
@catch_cancelled
async def get_transactions(
    workchain: Optional[int] = Query(None, description='Block workchain.'),
    shard: Optional[str] = Query(None, description='Block shard id. Must be sent with *workchain*. Example: `8000000000000000`'),
    seqno: Optional[int] = Query(None, description='Block seqno. Must be sent with *workchain* and *shard*. Must be sent in hex form.'),
    # account: Optional[str] = Query(None, description='The account address to get transactions. Can be sent in hex, base64 or base64url form.'),
    account: List[str] = Query(None, description='List of account addresses to get transactions. Can be sent in hex, base64 or base64url form.'),
    exclude_account: List[str] = Query(None, description='Exclude transactions on specified account addresses'),
    hash: Optional[str] = Query(None, description='Transaction hash. Acceptable in hex, base64 and base64url forms.'),
    lt: Optional[int] = Query(None, description='Transaction lt.'),
    start_utime: Optional[int] = Query(None, description='Query transactions with generation UTC timestamp **after** given timestamp.'),
    end_utime: Optional[int] = Query(None, description='Query transactions with generation UTC timestamp **before** given timestamp'),
    start_lt: Optional[int] = Query(None, description='Query transactions with `lt >= start_lt`'),
    end_lt: Optional[int] = Query(None, description='Query transactions with `lt <= end_lt`'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Returns transactions by specified filters.
    """

    if (account is not None) + (exclude_account is not None) > 1:
        raise Exception("Only one of account and exclude_account should be specified.")
    
    include_account = None
    if account is not None and len(account) == 1:
        account = address_to_raw(account[0])
    elif account is not None and len(account) > 1:
        include_account = [address_to_raw(incl_acc) for incl_acc in account]
        account = None
    else:
        account = None

    exclude_account = [address_to_raw(excl_acc) for excl_acc in exclude_account] if exclude_account else None

    validate_block_idx(workchain, shard, seqno)
    shard = hex_to_int(shard)
    hash = hash_to_b64(hash)

    res = await db.run_sync(crud.get_transactions,
                            workchain=workchain,
                            shard=shard,
                            seqno=seqno,
                            account=account,
                            include_account_list=include_account,
                            exclude_account_list=exclude_account,
                            hash=hash,
                            lt=lt,
                            start_lt=start_lt,
                            end_lt=end_lt,
                            start_utime=start_utime,
                            end_utime=end_utime,
                            limit=limit,
                            offset=offset,
                            sort=sort)
    return schemas.TransactionList.from_orm(res)


@router.get("/transactionsByMasterchainBlock", response_model=schemas.TransactionList, tags=['transactions'])
@catch_cancelled
async def get_transactions_by_masterchain_block(
    seqno: int = Query(..., description='Masterchain block seqno'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Returns transactions from masterchain block and from all shards.
    """
    mc_block = await db.run_sync(crud.get_blocks,
                                 workchain=MASTERCHAIN_INDEX,
                                 shard=MASTERCHAIN_SHARD,
                                 seqno=seqno,
                                 limit=1)
    if not len(mc_block):
        raise exceptions.BlockNotFound(workchain=MASTERCHAIN_INDEX,
                                       shard=MASTERCHAIN_SHARD,
                                       seqno=seqno)
    txs = await db.run_sync(crud.get_transactions_by_masterchain_seqno_v2,
                            masterchain_seqno=seqno,
                            limit=limit,
                            offset=offset,
                            sort=sort)
    return schemas.TransactionList.from_orm(txs)


@router.get('/transactionsByMessage', response_model=schemas.TransactionList, tags=['transactions'])
@catch_cancelled
async def get_transactions_by_message(
    direction: Optional[str] = Query(..., description='Message direction.', enum=['in', 'out']),
    msg_hash: Optional[str] = Query(..., description='Message hash. Acceptable in hex, base64 and base64url forms.'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    db: AsyncSession = Depends(get_db)):
    """
    Returns transactions whose inbound/outbound message has the specified hash. This endpoint returns list of Transaction objects
    since collisions of message hashes can occur.
    """
    msg_hash = hash_to_b64(msg_hash)
    db_transactions = await db.run_sync(crud.get_transactions_by_message,
                                        direction=direction,
                                        hash=msg_hash,
                                        include_block=False,
                                        limit=limit,
                                        offset=offset,
                                        sort='desc')
    return schemas.TransactionList.from_orm(db_transactions)


@router.get('/adjacentTransactions', response_model=schemas.TransactionList, tags=['transactions'])
@catch_cancelled
async def get_adjacent_transactions(
    hash: str = Query(..., description='Transaction hash. Acceptable in hex, base64 and base64url forms.'),
    direction: Optional[str] = Query('both', description='Direction transactions by lt.', enum=['in', 'out', 'both']),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Returns parent and/or children for specified transaction.
    """
    hash = hash_to_b64(hash)
    if direction == 'both':
        direction = None
    res = await db.run_sync(crud.get_adjacent_transactions,
                            hash=hash,
                            direction=direction,
                            limit=limit,
                            offset=offset,
                            sort=sort,
                            include_msg_body=True,
                            include_account_state=True)
    return schemas.TransactionList.from_orm(res)


@router.get('/traces', response_model=List[Optional[schemas.TransactionTrace]], include_in_schema=False)
@catch_cancelled
async def get_traces(
    tx_hash: List[str] = Query(None, description='List of transaction hashes'),
    trace_id: List[str] = Query(None, description='List of trace ids', include_in_schema=True),
    db: AsyncSession = Depends(get_db)):
    """
    Get batch of trace graph by ids
    """
    if tx_hash is not None and trace_id is not None or tx_hash is None and trace_id is None:
        raise ValueError('Exact one parameter should be used')
    
    if trace_id is not None:
        trace_id = [int(x) for x in trace_id]
    if tx_hash is not None:
        tx_hash = [hash_to_b64(h) for h in tx_hash]
    result = await db.run_sync(crud.get_traces, event_ids=trace_id, tx_hashes=tx_hash)
    return [schemas.TransactionTrace.from_orm(trace) if trace is not None else None for trace in result]


@router.get('/transactionTrace', response_model=schemas.TransactionTrace, include_in_schema=False)
@catch_cancelled
async def get_transaction_trace(
    hash: str = Query(..., description='Transaction hash. Acceptable in hex, base64 and base64url forms.'),
    sort: str = Query('asc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Get trace graph for specified transaction. NOTE: This method will be deprecated. Please, avoid using this method in production.
    """
    hash = hash_to_b64(hash)
    res = await db.run_sync(crud.get_transaction_trace,
                            hash=hash,
                            sort=sort)
    return schemas.TransactionTrace.from_orm(res)


@router.get('/messages', response_model=schemas.MessageList, tags=['transactions'])
@catch_cancelled
async def get_messages(
    hash: str = Query(None, description='Message hash. Acceptable in hex, base64 and base64url forms.'),    
    source: Optional[str] = Query(None, description='The source account address. Can be sent in hex, base64 or base64url form. Use value `null` to get external messages.'),
    destination: Optional[str] = Query(None, description='The destination account address. Can be sent in hex, base64 or base64url form. Use value `null` to get log messages.'),
    body_hash: Optional[str] = Query(None, description='Message body hash. Acceptable in hex, base64 and base64url forms.'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    db: AsyncSession = Depends(get_db)):
    """
    Returns messages by specified filters.
    """
    hash = hash_to_b64(hash)
    source = address_to_raw(source)
    destination = address_to_raw(destination)
    body_hash = hash_to_b64(body_hash)
    res = await db.run_sync(crud.get_messages,
                            hash=hash,
                            source=source,
                            destination=destination,
                            body_hash=body_hash,
                            limit=limit,
                            offset=offset)
    return schemas.MessageList.from_orm(res)


@router.get('/nft/collections', response_model=schemas.NFTCollectionList, tags=['nft'])
@catch_cancelled
async def get_nft_collections(
    collection_address: Optional[str] = Query(None, description='NFT collection address. Must be sent in hex, base64 or base64url forms.'),
    owner_address: Optional[str] = Query(None, description='Address of NFT collection owner. Must be sent in hex, base64 or base64url forms.'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    db: AsyncSession = Depends(get_db)):
    """
    Returns NFT collections.
    """
    collection_address = address_to_raw(collection_address)
    owner_address = address_to_raw(owner_address)
    res = await db.run_sync(crud.get_nft_collections,
                            address=collection_address,
                            owner_address=owner_address,
                            limit=limit,
                            offset=offset)
    return schemas.NFTCollectionList.from_orm(res)


@router.get('/nft/items', response_model=schemas.NFTItemList, tags=['nft'])
@catch_cancelled
async def get_nft_items(
    address: Optional[str] = Query(None, description='NFT address. Must be sent in hex, base64 or base64url forms.'),
    owner_address: Optional[str] = Query(None, description='Address of NFT owner. Must be sent in hex, base64 or base64url forms.'),
    collection_address: Optional[str] = Query(None, description='NFT collection address. Must be sent in hex, base64 or base64url forms.'),
    index: Optional[str] = Query(None, description='NFT Item index. Use it together with collection address.'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    db: AsyncSession = Depends(get_db)):
    """
    Returns NFT items.
    """
    if index is not None and collection_address is None:
        raise RuntimeError('Use index together with collection_address')
    address = address_to_raw(address)
    owner_address = address_to_raw(owner_address)
    collection_address = address_to_raw(collection_address)
    res = await db.run_sync(crud.get_nft_items,
                            address=address,
                            index=index,
                            owner_address=owner_address,
                            collection_address=collection_address,
                            limit=limit,
                            offset=offset)
    return schemas.NFTItemList.from_orm(res)


@router.get('/nft/transfers', response_model=schemas.NFTTransferList, tags=['nft'])
@catch_cancelled
async def get_nft_transfers(
    address: Optional[str] = Query(None, description='Address of NFT owner. Must be sent in hex, base64 or base64url forms.'),
    item_address: Optional[str] = Query(None, description='NFT item address. Must be sent in hex, base64 or base64url forms.'),
    collection_address: Optional[str] = Query(None, description='NFT collection address. Must be sent in hex, base64 or base64url forms.'),
    direction: Optional[str] = Query('both', description='Direction transactions by lt.', enum=['in', 'out', 'both']),
    start_utime: Optional[int] = Query(None, description='Query transactions with generation UTC timestamp **after** given timestamp.'),
    end_utime: Optional[int] = Query(None, description='Query transactions with generation UTC timestamp **before** given timestamp'),
    start_lt: Optional[int] = Query(None, description='Query transactions with `lt >= start_lt`'),
    end_lt: Optional[int] = Query(None, description='Query transactions with `lt <= end_lt`'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Returns NFT transfers by specified filters.
    """
    address = address_to_raw(address)
    item_address = address_to_raw(item_address)
    collection_address = address_to_raw(collection_address)
    if direction == 'both':
        direction = None
    res = await db.run_sync(crud.get_nft_transfers,
                            nft_item=item_address,
                            nft_collection=collection_address,
                            account=address,
                            direction=direction,
                            start_utime=start_utime,
                            end_utime=end_utime,
                            start_lt=start_lt,
                            end_lt=end_lt,
                            limit=limit * 2,
                            offset=offset,
                            sort=sort)
    result = []
    count = 0
    for row in res:
        if not row.transaction.description['aborted']:
            result.append(row)
            count += 1
        if count == limit:
            break
    return schemas.NFTTransferList.from_orm(result)


@router.get('/jetton/masters', response_model=schemas.JettonMasterList, tags=['jettons'])
@catch_cancelled
async def get_jetton_masters(
    address: str = Query(None, description="Jetton Master address. Must be sent in hex, base64 or base64url forms."),
    admin_address: str = Query(None, description="Address of Jetton Master's admin. Must be sent in hex, base64 or base64url forms."),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    db: AsyncSession = Depends(get_db)):
    """
    Returns Jetton masters by specified filters.
    """
    address = address_to_raw(address)
    admin_address = address_to_raw(admin_address)
    res = await db.run_sync(crud.get_jetton_masters,
                            address=address,
                            admin_address=admin_address,
                            limit=limit,
                            offset=offset)
    return schemas.JettonMasterList.from_orm(res)


@router.get('/jetton/wallets', response_model=schemas.JettonWalletList, tags=['jettons'])
@catch_cancelled
async def get_jetton_wallets(
    address: str = Query(None, description="Jetton wallet address. Must be sent in hex, base64 or base64url forms."),
    owner_address: str = Query(None, description="Address of Jetton wallet's owner. Must be sent in hex, base64 or base64url forms."),
    jetton_address: str = Query(None, description="Jetton Master. Must be sent in hex, base64 or base64url forms."),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    db: AsyncSession = Depends(get_db)):
    """
    Returns Jetton wallets by specified filters.
    """
    address = address_to_raw(address)
    owner_address = address_to_raw(owner_address)
    jetton_address = address_to_raw(jetton_address)
    res = await db.run_sync(crud.get_jetton_wallets,
                            address=address,
                            owner_address=owner_address,
                            jetton_address=jetton_address,
                            limit=limit,
                            offset=offset)
    return schemas.JettonWalletList.from_orm(res)


@router.get('/jetton/transfers', response_model=schemas.JettonTransferList, tags=['jettons'])
@catch_cancelled
async def get_jetton_transfers(
    address: Optional[str] = Query(None, description='Account address. Must be sent in hex, base64 or base64url forms.'),
    jetton_wallet: Optional[str] = Query(None, description='Jetton wallet address. Must be sent in hex, base64 or base64url forms.'),
    jetton_master: Optional[str] = Query(None, description='Jetton master address. Must be sent in hex, base64 or base64url forms.'),
    direction: Optional[str] = Query('both', description='Direction transactions by lt.', enum=['in', 'out', 'both']),
    start_utime: Optional[int] = Query(None, description='Query transactions with generation UTC timestamp **after** given timestamp.'),
    end_utime: Optional[int] = Query(None, description='Query transactions with generation UTC timestamp **before** given timestamp'),
    start_lt: Optional[int] = Query(None, description='Query transactions with `lt >= start_lt`'),
    end_lt: Optional[int] = Query(None, description='Query transactions with `lt <= end_lt`'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Returns Jetton transfers by specified filters.
    """
    address = address_to_raw(address)
    jetton_wallet = address_to_raw(jetton_wallet)
    jetton_master = address_to_raw(jetton_master)
    if direction == 'both':
        direction = None
    res = await db.run_sync(crud.get_jetton_transfers,
                            jetton_account=jetton_wallet,
                            jetton_master=jetton_master,
                            account=address,
                            direction=direction,
                            start_utime=start_utime,
                            end_utime=end_utime,
                            start_lt=start_lt,
                            end_lt=end_lt,
                            limit=limit * 2,
                            offset=offset,
                            sort=sort)
    result = []
    count = 0
    for row in res:
        if not row.transaction.description['aborted']:
            result.append(row)
            count += 1
        if count == limit:
            break
    return schemas.JettonTransferList.from_orm(result)


@router.get('/jetton/burns', response_model=schemas.JettonBurnList, tags=['jettons'])
@catch_cancelled
async def get_jetton_burns(
    address: Optional[str] = Query(None, description='Account address. Must be sent in hex, base64 or base64url forms.'),
    jetton_wallet: Optional[str] = Query(None, description='Jetton wallet address. Must be sent in hex, base64 or base64url forms.'),
    jetton_master: Optional[str] = Query(None, description='Jetton master address. Must be sent in hex, base64 or base64url forms.'),
    start_utime: Optional[int] = Query(None, description='Query transactions with generation UTC timestamp **after** given timestamp.'),
    end_utime: Optional[int] = Query(None, description='Query transactions with generation UTC timestamp **before** given timestamp'),
    start_lt: Optional[int] = Query(None, description='Query transactions with `lt >= start_lt`'),
    end_lt: Optional[int] = Query(None, description='Query transactions with `lt <= end_lt`'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Returns Jetton burns by specified filters.
    """
    address = address_to_raw(address)
    jetton_wallet = address_to_raw(jetton_wallet)
    jetton_master = address_to_raw(jetton_master)
    res = await db.run_sync(crud.get_jetton_burns,
                            jetton_account=jetton_wallet,
                            jetton_master=jetton_master,
                            account=address,
                            start_utime=start_utime,
                            end_utime=end_utime,
                            start_lt=start_lt,
                            end_lt=end_lt,
                            limit=limit * 2,
                            offset=offset,
                            sort=sort)
    result = []
    count = 0
    for row in res:
        if not row.transaction.description['aborted']:
            result.append(row)
            count += 1
        if count == limit:
            break
    return schemas.JettonBurnList.from_orm(result)


@router.get('/topAccountsByBalance', response_model=List[schemas.AccountBalance], include_in_schema=False)
@catch_cancelled
async def get_top_accounts_by_balance(
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    db: AsyncSession = Depends(get_db)):
    """
    Get list of accounts sorted descending by balance.
    """
    res = await db.run_sync(crud.get_top_accounts_by_balance,
                            limit=limit,
                            offset=offset)
    return [schemas.AccountBalance.from_orm(x) for x in res]


if settings.ton_http_api_endpoint:
    from indexer.api.api_v1.ton_http_api_proxy import router as proxy_router
    router.include_router(proxy_router)
