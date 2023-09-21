import logging
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, APIRouter, Depends, Query, Path, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
from starlette.exceptions import HTTPException as StarletteHTTPException
from sqlalchemy.ext.asyncio import AsyncSession

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


# masterchain

@router.get("/masterchainInfo", response_model=schemas.MasterchainInfo, response_model_exclude_none=True)
async def get_masterchain_info(db: AsyncSession = Depends(get_db)):
    last = await db.run_sync(crud.get_blocks,
                             workchain=MASTERCHAIN_INDEX,
                             shard=MASTERCHAIN_SHARD,
                             sort_seqno='desc',
                             limit=1)
    first = await db.run_sync(crud.get_blocks,
                             workchain=MASTERCHAIN_INDEX,
                             shard=MASTERCHAIN_SHARD,
                             sort_seqno='asc',
                             limit=1)
    if len(first) == 0 or len(last) == 0:
        raise exceptions.BlockNotFound(workchain=MASTERCHAIN_INDEX,
                                       shard=MASTERCHAIN_SHARD,
                                       seqno='latest')
    return schemas.MasterchainInfo(first=schemas.Block.from_orm(first[0]), last=schemas.Block.from_orm(last[0]))


# @router.get("/masterchain/block/latest", response_model=schemas.Block, response_model_exclude_none=True)
# async def get_masterchain_last_block(db: AsyncSession = Depends(get_db)):
#     """
#     Returns last known masterchain block.
#     """
#     result = await db.run_sync(crud.get_blocks,
#                                workchain=MASTERCHAIN_INDEX,
#                                shard=MASTERCHAIN_SHARD,
#                                sort_seqno='desc',
#                                limit=1)
#     if len(result) < 1:
#         raise exceptions.BlockNotFound(workchain=MASTERCHAIN_INDEX,
#                                        shard=MASTERCHAIN_SHARD,
#                                        seqno='latest')
#     return schemas.Block.from_orm(result[0])

# @router.get("/masterchain/block/first_indexed", response_model=schemas.Block, response_model_exclude_none=True)
# async def get_masterchain_first_indexed_block(db: AsyncSession = Depends(get_db)):
#     """
#     Returns first indexed masterchain block.
#     """
#     result = await db.run_sync(crud.get_blocks,
#                                workchain=MASTERCHAIN_INDEX,
#                                shard=MASTERCHAIN_SHARD,
#                                sort_seqno='asc',
#                                limit=1)
#     if len(result) < 1:
#         raise exceptions.BlockNotFound(workchain=MASTERCHAIN_INDEX,
#                                        shard=MASTERCHAIN_SHARD,
#                                        seqno='first_indexed')
#     return schemas.Block.from_orm(result[0])


# JsonRPC
def validate_block_idx(workchain, shard, seqno):
    if workchain is None:
        if shard is not None or seqno is not None:
            raise ValueError('workchain id required')
    if shard is None:
        if seqno is not None:
            raise ValueError('shard id required')
    return True


@router.get("/blocks", response_model=List[schemas.Block])
async def get_blocks(
    workchain: Optional[int] = Query(None, description='Block workchain.'),
    shard: Optional[str] = Query(None, description='Block shard id. Must be sent with *workchain*.'),
    seqno: Optional[int] = Query(None, description='Block block seqno. Must be sent with *workchain* and *shard*.'),
    start_utime: Optional[int] = Query(None, description='Query blocks with generation UTC timestamp **after** given timestamp.'),
    end_utime: Optional[int] = Query(None, description='Query blocks with generation UTC timestamp **before** given timestamp'),
    start_lt: Optional[int] = Query(None, description='Query blocks with `lt >= start_lt`'),
    end_lt: Optional[int] = Query(None, description='Query blocks with `lt <= end_lt`'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    sort: str = Query('desc', description='Sort results by UTC timestamp.', enum=['asc', 'desc']),
    db: AsyncSession = Depends(get_db)
):
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
                            sort_gen_utime=sort,)
    return [schemas.Block.from_orm(x) for x in res]


@router.get("/masterchainBlockShards", response_model=List[schemas.Block])
async def get_masterchain_block_shards(
    seqno: int = Query(..., description='Masterchain block seqno'),
    include_mc_block: bool = Query(False, description='Include masterchain block'),
    db: AsyncSession = Depends(get_db)):
    """
    Returns all worchain blocks, that appeared after previous masterchain block.

    **Note:** this method is not equivalent with [/api/v2/shards](https://toncenter.com/api/v2/#/blocks/get_shards_shards_get).
    """
    result = await db.run_sync(crud.get_masterchain_block_shards,
                               seqno=seqno,
                               include_mc_block=include_mc_block)
    return [schemas.Block.from_orm(r) for r in result]


@router.get("/transactions", response_model=List[schemas.Transaction])
async def get_transactions(
    workchain: Optional[int] = Query(None, description='Block workchain.'),
    shard: Optional[str] = Query(None, description='Block shard id. Must be sent with *workchain*.'),
    seqno: Optional[int] = Query(None, description='Block block seqno. Must be sent with *workchain* and *shard*. Must be sent in hex form.'),
    account: Optional[str] = Query(None, description='The account address to get transactions. Can be sent in hex, base64 or base64url form.'),
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
    Get transactions by specified filters.
    """
    validate_block_idx(workchain, shard, seqno)
    shard = hex_to_int(shard)
    account = address_to_raw(account)
    hash = hash_to_b64(hash)
    res = await db.run_sync(crud.get_transactions,
                            workchain=workchain,
                            shard=shard,
                            seqno=seqno,
                            account=account,
                            hash=hash,
                            lt=lt,
                            start_lt=start_lt,
                            end_lt=end_lt,
                            start_utime=start_utime,
                            end_utime=end_utime,
                            limit=limit,
                            offset=offset,
                            sort=sort)
    return [schemas.Transaction.from_orm(r) for r in res]


@router.get("/transactionsByMasterchainBlock", response_model=List[schemas.Transaction])
async def get_transactions_by_masterchain_block(
    seqno: int = Query(..., description='Masterchain block seqno'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Returns transactions from masterchain block and from all shards.
    """
    txs = await db.run_sync(crud.get_transactions_by_masterchain_seqno,
                            masterchain_seqno=seqno,
                            limit=limit,
                            offset=offset,
                            sort=sort)
    return [schemas.Transaction.from_orm(tx) for tx in txs]

@router.get('/transactionsByMessage', response_model=List[schemas.Transaction])
async def get_transactions_by_message(
    direction: Optional[str] = Query(..., description='Message direction.', enum=['in', 'out']),
    msg_hash: Optional[str] = Query(..., description='Message hash. Acceptable in hex, base64 and base64url forms.'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    db: AsyncSession = Depends(get_db)):
    """
    Get transactions whose inbound/outbound message has the specified hash. This endpoint returns list of Transaction objects
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
    return [schemas.Transaction.from_orm(tx) for tx in db_transactions]

@router.get('/adjacentTransactions', response_model=List[schemas.Transaction])
async def get_adjacent_transactions(
    hash: str = Query(..., description='Transaction hash. Acceptable in hex, base64 and base64url forms.'),
    direction: Optional[str] = Query('both', description='Direction transactions by lt.', enum=['in', 'out', 'both']),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Get parent and/or children for specified transaction.
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
    return [schemas.Transaction.from_orm(tx) for tx in res]


@router.get('/transactionTrace', response_model=schemas.TransactionTrace)
async def get_transaction_trace(
    hash: str = Query(..., description='Transaction hash. Acceptable in hex, base64 and base64url forms.'),
    sort: str = Query('asc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Get trace graph for specified transaction.
    """
    hash = hash_to_b64(hash)
    res = await db.run_sync(crud.get_transaction_trace,
                            hash=hash,
                            sort=sort)
    return schemas.TransactionTrace.from_orm(res)


@router.get('/messages', response_model=List[schemas.Message])
async def get_messages(
    hash: str = Query(None, description='Message hash. Acceptable in hex, base64 and base64url forms.'),    
    source: Optional[str] = Query(None, description='The source account address. Can be sent in hex, base64 or base64url form.'),
    destination: Optional[str] = Query(None, description='The destination account address. Can be sent in hex, base64 or base64url form.'),
    body_hash: Optional[str] = Query(None, description='Message body hash. Acceptable in hex, base64 and base64url forms.'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    db: AsyncSession = Depends(get_db)):
    """
    Get messages by specified filters.
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
    return [schemas.Message.from_orm(x) for x in res]


@router.get('/nft/collections', response_model=List[schemas.NFTCollection])
async def get_nft_collections(
    collection_address: Optional[str] = Query(None, description='NFT collection address. Must be sent in hex, base64 and base64url forms.'),
    owner_address: Optional[str] = Query(None, description='Address of NFT collection owner. Must be sent in hex, base64 and base64url forms.'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    db: AsyncSession = Depends(get_db)):
    """
    Get NFT collections.
    """
    collection_address = address_to_raw(collection_address)
    owner_address = address_to_raw(owner_address)
    res = await db.run_sync(crud.get_nft_collections,
                            address=collection_address,
                            owner_address=owner_address,
                            limit=limit,
                            offset=offset)
    return [schemas.NFTCollection.from_orm(x) for x in res]


@router.get('/nft/items', response_model=List[schemas.NFTItem])
async def get_nft_items(
    address: Optional[str] = Query(None, description='NFT address. Must be sent in hex, base64 and base64url forms.'),
    owner_address: Optional[str] = Query(None, description='Address of NFT owner. Must be sent in hex, base64 and base64url forms.'),
    collection_address: Optional[str] = Query(None, description='NFT collection address. Must be sent in hex, base64 and base64url forms.'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    db: AsyncSession = Depends(get_db)):
    """
    Get NFT items.
    """
    address = address_to_raw(address)
    owner_address = address_to_raw(owner_address)
    collection_address = address_to_raw(collection_address)
    res = await db.run_sync(crud.get_nft_items,
                            address=address,
                            owner_address=owner_address,
                            collection_address=collection_address,
                            limit=limit,
                            offset=offset)
    return [schemas.NFTItem.from_orm(x) for x in res]


@router.get('/nft/transfers', response_model=List[schemas.NFTTransfer])
async def get_nft_transfers(
    address: Optional[str] = Query(None, description='Address of NFT owner. Must be sent in hex, base64 and base64url forms.'),
    item_address: Optional[str] = Query(None, description='NFT item address. Must be sent in hex, base64 and base64url forms.'),
    collection_address: Optional[str] = Query(None, description='NFT collection address. Must be sent in hex, base64 and base64url forms.'),
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
    Get NFT transfers by specified filters.
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
                            limit=limit,
                            offset=offset,
                            sort=sort)
    return [schemas.NFTTransfer.from_orm(x) for x in res]


@router.get('/jetton/masters', response_model=List[schemas.JettonMaster])
async def get_jetton_masters(
    address: str = Query(None, description="Jetton Master address. Must be sent in hex, base64 and base64url forms."),
    admin_address: str = Query(None, description="Address of Jetton Master's admin. Must be sent in hex, base64 and base64url forms."),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    db: AsyncSession = Depends(get_db)):
    """
    Get Jetton masters by specified filters.
    """
    address = address_to_raw(address)
    admin_address = address_to_raw(admin_address)
    res = await db.run_sync(crud.get_jetton_masters,
                            address=address,
                            admin_address=admin_address,
                            limit=limit,
                            offset=offset)
    return [schemas.JettonMaster.from_orm(x) for x in res]


@router.get('/jetton/wallets', response_model=List[schemas.JettonWallet])
async def get_jetton_masters(
    address: str = Query(None, description="Jetton wallet address. Must be sent in hex, base64 and base64url forms."),
    owner_address: str = Query(None, description="Address of Jetton wallet's owner. Must be sent in hex, base64 and base64url forms."),
    jetton_address: str = Query(None, description="Jetton Master. Must be sent in hex, base64 and base64url forms."),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    db: AsyncSession = Depends(get_db)):
    """
    Get Jetton masters by specified filters.
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
    return [schemas.JettonWallet.from_orm(x) for x in res]


@router.get('/jetton/transfers', response_model=List[schemas.JettonTransfer])
async def get_jetton_transfers(
    address: Optional[str] = Query(None, description='Account address. Must be sent in hex, base64 and base64url forms.'),
    jetton_wallet: Optional[str] = Query(None, description='Jetton wallet address. Must be sent in hex, base64 and base64url forms.'),
    jetton_master: Optional[str] = Query(None, description='Jetton master address. Must be sent in hex, base64 and base64url forms.'),
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
    Get Jetton transfers by specified filters.
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
                            limit=limit,
                            offset=offset,
                            sort=sort)
    return [schemas.JettonTransfer.from_orm(x) for x in res]


@router.get('/jetton/burns', response_model=List[schemas.JettonBurn])
async def get_jetton_burns(
    address: Optional[str] = Query(None, description='Account address. Must be sent in hex, base64 and base64url forms.'),
    jetton_wallet: Optional[str] = Query(None, description='Jetton wallet address. Must be sent in hex, base64 and base64url forms.'),
    jetton_master: Optional[str] = Query(None, description='Jetton master address. Must be sent in hex, base64 and base64url forms.'),
    start_utime: Optional[int] = Query(None, description='Query transactions with generation UTC timestamp **after** given timestamp.'),
    end_utime: Optional[int] = Query(None, description='Query transactions with generation UTC timestamp **before** given timestamp'),
    start_lt: Optional[int] = Query(None, description='Query transactions with `lt >= start_lt`'),
    end_lt: Optional[int] = Query(None, description='Query transactions with `lt <= end_lt`'),
    limit: int = Query(128, description='Limit number of queried rows. Use with *offset* to batch read.', ge=1, le=256),
    offset: int = Query(0, description='Skip first N rows. Use with *limit* to batch read.', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Get Jetton burns by specified filters.
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
                            limit=limit,
                            offset=offset,
                            sort=sort)
    return [schemas.JettonBurn.from_orm(x) for x in res]
