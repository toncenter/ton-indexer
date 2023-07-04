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

from indexer.api.api_v2 import schemas
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
@router.get("/masterchain/block/latest", response_model=schemas.Block, response_model_exclude_none=True)
async def get_masterchain_last_block(db: AsyncSession = Depends(get_db)):
    """
    Returns last known masterchain block.
    """
    result = await db.run_sync(crud.get_blocks,
                               workchain=MASTERCHAIN_INDEX,
                               shard=MASTERCHAIN_SHARD,
                               sort_seqno='desc',
                               limit=1)
    if len(result) < 1:
        raise exceptions.BlockNotFound(workchain=MASTERCHAIN_INDEX,
                                       shard=MASTERCHAIN_SHARD,
                                       seqno='latest')
    return schemas.Block.from_orm(result[0])

@router.get("/masterchain/block/firstIndexed", response_model=schemas.Block, response_model_exclude_none=True)
async def get_masterchain_first_indexed_block(db: AsyncSession = Depends(get_db)):
    """
    Returns first indexed masterchain block.
    """
    result = await db.run_sync(crud.get_blocks,
                               workchain=MASTERCHAIN_INDEX,
                               shard=MASTERCHAIN_SHARD,
                               sort_seqno='asc',
                               limit=1)
    if len(result) < 1:
        raise exceptions.BlockNotFound(workchain=MASTERCHAIN_INDEX,
                                       shard=MASTERCHAIN_SHARD,
                                       seqno='first_indexed')
    return schemas.Block.from_orm(result[0])


@router.get("/masterchain/block/{seqno}", response_model=schemas.Block, response_model_exclude_none=True)
async def get_masterchain_block(
    seqno: int = Path(..., description='Masterchain block seqno'),
    db: AsyncSession = Depends(get_db)):
    """
    Returns masterchain block with specified seqno.
    """
    result = await db.run_sync(crud.get_blocks,
                               workchain=MASTERCHAIN_INDEX,
                               shard=MASTERCHAIN_SHARD,
                               seqno=seqno,
                               limit=1)
    if len(result) < 1:
        raise exceptions.BlockNotFound(workchain=MASTERCHAIN_INDEX,
                                       shard=MASTERCHAIN_SHARD,
                                       seqno=seqno)
    return schemas.Block.from_orm(result[0])


# BUG: not the same as toncenter.com/api/v2/shards
@router.get("/masterchain/block/{seqno}/shards", response_model=List[schemas.Block], response_model_exclude_none=True)
async def get_masterchain_block_shards(
    seqno: int = Path(..., description='Masterchain block seqno'),
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


@router.get("/masterchain/block/{seqno}/allTransactions", response_model=List[schemas.Transaction])
async def get_masterchain_block_transactions(
    seqno: int = Path(..., description='Masterchain block seqno'),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=0, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
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


@router.get("/workchain/{workchain}/transactions", response_model=List[schemas.Transaction])
async def get_workchain_block_transactions(
    workchain: int = Path(..., example=-1, description='Workchain id'),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Returns workchain transactions with generated utime or lt filter.
    """
    txs = await db.run_sync(crud.get_transactions,
                            workchain=workchain,
                            start_utime=start_utime,
                            end_utime=end_utime,
                            start_lt=start_lt,
                            end_lt=end_lt,
                            limit=limit,
                            offset=offset,
                            sort=sort)
    return [schemas.Transaction.from_orm(tx) for tx in txs]


@router.get("/workchain/{workchain}/shard/{shard}/block/{seqno}", response_model=schemas.Block, response_model_exclude_none=True)
async def get_workchain_block(
    workchain: int = Path(..., example=-1, description='Workchain id'),
    shard: str = Path(..., example='8000000000000000', description='Shard id, in hex form'),
    seqno: int = Path(..., description='Block seqno'),
    db: AsyncSession = Depends(get_db)):
    """
    Returns block with specified workchain, shard and seqno.
    """
    shard = hex_to_int(shard)
    result = await db.run_sync(crud.get_blocks,
                               workchain=workchain,
                               shard=shard,
                               seqno=seqno,
                               limit=1)
    if len(result) < 1:
        raise exceptions.BlockNotFound(workchain=workchain,
                                       shard=shard,
                                       seqno=seqno)
    return schemas.Block.from_orm(result[0])


@router.get("/workchain/{workchain}/shard/{shard}/block/{seqno}/transactions", response_model=List[schemas.Transaction])
async def get_workchain_block_transactions(
    workchain: int = Path(..., example=-1, description='Workchain id'),
    shard: str = Path(..., example='8000000000000000', description='Shard id, in hex form'),
    seqno: int = Path(..., description='Block seqno'),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Returns block with specified workchain, shard and seqno.
    """
    shard = hex_to_int(shard)
    txs = await db.run_sync(crud.get_transactions,
                            workchain=workchain,
                            shard=shard,
                            seqno=seqno,
                            limit=limit,
                            offset=offset,
                            sort=sort)
    return [schemas.Transaction.from_orm(tx) for tx in txs]


@router.get("/transaction/{hash}", response_model=schemas.Transaction)
async def get_transaction(
    hash: str = Path(..., description='Transaction hash'),
    db: AsyncSession = Depends(get_db)):
    hash = hash_to_b64(hash)
    txs = await db.run_sync(crud.get_transactions,
                            hash=hash,
                            limit=2)
    if len(txs) < 1:
        raise exceptions.TransactionNotFound(tx_hash=hash) 
    elif len(txs) > 1:
        raise exceptions.MultipleTransactionsFound(tx_hash=hash) 
    return schemas.Transaction.from_orm(txs[0])


@router.get("/transaction/{hash}/ancestor", response_model=schemas.Transaction)
async def get_transaction_ancestor(
    hash: str = Path(..., description='Transaction hash'),
    db: AsyncSession = Depends(get_db)):
    hash = hash_to_b64(hash)
    txs = await db.run_sync(crud.get_adjacent_transactions,
                            hash=hash,
                            direction='in',
                            limit=2)
    if len(txs) < 1:
        raise exceptions.TransactionNotFound(adjacent_tx_hash=hash, direction='in') 
    elif len(txs) > 1:
        raise exceptions.MultipleTransactionsFound(adjacent_tx_hash=hash, direction='in') 
    return schemas.Transaction.from_orm(txs[0])


@router.get("/transaction/{hash}/descendants", response_model=List[schemas.Transaction])
async def get_transaction_descendants(
    hash: str = Path(..., description='Transaction hash'),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    hash = hash_to_b64(hash)
    txs = await db.run_sync(crud.get_adjacent_transactions,
                            hash=hash,
                            direction='out',
                            limit=limit,
                            offset=offset,
                            sort=sort)
    if len(txs) < 1:
        raise exceptions.TransactionNotFound(adjacent_tx_hash=hash, direction='out') 
    return [schemas.Transaction.from_orm(tx) for tx in txs]


# accounts
@router.get('/account/{account}/transactions')
async def get_account_state(
    account: str = Path(..., description="The account address to get transactions. Can be sent in hex or base64url form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    account = address_to_raw(account)
    txs = await db.run_sync(crud.get_transactions,
                            start_utime=start_utime,
                            end_utime=end_utime,
                            start_lt=start_lt,
                            end_lt=end_lt,
                            account=account,
                            limit=limit,
                            offset=offset,
                            sort=sort)
    return [schemas.Transaction.from_orm(tx) for tx in txs]
