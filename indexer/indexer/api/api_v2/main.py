import logging
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, APIRouter, Depends, Query, Path, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
from starlette.exceptions import HTTPException as StarletteHTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from pytonlib.utils.address import detect_address
from pytonlib.utils.common import hex_to_b64str

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


# common tools
# TODO: Move to pytonlib
def hash_to_b64(b64_or_hex_hash):
    """
    Detect encoding of transactions hash and if necessary convert it to Base64.
    """
    if len(b64_or_hex_hash) == 44:
        # Hash is base64
        return b64_or_hex_hash
    if len(b64_or_hex_hash) == 64:
        # Hash is hex
        return hex_to_b64str(b64_or_hex_hash)
    raise ValueError(f"Invalid hash: '{b64_or_hex_hash}'")


def address_to_raw(address):
    try:
        raw_address = detect_address(address)["raw_form"]
    except Exception:
        raise HTTPException(status_code=416, detail="Invalid address")
    return raw_address


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

@router.get("/masterchain/block/first_indexed", response_model=schemas.Block, response_model_exclude_none=True)
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


@router.get("/masterchain/block/{seqno}/transactions", response_model=List[schemas.Transaction])
async def get_masterchain_block_transactions(
    seqno: int = Path(..., description='Masterchain block seqno'),
    include_shard_blocks: bool = Query(True, description="Include transactions from shard blocks"),
    include_msg_body: bool = Query(False, description="Include message bodies in transactions"),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=0, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Returns transactions from masterchain block and optionally from all shards.
    """
    if include_shard_blocks:
        txs = await db.run_sync(crud.get_transactions_by_masterchain_seqno,
                                masterchain_seqno=seqno,
                                include_msg_body=include_msg_body,
                                limit=limit,
                                offset=offset,
                                sort=sort)
    else:
        txs = await db.run_sync(crud.get_transactions,
                                workchain=MASTERCHAIN_INDEX,
                                shard=MASTERCHAIN_SHARD,
                                seqno=seqno,
                                include_msg_body=include_msg_body,
                                limit=limit,
                                offset=offset,
                                sort=sort)
    return [schemas.Transaction.from_orm(tx, include_msg_body=include_msg_body) for tx in txs]


@router.get("/workchain/{workchain}/transactions", response_model=List[schemas.Transaction])
async def get_workchain_block_transactions(
    workchain: int = Path(..., example=-1, description='Workchain id'),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    include_msg_body: bool = Query(False, description="Include message bodies in transactions"),
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
                            include_msg_body=include_msg_body,
                            limit=limit,
                            offset=offset,
                            sort=sort)
    return [schemas.Transaction.from_orm(tx, include_msg_body=include_msg_body) for tx in txs]


@router.get("/workchain/{workchain}/shard/{shard}/block/{seqno}", response_model=schemas.Block, response_model_exclude_none=True)
async def get_workchain_block(
    workchain: int = Path(..., example=-1, description='Workchain id'),
    shard: str = Path(..., example='-9223372036854775808', description='Shard id'),
    seqno: int = Path(..., description='Block seqno'),
    db: AsyncSession = Depends(get_db)):
    """
    Returns block with specified workchain, shard and seqno.
    """
    shard = int(shard)
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
    shard: str = Path(..., example='-9223372036854775808', description='Shard id'),
    seqno: int = Path(..., description='Block seqno'),
    include_msg_body: bool = Query(False, description="Include message bodies in transactions"),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    """
    Returns block with specified workchain, shard and seqno.
    """
    shard = int(shard)
    txs = await db.run_sync(crud.get_transactions,
                            workchain=workchain,
                            shard=shard,
                            seqno=seqno,
                            include_msg_body=include_msg_body,
                            limit=limit,
                            offset=offset,
                            sort=sort)
    return [schemas.Transaction.from_orm(tx, include_msg_body=include_msg_body) for tx in txs]


@router.get("/transaction/{hash}", response_model=schemas.Transaction)
async def get_transaction(
    hash: str = Path(..., description='Transaction hash'),
    include_msg_body: bool = Query(False, description="Include message bodies in transactions"),
    include_block: bool = Query(False, description="Include transaction block"),
    include_account_state: bool = Query(False, description="Include account states"),
    db: AsyncSession = Depends(get_db)):
    hash = hash_to_b64(hash)
    txs = await db.run_sync(crud.get_transactions,
                            hash=hash,
                            include_msg_body=include_msg_body,
                            include_block=include_block,
                            include_account_state=include_account_state,
                            limit=2)
    if len(txs) < 1:
        raise exceptions.TransactionNotFound(tx_hash=hash) 
    elif len(txs) > 1:
        raise exceptions.MultipleTransactionsFound(tx_hash=hash) 
    return schemas.Transaction.from_orm(txs[0], include_msg_body=include_msg_body)


@router.get("/transaction/{hash}/ancestor", response_model=schemas.Transaction)
async def get_transaction_ancestor(
    hash: str = Path(..., description='Transaction hash'),
    include_msg_body: bool = Query(False, description="Include message bodies in transactions"),
    db: AsyncSession = Depends(get_db)):
    hash = hash_to_b64(hash)
    txs = await db.run_sync(crud.get_adjacent_transactions,
                            hash=hash,
                            direction='in',
                            include_msg_body=include_msg_body,
                            limit=2)
    if len(txs) < 1:
        raise exceptions.TransactionNotFound(adjacent_tx_hash=hash, direction='in') 
    elif len(txs) > 1:
        raise exceptions.MultipleTransactionsFound(adjacent_tx_hash=hash, direction='in') 
    return schemas.Transaction.from_orm(txs[0], include_msg_body=include_msg_body)


@router.get("/transaction/{hash}/descendants", response_model=List[schemas.Transaction])
async def get_transaction_descendants(
    hash: str = Path(..., description='Transaction hash'),
    include_msg_body: bool = Query(False, description="Include message bodies in transactions"),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    hash = hash_to_b64(hash)
    txs = await db.run_sync(crud.get_adjacent_transactions,
                            hash=hash,
                            direction='out',
                            include_msg_body=include_msg_body,
                            limit=limit,
                            offset=offset,
                            sort=sort)
    if len(txs) < 1:
        raise exceptions.TransactionNotFound(adjacent_tx_hash=hash, direction='out') 
    return [schemas.Transaction.from_orm(tx, include_msg_body=include_msg_body) for tx in txs]


# workchain
# @router.get("/workchain/{worchain}/block/latest", response_model=schemas.Block, response_model_exclude_none=True)
# async def get_workchain_last_blocks(
#     workchain: int = Path(..., description='Workchain index'),
#     limit: int = Query(16, description='Limit number of blocks', ge=1, le=16),
#     offset: int = Query(0, 'Skip first <offset> block'),
#     db: AsyncSession = Depends(get_db)):
#     """
#     Returns last wockchain block.
#     """
#     result = await db.run_sync(crud.get_blocks,
#                                workchain=workchain,
#                                sort_seqno='desc',
#                                limit=limit,
#                                offset=offset)
#     if len(result) < 1:
#         raise exceptions.BlockNotFound(workchain=MASTERCHAIN_INDEX,
#                                        shard=MASTERCHAIN_SHARD,
#                                        seqno='latest')
#     return schemas.Block.from_orm(result[0])




# @router.get("/block")
# async def get_block(
#     workchain: int = Query(default=MASTERCHAIN_INDEX),
#     shard: str = Query(default="-9223372036854775808"),
#     seqno: int = Query(default=None),
#     session: Session = Depends(get_db)
# ):
#     stmt = select(Block)
#     if seqno is not None:
#         stmt = stmt.filter(Block.seqno == seqno)
#     stmt = stmt.filter(Block.workchain == workchain) \
#                .filter(Block.shard == int(shard)) \
#                .order_by(Block.seqno.desc())
#     result = await session.execute(stmt)
#     result = result.scalars().first()
#     return result


# @router.get("/account/{account_address}/")
# async def get_account_state(
#     account_address: str = Path(),
#     session: Session = Depends(get_db)
# ):
#     return


# @router.get("/account/{account_address}/transactions")
# async def get_account_transactions(
#     account_address: str = Path(),
#     after_lt: int = Query(default=None),
#     before_lt: int = Query(default=None),
#     limit: int = Query(default=128, ge=1, le=256),
#     include_messages: bool = Query(default=False),
#     session: Session = Depends(get_db)
# ):
#     stmt = select(Transaction).filter(Transaction.account == account_address)
#     stmt = stmt.order_by(Transaction.lt.desc()) \
#                .limit(limit)
#     if after_lt is not None:
#         stmt = stmt.filter(Transaction.lt >= after_lt)
#     if before_lt is not None:
#         stmt = stmt.filter(Transaction.lt < before_lt)
#     if include_messages:
#         stmt = stmt.join(Transaction.messages)
#     result = await session.execute(stmt)
#     result = result.scalars().all()
#     return result[::-1]
