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

from indexer.api.api_wordy import schemas
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
                               sort='desc',
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
                               sort='asc',
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


@router.get("/masterchain/block/{seqno}/all_transactions", response_model=List[schemas.Transaction])
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
@router.get('/account/{account}/transactions', response_model=List[schemas.Transaction])
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


@router.get('/account/{account}/nft_collections', response_model=List[schemas.NFTCollection])
async def get_account_nft_collections(
    account: str = Path(..., description="The account address to get NFTs. Can be sent in hex or base64url form."),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    db: AsyncSession = Depends(get_db)):
    account = address_to_raw(account)
    collections = await db.run_sync(crud.get_account_nft_collections,
                                    address=account,
                                    limit=limit,
                                    offset=offset)
    return [schemas.NFTCollection.from_orm(x) for x in collections]


@router.get('/account/{account}/owned_nft_collections', response_model=List[schemas.NFTCollection])
async def get_account_owned_nft_collections(
    account: str = Path(..., description="The account address to get NFTs. Can be sent in hex or base64url form."),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    db: AsyncSession = Depends(get_db)):
    account = address_to_raw(account)
    collections = await db.run_sync(crud.get_nft_collections,
                                    owner_address=account,
                                    limit=limit,
                                    offset=offset)
    return [schemas.NFTCollection.from_orm(x) for x in collections]
    

@router.get('/account/{account}/nfts', response_model=List[schemas.NFTItem])
async def get_account_all_nft_items(
    account: str = Path(..., description="The account address to get NFTs. Can be sent in hex or base64url form."),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    db: AsyncSession = Depends(get_db)):
    account = address_to_raw(account)
    nfts = await db.run_sync(crud.get_nft_items,
                             owner_address=account,
                             limit=limit,
                             offset=offset)
    return [schemas.NFTItem.from_orm(x) for x in nfts]


@router.get('/account/{account}/nft_transfers', response_model=List[schemas.NFTTransfer])
async def get_account_all_nft_transfers(
    account: str = Path(..., description="The account address to get NFT transfers. Can be sent in hex or base64url form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    account = address_to_raw(account)
    nft_txs = await db.run_sync(crud.get_nft_transfers,
                                account=account,
                                direction=None,  # NOTE: both incoming and outgoing
                                start_utime=start_utime,
                                end_utime=end_utime,
                                start_lt=start_lt,
                                end_lt=end_lt,
                                limit=limit,
                                offset=offset,
                                sort=sort)
    return [schemas.NFTTransfer.from_orm(x) for x in nft_txs]


@router.get('/account/{account}/nft_collection/{nft_collection}/nfts', response_model=List[schemas.NFTItem])
async def get_account_nft_collection_items(
    account: str = Path(..., description="The account address to get NFTs. Can be sent in hex or base64url form."),
    nft_collection: str = Path(..., description="The NFT collection address to filter nft transfers. Can be sent in hex or base64url form."),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    db: AsyncSession = Depends(get_db)):
    account = address_to_raw(account)
    nft_collection = address_to_raw(nft_collection)
    nfts = await db.run_sync(crud.get_nft_items,
                             owner_address=account,
                             collection_address=nft_collection,
                             limit=limit,
                             offset=offset)
    return [schemas.NFTItem.from_orm(x) for x in nfts]


@router.get('/account/{account}/nft_collection/{nft_collection}/nft_transfers', response_model=List[schemas.NFTTransfer])
async def get_account_nft_collection_transfers(
    account: str = Path(..., description="The account address to get NFT transfers. Can be sent in hex or base64url form."),
    nft_collection: str = Path(..., description="The NFT collection address to filter nft transfers. Can be sent in hex or base64url form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    account = address_to_raw(account)
    nft_collection = address_to_raw(nft_collection)
    nft_txs = await db.run_sync(crud.get_nft_transfers,
                                account=account,
                                nft_collection=nft_collection,
                                direction=None,  # NOTE: both incoming and outgoing
                                start_utime=start_utime,
                                end_utime=end_utime,
                                start_lt=start_lt,
                                end_lt=end_lt,
                                limit=limit,
                                offset=offset,
                                sort=sort)
    return [schemas.NFTTransfer.from_orm(x) for x in nft_txs]


@router.get('/account/{account}/jettons', response_model=List[schemas.JettonWallet])
async def get_account_jettons_list(
    account: str = Path(..., description="The account address to get Jetton transfers. Can be sent in hex or base64url form."),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    db: AsyncSession = Depends(get_db)):
    account = address_to_raw(account)
    jetton_wallets = await db.run_sync(crud.get_jetton_wallets,
                                       owner_address=account,
                                       limit=limit,
                                       offset=offset)
    return [schemas.JettonWallet.from_orm(x) for x in jetton_wallets]


@router.get('/account/{account}/jetton_transfers', response_model=List[schemas.JettonTransfer])
async def get_account_jetton_transfers(
    account: str = Path(..., description="The account address to get Jetton transfers. Can be sent in hex or base64url form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    account = address_to_raw(account)
    jtt_txs = await db.run_sync(crud.get_jetton_transfers,
                                account=account,
                                direction=None,  # NOTE: both incoming and outgoing
                                start_utime=start_utime,
                                end_utime=end_utime,
                                start_lt=start_lt,
                                end_lt=end_lt,
                                limit=limit,
                                offset=offset,
                                sort=sort)
    return [schemas.JettonTransfer.from_orm(x) for x in jtt_txs]


@router.get('/account/{account}/jetton_burns', response_model=List[schemas.JettonBurn])
async def get_account_jetton_burns(
    account: str = Path(..., description="The account address to get Jetton burns. Can be sent in hex or base64url form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    account = address_to_raw(account)
    jtt_txs = await db.run_sync(crud.get_jetton_burns,
                                account=account,
                                start_utime=start_utime,
                                end_utime=end_utime,
                                start_lt=start_lt,
                                end_lt=end_lt,
                                limit=limit,
                                offset=offset,
                                sort=sort)
    return [schemas.JettonBurn.from_orm(x) for x in jtt_txs]


@router.get('/account/{account}/jetton/{jetton}', response_model=schemas.JettonWallet)
async def get_account_jetton_wallet(
    account: str = Path(..., description="The account address to get Jetton transfers. Can be sent in hex or base64url form."),
    jetton: str = Path(..., description="The Jetton Master address to filter Jetton transfers. Can be sent in hex or base64url form."),
    db: AsyncSession = Depends(get_db)):
    account = address_to_raw(account)
    jetton_master = address_to_raw(jetton)
    jetton_wallets = await db.run_sync(crud.get_jetton_wallets,
                                       owner_address=account,
                                       jetton_address=jetton_master,
                                       limit=2,
                                       offset=0)
    if len(jetton_wallets) < 1:
        raise exceptions.JettonWalletNotFound(owner_address=account,
                                              jetton_master=jetton_master)
    elif len(jetton_wallets) > 1:
        raise exceptions.MultipleDataFound(jetton_owner=account,
                                           jetton_master=jetton_master)
    return schemas.JettonWallet.from_orm(jetton_wallets[0])


@router.get('/account/{account}/jetton/{jetton}/jetton_transfers', response_model=List[schemas.JettonTransfer])
async def get_account_jetton_wallet_transfers(
    account: str = Path(..., description="The account address to get Jetton transfers. Can be sent in hex or base64url form."),
    jetton: str = Path(..., description="The Jetton Master address to filter Jetton transfers. Can be sent in hex or base64url form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    account = address_to_raw(account)
    jetton_master = address_to_raw(jetton)
    jtt_txs = await db.run_sync(crud.get_jetton_transfers,
                                account=account,
                                direction=None,  # NOTE: both incoming and outgoing
                                jetton_master=jetton_master,
                                start_utime=start_utime,
                                end_utime=end_utime,
                                start_lt=start_lt,
                                end_lt=end_lt,
                                limit=limit,
                                offset=offset,
                                sort=sort)
    return [schemas.JettonTransfer.from_orm(x) for x in jtt_txs]


@router.get('/account/{account}/jetton/{jetton}/jetton_burns', response_model=List[schemas.JettonBurn])
async def get_account_jetton_wallet_burns(
    account: str = Path(..., description="The account address to get Jetton burns. Can be sent in hex or base64url form."),
    jetton: str = Path(..., description="The Jetton Master address to filter Jetton burns. Can be sent in hex or base64url form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    account = address_to_raw(account)
    jetton_master = address_to_raw(jetton)
    jtt_txs = await db.run_sync(crud.get_jetton_burns,
                                account=account,
                                jetton_master=jetton_master,
                                start_utime=start_utime,
                                end_utime=end_utime,
                                start_lt=start_lt,
                                end_lt=end_lt,
                                limit=limit,
                                offset=offset,
                                sort=sort)
    return [schemas.JettonBurn.from_orm(x) for x in jtt_txs]


# nfts
@router.get('/nft_collections', response_model=List[schemas.NFTCollection])
async def get_nft_collections_list(
    owner_address: Optional[str] = Query(None, description="Owner address. Can be sent in hex or base64url form."),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    db: AsyncSession = Depends(get_db)):
    owner_address = address_to_raw(owner_address)
    collections = await db.run_sync(crud.get_nft_collections,
                                    owner_address=owner_address,
                                    limit=limit,
                                    offset=offset)
    return [schemas.NFTCollection.from_orm(x) for x in collections]


@router.get('/nft_collection/{address}', response_model=schemas.NFTCollection)
async def get_nft_collection(
    address: str = Path(..., description="NFT collection address. Can be sent in hex or base64url form."),
    db: AsyncSession = Depends(get_db)):
    address = address_to_raw(address)
    collections = await db.run_sync(crud.get_nft_collections,
                                    address=address,
                                    limit=2,
                                    offset=0)
    if len(collections) < 1:
        raise exceptions.NFTCollectionNotFound(address=address)
    if len(collections) > 1:
        raise exceptions.MultipleDataFound(nft_collection_address=address)  # TODO: change exception type
    return schemas.NFTCollection.from_orm(collections[0])


@router.get('/nft_collection/{address}/nfts', response_model=List[schemas.NFTItem])
async def get_nft_collection_items(
    address: str = Path(..., description="NFT collection address. Can be sent in hex or base64url form."),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    db: AsyncSession = Depends(get_db)):
    address = address_to_raw(address)
    nfts = await db.run_sync(crud.get_nft_items,
                             collection_address=address,
                             limit=limit,
                             offset=offset)
    return [schemas.NFTItem.from_orm(x) for x in nfts]


@router.get('/nft_collection/{address}/nft_transfers', response_model=List[schemas.NFTTransfer])
async def get_nft_collection_transfers(
    address: str = Path(..., description="NFT collection address. Can be sent in hex or base64url form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    address = address_to_raw(address)
    nft_txs = await db.run_sync(crud.get_nft_transfers,
                                nft_collection=address,
                                start_utime=start_utime,
                                end_utime=end_utime,
                                start_lt=start_lt,
                                end_lt=end_lt,
                                limit=limit,
                                offset=offset,
                                sort=sort)
    return [schemas.NFTTransfer.from_orm(x) for x in nft_txs]


@router.get('/nft/{address}', response_model=schemas.NFTItem)
async def get_nft_item(
    address: str = Path(..., description="NFT item address. Can be sent in hex or base64url form."),
    db: AsyncSession = Depends(get_db)):
    address = address_to_raw(address)
    nfts = await db.run_sync(crud.get_nft_items,
                            address=address,
                            limit=2,
                            offset=0)
    if len(nfts) < 1:
        raise exceptions.NFTItemNotFound(address=address)
    elif len(nfts) > 1:
        raise exceptions.MultipleDataFound(nft_item_address=address)
    return schemas.NFTItem.from_orm(nfts[0])


@router.get('/nft/{address}/nft_transfers', response_model=List[schemas.NFTTransfer])
async def get_nft_item_transfers(
    address: str = Path(..., description="NFT item address. Can be sent in hex or base64url form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    address = address_to_raw(address)
    nft_txs = await db.run_sync(crud.get_nft_transfers,
                                nft_item=address,
                                start_utime=start_utime,
                                end_utime=end_utime,
                                start_lt=start_lt,
                                end_lt=end_lt,
                                limit=limit,
                                offset=offset,
                                sort=sort)
    return [schemas.NFTTransfer.from_orm(x) for x in nft_txs]


# jettons
@router.get('/jetton_masters', response_model=List[schemas.JettonMaster])
async def get_jetton_master_list(
    admin: str = Query(..., description="The admin account address to get Jetton Masters. Can be sent in hex or base64url form."),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    db: AsyncSession = Depends(get_db)):
    admin = address_to_raw(admin)
    jetton_masters = await db.run_sync(crud.get_jetton_masters,
                                       admin_address=admin,
                                       limit=limit,
                                       offset=offset)
    return [schemas.JettonMaster.from_orm(x) for x in jetton_masters]


@router.get('/jetton_master/{jetton}/jetton_wallets', response_model=List[schemas.JettonWallet])
async def get_jetton_master_wallets(
    jetton: str = Path(..., description="The Jetton master address to get Jetton wallets. Can be sent in hex or base64url form."),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    db: AsyncSession = Depends(get_db)):
    jetton = address_to_raw(jetton)
    jetton_wallets = await db.run_sync(crud.get_jetton_wallets,
                                       jetton_address=jetton,
                                       limit=limit,
                                       offset=offset)
    return [schemas.JettonWallet.from_orm(x) for x in jetton_wallets]


@router.get('/jetton_master/{jetton}/jetton_transfers', response_model=List[schemas.JettonTransfer])
async def get_jetton_master_transfers(
    jetton: str = Path(..., description="The Jetton Master address to filter Jetton transfers. Can be sent in hex or base64url form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    jetton_master = address_to_raw(jetton)
    jtt_txs = await db.run_sync(crud.get_jetton_transfers,
                                jetton_master=jetton_master,
                                start_utime=start_utime,
                                end_utime=end_utime,
                                start_lt=start_lt,
                                end_lt=end_lt,
                                limit=limit,
                                offset=offset,
                                sort=sort)
    return [schemas.JettonTransfer.from_orm(x) for x in jtt_txs]


@router.get('/jetton_master/{jetton}/jetton_burns', response_model=List[schemas.JettonBurn])
async def get_jetton_master_burns(
    jetton: str = Path(..., description="The Jetton Master address to filter Jetton burns. Can be sent in hex or base64url form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    jetton_master = address_to_raw(jetton)
    jtt_txs = await db.run_sync(crud.get_jetton_burns,
                                jetton_master=jetton_master,
                                start_utime=start_utime,
                                end_utime=end_utime,
                                start_lt=start_lt,
                                end_lt=end_lt,
                                limit=limit,
                                offset=offset,
                                sort=sort)
    return [schemas.JettonBurn.from_orm(x) for x in jtt_txs]



@router.get('/jetton_wallet/{jetton}', response_model=schemas.JettonWallet)
async def get_account_jetton_wallet(
    jetton: str = Path(..., description="The Jetton wallet address to filter Jetton transfers. Can be sent in hex or base64url form."),
    db: AsyncSession = Depends(get_db)):
    jetton_wallet = address_to_raw(jetton)
    jetton_wallets = await db.run_sync(crud.get_jetton_wallets,
                                       address=jetton_wallet,
                                       limit=2,
                                       offset=0)
    if len(jetton_wallets) < 1:
        raise exceptions.JettonWalletNotFound(jetton_wallet=jetton_wallet)
    elif len(jetton_wallets) > 1:
        raise exceptions.MultipleDataFound(jetton_wallet=jetton_wallet)
    return schemas.JettonWallet.from_orm(jetton_wallets[0])


@router.get('/jetton_wallet/{jetton}/jetton_transfers', response_model=List[schemas.JettonTransfer])
async def get_account_jetton_transfers(
    jetton: str = Path(..., description="The Jetton wallet address to filter Jetton transfers. Can be sent in hex or base64url form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    jetton_account = address_to_raw(jetton)
    jtt_txs = await db.run_sync(crud.get_jetton_transfers,
                                jetton_account=jetton_account,
                                direction=None,  # NOTE: both incoming and outgoing
                                start_utime=start_utime,
                                end_utime=end_utime,
                                start_lt=start_lt,
                                end_lt=end_lt,
                                limit=limit,
                                offset=offset,
                                sort=sort)
    return [schemas.JettonTransfer.from_orm(x) for x in jtt_txs]


@router.get('/jetton_wallet/{jetton}/jetton_burns', response_model=List[schemas.JettonBurn])
async def get_account_jetton_burns(
    jetton: str = Path(..., description="The Jetton wallet address to filter Jetton burns. Can be sent in hex or base64url form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    start_lt: Optional[int] = Query(None, description="Logical time to start searching transactions", ge=0, le=UINT64_MAX),
    end_lt: Optional[int] = Query(None, description="Logical time to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT64_MAX),
    limit: int = Query(128, description='Limit number of queried rows. Use to batch read', ge=1, le=256),
    offset: int = Query(0, description='Skip first <offset> rows. Use to batch read', ge=0),
    sort: str = Query('desc', description='Sort transactions by lt.', enum=['none', 'asc', 'desc']),
    db: AsyncSession = Depends(get_db)):
    jetton_account = address_to_raw(jetton)
    jtt_txs = await db.run_sync(crud.get_jetton_burns,
                                jetton_account=jetton_account,
                                start_utime=start_utime,
                                end_utime=end_utime,
                                start_lt=start_lt,
                                end_lt=end_lt,
                                limit=limit,
                                offset=offset,
                                sort=sort)
    return [schemas.JettonBurn.from_orm(x) for x in jtt_txs]


# JsonRPC
def validate_block_idx(workchain, shard, seqno):
    if workchain is None:
        if shard is not None or seqno is not None:
            raise ValueError('workchain id required')
    if shard is None:
        if seqno is not None:
            raise ValueError('shard id required')
    return True


@router.get("/rpc/blocks", response_model=List[schemas.Block])
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


@router.get("/rpc/masterchainBlockShards", response_model=List[schemas.Block])
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


@router.get("/rpc/transactions", response_model=List[schemas.Transaction])
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


@router.get("/rpc/transactionsByMasterchainBlock", response_model=List[schemas.Transaction])
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


@router.get('/rpc/adjacentTransactions', response_model=List[schemas.Transaction])
async def get_adjacent_transactions(
    hash: str = Query(None, description='Transaction hash. Acceptable in hex, base64 and base64url forms.'),
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
                            sort=sort)
    return [schemas.Transaction.from_orm(tx) for tx in res]


@router.get('/rpc/messages', response_model=List[schemas.Message])
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


@router.get('/rpc/nftCollections', response_model=List[schemas.NFTCollection])
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


@router.get('/rpc/nftItems', response_model=List[schemas.NFTItem])
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


@router.get('/rpc/nftTransfers', response_model=List[schemas.NFTTransfer])
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


@router.get('/rpc/jettonMasters', response_model=List[schemas.JettonMaster])
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


@router.get('/rpc/jettonWallets', response_model=List[schemas.JettonWallet])
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


@router.get('/rpc/jettonTransfers', response_model=List[schemas.JettonTransfer])
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


@router.get('/rpc/jettonBurns', response_model=List[schemas.JettonBurn])
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
