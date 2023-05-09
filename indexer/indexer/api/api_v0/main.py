import logging
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, APIRouter, Depends, Query, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
from starlette.exceptions import HTTPException as StarletteHTTPException

from sqlalchemy.orm import Session

from pytonlib.utils.address import detect_address

from indexer.api.api_v0 import schemas
from indexer.core.database import SessionMaker
from indexer.core import crud
from indexer.core.settings import Settings


settings = Settings()
router = APIRouter()

# Dependency
async def get_db():
    async with SessionMaker() as db:
        yield db

@router.get('/getTransactionsByMasterchainSeqno', response_model=List[schemas.Transaction])
async def get_transactions_by_masterchain_seqno(
    seqno: int = Query(..., description="Masterchain seqno"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    """
    Get transactions by masterchain seqno across all workchains and shardchains.
    """
    db_transactions = await db.run_sync(crud.get_transactions_by_masterchain_seqno, seqno, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@router.get('/getTransactionsByAddress', response_model=List[schemas.Transaction])
async def get_transactions_by_address(
    address: str = Query(..., description="The address to get transactions. Can be sent in any form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions"),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned."),
    limit: int = Query(20, description="Number of transactions to return, maximum limit is 1000", ge=1, lt=1000),
    offset: int = Query(0, description="Number of rows to omit before the beginning of the result set"),
    sort: str = Query("desc", description="Use `asc` to get oldest transactions first and `desc` to get newest first."),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    try:
        raw_address = detect_address(address)["raw_form"]
    except Exception:
        raise HTTPException(status_code=416, detail="Invalid address")
    db_transactions = await db.run_sync(crud.get_transactions_by_address, raw_address, start_utime, end_utime, limit, offset, sort, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@router.get('/getTransactionsInBlock', response_model=List[schemas.Transaction])
async def get_transactions_in_block(
    workchain: int = Query(..., description="Block workchain"),
    shard: int = Query(..., description="Block shard"),
    seqno: int = Query(..., description="Block seqno"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_transactions = await db.run_sync(crud.get_transactions_in_block, workchain, shard, seqno, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@router.get('/getChainLastTransactions', response_model=List[schemas.Transaction])
async def get_chain_last_transactions(
    workchain: Optional[int] = Query(..., description="Transactions workchain"),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions"),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned."),
    limit: int = Query(20, description="Number of transactions to return, maximum value is 1000", ge=1, lt=1000),
    offset: int = Query(0, description="Number of rows to omit before the beginning of the result set"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    """
    Get latest transaction in workchain. Response is sorted desceding by transaction timestamp.
    """
    db_transactions = await db.run_sync(crud.get_chain_last_transactions, workchain, start_utime, end_utime, limit, offset, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]    

@router.get('/getInMessageByTxID', response_model=Optional[schemas.Message], deprecated=True)
async def get_in_message_by_transaction(
    tx_lt: int = Query(..., description="Logical time of transaction"),
    tx_hash: str = Query(..., description="Transaction hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    """
    Deprecated in favor of `getTransactionByHash`.
    """
    db_message = await db.run_sync(crud.get_in_message_by_transaction_Deprecated, tx_lt, tx_hash, include_msg_body)
    return schemas.Message.message_from_orm(db_message, include_msg_body) if db_message else None

@router.get('/getOutMessagesByTxID', response_model=List[schemas.Message], deprecated=True)
async def get_out_message_by_transaction(
    tx_lt: int = Query(..., description="Transaction logical time"),
    tx_hash: str = Query(..., description="Transaction hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    """
    Deprecated in favor of `getTransactionByHash`.
    """
    db_messages = await db.run_sync(crud.get_out_messages_by_transaction_Deprecated, tx_lt, tx_hash, include_msg_body)
    return [schemas.Message.message_from_orm(m, include_msg_body) for m in db_messages]

@router.get('/getMessageByHash', response_model=List[schemas.Message])
async def get_message_by_hash(
    msg_hash: str = Query(..., description="Message hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_messages = await db.run_sync(crud.get_messages_by_hash, msg_hash, include_msg_body)
    return [schemas.Message.message_from_orm(m, include_msg_body) for m in db_messages]

@router.get('/getTransactionByHash', response_model=List[schemas.Transaction])
async def get_transaction_by_hash(
    tx_hash: str = Query(..., description="Transaction hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_transactions = await db.run_sync(crud.get_transactions_by_hash, tx_hash, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@router.get('/getBlockByTransaction', response_model=schemas.Block)
async def get_block_by_transaction(
    tx_hash: str = Query(..., description="Transaction hash"),
    db: Session = Depends(get_db)
    ):
    block = await db.run_sync(crud.get_block_by_transaction, tx_hash)
    return schemas.Block.block_from_orm_block_header(block)

@router.get('/lookupMasterchainBlock', response_model=schemas.Block)
async def lookup_masterchain_block(
    workchain: int,
    shard: int,
    seqno: int,
    db: Session = Depends(get_db)
    ):
    """
    Accepts shardchain block and returns corresponding masterchain.
    """
    if workchain == -1:
        raise HTTPException(status_code=416, detail="Provided block resides in masterchain")
    mc_block = await db.run_sync(crud.lookup_masterchain_block, workchain, shard, seqno)
    return schemas.Block.block_from_orm_block_header(mc_block)

@router.get('/getTransactionByInMessageHash', response_model=List[schemas.Transaction])
async def get_transaction_by_in_message_hash(
    msg_hash: str = Query(..., description="Transaction hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_transactions = await db.run_sync(crud.get_transactions_by_in_message_hash, msg_hash, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@router.get('/getSourceTransactionByMessage', response_model=schemas.Transaction)
async def get_source_transaction_by_message(
    source: str = Query(..., description="Source address"),
    destination: str = Query(..., description="Destination address"),
    msg_lt: int = Query(..., description="Creation lt of the message"),
    db: Session = Depends(get_db)
    ):
    """
    Get transaction of `source` address by incoming message on `destination` address.
    """
    db_transaction = await db.run_sync(crud.get_source_transaction_by_message, source, destination, msg_lt)
    return schemas.Transaction.transaction_from_orm(db_transaction, True)

@router.get('/getDestinationTransactionByMessage', response_model=schemas.Transaction)
async def get_destination_transaction_by_message(
    source: str = Query(..., description="Sender address"),
    destination: str = Query(..., description="Receiver address"),
    msg_lt: int = Query(..., description="Creation lt of the message"),
    db: Session = Depends(get_db)
    ):
    """
    Get transaction of `destination` address by outcoming message on `source` address.
    """
    db_transaction = await db.run_sync(crud.get_destination_transaction_by_message, source, destination, msg_lt)
    return schemas.Transaction.transaction_from_orm(db_transaction, True)


@router.get('/getBlocksByUnixTime', response_model=List[schemas.Block])
async def get_blocks_by_unix_time(
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching blocks"),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching blocks. If not specified latest blocks are returned."),
    workchain: Optional[int] = Query(None, description="Filter by workchain"),
    shard: Optional[int] = Query(None, description="Filter by shard"),
    limit: int = Query(20, description="Number of blocks to return, maximum limit is 1000", ge=1, lt=1000),
    offset: int = Query(0, description="Number of rows to omit before the beginning of the result set"),
    sort: str = Query("desc", description="Use `asc` to sort by ascending and `desc` to sort by descending"),
    db: Session = Depends(get_db)
    ):
    db_blocks = await db.run_sync(crud.get_blocks_by_unix_time, start_utime, end_utime, workchain, shard, limit, offset, sort)
    return [schemas.Block.block_from_orm_block_header(b) for b in db_blocks]


@router.get('/getActiveAccountsCountInPeriod', response_model=schemas.CountResponse)
async def get_active_accounts_count_in_period(
    start_utime: int = Query(..., description="UTC timestamp of period start"),
    end_utime: Optional[int] = Query(None, description="UTC timestamp of period end. If not specified now time is used."),
    db: Session = Depends(get_db)
    ):
    if end_utime is None:
        end_utime = int(datetime.utcnow().timestamp())
    if end_utime - start_utime > 60 * 60 * 24 * 7:
        raise HTTPException(status_code=416, detail="Max period is 7 days.")
    count = await db.run_sync(crud.get_active_accounts_count_in_period, start_utime, end_utime)
    return schemas.CountResponse(count=count)
