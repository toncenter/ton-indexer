from typing import List, Optional
from datetime import datetime

from fastapi import APIRouter, Depends, Query, Response
from fastapi.exceptions import HTTPException

from sqlalchemy.orm import Session

from pytonlib.utils.address import detect_address

from . import schemas
from indexer.database import SessionMaker
from indexer import crud

# TODO: Move to pytonlib
from pytonlib.utils.common import b64str_to_hex
def hash_to_b64(b64_or_hex_hash):
    """
    Detect encoding of transactions hash and if necessary convert it to Base64.
    """
    if len(b64_or_hex_hash) == 44:
        # Hash is base64
        return b64_or_hex_hash
    if len(b64_or_hex_hash) == 64:
        # Hash is hex
        return b64str_to_hex(b64_or_hex_hash)
    raise ValueError("Invalid hash")


# Dependencies
async def get_db():
    async with SessionMaker() as db:
        yield db

async def set_last_block_time_header(response: Response, db: Session = Depends(get_db)):
    db_blocks = await db.run_sync(crud.get_blocks_by_unix_time, None, None, -1, None, 1, 0, 'desc')
    if len(db_blocks):
        last_block_utime = db_blocks[0].gen_utime
    else:
        last_block_utime = -1
    response.headers["X-LAST-BLOCK-TIME"] = str(last_block_utime)
    return response

router = APIRouter(dependencies=[Depends(set_last_block_time_header)])

INT64_MIN = -2**63
INT64_MAX = 2**63 - 1
UINT64_MAX = 2**64 - 1
INT32_MIN = -2**31
INT32_MAX = 2**31 - 1
UINT32_MAX = 2**32 - 1



@router.get('/getTransactionsByMasterchainSeqno', response_model=List[schemas.Transaction])
async def get_transactions_by_masterchain_seqno(
    seqno: int = Query(..., description="Masterchain seqno", ge=0, le=UINT32_MAX),
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
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    limit: int = Query(20, description="Number of transactions to return, maximum limit is 1000", ge=1, lt=1000),
    offset: int = Query(0, description="Number of rows to omit before the beginning of the result set"),
    sort: str = Query("desc", description="Use `asc` to get oldest transactions first and `desc` to get newest first."),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    """
    Get transactions of specified address.
    """
    try:
        raw_address = detect_address(address)["raw_form"]
    except Exception:
        raise HTTPException(status_code=416, detail="Invalid address")
    db_transactions = await db.run_sync(crud.get_transactions_by_address, raw_address, start_utime, end_utime, limit, offset, sort, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@router.get('/getTransactionsInBlock', response_model=List[schemas.Transaction])
async def get_transactions_in_block(
    workchain: int = Query(..., description="Block workchain", ge=0, le=INT32_MAX),
    shard: int = Query(..., description="Block shard", ge=INT64_MIN, le=INT64_MAX),
    seqno: int = Query(..., description="Block seqno", ge=0, le=UINT32_MAX),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    """
    Get transactions included in specified block.
    """
    db_transactions = await db.run_sync(crud.get_transactions_in_block, workchain, shard, seqno, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@router.get('/getChainLastTransactions', response_model=List[schemas.Transaction])
async def get_chain_last_transactions(
    workchain: Optional[int] = Query(..., description="Transactions workchain", ge=0, le=INT32_MAX),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
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

@router.get('/getMessagesByHash', response_model=List[schemas.Message])
async def get_messages_by_hash(
    msg_hash: str = Query(..., description="Message hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    """
    Get messages with specified hash. This endpoint returns list of Message objects since collisions of message hashes can occur.
    The response is limited by 500 messages.
    """
    msg_hash = hash_to_b64(msg_hash)
    db_messages = await db.run_sync(crud.get_messages_by_hash, msg_hash, include_msg_body)
    if include_msg_body:
        def get_msg_body(msg):
            body = msg.content.body
            op = msg.op
            source_interfaces = msg.out_tx.account_code_hash_rel.interfaces if msg.out_tx else []
            dest_interfaces = msg.in_tx.account_code_hash_rel.interfaces if msg.in_tx else []
            return schemas.get_msg_body_annotation(body, op, source_interfaces, dest_interfaces)
        return [schemas.Message.message_from_orm(m, get_msg_body(m)) for m in db_messages]
    return [schemas.Message.message_from_orm(m, None) for m in db_messages]


@router.get('/getTransactionByHash', response_model=schemas.Transaction)
async def get_transaction_by_hash(
    tx_hash: str = Query(..., description="Transaction hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    """
    Get transaction with specified hash.
    """
    tx_hash = hash_to_b64(tx_hash)
    db_transaction = await db.run_sync(crud.get_transaction_by_hash, tx_hash, include_msg_body)
    return schemas.Transaction.transaction_from_orm(db_transaction, include_msg_body)

@router.get('/getBlockByTransaction', response_model=schemas.Block)
async def get_block_by_transaction(
    tx_hash: str = Query(..., description="Transaction hash"),
    db: Session = Depends(get_db)
    ):
    """
    Get block containing the specified transaction.
    """
    tx_hash = hash_to_b64(tx_hash)
    block = await db.run_sync(crud.get_block_by_transaction, tx_hash)
    return schemas.Block.block_from_orm_block_header(block)

@router.get('/lookupMasterchainBlock', response_model=schemas.Block)
async def lookup_masterchain_block(
    workchain: int = Query(..., description="Block workchain", ge=0, le=INT32_MAX),
    shard: int = Query(..., description="Block shardchain", ge=INT64_MIN, le=INT64_MAX),
    seqno: int = Query(..., description="Block seqno", ge=0, le=UINT32_MAX),
    db: Session = Depends(get_db)
    ):
    """
    Get corresponding masterchain block by shardchain block.
    """
    if workchain == -1:
        raise HTTPException(status_code=416, detail="Provided block resides in masterchain")
    mc_block = await db.run_sync(crud.lookup_masterchain_block, workchain, shard, seqno)
    return schemas.Block.block_from_orm_block_header(mc_block)

@router.get('/getTransactionsByInMessageHash', response_model=List[schemas.Transaction])
async def get_transaction_by_in_message_hash(
    msg_hash: str = Query(..., description="Inbound message hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    """
    Get transactions whose inbound message has the specified hash. This endpoint returns list of Transaction objects
    since collisions of message hashes can occur.
    """
    msg_hash = hash_to_b64(msg_hash)
    db_transactions = await db.run_sync(crud.get_transactions_by_in_message_hash, msg_hash, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@router.get('/getSourceTransactionByMessage', response_model=schemas.Transaction)
async def get_source_transaction_by_message(
    source: str = Query(..., description="Source address"),
    destination: str = Query(..., description="Destination address"),
    msg_lt: int = Query(..., description="Creation lt of the message", ge=0, le=UINT64_MAX),
    db: Session = Depends(get_db)
    ):
    """
    Get transaction of `source` address by incoming message on `destination` address.
    """
    try:
        source = detect_address(source)["raw_form"]
        destination = detect_address(destination)["raw_form"]
    except Exception:
        raise HTTPException(status_code=416, detail="Invalid address")
    db_transaction = await db.run_sync(crud.get_source_transaction_by_message, source, destination, msg_lt)
    return schemas.Transaction.transaction_from_orm(db_transaction, True)

@router.get('/getDestinationTransactionByMessage', response_model=schemas.Transaction)
async def get_destination_transaction_by_message(
    source: str = Query(..., description="Sender address"),
    destination: str = Query(..., description="Receiver address"),
    msg_lt: int = Query(..., description="Creation lt of the message", ge=0, le=UINT64_MAX),
    db: Session = Depends(get_db)
    ):
    """
    Get transaction of `destination` address by outcoming message on `source` address.
    """
    db_transaction = await db.run_sync(crud.get_destination_transaction_by_message, source, destination, msg_lt)
    return schemas.Transaction.transaction_from_orm(db_transaction, True)

@router.get('/getBlocks', response_model=List[schemas.Block])
async def get_blocks(
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching blocks", ge=0, le=UINT64_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching blocks. If not specified latest blocks are returned.", ge=0, le=UINT64_MAX),
    workchain: Optional[int] = Query(None, description="Filter by workchain", ge=0, le=INT32_MAX),
    shard: Optional[int] = Query(None, description="Filter by shard", ge=INT64_MIN, le=INT64_MAX),
    limit: int = Query(20, description="Number of blocks to return, maximum limit is 1000", ge=1, lt=1000),
    offset: int = Query(0, description="Number of rows to omit before the beginning of the result set"),
    sort: str = Query("desc", description="Use `asc` to sort by ascending and `desc` to sort by descending"),
    db: Session = Depends(get_db)
    ):
    """
    Get a list of blocks based on specified filters.
    """
    db_blocks = await db.run_sync(crud.get_blocks_by_unix_time, start_utime, end_utime, workchain, shard, limit, offset, sort)
    return [schemas.Block.block_from_orm_block_header(b) for b in db_blocks]


@router.get('/getActiveAccountsCountInPeriod', response_model=schemas.CountResponse)
async def get_active_accounts_count_in_period(
    start_utime: int = Query(..., description="UTC timestamp of period start", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp of period end. If not specified now time is used.", ge=0, le=UINT32_MAX),
    db: Session = Depends(get_db)
    ):
    """
    Get number of accounts with at least one transaction within the time period. Maximum period length is 7 days.
    """
    if end_utime is None:
        end_utime = int(datetime.utcnow().timestamp())
    if end_utime - start_utime > 60 * 60 * 24 * 7:
        raise HTTPException(status_code=416, detail="Max period is 7 days.")
    count = await db.run_sync(crud.get_active_accounts_count_in_period, start_utime, end_utime)
    return schemas.CountResponse(count=count)
