import logging

from typing import List, Optional
from datetime import datetime

from fastapi import APIRouter, Depends, Query, Response
from fastapi.exceptions import HTTPException

from sqlalchemy.orm import Session

from pytonlib.utils.address import detect_address
from pytonlib.utils.common import hex_to_b64str

from indexer.core.database import SessionMaker
from indexer.api.api_old import schemas
from indexer.core import crud, exceptions

logger = logging.getLogger(__name__)


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
    raise ValueError("Invalid hash")


def address_to_raw(address):
    try:
        raw_address = detect_address(address)["raw_form"]
    except Exception:
        raise HTTPException(status_code=416, detail="Invalid address")
    return raw_address


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


# Transactions
@router.get('/getTransactionsByMasterchainSeqno', )#response_model=List[schemas.Transaction])
async def get_transactions_by_masterchain_seqno(
    seqno: int = Query(..., description="Masterchain seqno", ge=0, le=UINT32_MAX),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    include_block: bool = Query(False, description="If true response contains corresponding block for each transaction"),
    include_account_state: bool = Query(False, description="If true response contains corresponding account states for each transaction"),
    limit: Optional[int] = Query(None, description='Limit transactions'),
    offset: Optional[int] = Query(None, description='Query offset'),
    db: Session = Depends(get_db)):
    """
    Get transactions by masterchain seqno across all workchains and shardchains.
    """
    db_transactions = await db.run_sync(crud.get_transactions_by_masterchain_seqno, 
                                        masterchain_seqno=seqno, 
                                        include_msg_body=include_msg_body, 
                                        include_block=include_block, 
                                        include_account_state=include_account_state, 
                                        limit=limit, 
                                        offset=offset)
    return db_transactions
    # return [schemas.Transaction.transaction_from_orm(t, include_msg_body, include_block) for t in db_transactions]


@router.get('/getTransactionsByAddress', )#response_model=List[schemas.Transaction])
async def get_transactions_by_address(
    address: str = Query(..., description="The address to get transactions. Can be sent in any form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    include_block: bool = Query(False, description="If true response contains corresponding block for each transaction"),
    include_account_state: bool = Query(False, description="If true response contains corresponding account states for each transaction"),
    limit: int = Query(32, description='Number of transactions to return, maximum limit is 1024', ge=1, le=1024),
    offset: int = Query(0, description='Number of rows to omit before the beginning of the result set'),
    sort: str = Query("desc", description="Use `asc` to get oldest transactions first and `desc` to get newest first."),
    db: Session = Depends(get_db)
    ):
    """
    Get transactions of specified address.
    """
    raw_address = address_to_raw(address)
    db_transactions = await db.run_sync(crud.get_transactions,
                                        account=raw_address,
                                        start_utime=start_utime,
                                        end_utime=end_utime,
                                        include_msg_body=include_msg_body,
                                        include_block=include_block,
                                        include_account_state=include_account_state,
                                        limit=limit,
                                        offset=offset,
                                        sort=sort)
    return db_transactions
    # return [schemas.Transaction.transaction_from_orm(t, include_msg_body, include_block) for t in db_transactions]


@router.get('/getTransactionsInBlock', )#response_model=List[schemas.Transaction])
async def get_block_transactions(
    workchain: int = Query(..., description="Block workchain", ge=INT32_MIN, le=INT32_MAX),
    shard: int = Query(..., description="Block shard", ge=INT64_MIN, le=INT64_MAX),
    seqno: int = Query(..., description="Block seqno", ge=0, le=UINT32_MAX),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    include_block: bool = Query(False, description="If true response contains corresponding block for each transaction"),
    include_account_state: bool = Query(False, description="If true response contains corresponding account states for each transaction"),
    limit: Optional[int] = Query(None, description='Number of transactions to return, maximum limit is 1024', ge=1, le=1024),
    offset: Optional[int] = Query(None, description='Number of rows to omit before the beginning of the result set'),
    db: Session = Depends(get_db)):
    """
    Get transactions included in specified block.
    """
    db_transactions = await db.run_sync(crud.get_transactions,
                                        workchain=workchain,
                                        shard=shard,
                                        seqno=seqno,
                                        include_msg_body=include_msg_body,
                                        include_block=include_block,
                                        include_account_state=include_account_state,
                                        limit=limit,
                                        offset=offset,
                                        sort='asc',)
    return db_transactions
    # return [schemas.Transaction.transaction_from_orm(t, include_msg_body, include_block) for t in db_transactions]


@router.get('/getChainLastTransactions', )#response_model=List[schemas.Transaction])
async def get_chain_last_transactions(
    workchain: int = Query(..., description="Transactions workchain", ge=INT32_MIN, le=INT32_MAX),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions", ge=0, le=UINT32_MAX),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned.", ge=0, le=UINT32_MAX),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    include_block: bool = Query(False, description="If true response contains corresponding block for each transaction"),
    include_account_state: bool = Query(False, description="If true response contains corresponding account states for each transaction"),
    limit: int = Query(32, description='Number of transactions to return, maximum limit is 1024', ge=1, le=1024),
    offset: int = Query(0, description='Number of rows to omit before the beginning of the result set'),
    # sort: str = Query("desc", description="Use `asc` to get oldest transactions first and `desc` to get newest first."),
    db: Session = Depends(get_db)):
    """
    Get latest transaction in workchain. Response is sorted desceding by transaction timestamp.
    """
    db_transactions = await db.run_sync(crud.get_transactions,
                                        workchain=workchain,
                                        start_utime=start_utime,
                                        end_utime=end_utime,
                                        include_msg_body=include_msg_body,
                                        include_block=include_block,
                                        include_account_state=include_account_state,
                                        limit=limit,
                                        offset=offset,
                                        sort='desc')
    return db_transactions
    # return [schemas.Transaction.transaction_from_orm(t, include_msg_body, include_block) for t in db_transactions]


@router.get('/getTransactionByHash', )#response_model=schemas.Transaction)
async def get_transaction_by_hash(
    tx_hash: str = Query(..., description="Transaction hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    include_block: bool = Query(False, description="If true response contains corresponding block for each transaction"),
    include_account_state: bool = Query(False, description="If true response contains corresponding account states for each transaction"),
    db: Session = Depends(get_db)):
    """
    Get transaction with specified hash.
    """
    tx_hash = hash_to_b64(tx_hash)
    db_transactions = await db.run_sync(crud.get_transactions,
                                        hash=tx_hash,
                                        include_msg_body=include_msg_body,
                                        include_block=include_block,
                                        include_account_state=include_account_state,
                                        limit=1)
    if len(db_transactions) < 1:
        raise exceptions.TransactionNotFound(hash=hash)
    return db_transactions[0]


@router.get('/getTransactionsByInMessageHash', )#response_model=List[schemas.Transaction])
async def get_transaction_by_in_message_hash(
    msg_hash: str = Query(..., description="Inbound message hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    include_block: bool = Query(False, description="If true response contains corresponding block for each transaction"),
    include_account_state: bool = Query(False, description="If true response contains corresponding account states for each transaction"),
    db: Session = Depends(get_db)
    ):
    """
    Get transactions whose inbound message has the specified hash. This endpoint returns list of Transaction objects
    since collisions of message hashes can occur.
    """
    msg_hash = hash_to_b64(msg_hash)
    db_transactions = await db.run_sync(crud.get_transactions_by_in_message_hash, 
                                        msg_hash=msg_hash,
                                        include_msg_body=include_msg_body,
                                        include_block=include_block,
                                        include_account_state=include_account_state)
    return db_transactions


@router.get('/getSourceTransactionByMessage', )#response_model=schemas.Transaction)
async def get_source_transaction_by_message(
    source: str = Query(..., description="Source address"),
    destination: str = Query(..., description="Destination address"),
    msg_lt: int = Query(..., description="Creation lt of the message", ge=0, le=UINT64_MAX),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    include_block: bool = Query(False, description="If true response contains corresponding block for each transaction"),
    include_account_state: bool = Query(False, description="If true response contains corresponding account states for each transaction"),
    db: Session = Depends(get_db)
    ):
    """
    Get transaction of `source` address by incoming message on `destination` address.
    """
    source = address_to_raw(source)
    destination = address_to_raw(destination)
    db_transactions = await db.run_sync(crud.get_transactions_by_message, 
                                       direction='out',
                                       source=source, 
                                       destination=destination,
                                       created_lt=msg_lt,
                                       include_msg_body=include_msg_body,
                                       include_block=include_block,
                                       include_account_state=include_account_state,)
    if len(db_transactions) < 1:
        raise exceptions.TransactionNotFound(message_type='out_msg',
                                             msg_source=source,
                                             msg_destination=destination,
                                             msg_lt=msg_lt)
    return db_transactions[0]


@router.get('/getDestinationTransactionByMessage', )#response_model=schemas.Transaction)
async def get_source_transaction_by_message(
    source: str = Query(..., description="Source address"),
    destination: str = Query(..., description="Destination address"),
    msg_lt: int = Query(..., description="Creation lt of the message", ge=0, le=UINT64_MAX),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    include_block: bool = Query(False, description="If true response contains corresponding block for each transaction"),
    include_account_state: bool = Query(False, description="If true response contains corresponding account states for each transaction"),
    db: Session = Depends(get_db)
    ):
    """
    Get transaction of `source` address by incoming message on `destination` address.
    """
    source = address_to_raw(source)
    destination = address_to_raw(destination)
    db_transactions = await db.run_sync(crud.get_transactions_by_message, 
                                       direction='in',
                                       source=source, 
                                       destination=destination, 
                                       created_lt=msg_lt,
                                       include_msg_body=include_msg_body,
                                       include_block=include_block,
                                       include_account_state=include_account_state,)
    if len(db_transactions) < 1:
        raise exceptions.TransactionNotFound(message_type='in_msg',
                                             msg_source=source,
                                             msg_destination=destination,
                                             msg_lt=msg_lt)
    return db_transactions[0]


# Messages
@router.get('/getMessagesByHash', )#response_model=List[schemas.Message])
async def get_messages_by_hash(
    msg_hash: str = Query(..., description="Message hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    limit: int = Query(32, description='Number of messages to return, maximum limit is 512', ge=1, le=512),
    offset: int = Query(0, description='Number of rows to omit before the beginning of the result set'),
    db: Session = Depends(get_db)
    ):
    """
    Get messages with specified hash. This endpoint returns list of Message objects since collisions of message hashes can occur.
    The response is limited by 512 messages.
    """
    msg_hash = hash_to_b64(msg_hash)
    db_messages = await db.run_sync(crud.get_messages, 
                                    hash=msg_hash, 
                                    include_msg_body=include_msg_body,
                                    limit=limit,
                                    offset=offset)
    return db_messages
    # return [schemas.Message.message_from_orm(m, include_msg_body) for m in db_messages]
