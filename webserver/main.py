import logging
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, Depends, Query, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
from starlette.exceptions import HTTPException as StarletteHTTPException

from sqlalchemy.orm import Session

from pytonlib.utils.address import detect_address

from config import settings

from webserver import schemas
from indexer.database import SessionMaker
from indexer import crud

logging.basicConfig(format='%(asctime)s %(module)-15s %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


description = ''
app = FastAPI(
    title="TON Index",
    description=description,
    version='0.1.0',
    root_path=settings.webserver.api_root_path,
    docs_url='/',
)

# Dependency
async def get_db():
    async with SessionMaker() as db:
        yield db

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse({'error' : str(exc.detail)}, status_code=exc.status_code)

@app.exception_handler(crud.DataNotFound)
async def tonlib_wront_result_exception_handler(request, exc):
    return JSONResponse({'error' : str(exc)}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

@app.exception_handler(Exception)
def generic_exception_handler(request, exc):
    return JSONResponse({'error' : str(exc)}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

@app.on_event("startup")
def startup():
    logger.info('Service started successfully')

@app.get('/getTransactionsByMasterchainSeqno', response_model=List[schemas.Transaction])
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

@app.get('/getTransactionsByAddress', response_model=List[schemas.Transaction])
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

@app.get('/getTransactionsInBlock', response_model=List[schemas.Transaction])
async def get_transactions_in_block(
    workchain: int = Query(..., description="Block workchain"),
    shard: int = Query(..., description="Block shard"),
    seqno: int = Query(..., description="Block seqno"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_transactions = await db.run_sync(crud.get_transactions_in_block, workchain, shard, seqno, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@app.get('/getChainLastTransactions', response_model=List[schemas.Transaction])
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

@app.get('/getInMessageByTxID', response_model=Optional[schemas.Message])
async def get_in_message_by_transaction(
    tx_lt: int = Query(..., description="Logical time of transaction"),
    tx_hash: str = Query(..., description="Transaction hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_message = await db.run_sync(crud.get_in_message_by_transaction, tx_lt, tx_hash, include_msg_body)
    return schemas.Message.message_from_orm(db_message, include_msg_body) if db_message else None

@app.get('/getOutMessagesByTxID', response_model=List[schemas.Message])
async def get_out_message_by_transaction(
    tx_lt: int = Query(..., description="Transaction logical time"),
    tx_hash: str = Query(..., description="Transaction hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_messages = await db.run_sync(crud.get_out_messages_by_transaction, tx_lt, tx_hash, include_msg_body)
    return [schemas.Message.message_from_orm(m, include_msg_body) for m in db_messages]

@app.get('/getMessageByHash', response_model=List[schemas.Message])
async def get_message_by_hash(
    msg_hash: str = Query(..., description="Message hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_messages = await db.run_sync(crud.get_messages_by_hash, msg_hash, include_msg_body)
    return [schemas.Message.message_from_orm(m, include_msg_body) for m in db_messages]

@app.get('/getTransactionByHash', response_model=List[schemas.Transaction])
async def get_transaction_by_hash(
    tx_hash: str = Query(..., description="Transaction hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_transactions = await db.run_sync(crud.get_transactions_by_hash, tx_hash, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@app.get('/getBlockByTransaction', response_model=schemas.Block)
async def get_block_by_transaction(
    tx_hash: str = Query(..., description="Transaction hash"),
    db: Session = Depends(get_db)
    ):
    block = await db.run_sync(crud.get_block_by_transaction, tx_hash)
    return schemas.Block.block_from_orm_block_header(block)

@app.get('/lookupMasterchainBlock', response_model=schemas.Block)
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

@app.get('/getTransactionByInMessageHash', response_model=List[schemas.Transaction])
async def get_transaction_by_in_message_hash(
    msg_hash: str = Query(..., description="Transaction hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_transactions = await db.run_sync(crud.get_transactions_by_in_message_hash, msg_hash, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@app.get('/getBlocksByUnixTime', response_model=List[schemas.Block])
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

@app.get('/getActiveAccountsCountInPeriod', response_model=schemas.CountResponse)
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
