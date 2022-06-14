import logging
from typing import List, Optional

from fastapi import FastAPI, Depends, Query, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
from starlette.exceptions import HTTPException as StarletteHTTPException

from sqlalchemy.orm import Session

from pytonlib.utils.address import detect_address

from config import settings

from webserver import schemas
from indexer.database import get_session
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
def get_db():
    db = get_session()()
    try:
        yield db
    finally:
        db.close()

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
def get_transactions_by_masterchain_seqno(
    seqno: int = Query(..., description="Masterchain seqno"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_transactions = crud.get_transactions_by_masterchain_seqno(db, seqno, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@app.get('/getTransactionsByAddress', response_model=List[schemas.Transaction])
def get_transactions_by_address(
    address: str = Query(..., description="The address to get transactions. Can be sent in any form."),
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching transactions"),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching transactions. If not specified latest transactions are returned."),
    limit: int = Query(20, description="Number of transactions to return"),
    offset: int = Query(0, description="Number of rows to omit before the beginning of the result set"),
    sort: str = Query("desc", description="Use `asc` to sort by ascending and `desc` to sort by descending"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    try:
        raw_address = detect_address(address)["raw_form"]
    except Exception:
        raise HTTPException(status_code=416, detail="Invalid address")
    db_transactions = crud.get_transactions_by_address(db, raw_address, start_utime, end_utime, limit, offset, sort, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@app.get('/getTransactionsInBlock', response_model=List[schemas.Transaction])
def get_transactions_in_block(
    workchain: int = Query(..., description="Block's workchain"),
    shard: int = Query(..., description="Block's shard"),
    seqno: int = Query(..., description="Block's seqno"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_transactions = crud.get_transactions_in_block(db, workchain, shard, seqno, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@app.get('/getInMessageByTxID', response_model=Optional[schemas.Message])
def get_in_message_by_transaction(
    tx_lt: int = Query(..., description="Logical time of transaction"),
    tx_hash: str = Query(..., description="Transaction hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_message = crud.get_in_message_by_transaction(db, tx_lt, tx_hash, include_msg_body)
    return schemas.Message.message_from_orm(db_message, include_msg_body) if db_message else None

@app.get('/getOutMessagesByTxID', response_model=List[schemas.Message])
def get_out_message_by_transaction(
    tx_lt: int = Query(..., description="Transaction logical time"),
    tx_hash: str = Query(..., description="Transaction hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_messages = crud.get_out_messages_by_transaction(db, tx_lt, tx_hash, include_msg_body)
    return [schemas.Message.message_from_orm(m, include_msg_body) for m in db_messages]

@app.get('/getMessageByHash', response_model=List[schemas.Message])
def get_message_by_hash(
    msg_hash: str = Query(..., description="Message hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_messages = crud.get_messages_by_hash(db, msg_hash, include_msg_body)
    return [schemas.Message.message_from_orm(m, include_msg_body) for m in db_messages]

@app.get('/getTransactionByHash', response_model=List[schemas.Transaction])
def get_transaction_by_hash(
    tx_hash: str = Query(..., description="Transaction hash"),
    include_msg_body: bool = Query(False, description="Whether return full message body or not"),
    db: Session = Depends(get_db)
    ):
    db_transactions = crud.get_transactions_by_hash(db, tx_hash, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@app.get('/getBlocksByUnixTime', response_model=List[schemas.Block])
def get_blocks_by_unix_time(
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching blocks"),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching blocks. If not specified latest blocks are returned."),
    workchain: Optional[int] = Query(None, description="Filter by workchain"),
    shard: Optional[int] = Query(None, description="Filter by shard"),
    limit: int = Query(20, description="Number of blocks to return"),
    offset: int = Query(0, description="Number of rows to omit before the beginning of the result set"),
    sort: str = Query("desc", description="Use `asc` to sort by ascending and `desc` to sort by descending"),
    db: Session = Depends(get_db)
    ):
    db_blocks = crud.get_blocks_by_unix_time(db, start_utime, end_utime, workchain, shard, limit, offset, sort)
    return [schemas.Block.block_from_orm_block_header(b) for b in db_blocks]

