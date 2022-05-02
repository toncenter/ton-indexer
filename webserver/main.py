import logging
from typing import List, Optional

from fastapi import FastAPI, Depends
from fastapi import Query

from sqlalchemy.orm import Session

from tApi.tonlib.address_utils import detect_address

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
    docs_url='/',
    # responses={
    #     422: {'description': 'Validation Error'},
    #     504: {'description': 'Lite Server Timeout'}
    # },
    # root_path='/api/v2',
)

# Dependency
def get_db():
    db = get_session()()
    try:
        yield db
    finally:
        db.close()


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
    raw_address = detect_address(address)["raw_form"]
    db_transactions = crud.get_transactions_by_address(db, raw_address, start_utime, end_utime, limit, offset, sort, include_msg_body)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_body) for t in db_transactions]

@app.get('/getBlocksByUnixTime', response_model=List[schemas.Block])
def get_blocks_by_unix_time(
    start_utime: Optional[int] = Query(None, description="UTC timestamp to start searching blocks"),
    end_utime: Optional[int] = Query(None, description="UTC timestamp to stop searching blocks. If not specified latest blocks are returned."),
    workchain: Optional[int] = Query(None, description="Filter by workchain"),
    shard: Optional[int] = Query(None, description="Filter by shard"),
    limit: int = Query(20, description="Number of transactions to return"),
    offset: int = Query(0, description="Number of rows to omit before the beginning of the result set"),
    sort: str = Query("desc", description="Use `asc` to sort by ascending and `desc` to sort by descending"),
    db: Session = Depends(get_db)
    ):
    db_blocks = crud.get_blocks_by_unix_time(db, start_utime, end_utime, workchain, shard, limit, offset, sort)
    return [schemas.Block.block_from_orm_block_header(b) for b in db_blocks]
    
