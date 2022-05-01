import logging
from typing import List, Optional

from fastapi import FastAPI, Depends
from fastapi import Query

from sqlalchemy.orm import Session

from tApi.tonlib.address_utils import detect_address

from webserver import schemas
from indexer.database import get_session
from indexer.crud import get_transactions_by_seqno, get_transactions_by_address

logging.basicConfig(format='%(asctime)s %(module)-15s %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)


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
    db_transactions = get_transactions_by_seqno(db, seqno)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_bodies) for t in db_transactions]

@app.get('/getTransactions', response_model=List[schemas.Transaction])
def get_transactions(
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
    db_transactions = get_transactions_by_address(db, raw_address, start_utime, end_utime, limit, offset, sort)
    return [schemas.Transaction.transaction_from_orm(t, include_msg_bodies) for t in db_transactions]


