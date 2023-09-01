import logging

from fastapi import FastAPI, status, Depends
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from indexer.api.api_v1.main import router as router_v1
from indexer.api.api_old.main import router as router_old
from indexer.api.api_wordy.main import router as router_wordy

from indexer.core import exceptions
from indexer.core.settings import Settings
from indexer.api.deps.apikey import api_key_dep


logging.basicConfig(format='%(asctime)s %(module)-15s %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)


settings = Settings()
description = 'TON Index collects data from a full node to PostgreSQL database and provides convenient API to an indexed blockchain.'
app = FastAPI(title="TON Index" if not settings.api_title else settings.api_title,
              description=description,
              version='1.0.0',
              root_path=settings.api_root_path,
              docs_url='/',
              dependencies=[Depends(api_key_dep)])


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse({'error' : str(exc.detail)}, status_code=exc.status_code)


@app.exception_handler(exceptions.DataNotFound)
async def tonlib_wront_result_exception_handler(request, exc):
    return JSONResponse({'error' : str(exc)}, status_code=status.HTTP_404_NOT_FOUND)


@app.exception_handler(exceptions.MultipleDataFound)
async def tonlib_wront_result_exception_handler(request, exc):
    return JSONResponse({'error' : str(exc)}, status_code=status.HTTP_404_NOT_FOUND)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse({'error' : str(exc)}, status_code=status.HTTP_400_BAD_REQUEST)


@app.exception_handler(Exception)
def generic_exception_handler(request, exc):
    return JSONResponse({'error' : str(exc)}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE)


@app.on_event("startup")
def startup():
    logger.info('Service started successfully')


app.include_router(router_v1, prefix='/v1', include_in_schema=True, deprecated=False)
app.include_router(router_old, prefix='/old', include_in_schema=False, deprecated=False, tags=['old'])
app.include_router(router_wordy, prefix='/wordy', include_in_schema=False, deprecated=False, tags=['wordy'])
