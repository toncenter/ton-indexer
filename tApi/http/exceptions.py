from tApi.http.models import TonResponse
from fastapi import status
from fastapi.responses import JSONResponse


# For unknown reason FastAPI can't handle generic Exception with exception_handler(...)
# https://github.com/tiangolo/fastapi/issues/2750
# As workaround - catch and handle this exception in the middleware.
def generic_exception_handler(exc):
    res = TonResponse(ok=False, error=str(exc), code=status.HTTP_503_SERVICE_UNAVAILABLE)
    return JSONResponse(res.dict(exclude_none=True), status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

def generic_http_exception_handler(exc):
    res = TonResponse(ok=False, error=str(exc.detail), code=exc.status_code)
    return JSONResponse(res.dict(exclude_none=True), status_code=res.code)