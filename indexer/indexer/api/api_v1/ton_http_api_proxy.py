import httpx
import json

from fastapi import APIRouter, Query, Body
from fastapi.exceptions import HTTPException

from indexer.api.api_v1 import schemas
from indexer.core.settings import Settings

settings = Settings()
router = APIRouter()

def from_ton_http_api_response(response: httpx.Response) -> dict:
    def get_error(body):
        try:
            body = body.decode('utf-8')
            body_json = json.loads(body)
            return body_json['error']
        except:
            return body
    
    def get_result(body):
        try:
            body = body.decode('utf-8')
            body_json = json.loads(body)
            return body_json['result']
        except Exception as e:
            raise HTTPException(status_code=response.status_code, detail=f"Error parsing ton-http-api response body: {e}")
        
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=get_error(response.content))

    return get_result(response.content)

@router.post('/message', response_model=schemas.SentMessage)
async def send_message(
    message: schemas.ExternalMessage = Body(..., description='Message in boc base64 format.')):
    """
    Send external message to TON network.
    """
    async with httpx.AsyncClient() as client:
        response = await client.post(f'{settings.ton_http_api_endpoint}/sendBocReturnHash', json={'boc': message})
    
    result = from_ton_http_api_response(response)

    return schemas.SentMessage.from_ton_http_api(result)
    
@router.post('/runGetMethod', response_model=schemas.RunGetMethodResponse)
async def run_get_method(
    run_get_method_request: schemas.RunGetMethodRequest = Body(..., description='RunGetMethod request body')
    ):
    """
    Run get method of smart contract. Stack supports only `num`, `cell` and `slice` types:
    ```
    [
        {
            "type": "num",
            "value": "0x12a"
        },
        {
            "type": "cell",
            "value": "te6..." // base64 encoded boc with cell
        },
        {
            "type": "slice",
            "value": "te6..." // base64 encoded boc with slice
        }
    ]
    ```
    """
    async with httpx.AsyncClient() as client:
        response = await client.post(f'{settings.ton_http_api_endpoint}/runGetMethod', json=run_get_method_request.to_ton_http_api())
    
    result = from_ton_http_api_response(response)

    return schemas.RunGetMethodResponse.from_ton_http_api(result)

@router.post('/estimateFee', response_model=schemas.EstimateFeeResponse)
async def estimate_fee(
    run_get_method_request: schemas.EstimateFeeRequest = Body(..., description='EstimateFee request body')
    ):
    """
    Estimate fee for external message.
    """
    async with httpx.AsyncClient() as client:
        response = await client.post(f'{settings.ton_http_api_endpoint}/estimateFee', json=run_get_method_request.to_ton_http_api())
    
    result = from_ton_http_api_response(response)

    return schemas.EstimateFeeResponse.from_ton_http_api(result)

@router.get('/account', response_model=schemas.Account)
async def get_account_information(
    address: str = Query(..., description='Account address. Can be sent in raw or user-friendly form.')
    ):
    """
    Get smart contract information.
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(f'{settings.ton_http_api_endpoint}/getAddressInformation?address={address}')
    
    result = from_ton_http_api_response(response)   

    return schemas.Account.from_ton_http_api(result)

@router.get('/wallet', response_model=schemas.WalletInfo)
async def get_wallet_information(
    address: str = Query(..., description='Smart contract address. Can be sent in raw or user-friendly form.')
    ):
    """
    Get wallet smart contract information. The following wallets are supported: `v1r1`, `v1r2`, `v1r3`, `v2r1`, `v2r2`, `v3r1`, `v3r2`, `v4r1`, `v4r2`.
    In case the account is not a wallet error code 409 is returned.
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(f'{settings.ton_http_api_endpoint}/getWalletInformation?address={address}')
    
    result = from_ton_http_api_response(response)

    if not result.get('wallet_type', '').startswith('wallet'):
        raise HTTPException(status_code=409, detail=f'Account {address} is not a wallet')

    return schemas.WalletInfo.from_ton_http_api(result)

