import random
import inspect
import time
import asyncio

from tApi.tonlib.client import TonlibClient
from tApi.tonlib.utils import TonLibWrongResult, retry_async
from pathlib import Path

from loguru import logger


def current_function_name():
    return inspect.stack()[1].function


class TonlibMultiClient:
    def __init__(self, 
                 loop,
                 config,
                 keystore,
                 cdll_path=None,
                 liteserver_blacklist=[],
                 verbosity_level=0):
        self.loop = loop
        self.config = config
        self.keystore = keystore
        self.cdll_path = cdll_path
        self.verbosity_level = verbosity_level

        self.liteserver_blacklist = liteserver_blacklist

        self.clients = {}
        self.archival_clients = []
        
    async def init(self, max_restarts=None):
        '''
          Try to init as many tonlib clients as there are liteservers in config
        '''
        self.read_output_tasks = []
        for ls_index, ls_config in enumerate(self.config["liteservers"]):
            if ls_config['ip'] in self.liteserver_blacklist or ls_index in self.liteserver_blacklist:
                logger.warning(f'Skipping blacklisted liteserver #{ls_index:03d}')
                continue

            keystore = f"{self.keystore}____{ls_index}"
            Path(keystore).mkdir(parents=True, exist_ok=True)

            client = TonlibClient(ls_index=ls_index,
                                  config=self.config,
                                  keystore=keystore, 
                                  loop=self.loop,
                                  cdll_path=self.cdll_path,
                                  verbosity_level=self.verbosity_level)
            asyncio.ensure_future(client.init(max_restarts), loop=self.loop)

            if ls_config.get('archval', False):
                self.archival_clients[ls_index]
            self.clients[ls_index] = client

    def _get_tonlib_client_by_index(self, clients, ls_index):
        if ls_index is not None:
            if ls_index in clients:
                return clients[ls_index]
            logger.warning(f'liteserver with index {ls_index} not found. Using random liteserver')
        index = random.choice(list(clients))
        return clients[index]

    def get_tonlib_client(self, archival=False, ls_index=None) -> TonlibClient:
        if archival:
            return self._get_tonlib_client_by_index(self.archival_clients, ls_index)
        return self._get_tonlib_client_by_index(self.clients, ls_index)

    # methods
    async def raw_get_transactions(self, account_address: str, from_transaction_lt: str, from_transaction_hash: str, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client.raw_get_transactions(account_address, from_transaction_lt, from_transaction_hash)

    async def getTransactions(self, account_address, from_transaction_lt=None, from_transaction_hash=None, to_transaction_lt=0, limit=10, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client.getTransactions(account_address, from_transaction_lt, from_transaction_hash, to_transaction_lt, limit)

    @retry_async(2, last_archval=False)
    async def raw_get_account_state(self, address: str, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        addr = await client.raw_get_account_state(address)
        if addr.get('@type','error') == 'error':
            raise TonLibWrongResult("raw.getAccountState failed", addr)
        return addr

    async def generic_get_account_state(self, address: str, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client.generic_get_account_state(address)

    async def raw_run_method(self, address, method, stack_data, output_layout=None, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client.raw_run_method(address, method, stack_data, output_layout)

    # TODO: make concurrency
    async def raw_send_message(self, serialized_boc, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client.raw_send_message(serialized_boc)

    async def _raw_create_query(self, destination, body, init_code=b'', init_data=b'', archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client._raw_create_query(destination, body, init_code, init_data)

    async def _raw_send_query(self, query_info, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client._raw_send_query(query_info)

    async def raw_create_and_send_query(self, destination, body, init_code=b'', init_data=b'', archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client.raw_create_and_send_query(destination, body, init_code, init_data)

    async def raw_create_and_send_message(self, destination, body, initial_account_state=b'', archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client.raw_create_and_send_message(destination, body, initial_account_state)

    async def raw_estimate_fees(self, destination, body, init_code=b'', init_data=b'', ignore_chksig=True, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client.raw_estimate_fees(destination, body, init_code, init_data, ignore_chksig)

    async def getMasterchainInfo(self, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client.getMasterchainInfo()

    @retry_async(2, last_archval=True)
    async def lookupBlock(self, workchain, shard, seqno=None, lt=None, unixtime=None, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        result = await client.lookupBlock(workchain, shard, seqno, lt, unixtime)

        # TODO: check return type and raise error
        return result

    @retry_async(2, last_archval=True)
    async def getShards(self, master_seqno=None, lt=None, unixtime=None, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        result = await client.getShards(master_seqno, lt, unixtime)
        
        # TODO: check return type and raise error
        return result

    async def raw_getBlockTransactions(self, fullblock, count, after_tx, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client.raw_getBlockTransactions(fullblock, count, after_tx)

    async def getBlockTransactions(self, workchain, shard, seqno, count, root_hash=None, file_hash=None, after_lt=None, after_hash=None, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client.getBlockTransactions(workchain, shard, seqno, count, root_hash, file_hash, after_lt, after_hash)

    @retry_async(2, last_archval=True)
    async def getBlockHeader(self, workchain, shard, seqno, root_hash=None, file_hash=None, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client.getBlockHeader(workchain, shard, seqno, root_hash, file_hash)

    async def tryLocateTxByOutcomingMessage(self, source, destination, creation_lt, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client.tryLocateTxByOutcomingMessage(source, destination, creation_lt)

    async def tryLocateTxByIncomingMessage(self, source, destination, creation_lt, archival: bool=False, ls_index: int=None):
        client = self.get_tonlib_client(archival=archival, ls_index=ls_index)
        return await client.tryLocateTxByIncomingMessage(source, destination, creation_lt)
