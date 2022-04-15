import asyncio
import threading

from tApi.tonlib import TonlibClient


class AsyncioEventLoopThread(threading.Thread):
    def __init__(self, *args, loop=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.loop = loop or asyncio.new_event_loop()
        self.running = False

    def run(self):
        self.running = True
        self.loop.run_forever()

    def run_coro(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, loop=self.loop).result()

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.join()
        self.running = False


class TonlibClientSync:
    def __init__(self,
                 ls_index,
                 config,
                 keystore,
                 cdll_path=None,
                 verbosity_level=0):
        self.thread = AsyncioEventLoopThread()
        self.thread.start()
        self.client_impl = TonlibClient(ls_index, config, keystore, self.thread.loop, cdll_path, verbosity_level)
        self.thread.run_coro(self.client_impl.init())

    def __del__(self):
        del self.client_impl
        self.thread.stop()

    def set_verbosity_level(self, level):
        return self.thread.run_coro(self.client_impl.set_verbosity_level(level))

    def raw_get_transactions(self, account_address: str, from_transaction_lt: str, from_transaction_hash: str):
        return self.thread.run_coro(self.client_impl.raw_get_transactions(account_address, from_transaction_lt, from_transaction_hash))

    async def raw_get_account_state(self, address: str):
        return self.thread.run_coro(self.client_impl.raw_get_account_state(address))

    async def generic_get_account_state(self, address: str):
        return self.thread.run_coro(self.client_impl.generic_get_account_state(address))

    async def _load_contract(self, address):
        return self.thread.run_coro(self.client_impl._load_contract(address))

    async def raw_run_method(self, address, method, stack_data, output_layout=None):
        return self.thread.run_coro(self.client_impl.raw_run_method(address, method, stack_data, output_layout))

    async def raw_send_message(self, serialized_boc):
        return self.thread.run_coro(self.client_impl.raw_send_message(serialized_boc))

    async def _raw_create_query(self, destination, body, init_code=b'', init_data=b''):
        return self.thread.run_coro(self.client_impl._raw_create_query(destination, body, init_code, init_data))

    async def _raw_send_query(self, query_info):
        return self.thread.run_coro(self.client_impl._raw_send_query(query_info))
        
    async def raw_create_and_send_query(self, destination, body, init_code=b'', init_data=b''):
        return self.thread.run_coro(self.client_impl.raw_create_and_send_query(destination, body, init_code, init_data))

    async def raw_create_and_send_message(self, destination, body, initial_account_state=b''):
        return self.thread.run_coro(self.client_impl.raw_create_and_send_message(destination, body, initial_account_state))

    async def raw_estimate_fees(self, destination, body, init_code=b'', init_data=b'', ignore_chksig=True):
        return self.thread.run_coro(self.client_impl.raw_estimate_fees(destination, body, init_code, init_data, ignore_chksig))

    async def raw_getBlockTransactions(self, fullblock, count, after_tx):
        return self.thread.run_coro(self.client_impl.raw_getBlockTransactions(fullblock, count, after_tx))

    async def getTransactions(self, account_address, from_transaction_lt=None, from_transaction_hash=None, to_transaction_lt=0, limit=10):
        return self.thread.run_coro(self.client_impl.getTransactions(account_address, from_transaction_lt, from_transaction_hash, to_transaction_lt, limit))

    async def getMasterchainInfo(self):
        return self.thread.run_coro(self.client_impl.getMasterchainInfo())

    async def lookupBlock(self, workchain, shard, seqno=None, lt=None, unixtime=None):
        return self.thread.run_coro(self.client_impl.lookupBlock(workchain, shard, seqno, lt, unixtime))

    async def getShards(self, master_seqno=None, lt=None, unixtime=None):
        return self.thread.run_coro(self.client_impl.getShards(master_seqno, lt, unixtime))

    async def getBlockTransactions(self, workchain, shard, seqno, count, root_hash=None, file_hash=None, after_lt=None, after_hash=None):
        return self.thread.run_coro(self.client_impl.getBlockTransactions(workchain, shard, seqno, count, root_hash, file_hash, after_lt, after_hash))

    async def getBlockHeader(self, workchain, shard, seqno, root_hash=None, file_hash=None):
        return self.thread.run_coro(self.client_impl.getBlockHeader(workchain, shard, seqno, root_hash, file_hash))

    async def tryLocateTxByIncomingMessage(self, source, destination, creation_lt):
        return self.thread.run_coro(self.client_impl.tryLocateTxByIncomingMessage(source, destination, creation_lt))

    async def tryLocateTxByOutcomingMessage(self, source, destination, creation_lt):
        return self.thread.run_coro(self.client_impl.tryLocateTxByOutcomingMessage(source, destination, creation_lt))
