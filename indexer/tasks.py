from celery.signals import worker_ready
from indexer.celery import app
import asyncio
import sys
import os
import json
import traceback

from pathlib import Path

from config import settings
from tApi.tonlib.client import TonlibClient

from indexer.database import *


MASTERCHAIN_INDEX = -1
MASTERCHAIN_SHARD = -9223372036854775808

index_worker = None

# @worker_ready.connect
# def worker_ready():
#     global index_worker
#     index_worker = IndexWorker(loop, 0)

class IndexWorker():
    def __init__(self, loop, ls_index):
        with open(settings.indexer.liteserver_config, 'r') as f:
            tonlib_config = json.loads(f.read())
        
        keystore = f'./private/ton_keystore_{ls_index}'
        Path(keystore).mkdir(parents=True, exist_ok=True)

        self.client = TonlibClient(ls_index, 
                      tonlib_config, 
                      keystore,
                      loop,
                      cdll_path=None,
                      verbosity_level=0)

        loop.run_until_complete(self.client.init())

    async def get_block_with_shards(self, seqno):
        master_block = await self.client.lookupBlock(MASTERCHAIN_INDEX, MASTERCHAIN_SHARD, seqno)
        master_block.pop('@type')
        master_block.pop('@extra')
        master_block['masterchain_seqno'] = seqno
        
        shards = await self.client.getShards(seqno)
        blocks = [master_block] + [
            {'workchain': s['workchain'],
            'shard': int(s['shard']),
            'seqno': s['seqno'],
            'root_hash': s['root_hash'],
            'file_hash': s['file_hash'],
            'masterchain_seqno': seqno} for s in shards['shards']
        ]
        return blocks

    async def get_block_header(self, block):
        return await self.client.getBlockHeader(block['workchain'],
                                                block['shard'],
                                                block['seqno'])

    async def get_block_transactions(self, block):
        txs = []
        after_lt = None
        after_hash = None
        incomplete = True
        # TODO: check this with incomplete == True
        while incomplete:
            loc = await self.client.getBlockTransactions(block['workchain'],
                                                         block['shard'],
                                                         block['seqno'],
                                                         count=1024,
                                                         after_lt=after_lt,
                                                         after_hash=after_hash,)
            incomplete = loc['incomplete']
            if len(loc['transactions']):
                after_lt = loc['transactions'][-1]['lt']
                after_hash = loc['transactions'][-1]['hash']
            if incomplete:
                logger.warning('Txs is incomplete block(workchain={workchain}, shard={shard}, seqno={seqno})'.format(**block))
            txs.extend(loc['transactions'])
        return txs
    async def get_transaction_details(self, tx):
        tx_full = await self.client.getTransactions(account_address=tx['account'], 
                                                    from_transaction_lt=tx['lt'], 
                                                    from_transaction_hash=tx['hash'],
                                                    limit=1)
        return tx, tx_full[0]
    
    async def get_raw_info(self, seqno):
        blocks = await self.get_block_with_shards(seqno)
        headers = await asyncio.gather(*[self.get_block_header(block) for block in blocks])
        transactions = await asyncio.gather(*[self.get_block_transactions(block) for block in blocks])
        transactions = [await asyncio.gather(*[self.get_transaction_details(tx) for tx in txes]) for txes in transactions]
        return blocks, headers, transactions

    async def process_mc_seqno(self, seqno: int):
        blocks, headers, transactions = await self.get_raw_info(seqno)
        session = get_session()()

        with session.begin():
            try:
                insert_by_seqno(session, blocks, headers, transactions)
                logger.info(f'Block(seqno={seqno}) inserted')
            except Exception as ee:
                logger.warning(f'Failed to insert block(seqno={seqno}): {traceback.format_exc()}')

@app.task()
def get_block(mc_seqno_list):
    loop = asyncio.get_event_loop()
    worker = IndexWorker(loop, 0)
    gathered = asyncio.gather(*[worker.process_mc_seqno(seqno) for seqno in mc_seqno_list])
    return loop.run_until_complete(gathered)

