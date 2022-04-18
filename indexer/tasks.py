from celery.signals import worker_process_init
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

loop = None
index_worker = None

@worker_process_init.connect()
def configure_worker(signal=None, sender=None, **kwargs):
    global loop
    global index_worker
    loop = asyncio.get_event_loop()
    index_worker = IndexWorker(loop, 0)

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

    async def _get_block_with_shards(self, seqno):
        master_block = await self.client.lookupBlock(MASTERCHAIN_INDEX, MASTERCHAIN_SHARD, seqno)
        master_block.pop('@type')
        master_block.pop('@extra')
        
        shards_blocks = []
        shards = await self.client.getShards(seqno)
        shards = shards['shards']
        if seqno > 0:
            prev_shards = await self.client.getShards(seqno - 1)
            prev_shards = prev_shards['shards']
            if len(prev_shards) == len(shards):
                for i, shard in enumerate(shards):
                    prev_shard_seqno = prev_shards[i]['seqno']
                    cur_shard_seqno = shard['seqno']
                    shards_blocks += await asyncio.gather(*[self.client.lookupBlock(shard['workchain'], shard['shard'], seqno) 
                                                                for seqno in range(prev_shard_seqno + 1, cur_shard_seqno + 1)])

        blocks = [master_block] + [
            {'workchain': s['workchain'],
            'shard': int(s['shard']),
            'seqno': s['seqno'],
            'root_hash': s['root_hash'],
            'file_hash': s['file_hash']} for s in shards_blocks
        ]
        return blocks

    async def _get_block_header(self, block):
        return await self.client.getBlockHeader(block['workchain'],
                                                block['shard'],
                                                block['seqno'])

    async def _get_block_transactions(self, block):
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

    async def _get_transaction_details(self, tx):
        tx_full = await self.client.getTransactions(account_address=tx['account'], 
                                                    from_transaction_lt=tx['lt'], 
                                                    from_transaction_hash=tx['hash'],
                                                    limit=1)
        return tx, tx_full[0]
    
    async def _get_raw_info(self, seqno):
        blocks = await self._get_block_with_shards(seqno)
        headers = await asyncio.gather(*[self._get_block_header(block) for block in blocks])
        transactions = await asyncio.gather(*[self._get_block_transactions(block) for block in blocks])
        transactions = [await asyncio.gather(*[self._get_transaction_details(tx) for tx in txes]) for txes in transactions]
        return blocks, headers, transactions

    async def process_mc_seqno(self, seqno: int):
        blocks, headers, transactions = await self._get_raw_info(seqno)
        session = get_session()()

        with session.begin():
            try:
                insert_by_seqno(session, blocks, headers, transactions)
                logger.info(f'Block(seqno={seqno}) inserted')
            except Exception as ee:
                logger.warning(f'Failed to insert block(seqno={seqno}): {traceback.format_exc()}')

    async def get_last_mc_block(self):
        mc_info = await self.client.getMasterchainInfo()
        return mc_info['last']

    async def get_shards(self, mc_seqno):
        return await self.client.getShards(mc_seqno)


@app.task(autoretry_for=(Exception,), retry_kwargs={'max_retries': 7, 'countdown': 0.5}, acks_late=True)
def get_block(mc_seqno_list):
    session = get_session()()
    with session.begin():
        non_exist = list(filter(lambda x: not mc_block_exists(session, x), mc_seqno_list))

    logger.info(f"{len(mc_seqno_list) - len(non_exist)} blocks already exist")
    gathered = asyncio.gather(*[index_worker.process_mc_seqno(seqno) for seqno in non_exist], return_exceptions=True)
    results = loop.run_until_complete(gathered)
    for r in results:
        if isinstance(r, Exception):
            raise Exception("At least one block in task failed")
    return results

@app.task(autoretry_for=(Exception,), retry_kwargs={'max_retries': 7, 'countdown': 0.5}, acks_late=True)
def get_last_mc_block():
    return loop.run_until_complete(index_worker.get_last_mc_block())

