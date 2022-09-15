from celery.signals import worker_process_init
from indexer.celery import app
import asyncio
import sys
import os
import json
import traceback
import queue

from pathlib import Path

from config import settings
from pytonlib import TonlibClient

from indexer.database import *
from indexer.crud import *


loop = None
index_worker = None

@worker_process_init.connect()
def configure_worker(signal=None, sender=None, **kwargs):
    global loop
    global index_worker
    loop = asyncio.get_event_loop()
    index_worker = IndexWorker(loop, 
                               0, 
                               use_ext_method=settings.indexer.use_ext_method, 
                               cdll_path=settings.indexer.cdll_path)

class IndexWorker():
    def __init__(self, loop, ls_index, use_ext_method=False, cdll_path=None):
        with open(settings.indexer.liteserver_config, 'r') as f:
            tonlib_config = json.loads(f.read())
        
        keystore = f'./private/ton_keystore_{ls_index}'
        Path(keystore).mkdir(parents=True, exist_ok=True)
    
        self.use_ext_method = use_ext_method

        self.client = TonlibClient(ls_index, 
                                   tonlib_config, 
                                   keystore,
                                   loop,
                                   cdll_path=cdll_path,
                                   verbosity_level=0,
                                   tonlib_timeout=60)

        loop.run_until_complete(self.client.init())

    async def _get_block_with_shards(self, seqno):
        master_block = await self.client.lookup_block(MASTERCHAIN_INDEX, MASTERCHAIN_SHARD, seqno)
        master_block.pop('@type')
        master_block.pop('@extra')
        
        shards_blocks = []
        shards = await self.client.get_shards(seqno)
        shards = shards['shards']
        if seqno > 0:
            prev_shards = await self.client.get_shards(seqno - 1)
            prev_shards = prev_shards['shards']

            shards_queue = queue.SimpleQueue()
            for s in shards:
                shards_queue.put(s)
            while not shards_queue.empty():
                cur_shard_block = shards_queue.get_nowait()
                if cur_shard_block in prev_shards:
                    continue
                if cur_shard_block in shards_blocks:
                    continue
                shards_blocks.append(cur_shard_block)
                cur_shard_block_header = await self.client.get_block_header(cur_shard_block['workchain'],
                                                                            cur_shard_block['shard'],
                                                                            cur_shard_block['seqno'])
                for prev_block in cur_shard_block_header['prev_blocks']:
                    shards_queue.put(prev_block)

        blocks = [master_block] + [
            {'workchain': s['workchain'],
            'shard': int(s['shard']),
            'seqno': s['seqno'],
            'root_hash': s['root_hash'],
            'file_hash': s['file_hash']} for s in shards_blocks
        ]
        return blocks

    async def _get_block_header(self, block):
        return await self.client.get_block_header(block['workchain'],
                                                  block['shard'],
                                                  block['seqno'])

    async def _get_block_transactions(self, block):
        txs = []
        after_lt = None
        after_hash = None
        incomplete = True
        # TODO: check this with incomplete == True
        while incomplete:
            loc = await self.client.get_block_transactions(block['workchain'],
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
    
    def _patch_msg(self, msg):
        msg['source'] = msg['source']['account_address']
        msg['destination'] = msg['destination']['account_address']
        return
    
    def _extract_tx(self, tx):
        tx_info = {
            '@type': 'blocks.shortTxId',
            'mode': 135,
            'account': tx['account'],
            'lt': tx['transaction_id']['lt'],
            'hash': tx['transaction_id']['hash'],
        }

        tx.pop('address')
        tx.pop('account')
        if 'in_msg' in tx:
            self._patch_msg(tx['in_msg'])
        if 'out_msgs' in tx:
            for msg in tx['out_msgs']:
                self._patch_msg(msg)
        return tx_info, tx
    
    async def _get_block_transactions_ext(self, block):
        txs = []
        after_lt = None
        after_hash = None
        incomplete = True
        # TODO: check this with incomplete == True
        while incomplete:
            loc = await self.client.get_block_transactions_ext(block['workchain'],
                                                               block['shard'],
                                                               block['seqno'],
                                                               count=1024,
                                                               after_lt=after_lt,
                                                               after_hash=after_hash)
            incomplete = loc['incomplete']
            if len(loc['transactions']):
                after_lt = loc['transactions'][-1]['transaction_id']['lt']
                after_hash = loc['transactions'][-1]['transaction_id']['hash']
            if incomplete:
                logger.warning('Txs is incomplete block(workchain={workchain}, shard={shard}, seqno={seqno})'.format(**block))
            txs.extend(loc['transactions'])
            
        txs_result = [self._extract_tx(tx) for tx in txs]
        return txs_result

    async def _get_transaction_details(self, tx):
        tx_full = await self.client.get_transactions(account=tx['account'], 
                                                     from_transaction_lt=tx['lt'], 
                                                     from_transaction_hash=tx['hash'],
                                                     limit=1,
                                                     decode_messages=False)
        return tx, tx_full[0]
    
    async def _get_raw_info(self, seqno):
        blocks = await self._get_block_with_shards(seqno)
        headers = await asyncio.gather(*[self._get_block_header(block) for block in blocks])
        transactions = await asyncio.gather(*[self._get_block_transactions(block) for block in blocks])
        transactions = [await asyncio.gather(*[self._get_transaction_details(tx) for tx in txes]) for txes in transactions]
        return blocks, headers, transactions
    
    async def _get_raw_info_ext(self, seqno):
        blocks = await self._get_block_with_shards(seqno)
        headers = await asyncio.gather(*[self._get_block_header(block) for block in blocks])
        transactions = await asyncio.gather(*[self._get_block_transactions_ext(block) for block in blocks])
        return blocks, headers, transactions
    
    def get_raw_info(self, seqno):
        if self.use_ext_method:
            return self._get_raw_info_ext(seqno)
        return self._get_raw_info(seqno)

    async def process_mc_seqno(self, seqno: int):
        blocks, headers, transactions = await self.get_raw_info(seqno)
        try:
            await insert_by_seqno_core(engine, blocks, headers, transactions)
            logger.info(f'Block(seqno={seqno}) inserted')
        except Exception as ee:
            logger.warning(f'Failed to insert block(seqno={seqno}): {traceback.format_exc()}')
            raise ee

    async def get_last_mc_block(self):
        mc_info = await self.client.get_masterchain_info()
        return mc_info['last']

async def _get_block(mc_seqno_list):
    async with SessionMaker() as session:
        existing_seqnos = await get_existing_seqnos_from_list(session, mc_seqno_list)
    seqnos_to_process = [seqno for seqno in mc_seqno_list if seqno not in existing_seqnos]

    logger.info(f"{len(mc_seqno_list) - len(seqnos_to_process)} blocks already exist")
    return seqnos_to_process, await asyncio.gather(*[index_worker.process_mc_seqno(seqno) for seqno in seqnos_to_process], return_exceptions=True)
    
@app.task(bind=True, max_retries=None,  acks_late=True)
def get_block(self, mc_seqno_list):
    seqnos_to_process, results = loop.run_until_complete(_get_block(mc_seqno_list))

    existing_seqnos = [seqno for seqno in mc_seqno_list if seqno not in seqnos_to_process]

    # overriding default retry logic
    max_retry_count = 10
    if self.request.retries < max_retry_count:
        for seqno, r in zip(seqnos_to_process, results):
            if isinstance(r, BaseException):
                logger.error(f'Processing seqno {seqno} raised exception: {r} while executing. Retrying {self.request.retries} / {max_retry_count}.')
                raise self.retry(exc=r, countdown=0.1 * self.request.retries)

    task_result = list(zip(seqnos_to_process, results)) 
    task_result += [(seqno, None) for seqno in existing_seqnos] # adding indexed seqnos from previous tries
    return task_result

@app.task(acks_late=True)
def get_last_mc_block():
    return loop.run_until_complete(index_worker.get_last_mc_block())

