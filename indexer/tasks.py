from celery.signals import worker_process_init
from indexer.celery import app
import asyncio
import sys
import os
import json
import traceback

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
                               use_ext_method=settings.use_ext_method, 
                               cdll_path=settings.cdll_path)

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
                                   verbosity_level=0)

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
            if len(prev_shards) == len(shards):
                for i, shard in enumerate(shards):
                    prev_shard_seqno = prev_shards[i]['seqno']
                    cur_shard_seqno = shard['seqno']
                    shards_blocks += await asyncio.gather(*[self.client.lookup_block(shard['workchain'], shard['shard'], seqno) 
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
        try:
            blocks, headers, transactions = await self.get_raw_info(seqno)
        except Exception as e:
            logger.warning(f'Failed to fetch block(seqno={seqno}): {traceback.format_exc()}')
            raise e
        session = get_session()()

        with session.begin():
            try:
                insert_by_seqno(session, blocks, headers, transactions)
                logger.info(f'Block(seqno={seqno}) inserted')
            except Exception as ee:
                logger.warning(f'Failed to insert block(seqno={seqno}): {traceback.format_exc()}')
                raise ee

    async def get_last_mc_block(self):
        mc_info = await self.client.get_masterchain_info()
        return mc_info['last']

    async def get_shards(self, mc_seqno):
        return await self.client.get_shards(mc_seqno)


@app.task(autoretry_for=(Exception,), retry_kwargs={'max_retries': 7, 'countdown': 0.5})
def get_block(mc_seqno_list):
    session = get_session()()
    with session.begin():
        existing_seqnos = get_existing_seqnos_from_list(session, mc_seqno_list)
    non_exist = [seqno for seqno in mc_seqno_list if seqno not in existing_seqnos]

    logger.info(f"{len(mc_seqno_list) - len(non_exist)} blocks already exist")
    gathered = asyncio.gather(*[index_worker.process_mc_seqno(seqno) for seqno in non_exist], return_exceptions=True)
    results = loop.run_until_complete(gathered)
    for r in results:
        if isinstance(r, BaseException):
            raise Exception(f"At least one block in task failed {r}")
    return results

@app.task(autoretry_for=(Exception,), retry_kwargs={'max_retries': 7, 'countdown': 0.5}, acks_late=True)
def get_last_mc_block():
    return loop.run_until_complete(index_worker.get_last_mc_block())

