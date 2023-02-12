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
from pytonlib.utils.tokens import *
from tvm_valuetypes import serialize_tvm_stack

from indexer.database import *
from indexer.crud import *

import uvloop

loop = None
index_worker = None

@worker_process_init.connect()
def configure_worker(signal=None, sender=None, **kwargs):
    global loop
    global index_worker
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    index_worker = IndexWorker(loop, 
                               0, 
                               use_ext_method=settings.indexer.use_ext_method, 
                               cdll_path=settings.indexer.cdll_path)

class InterfaceDetector:
    def __init__(self, client: TonlibClient):
        self.client = client

    async def detect_interfaces(self, mc_block: dict, address: str):
        load_smc_request = {
            '@type': 'withBlock',
            'id': mc_block,
            'function': {
                '@type': 'smc.load',
                'account_address': {
                    'account_address': address
                }
            }
        }
        load_smc_response = await self.client.tonlib_wrapper.execute(load_smc_request, timeout=self.client.tonlib_timeout)
        smc_id = load_smc_response['id']
        
        interfaces = await asyncio.gather(*[self.is_jetton_wallet(smc_id), self.is_jetton_master(smc_id), self.is_nft_item(smc_id), self.is_nft_collection(smc_id)])
        interfaces = [item for sublist in interfaces for item in sublist]

        return interfaces

    async def run_get_method(self, smc_id, method_name):
        method = {'@type': 'smc.methodIdName', 'name': method_name}
        request = {
            '@type': 'smc.runGetMethod',
            'id': smc_id,
            'method': method,
            'stack': []
        }
        r = await self.client.tonlib_wrapper.execute(request, timeout=self.client.tonlib_timeout)
        r['stack'] = serialize_tvm_stack(r['stack'])
        return r

    async def is_jetton_wallet(self, smc_id):
        r = await self.run_get_method(smc_id, 'get_wallet_data')
        if r['exit_code'] != 0:
            return []
        try:
            jetton_wallet_data = parse_jetton_wallet_data(r['stack'])
        except:
            return []
        return ["jetton_wallet"]

    async def is_jetton_master(self, smc_id):
        r = await self.run_get_method(smc_id, 'get_jetton_data')
        if r['exit_code'] != 0:
            return []
        try:
            jetton_master_data = parse_jetton_master_data(r['stack'])
        except:
            return []
        return ["jetton_master"]

    async def is_nft_item(self, smc_id):
        r = await self.run_get_method(smc_id, 'get_nft_data')
        if r['exit_code'] != 0:
            return []
        try:
            nft_item_data = parse_nft_item_data(r['stack'])
        except:
            return []
        interfaces = ["nft_item"]
        r = await self.run_get_method(smc_id, 'get_editor')
        if len(r['stack']) == 1:
            try:
                read_stack_cell(r['stack'][0])
                interfaces.append("nft_editable")
            except:
                pass
            
        return interfaces

    async def is_nft_collection(self, smc_id):
        r = await self.run_get_method(smc_id, 'get_collection_data')
        if r['exit_code'] != 0:
            return []
        try:
            nft_collection_data = parse_nft_collection_data(r['stack'])
        except:
            return []
        return ["nft_collection"]


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
                                   verbosity_level=3,
                                   tonlib_timeout=60)

        loop.run_until_complete(self.client.init())

    async def _get_block_with_shards(self, seqno):
        master_block = await self.client.lookup_block(MASTERCHAIN_INDEX, MASTERCHAIN_SHARD, seqno)
        # master_block.pop('@type')
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
    
    async def _get_code_hash(self, tx, tx_detail, mc_block):
        request = {
            '@type': 'withBlock',
            'id': mc_block,
            'function': {
                '@type': 'raw.getAccountState',
                'account_address': {
                    'account_address': tx['account']
                }
            }
        }
        acc_state = await self.client.tonlib_wrapper.execute(request, timeout=self.client.tonlib_timeout)
        tx_detail['code_hash'] = acc_state['code_hash']
        return tx, tx_detail

    async def _get_raw_info(self, seqno):
        blocks = await self._get_block_with_shards(seqno)
        headers = await asyncio.gather(*[self._get_block_header(block) for block in blocks])
        transactions = await asyncio.gather(*[self._get_block_transactions(block) for block in blocks])
        transactions = [await asyncio.gather(*[self._get_transaction_details(tx) for tx in txes]) for txes in transactions]
        # for tx_id, tx_details in transactions:
        #     tx_parsed = parse_tlb_object(tx_details["data"], Transaction)
        #     spec_actions = tx_parsed['description'].get('action', {}).get('spec_actions', 0)
        #     if spec_actions > 0:


        transactions = [await asyncio.gather(*[self._get_code_hash(tx, tx_detail, blocks[0]) for tx, tx_detail in txes]) for txes in transactions]

        return blocks, headers, transactions
    
    async def _get_raw_info_ext(self, seqno):
        blocks = await self._get_block_with_shards(seqno)
        headers = await asyncio.gather(*[self._get_block_header(block) for block in blocks])
        transactions = await asyncio.gather(*[self._get_block_transactions_ext(block) for block in blocks])
        transactions = [await asyncio.gather(*[self._get_code_hash(tx, tx_detail, blocks[0]) for tx, tx_detail in txes]) for txes in transactions]
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

    async def fetch_mc_seqno(self, seqno: int):
        blocks, headers, transactions = await self.get_raw_info(seqno)
        return blocks, headers, transactions

    async def insert_mc_seqno(self, seqno, bht):
        blocks, headers,  transactions = bht
        try:
            await insert_by_seqno_core(engine, blocks, headers, transactions)
            logger.info(f'Block(seqno={seqno}) inserted')
        except Exception as ee:
            logger.warning(f'Failed to insert block(seqno={seqno}): {traceback.format_exc()}')
            raise ee
        

    async def get_last_mc_block(self):
        mc_info = await self.client.get_masterchain_info()
        return mc_info['last']

    async def get_interfaces(self, mc_block, account, code_hash):
        detector = InterfaceDetector(self.client)
        interfaces = await detector.detect_interfaces(mc_block, account)
        return (code_hash, interfaces)

async def _get_block(mc_seqno_list):
    async with SessionMaker() as session:
        existing_seqnos = await get_existing_seqnos_from_list(session, mc_seqno_list)
    seqnos_to_process = [seqno for seqno in mc_seqno_list if seqno not in existing_seqnos]

    logger.info(f"{len(mc_seqno_list) - len(seqnos_to_process)} blocks already exist")
    return seqnos_to_process, await asyncio.gather(*[index_worker.process_mc_seqno(seqno) for seqno in seqnos_to_process], return_exceptions=True)
    
@app.task(bind=True, max_retries=None, acks_late=True)
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

async def _fetch_blocks(mc_seqno_list):
    # async with SessionMaker() as session:
    #     existing_seqnos = await get_existing_seqnos_from_list(session, mc_seqno_list)
    # seqnos_to_process = [seqno for seqno in mc_seqno_list if seqno not in existing_seqnos]
    seqnos_to_process = mc_seqno_list

    logger.info(f"{len(mc_seqno_list) - len(seqnos_to_process)} blocks already exist")
    bht = await asyncio.gather(*[index_worker.fetch_mc_seqno(seqno) for seqno in seqnos_to_process], return_exceptions=True)
    return list(zip(seqnos_to_process, bht))


@app.task(bind=True, max_retries=None, acks_late=True)
def fetch_blocks(self, mc_seqno_list):
    return loop.run_until_complete(_fetch_blocks(mc_seqno_list))

    # existing_seqnos = [seqno for seqno in mc_seqno_list if seqno not in seqnos_to_process]

    # overriding default retry logic
    # max_retry_count = 10
    # if self.request.retries < max_retry_count:
    #     for seqno, r in zip(seqnos_to_process, results):
    #         if isinstance(r, BaseException):
    #             logger.error(f'Processing seqno {seqno} raised exception: {r} while executing. Retrying {self.request.retries} / {max_retry_count}.')
    #             raise self.retry(exc=r, countdown=0.1 * self.request.retries)

    # task_result = list(zip(seqnos_to_process, results)) 
    # task_result += [(seqno, None) for seqno in existing_seqnos] # adding indexed seqnos from previous tries
    # return task_result

async def _insert_blocks(seqno_blocks_tuples):
    tasks = []
    seqnos_to_process = []
    for seqno, bht in seqno_blocks_tuples:
        tasks.append(index_worker.insert_mc_seqno(seqno, bht))
        seqnos_to_process.append(seqno)
    
    return zip(seqnos_to_process, await asyncio.gather(*tasks, return_exceptions=True))

@app.task(bind=True, max_retries=None, acks_late=True)
def insert_blocks(self, seqno_blocks_tuples):
    return loop.run_until_complete(_insert_blocks(seqno_blocks_tuples))



@app.task(acks_late=True)
def get_last_mc_block():
    return loop.run_until_complete(index_worker.get_last_mc_block())

async def _index_interfaces(seqno, bht):
    try:
        transactions = bht[2]
        code_hashes_accounts = {}
        for block_txs in transactions:
            for tx, tx_detail in block_txs:
                code_hashes_accounts[tx_detail['code_hash']] = tx['account']

        async with SessionMaker() as session:
            existing_code_hashes = await get_existing_code_hashes(session, code_hashes_accounts.keys())

        for hash in existing_code_hashes:
            code_hashes_accounts.pop(hash)

        if len(code_hashes_accounts) > 0:
            mc_block = bht[0][0]
            code_hash_interfaces = await asyncio.gather(*[index_worker.get_interfaces(mc_block, account, code_hash) for (code_hash, account) in code_hashes_accounts.items()])
            await insert_code_hash_interfaces_core(engine, code_hash_interfaces)
    except Exception as ee:
        logger.warning(f'Failed to index interfaces block(seqno={seqno}): {traceback.format_exc()}')
        raise ee

@app.task(acks_late=True)
def index_interfaces(seqno_bht: list):
    task = asyncio.gather(*[_index_interfaces(seqno, bht) for seqno, bht in seqno_bht], return_exceptions=True)
    results = loop.run_until_complete(task)

    task_result = list(zip(map(lambda x: x[0], seqno_bht), results)) 
    return task_result