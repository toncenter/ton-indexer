from indexer.celery import app
import asyncio
import sys
import json

from config import settings
from tApi.tonlib.client import TonlibClient
from indexer.models import BlockShort, BlockFull, Transaction, Message

MASTERCHAIN_INDEX = -1
MASTERCHAIN_SHARD = -9223372036854775808

class IndexWorker():
    def __init__(self, loop):
        with open(settings.indexer.liteserver_config, 'r') as f:
            tonlib_config = json.loads(f.read())
        self.client = TonlibClient(0, 
                      tonlib_config, 
                      './private/ton_keystore',
                      loop,
                      cdll_path=None,
                      verbosity_level=0)

        # loop.run_until_complete(self.client.init())

    async def process_mc_seqno(self, seqno: int):
        shards = await self.client.getShards(seqno)
        print(shards)
        mc_short_block = BlockShort({'workchain': MASTERCHAIN_INDEX, 'shard': MASTERCHAIN_SHARD, 'seqno': seqno})
        blocks = [mc_short_block] + [BlockShort(raw) for raw in shards['shards']]

        full_blocks = [await self._get_full_block(x) for x in blocks]
        for i, short_block in enumerate(blocks):
            block_transactions = await self._get_transactions(short_block)
            full_blocks[i].transactions = block_transactions

        # TODO: _add_to_db(full_blocks)

        return full_blocks
        

    async def _get_full_block(self, short_block: BlockShort):
        block_header = await self.client.getBlockHeader(workchain=short_block.workchain,
                                                      shard=short_block.shard,
                                                      seqno=short_block.seqno)

        return BlockFull(block_header)


    async def _get_transactions(self, short_block: BlockShort) -> list:
        transactions = []
        txs = await self.client.getBlockTransactions(short_block.workchain,
                                                short_block.shard,
                                                short_block.seqno,
                                                count=1024)
        assert txs['incomplete'] is False, 'Txs is incomplete' #TODO implement pagination
        transactions += txs['transactions']

        full_transactions = []
        for tx in transactions:
            tx_full = await self.client.getTransactions(account_address=tx['account'], 
                                                   from_transaction_lt=tx['lt'], 
                                                   from_transaction_hash=tx['hash'],
                                                   limit=1)
            tx_full = tx_full[0]
            assert tx['hash'] == tx_full['transaction_id']['hash']
            full_transactions.append(Transaction(tx['account'], tx_full))

        print('Num transactions:', len(transactions))
        return full_transactions 

async def x():
    loop = asyncio.get_running_loop()
    worker = IndexWorker(loop)
    await worker.client.init()
    await worker.process_mc_seqno(19739805)

@app.task()
def get_block():
    seqno = 19739805
    asyncio.run(x())
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(x(loop))
    # loop.run_until_complete()

