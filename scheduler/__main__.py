import time
import sys
from celery.signals import worker_ready
from indexer.celery import app
from indexer.tasks import get_block, get_last_mc_block
from indexer.database import init_database, delete_database
from config import settings
from loguru import logger

def forward_main(queue):
	delete_database() # TODO: Remove closer to production

	init_database()

	while True:
		try:
		    app.broker_connection().ensure_connection(max_retries=3)
		except Exception as ex:
		    logger.warning(f"Can't connect to celery broker. Trying again...")
		    time.sleep(3)
		    continue
		break

	while True:
		time.sleep(2)
		logger.info("Forward working")

def backward_main(queue):
	delete_database() # TODO: Remove closer to production

	init_database()

	while True:
		try:
		    app.broker_connection().ensure_connection(max_retries=3)
		except Exception as ex:
		    logger.warning(f"Can't connect to celery broker. Trying again...")
		    time.sleep(3)
		    continue
		break

	# TODO: 
	# 1. get shards of settings.indexer.init_mc_seqno and index mc and all shards
	# 2. for each shard query db to find already inserted blocks and exclude them from tasks

	parallel = settings.indexer.workers_count
	current_seqno = settings.indexer.init_mc_seqno
	start_time = time.time()
	while current_seqno > 0:
		print(f"Time: {time.time() - start_time} count: {settings.indexer.init_mc_seqno - current_seqno} seqno: {current_seqno}", flush=True)
		args = []
		for i in range(parallel):
			args.append(range(current_seqno - i * settings.indexer.blocks_per_task, 
				current_seqno - (i + 1) * settings.indexer.blocks_per_task, -1))
		results = []
		for i in range(parallel):
			results.append(get_block.apply_async([args[i]], serializer='pickle', queue=queue))

		for r in results:
			r.get()
		current_seqno = current_seqno - parallel * settings.indexer.blocks_per_task



if __name__ == "__main__":
	if sys.argv[1] == 'backward':
		backward_main(sys.argv[2])
	elif sys.argv[1] == 'forward':
		forward_main(sys.argv[2])
	else:
		raise Exception("Pass direction in argument: backward/forward")