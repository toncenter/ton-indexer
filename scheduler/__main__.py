import time
import sys
from celery.signals import worker_ready
from indexer.celery import app
from indexer.tasks import get_block, get_last_mc_block
from indexer.database import init_database, delete_database
from config import settings
from loguru import logger

def forward_main(queue):
	# delete_database() # TODO: Remove closer to production
	init_database()

	while True:
		try:
		    app.broker_connection().ensure_connection(max_retries=3)
		except Exception as ex:
		    logger.warning(f"Can't connect to celery broker. Trying again...")
		    time.sleep(3)
		    continue
		break

	current_seqno = settings.indexer.init_mc_seqno + 1
	while True:
		last_mc_block = get_last_mc_block.apply_async([], serializer='pickle', queue=queue).get()
		if last_mc_block['seqno'] < current_seqno:
			time.sleep(0.2)
			continue

		for seqno in range(current_seqno, last_mc_block['seqno'] + 1):
			get_block.apply_async([[seqno]], serializer='pickle', queue=queue).get()

		current_seqno = last_mc_block['seqno'] + 1

		time.sleep(0.2)
		logger.info(f"Current seqno: {current_seqno}")

def backward_main(queue):
	# delete_database() # TODO: Remove closer to production

	init_database()

	while True:
		try:
		    app.broker_connection().ensure_connection(max_retries=3)
		except Exception as ex:
		    logger.warning(f"Can't connect to celery broker. Trying again...")
		    time.sleep(3)
		    continue
		break

	parallel = settings.indexer.workers_count
	current_seqno = settings.indexer.init_mc_seqno
	start_time = time.time()

	tasks_in_progress = []
	while current_seqno >= 0:
		tasks_in_progress = [task for task in tasks_in_progress if not task.ready()]
		if len(tasks_in_progress) >= parallel:
			time.sleep(0.05)
			continue

		bottom_bound = max(current_seqno - settings.indexer.blocks_per_task + 1, 0)
		next_chunk = range(current_seqno, bottom_bound - 1, -1)

		tasks_in_progress.append(get_block.apply_async([next_chunk], serializer='pickle', queue=queue))
		current_seqno = bottom_bound - 1

		print(f"Time: {time.time() - start_time} count: {settings.indexer.init_mc_seqno - current_seqno} seqno: {current_seqno}", flush=True)

if __name__ == "__main__":
	if sys.argv[1] == 'backward':
		backward_main(sys.argv[2])
	elif sys.argv[1] == 'forward':
		forward_main(sys.argv[2])
	else:
		raise Exception("Pass direction in argument: backward/forward")
