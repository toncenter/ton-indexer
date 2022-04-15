import time
from indexer.tasks import get_block
from indexer.database import init_database, delete_database
from config import settings


def main():
	time.sleep(5)
	delete_database()
	time.sleep(5)
	init_database()
	time.sleep(5)

	start_seqno = 19739805
	parallel = settings.indexer.workers_count
	current_seqno = start_seqno
	start_time = time.time()
	while current_seqno > 0:
		print(f"Time: {time.time() - start_time} count: {start_seqno - current_seqno}", flush=True)
		print(f"Seqno: {current_seqno}", flush=True)
		args = []
		for i in range(parallel):
			args.append(range(current_seqno - i * settings.indexer.blocks_per_task, 
				current_seqno - (i + 1) * settings.indexer.blocks_per_task, -1))
		results = []
		for i in range(parallel):
			results.append(get_block.apply_async([args[i]], serializer='pickle'))

		for r in results:
			r.get()
		current_seqno = current_seqno - parallel * settings.indexer.blocks_per_task



if __name__ == "__main__":
	main()