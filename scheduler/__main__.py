# import asyncio
# import sys

# async def index_backward(start_seqno: int):
#     # TODO:
#     # 1. Find minimum seqno existing in db
#     # 2. for seqno in xrange(min_existing_seqno, 0, -1):
#     #        get all data about masterchain block
#     #        put it to db
#     pass

# async def index_forward():

#     pass

# async def main(loop):
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--direction', '-d', type=str)
#     parser.add_argument('--startseqno', '-s', type=int)
#     args = parser.parse_args()

#     # TODO:
#     # connect to postgres and create tables if don't exist

#     if args.direction == 'forward':
#         asyncio.ensure_future(index_forward(args.startseqno), loop=loop)
#     elif args.direction == 'backward':
#         asyncio.ensure_future(index_backward(args.startseqno), loop=loop)
#     else:
#         sys.exit("Direction can be either forward or backward.")

#     # TODO:
#     # task to print velocity of indexing

#     while True:
#         asyncio.sleep(1)

# if __name__ == "__main__":
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(main(loop))

import time
from indexer.tasks import get_block
from config import settings

def main():
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
			print(r.get(), flush=True)
		current_seqno = current_seqno - parallel * settings.indexer.blocks_per_task



if __name__ == "__main__":
	main()