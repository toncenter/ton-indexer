import asyncio
import sys

async def index_backward(start_seqno: int):
    # TODO:
    # 1. Find minimum seqno existing in db
    # 2. for seqno in xrange(min_existing_seqno, 0, -1):
    #        get all data about masterchain block
    #        put it to db
    pass

async def index_forward():

    pass

async def main(loop):
    parser = argparse.ArgumentParser()
    parser.add_argument('--direction', '-d', type=str)
    parser.add_argument('--startseqno', '-s', type=int)
    args = parser.parse_args()

    # TODO:
    # connect to postgres and create tables if don't exist

    if args.direction == 'forward':
        asyncio.ensure_future(index_forward(args.startseqno), loop=loop)
    elif args.direction == 'backward':
        asyncio.ensure_future(index_backward(args.startseqno), loop=loop)
    else:
        sys.exit("Direction can be either forward or backward.")

    # TODO:
    # task to print velocity of indexing

    while True:
        asyncio.sleep(1)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))