#!/usr/bin/env python3
"""
Schedules already classified traces for reclassification by putting rows into _classifier_tasks,
the queue the event classifier actually reads. A task carrying a trace_id reclassifies that trace
unconditionally: the classifier deletes the stale actions of the trace before writing new ones.

Made for retrofitting new action types onto history. The scopes select the traces the tg-wallet
work touches:

  --tg-wallet    traces of accounts running the Telegram wallet code (change_wallet_key,
                 gasless_request, the corrected request opcode)
  --w5-gasless   traces carrying a wallet v5 internal signed request (gasless_request)
  --trace-id     one trace, repeatable - handy for checking a fix before a bulk run

Every task gets the mc_seqno of its trace. That is deliberate: the classifier claims tasks with
`order by mc_seqno desc nulls first`, so tasks without an mc_seqno would be served BEFORE the
live blocks and stall current indexing, while old seqnos are picked up only when the live queue
is idle. Scheduling is paced with start_after on top of that (--rate).

Counting first is recommended:

  python reclassify_traces.py --w5-gasless --dry-run
  python reclassify_traces.py --w5-gasless --rate 200 --max-queue 50000

The database comes from the same setting as the classifier: TON_INDEXER_PG_DSN.
"""
import argparse
import json
import logging
import os
import sys
import time

from sqlalchemy import text

from indexer.core.database import SyncSessionMaker

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger('reclassify')

# an immutable stub jumping to the wallet code stored in config[-123], so one hash covers
# every revision of the Telegram wallet
TG_WALLET_CODE_HASH = 'kUmuUcHkaJcQzr94MCl7Fqz7rbNjqSClN4k+f/7sp2g='
# wallet v5 signed request delivered as an internal message ('sint')
W5_SIGNED_REQUEST_INTERNAL = 0x73696E74

# messages_opcode_order_lt_v2_idx / messages_destination_order_lt_v2_idx are ordered by
# (coalesce(created_lt, tx_lt), msg_hash) - paging along that keeps every page an index scan
PAGE_ORDER = 'coalesce(m.created_lt, m.tx_lt)'

SCHEDULE_QUERY = """
    insert into _classifier_tasks (trace_id, mc_seqno, start_after)
    select t.trace_id, t.mc_seqno_end, now() + make_interval(secs => :delay)
    from traces t
    where t.trace_id = any(:trace_ids)
      and t.state = 'complete'
      and not exists (select 1 from _classifier_tasks c where c.trace_id = t.trace_id)
"""

QUEUE_DEPTH_QUERY = "select count(*) from _classifier_tasks where claimed_at is null"


def load_state(path):
    if path and os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_state(path, state):
    if not path:
        return
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(state, f)
    os.replace(tmp, path)


def tg_wallet_accounts(session):
    """Every account currently running the Telegram wallet code."""
    rows = session.execute(
        text('select account from latest_account_states where code_hash = :code_hash'),
        {'code_hash': TG_WALLET_CODE_HASH},
    ).fetchall()
    return [row[0] for row in rows]


def page_query(where: str) -> str:
    return f"""
        select m.trace_id, {PAGE_ORDER} as ord, m.msg_hash
        from messages m
        where {where}
          and m.trace_id is not null
          and ({PAGE_ORDER}, m.msg_hash) > (:last_ord, :last_hash)
          and (:from_utime is null or m.created_at >= :from_utime)
          and (:to_utime is null or m.created_at <= :to_utime)
        order by {PAGE_ORDER}, m.msg_hash
        limit :batch_size
    """


def wait_for_queue(session, args):
    """Keeps the backlog we add bounded, so live classification is not buried under it."""
    if not args.max_queue:
        return
    while True:
        depth = session.execute(text(QUEUE_DEPTH_QUERY)).scalar()
        if depth <= args.max_queue:
            return
        logger.info('queue depth %d above --max-queue %d, waiting', depth, args.max_queue)
        time.sleep(args.queue_poll)


def schedule(session, trace_ids, args, scheduled_so_far):
    if args.dry_run:
        return 0
    delay = scheduled_so_far / args.rate if args.rate else 0
    result = session.execute(text(SCHEDULE_QUERY), {'trace_ids': trace_ids, 'delay': delay})
    session.commit()
    return result.rowcount or 0


def run_scope(session, name, where, params, args, state):
    """Walks one scope page by page, scheduling every trace it finds."""
    cursor = state.get(name, {'ord': 0, 'hash': ''})
    query = text(page_query(where))
    seen_traces, scheduled, pages = 0, 0, 0
    while True:
        rows = session.execute(query, {
            **params,
            'last_ord': cursor['ord'],
            'last_hash': cursor['hash'],
            'from_utime': args.from_utime,
            'to_utime': args.to_utime,
            'batch_size': args.batch_size,
        }).fetchall()
        if not rows:
            break

        wait_for_queue(session, args)
        trace_ids = sorted({row[0] for row in rows})
        seen_traces += len(trace_ids)
        scheduled += schedule(session, trace_ids, args, scheduled)

        cursor = {'ord': int(rows[-1][1]), 'hash': rows[-1][2]}
        state[name] = cursor
        save_state(args.state_file, state)

        pages += 1
        if pages % 20 == 0:
            logger.info('%s: %d pages, %d traces seen, %d scheduled, cursor lt=%s',
                        name, pages, seen_traces, scheduled, cursor['ord'])
        if args.limit and seen_traces >= args.limit:
            logger.info('%s: reached --limit', name)
            break
        if args.sleep:
            time.sleep(args.sleep)

    logger.info('%s: done, %d traces seen, %d scheduled%s',
                name, seen_traces, scheduled, ' (dry run)' if args.dry_run else '')
    return seen_traces, scheduled


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--tg-wallet', action='store_true',
                        help='traces of accounts running the Telegram wallet code')
    parser.add_argument('--w5-gasless', action='store_true',
                        help='traces carrying a wallet v5 internal signed request')
    parser.add_argument('--trace-id', action='append', default=[],
                        help='reclassify one trace, repeatable')
    parser.add_argument('--from-utime', type=int, help='only messages created at or after this unix time')
    parser.add_argument('--to-utime', type=int, help='only messages created at or before this unix time')
    parser.add_argument('--batch-size', type=int, default=1000, help='messages read per page')
    parser.add_argument('--limit', type=int, help='stop after roughly this many traces per scope')
    parser.add_argument('--rate', type=float, default=100.0,
                        help='tasks per second to spread start_after over, 0 to schedule them all at once')
    parser.add_argument('--max-queue', type=int, default=100000,
                        help='pause while more than this many tasks are waiting, 0 to never wait')
    parser.add_argument('--queue-poll', type=float, default=10.0, help='seconds between queue depth checks')
    parser.add_argument('--sleep', type=float, default=0.1, help='pause between pages, seconds')
    parser.add_argument('--state-file', default='reclassify_traces.state.json',
                        help='where the per-scope cursor is kept, for resuming')
    parser.add_argument('--dry-run', action='store_true', help='only count, change nothing')
    args = parser.parse_args()

    if not (args.tg_wallet or args.w5_gasless or args.trace_id):
        parser.error('pick at least one of --tg-wallet, --w5-gasless, --trace-id')

    state = load_state(args.state_file)
    with SyncSessionMaker() as session:
        if args.trace_id:
            scheduled = schedule(session, args.trace_id, args, 0)
            logger.info('scheduled %d of %d explicitly listed traces%s',
                        scheduled, len(args.trace_id), ' (dry run)' if args.dry_run else '')

        if args.w5_gasless:
            run_scope(session, 'w5_gasless', 'm.opcode = :opcode',
                      {'opcode': W5_SIGNED_REQUEST_INTERNAL}, args, state)

        if args.tg_wallet:
            accounts = tg_wallet_accounts(session)
            logger.info('found %d accounts running the Telegram wallet code', len(accounts))
            for i, account in enumerate(accounts, 1):
                # every trace of such a wallet contains a message addressed to it: the request
                # itself, a top-up, or the external that deployed it
                run_scope(session, f'tg_wallet:{account}', 'm.destination = :account',
                          {'account': account}, args, state)
                if i % 50 == 0:
                    logger.info('tg-wallet: %d/%d accounts done', i, len(accounts))

    logger.info('finished')
    return 0


if __name__ == '__main__':
    sys.exit(main())
