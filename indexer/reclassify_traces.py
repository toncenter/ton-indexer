#!/usr/bin/env python3
"""
Marks already classified traces as unclassified so that the running event classifier picks them
up again. It only flips traces.classification_state - the classifier deletes the stale actions of
a trace before writing new ones, so this is a full reclassification of the selected traces.

Made for retrofitting new action types onto history. The scopes below select the traces the
tg-wallet work touches:

  --tg-wallet    traces of accounts running the Telegram wallet code (change_wallet_key,
                 gasless_request, the corrected request opcode)
  --w5-gasless   traces carrying a wallet v5 internal signed request (gasless_request)
  --trace-id     one trace, repeatable - handy for checking a fix before a bulk run

Walks the messages table along an existing index and keeps a cursor per scope in a state file,
so a run can be interrupted and resumed. Counting first is recommended:

  python reclassify_traces.py --w5-gasless --dry-run
  python reclassify_traces.py --w5-gasless --batch-size 2000 --sleep 0.5

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


UPDATE_QUERY = """
    update traces set classification_state = 'unclassified'
    where trace_id = any(:trace_ids)
      and state = 'complete'
      and classification_state <> 'unclassified'
"""


def run_scope(session, name, where, params, args, state):
    """Walks one scope page by page, flipping the traces it finds."""
    cursor = state.get(name, {'ord': 0, 'hash': ''})
    query = text(page_query(where))
    seen_traces, flipped, pages = 0, 0, 0
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

        trace_ids = sorted({row[0] for row in rows})
        seen_traces += len(trace_ids)
        if not args.dry_run:
            result = session.execute(text(UPDATE_QUERY), {'trace_ids': trace_ids})
            session.commit()
            flipped += result.rowcount or 0

        cursor = {'ord': int(rows[-1][1]), 'hash': rows[-1][2]}
        state[name] = cursor
        save_state(args.state_file, state)

        pages += 1
        if pages % 20 == 0:
            logger.info('%s: %d pages, %d traces seen, %d flipped, cursor lt=%s',
                        name, pages, seen_traces, flipped, cursor['ord'])
        if args.limit and seen_traces >= args.limit:
            logger.info('%s: reached --limit', name)
            break
        if args.sleep:
            time.sleep(args.sleep)

    logger.info('%s: done, %d traces seen, %d flipped%s',
                name, seen_traces, flipped, ' (dry run)' if args.dry_run else '')
    return seen_traces, flipped


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
            if args.dry_run:
                logger.info('would flip %d explicitly listed traces', len(args.trace_id))
            else:
                result = session.execute(text(UPDATE_QUERY), {'trace_ids': args.trace_id})
                session.commit()
                logger.info('flipped %d of %d explicitly listed traces',
                            result.rowcount or 0, len(args.trace_id))

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
