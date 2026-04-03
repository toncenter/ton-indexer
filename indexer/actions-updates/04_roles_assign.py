import argparse
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import asyncpg

from indexer.core.settings import Settings
from indexer.core.database import (
    ECON_OUT, ECON_IN, ECON_BOTH, INITIATOR, INIT_OUT, INIT_BOTH, OBSERVER,
)
from indexer.events.blocks.utils.role_assignment import (
    _custom_role_functions,
    _default_role_assignment,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

_nft_participating_types = {
    'nft_transfer', 'nft_purchase', 'dns_purchase', 'nft_mint',
    'nft_put_on_sale', 'nft_put_on_auction', 'teleitem_start_auction',
    'nft_cancel_sale', 'nft_cancel_auction', 'nft_finish_auction', 'teleitem_cancel_auction',
    'nft_discovery', 'dns_release',
}


@dataclass
class ActionProxy:
    type: str = ''
    source: Optional[str] = None
    source_secondary: Optional[str] = None
    destination: Optional[str] = None
    destination_secondary: Optional[str] = None
    asset: Optional[str] = None
    asset_secondary: Optional[str] = None
    value: Optional[int] = None
    amount: Optional[int] = None
    accounts: list = field(default_factory=list)
    account_roles: dict = field(default_factory=dict)

    jetton_swap_data: Optional[dict] = None
    layerzero_send_data: Optional[dict] = None
    layerzero_dvn_verify_data: Optional[dict] = None
    staking_data: Optional[dict] = None


def compute_roles(action: ActionProxy, trace_initiator: str | None = None) -> dict[str, int]:
    handler = _custom_role_functions.get(action.type, _default_role_assignment)
    roles = handler(action)

    if trace_initiator is not None and trace_initiator in roles:
        roles[trace_initiator] |= INITIATOR

    # nft item always gets ECON_BOTH regardless of direction
    if action.type in _nft_participating_types and action.asset_secondary and action.asset_secondary in roles:
        roles[action.asset_secondary] = ECON_BOTH

    return roles


ACTIONS_BATCH_QUERY = """
SELECT
    a.action_id,
    a.trace_id,
    a.type,
    a.source,
    a.source_secondary,
    a.destination,
    a.destination_secondary,
    a.asset,
    a.asset_secondary,
    a.value,
    a.amount,
    a.trace_end_lt,
    (a.jetton_swap_data).dex_incoming_transfer,
    (a.jetton_swap_data).dex_outgoing_transfer,
    (a.layerzero_send_data).endpoint AS lz_endpoint,
    (a.layerzero_dvn_verify_data).dvn AS lz_dvn,
    (a.staking_data).provider AS staking_provider
FROM actions a
WHERE a.trace_end_lt <= $1
  AND a.end_lt IS NOT NULL
  {type_filter}
ORDER BY a.trace_end_lt DESC, a.action_id DESC
LIMIT $2
"""

MAX_LT_QUERY = """
SELECT MAX(trace_end_lt) FROM actions WHERE end_lt IS NOT NULL
"""

# trace_id is the root tx hash
TRACE_INITIATORS_QUERY = """
SELECT hash, account FROM transactions WHERE hash = ANY($1)
"""

ACTION_ACCOUNTS_QUERY = """
SELECT trace_id, action_id, account
FROM action_accounts
WHERE trace_id = ANY($1)
"""

UPDATE_ROLE_QUERY = """
UPDATE action_accounts
SET role = $1
WHERE trace_id = $2 AND action_id = $3 AND account = $4
"""


def _to_dict(t):
    if t is None:
        return {}
    return t if isinstance(t, dict) else dict(t)


def build_swap_data(inc_transfer, out_transfer) -> Optional[dict]:
    if inc_transfer is None and out_transfer is None:
        return None
    return {
        'dex_incoming_transfer': _to_dict(inc_transfer),
        'dex_outgoing_transfer': _to_dict(out_transfer),
    }


def build_action_proxy(row) -> ActionProxy:
    proxy = ActionProxy(
        type=row['type'],
        source=row['source'],
        source_secondary=row['source_secondary'],
        destination=row['destination'],
        destination_secondary=row['destination_secondary'],
        asset=row['asset'],
        asset_secondary=row['asset_secondary'],
        value=row['value'],
        amount=row['amount'],
    )

    proxy.jetton_swap_data = build_swap_data(
        row['dex_incoming_transfer'],
        row['dex_outgoing_transfer'],
    )

    if row['lz_endpoint'] is not None:
        proxy.layerzero_send_data = {'endpoint': row['lz_endpoint']}

    if row['lz_dvn'] is not None:
        proxy.layerzero_dvn_verify_data = {'dvn': row['lz_dvn']}

    if row['staking_provider'] is not None:
        proxy.staking_data = {'provider': row['staking_provider']}

    return proxy


async def process_batch(conn, offset_lt, batch_size, type_filter, dry_run):
    query = ACTIONS_BATCH_QUERY.format(type_filter=type_filter)
    rows = await conn.fetch(query, offset_lt, batch_size)
    if not rows:
        return 0, 0, offset_lt

    trace_ids = list(set(row['trace_id'] for row in rows))
    initiator_rows = await conn.fetch(TRACE_INITIATORS_QUERY, trace_ids)
    trace_initiators = {r['hash']: r['account'] for r in initiator_rows}

    aa_rows = await conn.fetch(ACTION_ACCOUNTS_QUERY, trace_ids)
    aa_map = {}
    for r in aa_rows:
        key = (r['trace_id'], r['action_id'])
        if key not in aa_map:
            aa_map[key] = []
        aa_map[key].append(r['account'])

    processed = 0
    update_args = []

    for row in rows:
        accounts = aa_map.get((row['trace_id'], row['action_id']))
        if not accounts:
            continue

        proxy = build_action_proxy(row)
        proxy.accounts = accounts
        trace_initiator = trace_initiators.get(row['trace_id'])
        for account, role in compute_roles(proxy, trace_initiator).items():
            update_args.append((role, row['trace_id'], row['action_id'], account))
        processed += 1

    if update_args and not dry_run:
        await conn.executemany(UPDATE_ROLE_QUERY, update_args)

    last_lt = rows[-1]['trace_end_lt']  # smallest in batch (DESC order)
    return processed, len(update_args), last_lt


async def main():
    parser = argparse.ArgumentParser(description='Populate action_accounts roles from action fields')
    parser.add_argument('--batch-size', type=int, default=1000, help='Actions per batch')
    parser.add_argument('--sleep', type=float, default=0.1, help='Seconds between batches')
    parser.add_argument('--limit', type=int, default=0, help='Max actions (0=unlimited)')
    parser.add_argument('--offset-lt', type=int, default=0, help='Start from this trace_end_lt (0 = max in DB)')
    parser.add_argument('--action-type', action='append', dest='action_types', help='Filter by type (repeatable)')
    parser.add_argument('--dry-run', action='store_true', help='Compute roles but do not write to DB')
    args = parser.parse_args()

    type_filter = ''
    if args.action_types:
        types_str = ', '.join(f"'{t}'" for t in args.action_types)
        type_filter = f'AND a.type IN ({types_str})'

    s = Settings()
    dsn = s.pg_dsn.replace('postgresql+asyncpg://', 'postgresql://')

    logger.info("Connecting to database...")
    conn = await asyncpg.connect(dsn)

    offset_lt = args.offset_lt
    if offset_lt == 0:
        offset_lt = await conn.fetchval(MAX_LT_QUERY)
        if offset_lt is None:
            logger.info("No actions found in DB")
            await conn.close()
            return
        logger.info(f"Starting from max lt: {offset_lt}")

    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'UPDATE'}, "
                 f"batch_size={args.batch_size}, sleep={args.sleep}s")

    total_processed = 0
    total_updated = 0
    batch_num = 0
    start_time = time.time()

    try:
        while True:
            batch_num += 1
            processed, updated, last_lt = await process_batch(
                conn, offset_lt, args.batch_size, type_filter, args.dry_run
            )

            if processed == 0:
                break

            total_processed += processed
            total_updated += updated

            offset_lt = last_lt - 1

            elapsed = time.time() - start_time
            rate = total_processed / elapsed if elapsed > 0 else 0

            if batch_num % 10 == 0:
                logger.info(f"#{batch_num}: {total_processed} actions, {total_updated} roles, "
                             f"{rate:.0f}/s, resume: --offset-lt {offset_lt}")

            if args.limit and total_processed >= args.limit:
                logger.info(f"Reached limit of {args.limit}")
                break

            if args.sleep > 0:
                await asyncio.sleep(args.sleep)

    except KeyboardInterrupt:
        logger.info("Interrupted")
    except Exception:
        logger.exception("Failed")
    finally:
        await conn.close()
        elapsed = time.time() - start_time
        rate = total_processed / elapsed if elapsed > 0 else 0
        logger.info(f"Done: {total_processed} actions, {total_updated} roles, "
                     f"{elapsed:.1f}s ({rate:.0f}/s), resume: --offset-lt {offset_lt}")


if __name__ == '__main__':
    asyncio.run(main())
