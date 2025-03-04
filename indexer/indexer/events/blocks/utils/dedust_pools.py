import json

import requests

from indexer.events import context
from indexer.events.blocks.utils import AccountId

POOLS_URL = 'https://api.dedust.io/v2/pools'
AGENT = 'Chrome'
FALLBACK_FILENAME = 'dedust_pools.json'

def init_pools_data():
    try:
        response = requests.get(POOLS_URL, headers={'User-Agent': AGENT})
        response.raise_for_status()
        pools_raw = response.json()
        if len(pools_raw) > 0:
            dedust_pools = parse_raw_pools_data(pools_raw)
            context.dedust_pools.set(dedust_pools)
            with open(FALLBACK_FILENAME, 'w') as f:
                json.dump(pools_raw, f)
        else:
            raise Exception('Empty response')
    except Exception as fetch_exception:
        print(f'Failed to fetch dedust pools data: {fetch_exception}')
        try:
            with open(FALLBACK_FILENAME, 'r') as f:
                pools_raw = json.load(f)
                dedust_pools = parse_raw_pools_data(pools_raw)
                context.dedust_pools.set(dedust_pools)
        except Exception as e:
            print(f'Failed to load dedust pools data from file: {e}')
            raise e

def parse_raw_pools_data(pools_data: list[dict]) -> dict:
    pools = {}
    for pool in pools_data:
        account = AccountId(pool['address'])
        assets = []
        for raw_asset in pool['assets']:
            is_ton = raw_asset['type'] == 'native'
            asset = {
                'is_ton': is_ton,
                'address': AccountId(raw_asset['address']).as_str() if not is_ton else None
            }
            if is_ton:
                assert 'address' not in raw_asset

            assets.append(asset)
        pools[account.as_str()] = {
            'assets': assets,
        }
    return pools