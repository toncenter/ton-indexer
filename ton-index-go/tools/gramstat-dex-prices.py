#!/usr/bin/python3
"""Restricted stdin/stdout price reader. Install on the approved price reader host.

Input: {"day":"YYYY-MM-DD","assets":["0:...","USDT"],"mode":"window"|"opening"}.
Output: TSV asset, minute epoch, raw price (or '-'), latest observation epoch.
Opening reads use 'done-opening'; window reads use 'done'.
"""
import datetime as dt
import json
import re
import sys

from clickhouse_driver import Client


def main():
    payload = sys.stdin.buffer.read(2_000_001)
    if len(payload) > 2_000_000:
        raise ValueError("oversized request")
    request = json.loads(payload)
    day = dt.date.fromisoformat(request["day"])
    assets = sorted(set(request["assets"]))
    mode = request.get("mode", "window")
    if (mode not in ("window", "opening") or (mode == "opening" and "USDT" in assets)
            or len(assets) > 16384 or not all(a == "USDT" or re.fullmatch(r"-?\d+:[A-F0-9]{64}", a) for a in assets)
            or day >= dt.datetime.now(dt.timezone.utc).date()):
        raise ValueError("invalid quote request")
    with open("/etc/gramstat-dex-prices.json", encoding="utf-8") as f:
        config = json.load(f)
    client = Client(database="dex_prices_hist", connect_timeout=5, send_receive_timeout=30,
                    **config, settings={"readonly": 1, "max_threads": 2, "max_memory_usage": 268435456,
                                        "max_execution_time": 20, "max_rows_to_read": 2_000_000,
                                        "max_result_rows": 100_000})
    start = int(dt.datetime.combine(day, dt.time(), dt.timezone.utc).timestamp())
    params = {"start": start - 900, "end": start + 86400}
    rows = client.execute("SELECT toUnixTimestamp(done_minute) FROM aggregate_state FINAL WHERE job='price_1m'")
    if len(rows) != 1 or rows[0][0] < params["end"] - 60:
        raise ValueError("price history is not final")
    def emit(query):
        for a, minute, price, newest in client.execute_iter(query, params):
            print(a, minute, price, newest, sep="\t")
    interval = "minute >= toDateTime(%(start)s,'UTC') AND minute < toDateTime(%(end)s,'UTC')"
    # Keep invalid observations as tombstones: never fall back across one.
    value = "if(not stale AND pools>0 AND isFinite(price_raw) AND price_raw>0, toString(price_raw), '-')"
    if mode == "opening":
        # Read at most one predecessor per requested asset, including an
        # invalidation. Do not seek the last *non-null* price. Ordering by the
        # calculation version also resolves duplicate minutes before merges.
        sql = f"""SELECT asset,argMax(tuple(toUnixTimestamp(minute),{value},toUnixTimestamp(newest_utime)),
                          tuple(minute,computed_at)) FROM price_1m
                  WHERE asset IN %(assets)s AND minute<toDateTime(%(start)s,'UTC') GROUP BY asset"""
        def opening(keys):
            try:
                rows = client.execute(sql, {**params, 'assets':tuple(keys)})
            except Exception as exc:
                if getattr(exc, 'code', None) != 158 or len(keys) < 2:
                    raise
                mid = len(keys)//2
                opening(keys[:mid])
                opening(keys[mid:])
                return
            for a, obs in rows:
                print(a, *obs, sep="\t")
        for i in range(0,len(assets),16):
            opening(assets[i:i+16])
        print("done-opening")
        return
    if "USDT" in assets:
        assets.remove("USDT")
        emit(f"SELECT 'USDT',toUnixTimestamp(minute),{value},toUnixTimestamp(minute) FROM anchor_1m FINAL WHERE {interval} ORDER BY minute")
    for i in range(0, len(assets), 32):
        params["assets"] = tuple(assets[i:i+32])
        emit(f"SELECT asset,toUnixTimestamp(minute),{value},toUnixTimestamp(newest_utime) FROM price_1m FINAL WHERE asset IN %(assets)s AND {interval} ORDER BY asset,minute")
    print("done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Some driver exception texts contain connection details.
        print("quote request failed:", type(e).__name__, getattr(e, "code", ""), file=sys.stderr)
        sys.exit(1)
