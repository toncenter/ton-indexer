#!/usr/bin/python3
"""Value the known sides of recognized AMM reserves at a closed UTC day end.

Read-only on the price database. No actions, external price feed or CellDB.
"""
import collections
import datetime as dt
from fractions import Fraction
import json
import sys
import time

USDT = '0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE'
GRAM = frozenset(('', '0:8CDC1D7640AD5EE326527FC1AD0514F468B30DC84B0173F0E155F451B4E11F7C',
                  '0:671963027F7F85659AB55B821671688601CDCF1EE674FC7FBBB1A776A18D34A3'))
VENUES = ('stonfi', 'dedust')


FIRST = dt.date(2022, 11, 18)


def value_pools(states, quotes, audit=False):
    result = {v: collections.Counter() for v in VENUES}
    top = {v: [] for v in VENUES}
    for pool, (dex, assets, reserves) in states.items():
        if dex not in result:
            continue
        known = native = missing = 0
        for a, n in zip(assets, reserves):
            if not n:
                continue
            if a in GRAM:
                native += n
                known += n
            elif a not in quotes:
                missing += 1
            else:
                p = quotes[a]
                # One rounding per side, to the nearest nanogram (ties upwards).
                known += (2*n*p.numerator + p.denominator) // (2*p.denominator)
        label = 'none' if all(n == 0 or (a not in GRAM and a not in quotes)
                              for a, n in zip(assets, reserves)) else 'partial' if missing else 'full'
        c = result[dex]
        c[label] += 1
        c[label+'_nano'] += known
        c['native_nano'] += native
        if audit and known:
            top[dex].append((known, pool, label, assets, reserves))
    return result, {v: sorted(items, reverse=True)[:5] for v, items in top.items()}


def checked_close(c, day):
    end = int(dt.datetime.combine(day + dt.timedelta(days=1), dt.time(), dt.timezone.utc).timestamp())
    final = c.execute("SELECT toUnixTimestamp(done_minute) FROM aggregate_state FINAL WHERE job='price_1m'")
    live = c.execute("SELECT toUnixTimestamp(block_utime),mc_seqno FROM ingest_head FINAL WHERE writer='live'")
    if len(final) != 1 or len(live) != 1 or final[0][0] < end-60 or live[0][0] < end:
        raise ValueError("incomplete source day")
    return end


def rates(c, end):
    bounds = dict(lo=end-900, end=end, last=end-60)
    q = """SELECT asset,argMax(tuple(toString(price_raw),stale,pools,toUnixTimestamp(newest_utime)),
            tuple(minute,computed_at)) FROM price_1m
            WHERE minute>=toDateTime(%(lo)s,'UTC') AND minute<toDateTime(%(end)s,'UTC') AND {pred} GROUP BY asset"""
    out = {}
    def read(pred):
        return c.execute(q.format(pred=pred),bounds)
    for a, (p, stale, pools, newest) in bounded(read,'asset'):
        if a not in GRAM and a != USDT and not stale and pools and end-900 <= newest < end and p:
            if p.lower() not in ('nan','inf','-inf') and Fraction(p)>0:
                out[a] = Fraction(p)
    a = c.execute("""SELECT toString(price_raw) FROM anchor_1m FINAL
        WHERE minute=toDateTime(%(last)s,'UTC') AND NOT stale AND pools>0
        AND isFinite(price_raw) AND price_raw>0""", bounds)
    if len(a)>1:
        raise ValueError("duplicate anchor")
    if a:
        out[USDT] = 1/Fraction(a[0][0])  # raw USDT per nanogram
    return out


def states_at(c, day):
    # Daily rows contain changes, not inventories. Latest version before the
    # cutoff includes removals. Do not filter zero/removed reserves before argMax.
    ends = collections.defaultdict(list)
    q = """SELECT pool,argMax(tuple(day,version,removed,reserve0,reserve1),version)
           FROM pool_daily WHERE day<=toDate(%(day)s) AND {pred} GROUP BY pool"""
    head = 0
    for pred in ("pool<'0:8'", "pool>='0:8'"):
        for p,(changed,v,removed,r0,r1) in c.execute(q.format(pred=pred),{'day':str(day)}):
            head = max(head,v>>16)
            if not removed and (r0 or r1):
                ends[changed.year*100+changed.month].append((p,v,r0,r1))
    result = {}
    # Resolve pair and venue at the very SAME version, not from today's metadata.
    # Reconcile raw reserves against the independently maintained daily view.
    for month, rows in sorted(ends.items()):
        rows.sort()
        for i in range(0,len(rows),4096):
            chunk = rows[i:i+4096]
            want = {p:(v,r0,r1) for p,v,r0,r1 in chunk}
            tables=[{'name':'wanted','structure':[('pool','String'),('mc','UInt32'),('mv','UInt16')],
                     'data':[(p,v>>16,v&65535) for p,v,_,_ in chunk]}]
            q = """SELECT DISTINCT pool,mc_seqno,math_version,dex,asset0,asset1,reserve0,reserve1,removed
                   FROM pool_snapshots WHERE toYYYYMM(block_utime)=%(month)s AND pool BETWEEN %(lo)s AND %(hi)s
                   AND (pool,mc_seqno,math_version) IN (SELECT pool,mc,mv FROM wanted)"""
            for p,mc,mv,venue,a,b,x,y,removed in c.execute(q,dict(month=month,lo=chunk[0][0],hi=chunk[-1][0]),external_tables=tables):
                if p in result or want.get(p) != ((mc<<16)|mv,x,y) or removed or a==b:
                    raise ValueError("conflicting pool snapshot: "+p)
                result[p]=(venue,(a,b),(x,y))
            if not all(p in result for p in want):
                raise ValueError("missing pool metadata")
    return result,head


def bounded(read, column):
    """A single time range may touch too many primary-key marks in old parts.
    Retry only a read-limit exception using disjoint address ranges."""
    try:
        rows = read("1")
    except Exception as e:
        if getattr(e, 'code', None) != 158:
            raise
        cuts = [None]+['0:'+s for s in '123456789ABCDEF']+[None]
        for a,b in zip(cuts,cuts[1:]):
            pred = [f"{column}>='{a}'"] if a else []
            if b:
                pred.append(f"{column}<'{b}'")
            yield from read(' AND '.join(pred))
    else:
        yield from rows


def advance(c, day, states, head):
    # Resolve only changed pools. Require agreement between raw snapshots
    # and the daily materialized view before replacing any previous state.
    params = {'day': str(day), 'hi': str(day+dt.timedelta(days=1)), 'month': day.year*100+day.month}
    raw = """SELECT pool,argMax(tuple(dex,asset0,asset1,removed,reserve0,reserve1,
              bitOr(bitShiftLeft(toUInt64(mc_seqno),16),toUInt64(math_version))),tuple(mc_seqno,math_version))
             FROM pool_snapshots WHERE toYYYYMM(block_utime)=%(month)s
              AND block_utime>=toDateTime(%(day)s,'UTC') AND block_utime<toDateTime(%(hi)s,'UTC')
              AND {pred} GROUP BY pool"""
    daily = dict(c.execute("""SELECT pool,argMax(tuple(version,removed,reserve0,reserve1),version)
            FROM pool_daily WHERE day=toDate(%(day)s) GROUP BY pool""",params))
    for p,(dex,a,b,removed,x,y,v) in bounded(lambda pred:c.execute(raw.format(pred=pred),params),'pool'):
        if (v,removed,x,y) != daily.pop(p,None):
            raise ValueError("daily and raw reserves disagree: "+p)
        head=max(head,v>>16)
        if removed or not (x or y):
            states.pop(p,None)
        elif a==b:
            raise ValueError("invalid funded pool: "+p)
        else:
            states[p]=(dex,(a,b),(x,y))
    if daily:
        raise ValueError("missing raw pool updates")
    return head


def projection(day, head, counts):
    b={}
    total=priced=count=partial=0
    for v in VENUES:
        c=counts[v]
        for key in ('full','partial','none'):
            b[v+':'+key]=c[key]
        for key in ('full','partial','native'):
            whole,rem=divmod(c[key+'_nano'],10**9)
            if whole>=2**64:
                raise OverflowError('priced reserves exceed storage format')
            b[v+':'+key+'_gram']=whole
            b[v+':'+key+'_nano']=rem
        total+=c['full_nano']+c['partial_nano']
        priced+=c['full']+c['partial']
        count+=c['full']+c['partial']+c['none']
        partial+=c['partial']+c['none']
    if not head:
        raise ValueError("no dated source state")
    end=day+dt.timedelta(days=1)
    return dict(metric='dex_tvl_gram_v1',day=str(day),value=count,
                amount_nanoton=str(total) if priced or not count else None,
                is_partial=int(bool(partial)),source_mc_seqno=head,data_through=str(end)+' 00:00:00',breakdown=b)


def run(c, lo, hi, emit, audit=False):
    if not FIRST<=lo<hi<=dt.datetime.now(dt.timezone.utc).date() or (hi-lo).days>31:
        raise ValueError('expected 1..31 closed days in the source history')
    checked_close(c,hi-dt.timedelta(days=1))
    started=time.monotonic()
    states,head=states_at(c,lo)
    for i in range((hi-lo).days):
        day=lo+dt.timedelta(days=i)
        end=int(dt.datetime.combine(day+dt.timedelta(days=1),dt.time(),dt.timezone.utc).timestamp())
        if i:
            head=advance(c,day,states,head)
        counts,top=value_pools(states,rates(c,end),audit)
        row=projection(day,head,counts)
        if audit:
            print(json.dumps(dict(day=str(day),byDex=counts,top=top,seconds=round(time.monotonic()-started,3))),file=sys.stderr)
        emit(row)


def client():
    from clickhouse_driver import Client
    with open('/etc/gramstat-dex-prices.json',encoding='utf-8') as f:
        config=json.load(f)
    return Client(**config,database='dex_prices_hist',connect_timeout=5,send_receive_timeout=40,
                  settings={'readonly':1,'max_execution_time':25,'max_threads':2,
                            'max_memory_usage':300000000,'max_rows_to_read':5000000,'max_result_rows':100000})


if __name__ == '__main__':
    # Forced SSH command: fixed database and fixed queries only.
    try:
        req=json.loads(sys.stdin.buffer.read(512))
        run(client(),dt.date.fromisoformat(req['from']),dt.date.fromisoformat(req['to']),
            lambda row:print(json.dumps(row),flush=True))
    except Exception as exc:
        print('TVL source read failed:',type(exc).__name__,getattr(exc,'code',''),file=sys.stderr)
        sys.exit(1)
