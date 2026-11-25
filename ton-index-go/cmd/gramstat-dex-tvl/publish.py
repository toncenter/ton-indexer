#!/usr/bin/python3
"""Publish closed-day aggregates returned by the fixed TVL reader."""
import argparse
import base64
import datetime as dt
import http.client
import json
import os
import subprocess
from urllib.parse import urlsplit

METRIC = 'dex_tvl_gram_v1'
FIRST = dt.date(2022, 11, 18)
ONE = dt.timedelta(days=1)
VENUES = ('stonfi', 'dedust')


def query(sql, body=b''):
    u = urlsplit(os.environ['GRAMSTAT_CH_URL'])
    if u.scheme not in ('http','https') or not u.hostname or u.username or u.query or u.fragment:
        raise ValueError('invalid sink')
    conn = (http.client.HTTPSConnection if u.scheme=='https' else http.client.HTTPConnection)(u.hostname,u.port,timeout=20)
    token = (os.environ['GRAMSTAT_CH_USER']+':'+os.environ['GRAMSTAT_CH_PASSWORD']).encode()
    try:
        conn.request('POST',(u.path or '/')+'?wait_end_of_query=1&async_insert=0',sql.encode()+b'\n'+body,
                     {'Authorization':'Basic '+base64.b64encode(token).decode()})
        r=conn.getresponse()
        data=r.read(1_000_001)
        if r.status!=200 or r.getheader('X-ClickHouse-Exception-Code') or len(data)>1_000_000 or data.startswith(b'Code:'):
            raise ValueError(f'ClickHouse sink rejected request: HTTP {r.status}')
        return data
    finally:
        conn.close()


def check(row,day):
    if (row['metric']!=METRIC or row['day']!=str(day) or row['data_through']!=str(day+ONE)+' 00:00:00'
            or not 0<row['source_mc_seqno']<2**32):
        raise ValueError('bad snapshot identity')
    b=row['breakdown']
    expected={'method'} | {v+':'+s for v in VENUES for s in
              ('full','estimated','partial','none','native_gram','native_nano','full_gram','full_nano','partial_gram','partial_nano','estimated_gram','estimated_nano')}
    if set(b)!=expected or b['method']!=2 or any(type(n) is not int or not 0<=n<2**64 for n in b.values()):
        raise ValueError('invalid categories')
    counts=[0,0,0,0]
    total=0
    for v in VENUES:
        def val(s):
            return b[v+':'+s]
        def amount(s):
            n=val(s+'_nano')
            if n>=10**9:
                raise ValueError('invalid nanogram remainder')
            return 10**9*val(s+'_gram')+n
        for i,k in enumerate(('full','estimated','partial','none')):
            counts[i]+=val(k)
        f,e,p=amount('full'),amount('estimated'),amount('partial')
        if ((f and not val('full')) or (p and not val('partial')) or (e and not val('estimated')) or e%2 or amount('native')>f+e//2+p):
            raise ValueError('inconsistent reserve attribution')
        total+=f+e+p
    expected_total=str(total) if sum(counts[:3]) or not counts[3] else None
    if (row['value']!=sum(counts) or row['is_partial']!=int(bool(counts[2]+counts[3])) or
            row['amount_nanoton']!=expected_total):
        raise ValueError('inconsistent snapshot totals')


def ranges(lo,hi,done):
    # Newest first, chronological inside a batch. No concurrent backfill on the source.
    while hi>lo:
        if hi-ONE in done:
            hi-=ONE
            continue
        start=hi-ONE
        while start>lo and (hi-start).days<31 and start-ONE not in done:
            start-=ONE
        yield start,hi
        hi=start


def main():
    today=dt.datetime.now(dt.timezone.utc).date()
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--from',dest='lo',type=dt.date.fromisoformat,default=FIRST)
    parser.add_argument('--to',dest='hi',type=dt.date.fromisoformat,default=today)
    parser.add_argument('--write',action='store_true')
    parser.add_argument('--missing',action='store_true')
    parser.add_argument('--ssh-config',default='/etc/gramstat-dex/ssh_tvl_config')
    args=parser.parse_args()
    if not FIRST<=args.lo<args.hi<=today:
        raise ValueError('expected closed source days')
    done=set()
    if args.missing:
        done={dt.date.fromisoformat(d) for d in query(
            f"SELECT day FROM gramstat.daily_metrics FINAL WHERE metric='{METRIC}' AND breakdown['method']=2 ORDER BY day FORMAT TSV").decode().splitlines()}
    for lo,hi in ranges(args.lo,args.hi,done):
        raw=subprocess.run(['/usr/bin/ssh','-T','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes',
                            '-F',args.ssh_config,'gramstat-tvl'],
                           input=json.dumps({'from':str(lo),'to':str(hi)}),stdout=subprocess.PIPE,
                           text=True,timeout=1200,check=True).stdout
        rows=[json.loads(s) for s in raw.splitlines()]
        if len(rows)!=(hi-lo).days:
            raise ValueError('incomplete source range')
        now=dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
        for i,row in enumerate(rows):
            check(row,lo+i*ONE)
            row['calculated_at']=now
        if args.write:
            body=''.join(json.dumps(row)+'\n' for row in rows).encode()
            query('INSERT INTO gramstat.daily_metrics FORMAT JSONEachRow',body)
            print(f'{lo} .. {hi} (exclusive): published {len(rows)} days',flush=True)
        else:
            for row in rows:
                print(json.dumps(row),flush=True)


if __name__ == '__main__':
    main()
