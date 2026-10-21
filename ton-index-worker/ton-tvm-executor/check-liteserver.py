#!/usr/bin/env python3
"""Compares get_account of ton-tvm-executor with a liteserver on random blocks.

For every block N sampled: the elector and the config account (they change in
every masterchain block), accounts of transactions of the first shard block
of N, and a random address, which exists nowhere. Each one at after_block and
before_block, against getaccount of lite-client at N and N-1: the shard block
of the state, the last transaction (lt and hash), the balance, and whether
the account exists. The ShardAccount BOC of every answer must hold the same
last transaction.

Needs lite-client and grpcurl. Run on the host of the liteserver, e.g.:

  ./check-liteserver.py --executor 192.0.2.10:10060 --liteserver 127.0.0.1:20700 \
      --liteserver-key /tmp/ls.pub --proto-dir ~/tvm-proto --samples 40
"""
import argparse
import base64
import json
import os
import random
import re
import statistics
import subprocess
import sys
import time

ELECTOR = "-1:" + "33" * 32
CONFIG = "-1:" + "55" * 32
BLOCK_ID = r"\((-?\d+),([0-9a-fA-F]{16}),(\d+)\):([0-9A-F]{64}):([0-9A-F]{64})"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--executor", required=True)
    p.add_argument("--liteserver", required=True)
    p.add_argument("--liteserver-key", required=True)
    p.add_argument("--proto-dir", required=True)
    p.add_argument("--lite-client", default="/usr/bin/ton/lite-client/lite-client")
    p.add_argument("--grpcurl", default=os.path.expanduser("~/bin/grpcurl"))
    p.add_argument("--samples", type=int, default=40, help="blocks over the whole history")
    p.add_argument("--head-samples", type=int, default=5, help="blocks near the head")
    p.add_argument("--from-seqno", type=int, default=1_000_000)
    p.add_argument("--seed", type=int, default=None)
    return p.parse_args()


class LiteClient:
    def __init__(self, args):
        self.cmd = [args.lite_client, "-a", args.liteserver, "-p", args.liteserver_key, "-v", "3", "-c"]

    def run(self, command):
        out = subprocess.run(self.cmd + [command], capture_output=True, text=True, timeout=60)
        return out.stdout + out.stderr

    def head(self):
        m = re.search(r"latest masterchain block known to server is " + BLOCK_ID, self.run("last"))
        return int(m.group(3))

    def block_id(self, seqno):
        out = self.run(f"byseqno -1:8000000000000000 {seqno}")
        m = re.search(r"\(-1,8000000000000000," + str(seqno) + r"\):[0-9A-F]{64}:[0-9A-F]{64}", out)
        if not m:
            raise RuntimeError(f"no block {seqno}: {out[-300:]}")
        return m.group(0)

    def first_shard(self, mc_block):
        m = re.search(r"shard #1 : (" + BLOCK_ID + ")", self.run(f"allshards {mc_block}"))
        return m.group(1) if m else None

    def accounts(self, shard_block, count):
        out = self.run(f"listblocktrans {shard_block} {count}")
        return ["0:" + a.lower() for a in re.findall(r"account ([0-9A-F]{64})", out)]

    def account(self, address, block):
        out = self.run(f"getaccount {address} {block}")
        m = re.search(r"got account state for \S+ with respect to blocks (" + BLOCK_ID + r")(?: and (" + BLOCK_ID + "))?", out)
        if not m:
            raise RuntimeError(f"no account state of {address} at {block}: {out[-300:]}")
        shard_block = m.group(7) or m.group(1)
        if "account state is empty" in out:
            return {"exists": False, "shard_block": shard_block}
        lt = re.search(r"last transaction lt = (\d+) hash = ([0-9A-F]{64})", out)
        balance = re.search(r"account balance is (\d+)ng", out)
        return {"exists": True, "shard_block": shard_block, "lt": int(lt.group(1)), "hash": lt.group(2).lower(),
                "balance": int(balance.group(1))}


def executor_calls(args, seqno, root_hash, calls):
    def address(a):
        wc, hexid = a.split(":")
        return {"workchain": int(wc), "accountId": base64.b64encode(bytes.fromhex(hexid)).decode()}

    request = {"mcSeqno": seqno, "mcRootHash": base64.b64encode(bytes.fromhex(root_hash)).decode(),
               "request": {"calls": []}}
    for a, at in calls:
        ref = {"address": address(a)}
        if at == "before":
            ref["at"] = {"beforeBlock": {}}
        request["request"]["calls"].append({"getAccount": {"account": ref}})
    start = time.monotonic()
    out = subprocess.run([args.grpcurl, "-plaintext", "-max-msg-sz", str(256 << 20), "-import-path", args.proto_dir,
                          "-proto", "ton/tvm/v1/executor.proto", "-d", json.dumps(request), args.executor,
                          "ton.tvm.v1.Executor/Execute"], capture_output=True, text=True, timeout=300)
    elapsed = time.monotonic() - start
    if out.returncode != 0:
        raise RuntimeError(f"executor at {seqno}: {out.stdout} {out.stderr}")
    return json.loads(out.stdout)["results"], elapsed


def boc_root_data(boc):
    """The data of the root cell of a BOC with one root."""
    if boc[:4] != bytes.fromhex("b5ee9c72"):
        raise ValueError("not a BOC")
    flags = boc[4]
    size, off_bytes = flags & 7, boc[5]
    cells = int.from_bytes(boc[6:6 + size], "big")
    if int.from_bytes(boc[6 + size:6 + 2 * size], "big") != 1:
        raise ValueError("not one root")
    pos = 6 + 3 * size + off_bytes  # cells, roots, absent, tot_cells_size
    root = int.from_bytes(boc[pos:pos + size], "big")
    pos += size  # the root list
    if flags & 0x80:
        pos += cells * off_bytes  # the index
    for i in range(root + 1):
        d1, d2 = boc[pos], boc[pos + 1]
        pos += 2 + (32 + 2) * ((d1 >> 5) + 1 if d1 & 16 else 0)  # the hashes and depths, if stored
        data = boc[pos:pos + (d2 + 1) // 2]
        pos += (d2 + 1) // 2 + size * (d1 & 7)
        if i == root:
            return data


def shard_text(block):
    shard = block.get("shardId", {})
    bits, prefix = shard.get("shardPfxBits", 0), int(shard.get("shardPrefix", "0"))
    return f"({shard.get('workchainId', 0)},{prefix | (1 << (63 - bits)):016X},{block.get('seqNo', 0)})"


def compare(ours, theirs):
    """The differences of an executor answer from a liteserver one."""
    if "account" not in ours:
        return [f"executor: {ours}"]
    snapshot = ours["account"]
    value = snapshot["value"]
    diffs = []
    block = snapshot["block"]
    ours_block = shard_text(block) + ":" + base64.b64decode(block["rootHash"]).hex()
    if ours_block.upper() != theirs["shard_block"].rsplit(":", 1)[0].upper():
        diffs.append(f"shard block {shard_text(block)} vs {theirs['shard_block']}")
    exists = "accountNone" not in value["account"]
    if exists != theirs["exists"]:
        diffs.append(f"exists {exists} vs {theirs['exists']}")
    lt = int(value.get("lastTransLt", "0"))
    tx_hash = base64.b64decode(value["lastTransHash"]).hex()
    if theirs["exists"]:
        balance = int(value["account"].get("account", {}).get("storage", {}).get("balance", {}).get("grams", "0"))
        if (lt, tx_hash, balance) != (theirs["lt"], theirs["hash"], theirs["balance"]):
            diffs.append(f"lt {lt} hash {tx_hash[:16]} balance {balance} vs "
                         f"lt {theirs['lt']} hash {theirs['hash'][:16]} balance {theirs['balance']}")
    if value.get("bocOmitted"):
        if "shardAccountBoc" in value:
            diffs.append("boc_omitted with a BOC")
    else:
        data = boc_root_data(base64.b64decode(value["shardAccountBoc"]))
        if data[:32].hex() != tx_hash or int.from_bytes(data[32:40], "big") != lt:
            diffs.append("ShardAccount BOC holds another last transaction")
    return diffs


def main():
    args = parse_args()
    rnd = random.Random(args.seed)
    lc = LiteClient(args)
    head = lc.head()
    seqnos = sorted(rnd.randrange(args.from_seqno, head - 100) for _ in range(args.samples))
    seqnos += [head - 10 - i for i in range(args.head_samples)]
    checked, failed, latencies, omitted = 0, 0, [], 0
    for seqno in seqnos:
        block, prev = lc.block_id(seqno), lc.block_id(seqno - 1)
        addresses = [ELECTOR, CONFIG]
        shard = lc.first_shard(block)
        if shard:
            addresses += lc.accounts(shard, 3)
        addresses.append("0:" + rnd.randbytes(32).hex())
        calls = [(a, at) for a in addresses for at in ("after", "before")]
        try:
            results, elapsed = executor_calls(args, seqno, block.split(":")[1], calls)
        except RuntimeError as e:
            print(f"block {seqno}: {e}")
            failed += len(calls)
            continue
        latencies.append(elapsed)
        for (address, at), ours in zip(calls, results):
            theirs = lc.account(address, block if at == "after" else prev)
            diffs = compare(ours, theirs)
            checked += 1
            omitted += bool(ours.get("account", {}).get("value", {}).get("bocOmitted"))
            if diffs:
                failed += 1
                print(f"block {seqno} {address} {at}: " + "; ".join(diffs))
        print(f"block {seqno}: {len(calls)} calls in {elapsed * 1000:.0f} ms", flush=True)
    print(f"checked {checked}, failed {failed}, boc_omitted {omitted}; per request: "
          f"median {statistics.median(latencies) * 1000:.0f} ms, max {max(latencies) * 1000:.0f} ms")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
