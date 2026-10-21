#!/usr/bin/env python3
"""Compares get_account or get_method of ton-tvm-executor with a liteserver on random blocks.

For every block N sampled: the elector and the config account (they change in
every masterchain block), accounts of transactions of the first shard block
of N, and a random address, which exists nowhere. Each one at after_block and
before_block, against getaccount of lite-client at N and N-1: the shard block
of the state, the last transaction (lt and hash), the balance, and whether
the account exists. The ShardAccount BOC of every answer must hold the same
last transaction.

With --get-methods, get-methods of those accounts at after_block instead,
against runmethod of lite-client at N: the exit code, and the result stack as
lite-client prints it, cells by their hashes. A list of methods is tried on
every account; those it does not have fail alike.

Needs lite-client and grpcurl. Run on the host of the liteserver, e.g.:

  ./check-liteserver.py --executor 192.0.2.10:10060 --liteserver 127.0.0.1:20700 \
      --liteserver-key /tmp/ls.pub --proto-dir ~/tvm-proto --samples 40
"""
import argparse
import base64
import hashlib
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
    p.add_argument("--get-methods", action="store_true", help="check get-methods instead of accounts")
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

    def run_method(self, address, block, method, args):
        """The remote result: ("error", exit code), ("result", stack text) or None for no state to run."""
        out = self.run(" ".join(["runmethod", address, block, method] + [str(a) for a in args]))
        if "cannot serialize resulting stack" in out:
            # Within 1000 recursive calls, which a list of a few hundred pairs is not.
            return ("unserializable", None)
        m = re.search(r"^remote result(?: \(not to be trusted\))?:\s*(.*?)\s*$", out, re.M)
        if not m:
            return None
        text = m.group(1)
        if text.startswith("error"):
            return ("error", int(text.split()[1]))
        return ("result", " ".join(text.split()))

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


def address_pb(a):
    wc, hexid = a.split(":")
    return {"workchain": int(wc), "accountId": base64.b64encode(bytes.fromhex(hexid)).decode()}


def executor_calls(args, seqno, root_hash, calls):
    """calls are (address, at) for get_account, or already made Call messages."""
    request = {"mcSeqno": seqno, "mcRootHash": base64.b64encode(bytes.fromhex(root_hash)).decode(),
               "request": {"calls": []}}
    for call in calls:
        if isinstance(call, dict):
            request["request"]["calls"].append(call)
            continue
        a, at = call
        ref = {"address": address_pb(a)}
        if at == "before":
            ref["at"] = {"beforeBlock": {}}
        request["request"]["calls"].append({"getAccount": {"account": ref}})
    start = time.monotonic()
    out = subprocess.run([args.grpcurl, "-plaintext", "-max-msg-sz", str(256 << 20), "-import-path", args.proto_dir,
                          "-proto", "ton/tvm/v1/executor.proto", "-d", "@", args.executor,
                          "ton.tvm.v1.Executor/Execute"], input=json.dumps(request), capture_output=True, text=True,
                         timeout=300)
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


def boc_cells(boc):
    """The cells of a BOC, root first: (d1, d2, data, refs) each."""
    if boc[:4] != bytes.fromhex("b5ee9c72"):
        raise ValueError("not a BOC")
    flags = boc[4]
    size, off_bytes = flags & 7, boc[5]
    count = int.from_bytes(boc[6:6 + size], "big")
    roots = int.from_bytes(boc[6 + size:6 + 2 * size], "big")
    pos = 6 + 3 * size + off_bytes + roots * size
    if flags & 0x80:
        pos += count * off_bytes
    cells = []
    for _ in range(count):
        d1, d2 = boc[pos], boc[pos + 1]
        pos += 2 + (32 + 2) * ((d1 >> 5) + 1 if d1 & 16 else 0)
        data = boc[pos:pos + (d2 + 1) // 2]
        pos += (d2 + 1) // 2
        refs = [int.from_bytes(boc[pos + i * size:pos + (i + 1) * size], "big") for i in range(d1 & 7)]
        pos += size * (d1 & 7)
        cells.append((d1 & ~16, d2, data, refs))
    return cells


def cell_hash(boc):
    """The hash of the root, as lite-client prints it; None for a cell with a level."""
    cells = boc_cells(boc)
    hashes, depths = [None] * len(cells), [0] * len(cells)
    for i in reversed(range(len(cells))):
        d1, d2, data, refs = cells[i]
        if d1 >> 5 or any(hashes[r] is None for r in refs):
            continue
        depths[i] = 1 + max(depths[r] for r in refs) if refs else 0
        repr_ = bytes([d1, d2]) + data + b"".join(depths[r].to_bytes(2, "big") for r in refs)
        hashes[i] = hashlib.sha256(repr_ + b"".join(hashes[r] for r in refs)).digest()
    return hashes[0].hex().upper() if hashes[0] else None


def stack_text(entries):
    """A result stack as lite-client prints it."""
    def entry(e):
        if "null" in e:
            return "()"
        if "nan" in e:
            return "NaN"
        if "integer" in e:
            return str(int.from_bytes(base64.b64decode(e["integer"]), "big", signed=True))
        if "cell" in e:
            # * for a tree with Merkle proofs, whose hash is not computed here.
            return "C{%s}" % (cell_hash(base64.b64decode(e["cell"])) or "*")
        if "slice" in e:
            s = e["slice"]
            d1, d2, data, _ = boc_cells(base64.b64decode(s["cellBoc"]))[0]
            return "CS{Cell{%s} bits: %d..%d; refs: %d..%d}" % (
                (bytes([d1, d2]) + data).hex(), s.get("startBit", 0), s.get("endBit", 0), s.get("startRef", 0),
                s.get("endRef", 0))
        if "tuple" in e:
            return "[" + " ".join(entry(i) for i in e["tuple"].get("items", [])) + "]"
        if "list" in e:
            return "(" + " ".join(entry(i) for i in e["list"].get("items", [])) + ")"
        return "?" + json.dumps(e)
    return " ".join(["["] + [entry(e) for e in entries] + ["]"])


def int_pb(n):
    return {"integer": base64.b64encode(n.to_bytes((n + (n < 0)).bit_length() // 8 + 1, "big", signed=True)).decode()}


# client_method_gas_limit of liteserver, given to the executor as gas_limit so
# that running out of gas compares too: the default of v1 is higher.
LITESERVER_GAS = 300000

# Tried on every account of its kind; an account without one fails alike.
METHODS = {
    ELECTOR: [("active_election_id", []), ("participant_list", []), ("past_elections", []),
              ("compute_returned_stake", [0])],
    CONFIG: [("seqno", []), ("get_public_key", [])],
}
OTHER_METHODS = [("seqno", []), ("get_public_key", []), ("get_wallet_data", []), ("get_jetton_data", []),
                 ("get_nft_data", []), ("get_subwallet_id", []), ("get_plugin_list", [])]


def compare_get_method(ours, theirs):
    """The differences, or None when the liteserver gave nothing to compare with."""
    if theirs is not None and theirs[0] == "unserializable":
        return None
    if theirs is None:
        code = ours.get("error", {}).get("code")
        return [] if code == "ERROR_CODE_ACCOUNT_NOT_ACTIVE" else [f"no state to run at the liteserver, executor: {ours}"]
    if "getMethod" not in ours:
        return [f"executor: {ours}"]
    result = ours["getMethod"]
    exit_code = result.get("exitCode", 0)
    if theirs[0] == "error":
        return [] if exit_code == theirs[1] else [f"exit {exit_code} vs {theirs[1]}"]
    if exit_code != 0:
        return [f"exit {exit_code} vs 0"]
    text = stack_text(result.get("stack", []))
    pattern = re.escape(text).replace(re.escape("C{*}"), r"C\{[0-9A-F]{64}\}")
    return [] if re.fullmatch(pattern, theirs[1]) else [f"stack {text[:300]} vs {theirs[1][:300]}"]


def check_get_methods(args, lc, rnd, seqnos):
    checked, failed, skipped, unhashed, latencies = 0, 0, 0, 0, []
    outcomes = {}  # (method, exit code or error) to count
    for seqno in seqnos:
        block = lc.block_id(seqno)
        addresses = [ELECTOR, CONFIG]
        shard = lc.first_shard(block)
        if shard:
            addresses += lc.accounts(shard, 3)
        calls = [(a, method, margs) for a in addresses for method, margs in METHODS.get(a, OTHER_METHODS)]
        made = [{"getMethod": {"account": {"onchain": {"address": address_pb(a)}}, "name": method,
                               "stack": [int_pb(x) for x in margs], "gasLimit": str(LITESERVER_GAS)}}
                for a, method, margs in calls]
        try:
            results, elapsed = executor_calls(args, seqno, block.split(":")[1], made)
        except RuntimeError as e:
            print(f"block {seqno}: {e}")
            failed += len(calls)
            continue
        latencies.append(elapsed)
        for (address, method, margs), ours in zip(calls, results):
            diffs = compare_get_method(ours, lc.run_method(address, block, method, margs))
            if diffs is None:
                skipped += 1
                continue
            checked += 1
            unhashed += "C{*}" in stack_text(ours.get("getMethod", {}).get("stack", []))
            outcome = ours["getMethod"].get("exitCode", 0) if "getMethod" in ours else ours.get("error", {}).get("code")
            outcomes[(method, outcome)] = outcomes.get((method, outcome), 0) + 1
            if diffs:
                failed += 1
                print(f"block {seqno} {address} {method}: " + "; ".join(diffs))
        print(f"block {seqno}: {len(calls)} calls in {elapsed * 1000:.0f} ms", flush=True)
    for (method, outcome), n in sorted(outcomes.items(), key=lambda kv: (kv[0][0], str(kv[0][1]))):
        print(f"  {method}: {outcome} x{n}")
    print(f"checked {checked}, failed {failed}, not serializable at the liteserver {skipped}, "
          f"with cells of Merkle proofs not hashed {unhashed}; per request: "
          f"median {statistics.median(latencies) * 1000:.0f} ms, max {max(latencies) * 1000:.0f} ms")
    sys.exit(1 if failed else 0)


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
    if args.get_methods:
        return check_get_methods(args, lc, rnd, seqnos)
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
