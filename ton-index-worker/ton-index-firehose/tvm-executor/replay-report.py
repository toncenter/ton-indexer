#!/usr/bin/env python3
"""Sums up runs of ton-tvm-replay, and compares a run with a reference.

  replay-report.py summary RUN.jsonl [--sample SAMPLE.json]
  replay-report.py reference RUN.jsonl > REFERENCE.jsonl
  replay-report.py compare REFERENCE.jsonl RUN.jsonl

A run has a line of JSON per round. The sample, made with the run, names the
groups of rounds (the start of each global version, rounds spread over it,
random ones) for the summary. A reference is a run without timings: the same
rounds replayed by another build must give it again, mismatches included, or
compare names the differences and exits with 1.
"""
import argparse
import collections
import json
import re
import sys


def load(path):
    rounds = {}
    for line in open(path):
        line = line.strip()
        if line:
            r = json.loads(line)
            rounds[r["seqno"]] = r
    return rounds


def reason(m):
    """The reason of a mismatch without its lt."""
    return re.sub(r"^replay lt \d+: ", "", m["reason"])


def cause(m):
    """What differs: the modified fields of the transaction, without values."""
    fields = []
    for part in m.get("diff", "").split("; "):
        found = re.match(r"(modified|added|deleted): ([^:]+)", part)
        if found:
            field = re.sub(r"\[\d+\]", "[]", found.group(2))
            fields.append(f"{found.group(1)} {field.split('.')[-1] if '.' in field else field}")
    return ", ".join(sorted(set(fields))) or reason(m)


def summary(args):
    rounds = load(args.run)
    groups = {"all": sorted(rounds)}
    if args.sample:
        sample = json.load(open(args.sample))
        groups.update({name: [s for s in seqnos if s in rounds] for name, seqnos in sample["groups"].items()})

    def totals(seqnos):
        t = collections.Counter()
        for s in seqnos:
            r = rounds[s]
            t["rounds"] += 1
            for key in ("blocks", "accounts", "transactions", "replayed", "skipped"):
                t[key] += r[key]
            t["mismatches"] += len(r["mismatches"])
            t["unsupported"] += sum(r["unsupported"].values())
            t["errors"] += len(r["errors"])
            t["rounds with errors"] += bool(r["errors"])
        return t

    print(f"{'group':<14} {'rounds':>6} {'txs':>9} {'replayed':>9} {'%':>7} {'mism':>6} {'skip':>6} {'unsup':>6} {'err':>4}")
    for name, seqnos in groups.items():
        t = totals(seqnos)
        share = 100.0 * t["replayed"] / t["transactions"] if t["transactions"] else 100.0
        print(f"{name:<14} {t['rounds']:>6} {t['transactions']:>9} {t['replayed']:>9} {share:>6.2f}% "
              f"{t['mismatches']:>6} {t['skipped']:>6} {t['unsupported']:>6} {t['errors']:>4}")

    by_version = collections.defaultdict(list)
    for s, r in rounds.items():
        by_version[r["version"]].append(s)
    print("\nby global version of the round:")
    for version in sorted(by_version):
        t = totals(by_version[version])
        share = 100.0 * t["replayed"] / t["transactions"] if t["transactions"] else 100.0
        print(f"  v{version:<3} {t['rounds']:>5} rounds {t['transactions']:>8} txs {share:>7.2f}% replayed, "
              f"{t['mismatches']} mismatches")

    causes = collections.defaultdict(list)
    for s, r in sorted(rounds.items()):
        for m in r["mismatches"]:
            causes[(r["version"], m["type"], cause(m))].append((s, m))
    if causes:
        print("\nmismatches by version, type and cause:")
        for (version, kind, what), items in sorted(causes.items(), key=lambda kv: (kv[0][0], -len(kv[1]))):
            seqnos = [s for s, _ in items]
            accounts = collections.Counter(m["account"] for _, m in items)
            top = ", ".join(f"{a[:12]}…×{n}" for a, n in accounts.most_common(3))
            print(f"  v{version} {kind:<5} {len(items):>5}× {what}  [rounds {min(seqnos)}..{max(seqnos)}; {top}]")
            print(f"      e.g. {items[0][0]} {items[0][1]['account']} lt {items[0][1]['lt']}: {items[0][1]['diff'][:240]}")
    unsupported = collections.Counter()
    for r in rounds.values():
        unsupported.update(r["unsupported"])
    if unsupported:
        print("\nunsupported transaction types:", dict(unsupported))
    errors = [(s, e) for s, r in sorted(rounds.items()) for e in r["errors"]]
    if errors:
        print("\nerrors:")
        for s, e in errors[:20]:
            print(f"  {s}: {e[:240]}")
    seconds = sum(r["seconds"] for r in rounds.values())
    print(f"\nround time: {seconds:.0f} s summed over rounds")


def normalized(r):
    out = {k: v for k, v in r.items() if k != "seconds"}
    out["mismatches"] = sorted(out["mismatches"], key=lambda m: (m["block"], m["account"], m["lt"]))
    return out


def reference(args):
    for seqno, r in sorted(load(args.run).items()):
        print(json.dumps(normalized(r), sort_keys=True))


def compare(args):
    expected, actual = load(args.reference), load(args.run)
    problems = 0
    for seqno, want in sorted(expected.items()):
        got = actual.get(seqno)
        if got is None:
            print(f"{seqno}: not in the run")
            problems += 1
            continue
        want, got = normalized(want), normalized(got)
        if want == got:
            continue
        problems += 1
        for key in sorted(set(want) | set(got)):
            if want.get(key) == got.get(key):
                continue
            if key == "mismatches":
                before = {json.dumps(m, sort_keys=True) for m in want.get(key, [])}
                after = {json.dumps(m, sort_keys=True) for m in got.get(key, [])}
                for m in sorted(before - after):
                    print(f"{seqno}: mismatch gone: {m[:400]}")
                for m in sorted(after - before):
                    print(f"{seqno}: mismatch new: {m[:400]}")
            else:
                print(f"{seqno}: {key}: {json.dumps(want.get(key))[:300]} -> {json.dumps(got.get(key))[:300]}")
    print(f"{len(expected)} rounds, {problems} differ")
    sys.exit(1 if problems else 0)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("summary")
    s.add_argument("run")
    s.add_argument("--sample")
    s.set_defaults(func=summary)
    r = sub.add_parser("reference")
    r.add_argument("run")
    r.set_defaults(func=reference)
    c = sub.add_parser("compare")
    c.add_argument("reference")
    c.add_argument("run")
    c.set_defaults(func=compare)
    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
