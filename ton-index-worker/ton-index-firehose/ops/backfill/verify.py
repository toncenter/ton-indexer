#!/usr/bin/env python3
"""Checks the merged-blocks bundles of a block range in the store.

    verify.py --rclone RCLONE --src garage:firehose/merged-blocks --from N --to M [--states DIR...]

Every bundle of the range must be there and hold its blocks in order, each
linked to the one before by parent_num and parent_id, across bundles too.
Needs zstd.

With --states, the bundles are not all read. The runner already made sure of
the inside of each chunk: the tool writes rounds strictly in order, the runner
checked the bundle names and the upload checked their MD5. What is left is
checked: the done chunks of the runner states cover the range once, the store
lists every bundle of it with the sizes the runner uploaded, and the bundles
on both sides of every chunk boundary link up.
"""
import argparse
import concurrent.futures
import json
import pathlib
import struct
import subprocess
import sys
import tempfile

BUNDLE = 100
PAYLOAD_TYPE = b"type.googleapis.com/sf.ton.type.v1.Block"


def varint(data, off):
    value = shift = 0
    while True:
        byte = data[off]
        off += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, off
        shift += 7


def envelope(data):
    """number, id, parent_num, parent_id of a sf.bstream.v1.Block."""
    fields = {}
    off = 0
    while off < len(data):
        key, off = varint(data, off)
        number, wire = key >> 3, key & 7
        if wire == 0:
            value, off = varint(data, off)
        elif wire == 2:
            size, off = varint(data, off)
            value, off = data[off:off + size], off + size
        else:
            raise ValueError(f"unexpected wire type {wire}")
        fields[number] = value
    return fields.get(1, 0), fields.get(2, b"").decode(), fields.get(10, 0), fields.get(3, b"").decode()


def read_bundle(path):
    """The envelopes of the bundle, or an error."""
    unzstd = subprocess.run(["zstd", "-dcq", str(path)], capture_output=True)
    if unzstd.returncode != 0:
        return f"cannot read: {unzstd.stderr.decode(errors='replace').strip()[-200:]}"
    data = unzstd.stdout
    if data[:5] != b"dbin\x01":
        return "not a dbin v1 file"
    type_size = struct.unpack(">H", data[5:7])[0]
    if data[7:7 + type_size] != PAYLOAD_TYPE:
        return f"payload type {data[7:7 + type_size]!r}"
    off = 7 + type_size
    blocks = []
    while off < len(data):
        (size,) = struct.unpack(">I", data[off:off + 4])
        blocks.append(envelope(data[off + 4:off + 4 + size]))
        off += 4 + size
    if off != len(data):
        return "truncated"
    return blocks


def read_batch(rclone, src, bases):
    """Downloads the bundles and returns their names with their envelopes or errors."""
    names = [f"{base:010d}.dbin.zst" for base in bases]
    with tempfile.TemporaryDirectory(prefix="verify-") as tmp:
        tmp = pathlib.Path(tmp)
        (tmp / "list").write_text("".join(f"{name}\n" for name in names))
        # Files are looked up one by one: a listing of the store grows to a million bundles.
        subprocess.run([rclone, "copy", src, str(tmp / "bundles"), "--files-from-raw", str(tmp / "list"),
                        "--no-traverse", "--transfers", "16"], capture_output=True)
        return [(name, read_bundle(tmp / "bundles" / name) if (tmp / "bundles" / name).exists() else "missing")
                for name in names]


def check_blocks(items):
    """Checks bundles of consecutive bases, (base, name, envelopes or error) in order; returns blocks, errors."""
    errors = 0
    checked = 0
    prev = None  # number and id of the last block checked
    for base, name, blocks in items:
        if isinstance(blocks, str):
            print(f"{name}: {blocks}")
            errors += 1
            prev = None
            continue
        # Block 0 is the zero state, not a block.
        want = list(range(max(base, 1), base + BUNDLE))
        got = [number for number, _, _, _ in blocks]
        if got != want:
            print(f"{name}: blocks {got[:3]}..{got[-3:]} ({len(got)}), want {want[0]}..{want[-1]}")
            errors += 1
            prev = None
            continue
        for number, block_id, parent_num, parent_id in blocks:
            if parent_num != number - 1 or (prev is not None and (prev[0], prev[1]) != (parent_num, parent_id)):
                print(f"{name}: block {number} has parent {parent_num} {parent_id}, the block before is {prev}")
                errors += 1
            prev = (number, block_id)
        checked += len(blocks)
    return checked, errors


def check_all(args, bases):
    batches = [bases[i:i + args.batch] for i in range(0, len(bases), args.batch)]
    with concurrent.futures.ThreadPoolExecutor(args.jobs) as pool:
        results = (item for batch in pool.map(lambda b: read_batch(args.rclone, args.src, b), batches)
                   for item in batch)
        checked, errors = check_blocks((base, name, blocks) for base, (name, blocks) in zip(bases, results))
    print(f"{checked} blocks in {len(bases)} bundles checked, {errors} errors")
    return errors


def check_states(args, bases):
    chunks = []
    for state in args.states:
        chunks += [json.loads(f.read_text()) for f in pathlib.Path(state, "chunks").glob("*.json")]
    done = sorted((c for c in chunks if c["status"] == "done"), key=lambda c: c["from"])
    errors = 0
    # Chunks handed to another runner are done in its state.
    seqno = args.first
    for c in done:
        if c["from"] != seqno:
            print(f"chunks: {seqno}..{c['from'] - 1} not done" if c["from"] > seqno else f"chunks: {c['id']} overlaps")
            errors += 1
        seqno = max(seqno, c["to"] + 1)
    if seqno != args.last + 1:
        print(f"chunks: done up to {seqno - 1}, not {args.last}")
        errors += 1
    print(f"{len(done)} done chunks of {len(chunks)} in {len(args.states)} states, {errors} errors")

    # Sizes as the store lists them, against the sizes the runner uploaded.
    listing = subprocess.run([args.rclone, "lsf", "--files-only", "--format", "sp", args.src],
                             capture_output=True, text=True, check=True).stdout
    sizes = {}
    for line in listing.splitlines():
        size, name = line.split(";", 1)
        sizes[name] = int(size)
    names = [f"{base:010d}.dbin.zst" for base in bases]
    missing = [name for name in names if name not in sizes]
    empty = [name for name in names if sizes.get(name) == 0]
    stored = sum(sizes.get(name, 0) for name in names)
    uploaded = sum(c.get("bytes", 0) for c in done)
    print(f"store: {len(sizes)} bundles, {len(missing)} of {len(names)} missing, {len(empty)} empty, "
          f"{stored} bytes stored, {uploaded} uploaded")
    for name in missing[:20]:
        print(f"{name}: missing")
    errors += len(missing) + len(empty) + (stored != uploaded)

    # The bundles around every chunk boundary, and the first and the last.
    groups = [[bases[0]], [bases[-1]]]
    groups += [[c["from"] - BUNDLE, c["from"]] for c in done if c["from"] > bases[0] + BUNDLE]
    with concurrent.futures.ThreadPoolExecutor(args.jobs) as pool:
        results = list(pool.map(
            lambda g: check_blocks((b, *item) for b, item in zip(g, read_batch(args.rclone, args.src, g))), groups))
    checked = sum(r[0] for r in results)
    print(f"{len(groups) - 2} chunk boundaries: {checked} blocks checked, {sum(r[1] for r in results)} errors")
    return errors + sum(r[1] for r in results)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rclone", default="rclone")
    parser.add_argument("--src", required=True)
    parser.add_argument("--from", dest="first", type=int, required=True)
    parser.add_argument("--to", dest="last", type=int, required=True)
    parser.add_argument("--states", nargs="+", help="runner state directories: check the chunk boundaries only")
    parser.add_argument("--jobs", type=int, default=4, help="batches downloaded and read at once")
    parser.add_argument("--batch", type=int, default=100, help="bundles in a batch")
    args = parser.parse_args()

    bases = range(args.first - args.first % BUNDLE, args.last + 1, BUNDLE)
    errors = check_states(args, bases) if args.states else check_all(args, bases)
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
