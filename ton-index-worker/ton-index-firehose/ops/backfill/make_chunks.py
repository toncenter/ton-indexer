#!/usr/bin/env python3
"""Splits a masterchain range into backfill chunks of about equal work.

The work of a block grows with the size of the archive packages it is in, so
chunks are cut by archive volume, read from `du -s --block-size=1M arch*` of
archive/packages (one directory per 100000 masterchain blocks).

    make_chunks.py <du output> <from> <to> <max MB per chunk> <max blocks per chunk>

Prints `from to prewarm` lines. Every chunk but the first starts at a multiple
of 100 and every chunk ends just below one, as merged-blocks bundles need.
"""
import sys

PACKAGE_DIR_BLOCKS = 100000
# Blocks came every 2-5 s before seqno 59M and about every 0.4 s after. The
# TraceAssembler prewarm has to cover the traces still going at the start of a
# chunk: 500 blocks were too few at 40M; the runner raises it on a broken trace.
FAST_BLOCKS_FROM = 59_000_000


def prewarm(seqno):
    return 3000 if seqno >= FAST_BLOCKS_FROM else 2000


def main():
    du_path, first, last, max_mb, max_blocks = sys.argv[1], *map(int, sys.argv[2:6])
    sizes = {}
    for line in open(du_path):
        mb, name = line.split()
        sizes[int(name.removeprefix("arch"))] = int(mb)
    known = sorted(sizes)
    chunks = []
    start = first
    mb = 0.0
    seqno = first
    while seqno <= last:
        package = seqno // PACKAGE_DIR_BLOCKS
        end = min((package + 1) * PACKAGE_DIR_BLOCKS - 1, last)
        # packages written after the listing are as heavy as the last one listed
        size = sizes.get(package, sizes[known[-1]]) * (end - seqno + 1) / PACKAGE_DIR_BLOCKS
        if seqno > start and (mb + size > max_mb or end - start + 1 > max_blocks):
            chunks.append((start, seqno - 1))
            start, mb = seqno, 0.0
        mb += size
        seqno = end + 1
        # a short first chunk tests the start quickly
        if start == 1 and seqno > start:
            chunks.append((start, seqno - 1))
            start, mb = seqno, 0.0
    if start <= last:
        chunks.append((start, last))
    for a, b in chunks:
        assert (a == 1 or a % 100 == 0) and (b + 1) % 100 == 0, (a, b)
        print(a, b, prewarm(a))


if __name__ == "__main__":
    main()
