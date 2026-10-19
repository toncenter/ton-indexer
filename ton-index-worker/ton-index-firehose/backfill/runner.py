#!/usr/bin/env python3
"""Runs the backfill of merged-blocks bundles chunk by chunk and uploads them.

    runner.py init   --state DIR --chunks FILE       queue the chunks of make_chunks.py
    runner.py run    --state DIR [options]           convert and upload the queued chunks
    runner.py status --state DIR                     counts and rates
    runner.py reset  --state DIR                     queue the failed chunks again

A chunk is a bounded run of ton-index-firehose: a run cannot be resumed, so a
chunk that fails is converted again from its start. It is done once its
bundles are all there, uploaded and checked against the store; then its local
copy is removed. The state of each chunk is a JSON file in DIR/chunks, so the
runner can be stopped (SIGTERM) and started again: running chunks go back to
the queue.
"""
import argparse
import datetime
import json
import os
import pathlib
import resource
import shlex
import shutil
import signal
import subprocess
import threading
import time

BUNDLE = 100
# The tool opens every SST file of the cell db.
OPEN_FILES = 1048576
MAX_PREWARM = 20000
MAX_ATTEMPTS = 3


def log(message):
    print(f"{datetime.datetime.now(datetime.timezone.utc):%Y-%m-%d %H:%M:%S} {message}", flush=True)


class Chunks:
    """The chunk states in DIR/chunks, one JSON file each."""

    def __init__(self, state_dir):
        self.dir = pathlib.Path(state_dir) / "chunks"
        self.lock = threading.Lock()

    def path(self, chunk_id):
        return self.dir / f"{chunk_id}.json"

    def load_all(self):
        return [json.loads(p.read_text()) for p in sorted(self.dir.glob("*.json"))]

    def save(self, chunk):
        tmp = self.path(chunk["id"]).with_suffix(".tmp")
        tmp.write_text(json.dumps(chunk))
        tmp.replace(self.path(chunk["id"]))

    def take(self, only=None):
        """Marks the first pending chunk running and returns it; only: the first seqnos to pick from."""
        with self.lock:
            for chunk in self.load_all():
                if chunk["status"] == "pending" and (only is None or chunk["from"] in only):
                    chunk["status"] = "running"
                    chunk["attempts"] += 1
                    chunk["started"] = time.time()
                    self.save(chunk)
                    return chunk
        return None

    def update(self, chunk, **fields):
        with self.lock:
            chunk.update(fields)
            self.save(chunk)


def init(args):
    chunks = Chunks(args.state)
    chunks.dir.mkdir(parents=True, exist_ok=True)
    existing = {c["id"] for c in chunks.load_all()}
    added = 0
    for line in open(args.chunks):
        first, last, prewarm = map(int, line.split())
        chunk_id = f"{first:010d}-{last:010d}"
        if chunk_id in existing:
            continue
        chunks.save({"id": chunk_id, "from": first, "to": last, "prewarm": prewarm, "status": "pending",
                     "attempts": 0})
        added += 1
    log(f"queued {added} chunks in {chunks.dir}")


def reset(args):
    chunks = Chunks(args.state)
    for chunk in chunks.load_all():
        if chunk["status"] == "failed":
            chunks.update(chunk, status="pending", attempts=0)
            log(f"{chunk['id']}: queued again")


def expected_bundles(chunk):
    return [f"{base:010d}.dbin.zst" for base in range(chunk["from"] - chunk["from"] % BUNDLE, chunk["to"] + 1, BUNDLE)]


class Runner:
    def __init__(self, args):
        self.args = args
        self.chunks = Chunks(args.state)
        self.work = pathlib.Path(args.work)
        self.stopping = threading.Event()
        self.processes = {}
        self.processes_lock = threading.Lock()

    def stop(self, *_):
        if not self.stopping.is_set():
            log("stopping: no new chunks, running ones are stopped and queued again")
        self.stopping.set()
        with self.processes_lock:
            for process in self.processes.values():
                if process.poll() is None:
                    process.terminate()

    def command(self, chunk, chunk_dir):
        tool = [self.args.bin, "--db", self.args.db, "--working-dir", str(chunk_dir / "wd"),
                "--from", str(chunk["from"]), "--to", str(chunk["to"]),
                "--merged-blocks-dir", str(chunk_dir / "merged"), "--prewarm", str(chunk["prewarm"]),
                *shlex.split(self.args.tool_args)]
        return ["env", *self.args.env, *tool]

    def convert(self, chunk):
        """Runs the tool on the chunk; returns an error or None, and its log."""
        chunk_dir = self.work / chunk["id"]
        shutil.rmtree(chunk_dir, ignore_errors=True)
        (chunk_dir / "merged").mkdir(parents=True)
        (chunk_dir / "wd").mkdir()
        with open(chunk_dir / "run.log", "wb") as out:
            process = subprocess.Popen(self.command(chunk, chunk_dir), stdout=out, stderr=subprocess.STDOUT,
                                       stdin=subprocess.DEVNULL)
            with self.processes_lock:
                self.processes[chunk["id"]] = process
            rc = process.wait()
            with self.processes_lock:
                del self.processes[chunk["id"]]
        text = (chunk_dir / "run.log").read_text(errors="replace")
        if rc != 0:
            return f"exit code {rc}", text
        if f"Finished bounded archive range {chunk['from']}..{chunk['to']}" not in text:
            return "the range did not finish", text
        files = set(os.listdir(chunk_dir / "merged"))
        missing = [name for name in expected_bundles(chunk) if name not in files]
        extra = files - set(expected_bundles(chunk))
        if missing or extra:
            return f"{len(missing)} bundles missing, {len(extra)} other files", text
        return None, text

    def rclone(self, *args):
        return subprocess.run([self.args.rclone, *args, *shlex.split(self.args.rclone_args)],
                              capture_output=True, text=True)

    def upload(self, chunk):
        merged = self.work / chunk["id"] / "merged"
        bundles = self.work / chunk["id"] / "bundles.txt"
        bundles.write_text("".join(f"{name}\n" for name in expected_bundles(chunk)))
        # Files are looked up one by one: a listing of the store grows to a million bundles.
        files = ["--files-from-raw", str(bundles), "--no-traverse"]
        for attempt in range(3):
            if attempt > 0:
                time.sleep(30)
            copy = self.rclone("copy", str(merged), self.args.dest, *files)
            check = copy.returncode == 0 and self.rclone("check", str(merged), self.args.dest, "--one-way", *files)
            if copy.returncode == 0 and check.returncode == 0:
                return None
            log(f"{chunk['id']}: upload attempt {attempt + 1} failed: "
                f"{(copy.stderr if copy.returncode else check.stderr).strip()[-300:]}")
        return "upload failed"

    def keep_log(self, chunk, name):
        logs = pathlib.Path(self.args.state) / "logs"
        logs.mkdir(exist_ok=True)
        shutil.copy(self.work / chunk["id"] / "run.log", logs / name)

    def process(self, chunk):
        chunk_dir = self.work / chunk["id"]
        while True:
            log(f"{chunk['id']}: converting, attempt {chunk['attempts']}, prewarm {chunk['prewarm']}")
            started = time.time()
            error, text = self.convert(chunk)
            elapsed = time.time() - started
            if self.stopping.is_set():
                self.chunks.update(chunk, status="pending", attempts=chunk["attempts"] - 1)
                shutil.rmtree(chunk_dir, ignore_errors=True)
                return
            if error is None:
                break
            # A trace starting before the prewarm is broken: a longer prewarm takes it in.
            broken = "is broken" in text
            retry = chunk["attempts"] < MAX_ATTEMPTS
            log(f"{chunk['id']}: {error}{' (broken trace)' if broken else ''}{'' if retry else ', failed'}")
            self.keep_log(chunk, f"{chunk['id']}.{chunk['attempts']}.log")
            shutil.rmtree(chunk_dir, ignore_errors=True)
            if not retry:
                self.chunks.update(chunk, status="failed", error=error)
                return
            self.chunks.update(chunk, attempts=chunk["attempts"] + 1, error=error,
                               prewarm=min(chunk["prewarm"] * 4, MAX_PREWARM) if broken else chunk["prewarm"])
        size = sum(f.stat().st_size for f in (chunk_dir / "merged").iterdir())
        blocks = chunk["to"] - chunk["from"] + 1
        log(f"{chunk['id']}: converted in {elapsed:.0f}s, {blocks / elapsed:.1f} blocks/s, "
            f"{size / 2**30:.1f} GiB; uploading")
        error = self.upload(chunk)
        if error is not None:
            self.chunks.update(chunk, status="failed", error=error)
            return
        self.keep_log(chunk, f"{chunk['id']}.log")
        shutil.rmtree(chunk_dir, ignore_errors=True)
        self.chunks.update(chunk, status="done", convert_seconds=round(elapsed), bytes=size,
                           finished=time.time(), error=None)
        log(f"{chunk['id']}: done")

    def worker(self):
        while not self.stopping.is_set():
            with self.taken_lock:
                if self.args.limit is not None and self.taken >= self.args.limit:
                    return
                chunk = self.chunks.take(self.args.only)
                if chunk is None:
                    return
                self.taken += 1
            try:
                self.process(chunk)
            except Exception as e:  # keep the other workers going
                log(f"{chunk['id']}: runner error {e!r}")
                self.chunks.update(chunk, status="failed", error=repr(e))

    def run(self):
        # The tool inherits the limit.
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (OPEN_FILES, OPEN_FILES))
        except (ValueError, OSError):
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
            log(f"open files limited to {hard}")
        for chunk in self.chunks.load_all():
            if chunk["status"] == "running":  # left by a stopped runner
                self.chunks.update(chunk, status="pending", attempts=chunk["attempts"] - 1)
        self.work.mkdir(parents=True, exist_ok=True)
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)
        self.taken = 0
        self.taken_lock = threading.Lock()
        threads = []
        for index in range(self.args.workers):
            # Workers start apart, so their cell db loads and uploads do not all coincide.
            if index > 0 and self.stopping.wait(self.args.stagger):
                break
            thread = threading.Thread(target=self.worker)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        log("runner finished")


def status(args):
    chunks = Chunks(args.state).load_all()
    counts = {}
    for chunk in chunks:
        counts[chunk["status"]] = counts.get(chunk["status"], 0) + 1
    done = [c for c in chunks if c["status"] == "done"]
    blocks_total = sum(c["to"] - c["from"] + 1 for c in chunks)
    blocks_done = sum(c["to"] - c["from"] + 1 for c in done)
    print(f"chunks: {counts}; blocks {blocks_done}/{blocks_total} ({100 * blocks_done / max(blocks_total, 1):.1f}%), "
          f"{sum(c.get('bytes', 0) for c in done) / 2**30:.1f} GiB uploaded")
    recent = [c for c in done if c.get("finished", 0) > time.time() - 3600]
    if recent:
        print(f"last hour: {len(recent)} chunks, {sum(c['to'] - c['from'] + 1 for c in recent)} blocks")
    for chunk in chunks:
        if chunk["status"] in ("running", "failed"):
            print(f"  {chunk['id']} {chunk['status']} attempts={chunk['attempts']} {chunk.get('error') or ''}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("init")
    p.add_argument("--state", required=True)
    p.add_argument("--chunks", required=True, help="from to prewarm lines of make_chunks.py")
    p = sub.add_parser("run")
    p.add_argument("--state", required=True)
    p.add_argument("--work", required=True, help="where chunks are converted to before the upload")
    p.add_argument("--bin", required=True, help="ton-index-firehose")
    p.add_argument("--db", required=True)
    p.add_argument("--tool-args", default="", help="more ton-index-firehose options")
    p.add_argument("--env", action="append", default=[], help="KEY=VALUE for the tool, such as LD_PRELOAD")
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--stagger", type=float, default=60, help="seconds between worker starts")
    p.add_argument("--limit", type=int, help="convert at most this many chunks")
    p.add_argument("--only", type=lambda v: {int(x) for x in v.split(",")},
                   help="convert only the chunks starting at these seqnos, comma-separated")
    p.add_argument("--rclone", default="rclone")
    p.add_argument("--rclone-args", default="--transfers 16 --checkers 16")
    p.add_argument("--dest", required=True, help="rclone destination, such as garage:firehose/merged-blocks")
    for cmd in ("status", "reset"):
        p = sub.add_parser(cmd)
        p.add_argument("--state", required=True)
    args = parser.parse_args()
    {"init": init, "run": lambda args: Runner(args).run(), "status": status, "reset": reset}[args.cmd](args)


if __name__ == "__main__":
    main()
