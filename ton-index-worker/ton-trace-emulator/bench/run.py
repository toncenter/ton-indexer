#!/usr/bin/env python3
"""Run the pipeline benchmark against an owned, disposable Redis (stdlib only)."""
import argparse
import csv
import json
import math
import os
import re
from pathlib import Path
import subprocess
import tempfile
import time


def rows(path):
    with path.open() as f:
        return list(csv.DictReader(f))


def info(cli):
    data = subprocess.check_output(cli + ["INFO", "ALL"], text=True, timeout=3)
    return dict(line.split(":", 1) for line in data.splitlines() if ":" in line and not line.startswith("#"))


def stop(process):
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def summarize(output, before, after, redis_samples, cli, command, exit_code):
    summary = {}
    for row in rows(output / "summary.csv"):
        try:
            summary[row["metric"]] = float(row["value"])
        except ValueError:
            summary[row["metric"]] = row["value"]
    blocks = rows(output / "blocks.csv")
    lags = sorted(float(row["lag_ms"]) for row in blocks if row["success"] == "1")
    summary["finalized_lag_ms"] = {
        name: lags[min(len(lags) - 1, math.ceil(p * len(lags)) - 1)] if lags else None
        for name, p in [("p50", .50), ("p95", .95), ("p99", .99), ("max", 1)]
    }
    summary["finalized_last_lag_ms"] = float(blocks[-1]["lag_ms"]) if blocks else None
    summary["max_source_lateness_ms"] = max((float(row["source_lateness_ms"]) for row in blocks), default=0)
    measured_until = summary["drained_at_seconds"] or summary["elapsed_seconds"]
    summary["drain_seconds"] = max(0, measured_until - summary["input_horizon_seconds"])
    summary["redis_cpu_seconds"] = sum(float(after[k]) - float(before[k])
                                        for k in ["used_cpu_user", "used_cpu_sys"])
    summary["redis_peak_busy_cores"] = max((sample["busy_cores"] for sample in redis_samples), default=0)
    summary["redis_peak_memory_bytes"] = int(after["used_memory_peak"])
    summary["redis_commands"] = int(after["total_commands_processed"]) - int(before["total_commands_processed"])
    summary["redis_commandstats"] = {k: v for k, v in after.items() if k.startswith("cmdstat_")}
    # Actor timing is elapsed time inside actor messages, including OS preemption.
    # Report the busiest full sampling interval, excluding the final idle settle.
    actor_peaks = {}
    for row in rows(output / "actors.csv"):
        if float(row["interval_s"]) >= .5 and float(row["elapsed_s"]) <= measured_until + .01:
            actor_peaks[row["actor"]] = max(actor_peaks.get(row["actor"], 0), float(row["busy_cores"]))
    summary["actor_peak_busy_cores"] = dict(sorted(actor_peaks.items(), key=lambda item: -item[1]))
    key = summary["sample_trace_key"]
    fields = ["mch_classify_state", "root_node", "update_seq"]
    values = subprocess.check_output(cli + ["HMGET", key] + fields, text=True, timeout=3).splitlines()
    summary["sample_trace"] = dict(zip(fields, values))
    keys = subprocess.check_output(cli + ["HKEYS", key], text=True, timeout=3).splitlines()
    summary["sample_trace"]["retained_nodes"] = sum(bool(re.fullmatch(r"[A-Za-z0-9+/]{43}=", field)) for field in keys)
    summary["sample_trace"]["expected_nodes"] = int(summary["nodes_per_trace"])
    summary["sample_trace"]["complete"] = summary["sample_trace"]["retained_nodes"] == int(summary["nodes_per_trace"])
    summary["command"] = command
    summary["exit_code"] = exit_code
    summary["host"] = {"cpu_count": os.cpu_count(), "uname": list(os.uname())}
    summary["redis_version"] = after["redis_version"]
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({k: summary[k] for k in ["completed_blocks", "failed_blocks", "timed_out",
                     "finalized_lag_ms", "max_block_backlog", "promoted_blocks", "promotion_fallback_blocks",
                     "actor_peak_busy_cores", "redis_peak_busy_cores", "sample_trace"]}, indent=2))
    print(f"Full results: {output / 'summary.json'}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path(__file__).resolve().parents[2] /
                        "build/ton-trace-emulator/bench-ton-trace-pipeline")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=300, help="Whole run, including generation (seconds)")
    args, extra = parser.parse_known_args()
    if any(arg == "--redis" or arg.startswith("--redis=") for arg in extra):
        parser.error("Redis is always created by this runner; --redis is not accepted")
    if args.timeout <= 0 or not math.isfinite(args.timeout):
        parser.error("--timeout must be positive and finite")
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    benchmark = None
    redis = None
    with tempfile.TemporaryDirectory(prefix="ton-trace-bench-") as temporary:
        socket = str(Path(temporary) / "redis.sock")
        cli = ["redis-cli", "-s", socket, "--raw"]
        with (output / "redis.log").open("w") as redis_log, (output / "benchmark.log").open("w") as log:
            try:
                redis = subprocess.Popen(["redis-server", "--port", "0", "--unixsocket", socket,
                    "--unixsocketperm", "700", "--save", "", "--appendonly", "no", "--dir", temporary,
                    "--loglevel", "warning"], stdout=redis_log, stderr=subprocess.STDOUT)
                deadline = time.monotonic() + 5
                while not Path(socket).exists():
                    if redis.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError(f"Private Redis did not start; see {output / 'redis.log'}")
                    time.sleep(.02)
                subprocess.run(cli + ["PING"], check=True, capture_output=True, timeout=3)
                before = info(cli)
                command = [str(args.binary.resolve()), *extra, "--redis", "unix://" + socket,
                           "--output-dir", str(output)]
                (output / "command.json").write_text(json.dumps(command, indent=2) + "\n")
                benchmark = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
                started = previous_time = time.monotonic()
                previous_cpu = sum(float(before[k]) for k in ["used_cpu_user", "used_cpu_sys"])
                samples = []
                while benchmark.poll() is None:
                    if time.monotonic() - started > args.timeout:
                        raise TimeoutError(f"Benchmark exceeded {args.timeout}s; see {output / 'benchmark.log'}")
                    time.sleep(.5)
                    current = info(cli)
                    now = time.monotonic()
                    cpu = sum(float(current[k]) for k in ["used_cpu_user", "used_cpu_sys"])
                    samples.append({"runner_elapsed_s": now - started, "busy_cores": (cpu - previous_cpu) /
                                    (now - previous_time), "memory_bytes": int(current["used_memory"]),
                                    "commands": int(current["total_commands_processed"])})
                    previous_cpu, previous_time = cpu, now
                after = info(cli)
                (output / "redis-samples.json").write_text(json.dumps(samples, indent=2) + "\n")
                if (output / "summary.csv").exists():
                    summarize(output, before, after, samples, cli, command, benchmark.returncode)
                else:
                    print((output / "benchmark.log").read_text()[-10000:])
                return benchmark.returncode
            finally:
                stop(benchmark)
                stop(redis)


if __name__ == "__main__":
    raise SystemExit(main())
