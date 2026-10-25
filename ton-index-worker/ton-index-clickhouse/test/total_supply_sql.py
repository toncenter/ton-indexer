"""Hermetic ClickHouse checks using the production DDL and resume query.

Usage: python3 total_supply_sql.py /path/to/test-gramstat /path/to/clickhouse
Only creates databases in a fresh clickhouse-local temporary directory.
"""

import subprocess
import sys
import tempfile


def main():
    binary, clickhouse = sys.argv[1:]

    def exported(option):
        return subprocess.check_output([binary, option], text=True)

    schema = exported("--schema")
    completed = exported("--completed-query")
    legacy = exported("--legacy-query")
    sql = schema + """
CREATE TABLE blocks (workchain Int32, shard Int64, seqno Int32)
ENGINE = ReplacingMergeTree ORDER BY (workchain, shard, seqno);
INSERT INTO blocks VALUES (-1, -9223372036854775808, 100),
    (-1, -9223372036854775808, 101), (-1, -9223372036854775808, 101),
    (-1, -9223372036854775808, 102), (0, -9223372036854775808, 103),
    (-1, -9223372036854775808, 105), (-1, -9223372036854775808, 106);

-- Out of order, duplicate replay, same-second MCs, missing hour, UTC midnight
-- and month boundaries. The large totals must survive exact decimal storage.
INSERT INTO gramstat.total_supply VALUES
    (101, '2026-09-01 00:59:59', 100000000000000000102, 100000000000000000001, 23, 31, 47),
    (100, '2026-09-01 00:59:59', 100000000000000000101, 100000000000000000000, 23, 31, 47),
    (101, '2026-09-01 00:59:59', 100000000000000000102, 100000000000000000001, 23, 31, 47),
    (103, '2026-09-01 02:00:00', 100000000000000000104, 100000000000000000003, 23, 31, 47),
    (104, '2026-09-02 00:00:00', 100000000000000000105, 100000000000000000004, 23, 31, 47),
    (105, '2026-10-01 00:00:00', 100000000000000000106, 100000000000000000005, 23, 31, 47),
    (106, '2026-10-01 01:00:00', 100000000000000000107, 100000000000000000006, 23, 31, 47);
"""
    hourly = """
SELECT toStartOfInterval(gen_utime, INTERVAL 1 HOUR, 'UTC') AS bucket,
       argMax(total, tuple(gen_utime, mc_seqno))
FROM gramstat.total_supply
WHERE gen_utime >= toDateTime('2026-09-01', 'UTC')
  AND gen_utime < toDateTime('2026-09-02', 'UTC')
GROUP BY bucket ORDER BY bucket;
"""
    sql += f"SELECT groupArray(seqno) FROM ({completed} ORDER BY seqno);\n"
    sql += f"SELECT groupArray(seqno) FROM ({legacy} ORDER BY seqno);\n"
    sql += hourly
    sql += """
SELECT toStartOfInterval(gen_utime, INTERVAL 1 DAY, 'UTC') AS bucket,
       argMax(tuple(total, accounts, dispatch_queue, unprocessed_messages, unrecovered_validator_fees),
              tuple(gen_utime, mc_seqno))
FROM gramstat.total_supply
WHERE gen_utime >= toDateTime('2026-09-01', 'UTC')
  AND gen_utime < toDateTime('2026-10-01', 'UTC')
GROUP BY bucket ORDER BY bucket;
OPTIMIZE TABLE gramstat.total_supply FINAL;
SELECT count() FROM gramstat.total_supply;
"""
    sql += hourly
    # Simulate repairing a block whose supply insert failed before restart.
    sql += """
INSERT INTO gramstat.total_supply VALUES
    (102, '2026-09-01 01:00:00', 100000000000000000103, 100000000000000000002, 23, 31, 47);
"""
    sql += f"SELECT groupArray(seqno) FROM ({completed} ORDER BY seqno);\n"

    with tempfile.TemporaryDirectory(prefix="gramstat-sql-") as path:
        result = subprocess.run(
            [clickhouse, "local", "--path", path, "--multiquery", "--output-format", "TSVRaw", "--query", sql],
            text=True, capture_output=True, timeout=90,
        )
        assert result.returncode == 0, result.stderr
    expected = [
        "[100,101,105]",  # excludes missing supply, shard-only and missing MC rows
        "[100,101,102,105]",
        "2026-09-01 00:00:00\t100000000000000000102",
        "2026-09-01 02:00:00\t100000000000000000104",
        "2026-09-01 00:00:00\t(100000000000000000104,100000000000000000003,23,31,47)",
        "2026-09-02 00:00:00\t(100000000000000000105,100000000000000000004,23,31,47)",
        "6",
        "2026-09-01 00:00:00\t100000000000000000102",
        "2026-09-01 02:00:00\t100000000000000000104",
        "[100,101,102,105]",
    ]
    actual = result.stdout.strip().splitlines()
    assert actual == expected, (actual, expected, result.stderr)
    print("ClickHouse schema, exact amounts, hourly/daily points, replay and resume: OK")


if __name__ == "__main__":
    main()
