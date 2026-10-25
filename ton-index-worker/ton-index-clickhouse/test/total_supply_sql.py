"""Hermetic ClickHouse checks using the production DDL and resume query.

Usage: python3 total_supply_sql.py /path/to/test-gramstat /path/to/clickhouse
Only creates databases in a fresh clickhouse-local temporary directory.
"""

import subprocess
from pathlib import Path
import sys
import tempfile


def main():
    binary, clickhouse = sys.argv[1:]

    def exported(option):
        return subprocess.check_output([binary, option], text=True)

    schema = exported("--schema")
    completed = exported("--completed-query")
    legacy = exported("--legacy-query")
    docs = (Path(__file__).resolve().parents[3] / "docs/gramstat.md").read_text()
    accounts = docs.split("<!-- accounts-clickhouse -->", 1)[1].split("```sql\n", 1)[1].split("```", 1)[0]

    def account_points(end_mc=5, from_day="2026-09-01"):
        return (accounts.replace("{end_mc:UInt32}", str(end_mc))
                .replace("{from_day:Date}", f"toDate('{from_day}')")
                .replace("{to_day:Date}", "toDate('2026-09-03')"))

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
INSERT INTO gramstat.account_deltas VALUES
    (100, '2026-09-01 00:59:59', 0, 0, 0),
    (101, '2026-09-01 00:59:59', -1, 1, 0),
    (101, '2026-09-01 00:59:59', -1, 1, 0),
    (103, '2026-09-01 02:00:00', 0, 0, 0),
    (104, '2026-09-02 00:00:00', 0, 0, 0),
    (105, '2026-10-01 00:00:00', 0, 0, 0);
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

    # Supply was repaired, but deltas are still missing: the MC remains incomplete.
    sql += "INSERT INTO gramstat.account_deltas VALUES (102, '2026-09-01 01:00:00', 0, 0, 0);\n"
    sql += f"SELECT groupArray(seqno) FROM ({completed} ORDER BY seqno);\n"
    sql += """
INSERT INTO gramstat.account_deltas VALUES
    (1, '2026-08-31 23:59:59', 10, 20, 1),
    (3, '2026-09-01 23:59:59', 0, -1, 1),
    (2, '2026-09-01 00:00:00', -2, 3, 0),
    (4, '2026-09-02 00:00:00', 0, 0, 0),
    (5, '2026-09-02 12:00:00', 100, 0, -1),
    (7, '2026-09-02 13:00:00', 0, 0, 0);
-- Replay and corrected replay, before background merges.
INSERT INTO gramstat.account_deltas VALUES
    (2, '2026-09-01 00:00:00', -2, 3, 0),
    (5, '2026-09-02 12:00:00', 1, 0, -1);
"""
    sql += account_points()
    sql += account_points(from_day="2026-09-02")
    sql += account_points(end_mc=7)  # MC 6 is missing: no absolute points
    sql += "OPTIMIZE TABLE gramstat.account_deltas FINAL;\n"
    sql += account_points()

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
        "[100,101,105]",  # supply repaired, account deltas still absent
        "[100,101,102,105]",
        "2026-09-01\t8\t22\t2\t32",
        "2026-09-02\t9\t22\t1\t32",
        "2026-09-02\t9\t22\t1\t32",
        "2026-09-01\t8\t22\t2\t32",
        "2026-09-02\t9\t22\t1\t32",
    ]
    actual = result.stdout.strip().splitlines()
    assert actual == expected, (actual, expected, result.stderr)
    print("ClickHouse schema, exact amounts, hourly/daily points, replay and resume: OK")


if __name__ == "__main__":
    main()
