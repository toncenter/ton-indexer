"""Hermetic ClickHouse checks using the production DDL and resume query.

Usage: python3 total_supply_sql.py /path/to/test-gramstat /path/to/clickhouse
Only creates databases in a fresh clickhouse-local temporary directory.
"""

import base64
import subprocess
from pathlib import Path
import sys
import tempfile


def check_jetton_history(binary, clickhouse):
    def exported(option):
        return subprocess.check_output([binary, option], text=True)

    fixture = (Path(__file__).resolve().parents[2] / "tondb-scanner/test/jetton-wallet-history.sql").read_text()
    query = exported("--jetton-daily-query") + " ORDER BY day;\n"
    schema = exported("--jetton-schema")
    sql = "CREATE DATABASE history ENGINE=Atomic; USE history;\n"
    sql += "CREATE TABLE blocks (workchain Int32, seqno UInt32, gen_utime UInt32) ENGINE=Memory;\n"
    sql += schema + fixture
    # Repeat only the history INSERT; states from different MCs must survive FINAL.
    sql += "INSERT INTO jetton_wallet_states" + fixture.split("INSERT INTO jetton_wallet_states", 1)[1]
    sql += "SELECT count() FROM jetton_wallet_states FINAL;\n"
    sql += "SELECT balance FROM jetton_wallet_states FINAL WHERE account='0:A' AND mc_seqno=1;\n"
    sql += query
    sql += "OPTIMIZE TABLE jetton_wallet_states FINAL;\n" + query
    sql += exported("--jetton-daily-view")
    sql += "SYSTEM REFRESH VIEW jetton_wallet_counts_daily; SYSTEM WAIT VIEW jetton_wallet_counts_daily;\n"
    sql += "SELECT * FROM jetton_wallet_counts_daily ORDER BY day;\n"
    sql += "INSERT INTO jetton_wallet_states VALUES ('0:A',3,1788307200,30,true,'30');\n"
    sql += "SYSTEM REFRESH VIEW jetton_wallet_counts_daily; SYSTEM WAIT VIEW jetton_wallet_counts_daily;\n"
    sql += "SELECT * FROM jetton_wallet_counts_daily ORDER BY day;\n"
    with tempfile.TemporaryDirectory(prefix="jetton-ch-") as directory:
        result = subprocess.run([clickhouse, "local", "--path", directory, "--multiquery"],
                                input=sql, text=True, capture_output=True, timeout=90)
        if result.returncode:
            raise RuntimeError(result.stdout + result.stderr)
    daily = ["2026-09-01\t2\t2\t1\t0", "2026-09-02\t4\t\\N\t\\N\t1",
             "2026-09-03\t5\t\\N\t\\N\t1", "2026-09-04\t6\t1\t0\t0", "2026-09-05\t7\t1\t1\t0"]
    repaired = [daily[0], "2026-09-02\t4\t2\t2\t0", "2026-09-03\t5\t2\t2\t0", *daily[3:]]
    expected = ["11", "100000000000000000000000000000000000000000000000001"] + daily * 3 + repaired
    assert result.stdout.strip().splitlines() == expected, result.stdout
    print("ClickHouse jetton history, unknown intervals, replay and refreshable MV: OK")


def main():
    binary, clickhouse = sys.argv[1:]
    check_jetton_history(binary, clickhouse)

    def exported(option):
        return subprocess.check_output([binary, option], text=True)

    schema = exported("--schema")
    completed = exported("--completed-query")
    legacy = exported("--legacy-query")
    docs = (Path(__file__).resolve().parents[3] / "docs/gramstat.md").read_text()
    accounts = docs.split("<!-- accounts-clickhouse -->", 1)[1].split("```sql\n", 1)[1].split("```", 1)[0]
    deployments = docs.split("<!-- deployments-clickhouse -->", 1)[1].split("```sql\n", 1)[1].split("```", 1)[0]
    activity = docs.split("<!-- active-wallets-clickhouse -->", 1)[1].split("```sql\n", 1)[1].split("```", 1)[0]
    code1, code2, other = [base64.b64encode(bytes([i]) * 32).decode() for i in range(3)]

    def wallet_points(query):
        return (query.replace("{wallet_hashes:Array(String)}", f"['{code1}', '{code2}']")
                .replace("{from:UInt32}", "1788220800").replace("{to:UInt32}", "1788393600")
                .replace("{lt_from:UInt64}", "0").replace("{lt_to:UInt64}", "100"))

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

    # A crash after deployments, before deltas: replayed events remain one row,
    # and zero-event MCs also become complete only after their delta is inserted.
    deployment_completed = completed.replace("100", "200").replace("105", "201")
    sql += f"""
INSERT INTO blocks VALUES (-1, -9223372036854775808, 200), (-1, -9223372036854775808, 201);
INSERT INTO gramstat.total_supply VALUES
    (200, '2026-09-01 00:00:00', 101, 0, 23, 31, 47),
    (201, '2026-09-01 00:00:01', 101, 0, 23, 31, 47);
INSERT INTO gramstat.deployments VALUES (200, '2026-09-01 00:00:00', '0:A', 1, '{code1}', '{code2}', 0);
SELECT groupArray(seqno) FROM ({deployment_completed} ORDER BY seqno);
INSERT INTO gramstat.deployments VALUES (200, '2026-09-01 00:00:00', '0:A', 1, '{code1}', '{code2}', 0);
INSERT INTO gramstat.account_deltas VALUES
    (200, '2026-09-01 00:00:00', 0, 0, 0), (201, '2026-09-01 00:00:01', 0, 0, 0);
SELECT groupArray(seqno) FROM ({deployment_completed} ORDER BY seqno);
SELECT count() FROM gramstat.deployments FINAL;
TRUNCATE TABLE gramstat.deployments;
"""
    addresses = ["0:" + f"{i:064X}" for i in range(1, 6)]
    a, b, c, d, e = addresses
    master = "-1:" + f"{2:064X}"
    # Earlier wallet, earlier non-wallet, reactivation, zero outgoing, NULL code,
    # two wallet hashes, different workchains, and the exclusive period boundary.
    events = [
        (1, "2026-08-31 23:59:59", a, 10, code1, 0),
        (1, "2026-08-31 23:59:59", b, 11, other, 0),
        (2, "2026-09-01 00:00:00", b, 20, code2, 0),
        (2, "2026-09-01 00:00:00", d, 25, None, 0),
        (3, "2026-09-01 12:00:00", a, 30, code2, 2),
        (3, "2026-09-01 12:00:00", master, 31, code1, 0),
        (4, "2026-09-02 00:00:00", b, 40, code1, 0),
        (4, "2026-09-02 00:00:00", c, 45, code1, 0),
        (5, "2026-09-03 00:00:00", e, 60, code1, 0),
    ]
    values = ",\n".join(
        f"({mc}, '{stamp}', '{addr}', {lt}, '{code1}', " +
        (f"'{code}'" if code else "NULL") + f", {out_count})"
        for mc, stamp, addr, lt, code, out_count in events)
    sql += "INSERT INTO gramstat.deployments VALUES " + values + ";\n"
    sql += "INSERT INTO gramstat.deployments VALUES " + values + ";\n"  # duplicate retry
    sql += f"INSERT INTO gramstat.deployments VALUES (3, '2026-09-01 12:00:00', '{a}', 30, '{code1}', '{code2}', 3);\n"
    sql += "SELECT count(), countIf(isNull(init_code_hash)), sum(out_msgs_count) FROM gramstat.deployments FINAL;\n"
    sql += wallet_points(deployments)
    sql += "OPTIMIZE TABLE gramstat.deployments FINAL;\n"
    sql += wallet_points(deployments)
    sql += """
CREATE TABLE messages (tx_account String, tx_lt UInt64, tx_now UInt32,
    direction String, source Nullable(String), bounced Nullable(Bool))
ENGINE = Memory;
"""
    sends = [(a, 29, 1788220801, 0), (a, 29, 1788220801, 0),
             (b, 19, 1788220800, 0), (b, 20, 1788220801, 0),
             (c, 27, 1788220802, 0), (master, 31, 1788264000, None),
             (b, 49, 1788307201, 0), (c, 45, 1788307200, None),
             (d, 50, 1788307201, 0), (a, 50, 1788307201, 1)]
    sql += "INSERT INTO messages VALUES " + ",\n".join(
        f"('{addr}', {lt}, {stamp}, 'out', '{addr}', " +
        ("NULL" if bounced is None else str(bounced)) + ")"
        for addr, lt, stamp, bounced in sends) + ";\n"
    sql += wallet_points(activity)
    sql += wallet_points(activity.split("SELECT day, count() AS active_wallets", 1)[0] +
                         "SELECT uniqExact(account) FROM active;\n")

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
    expected += ["[]", "[200,201]", "1", "9\t1\t3",
                 "2026-09-01\t2", "2026-09-02\t1",
                 "2026-09-01\t2", "2026-09-02\t1",
                 "2026-09-01\t3", "2026-09-02\t2", "4"]
    actual = result.stdout.strip().splitlines()
    assert actual == expected, (actual, expected, result.stderr)
    print("ClickHouse schema, exact amounts, hourly/daily points, replay and resume: OK")


if __name__ == "__main__":
    main()
