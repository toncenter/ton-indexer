"""Exercise the production ClickHouse schema and actual serialized writer columns."""

from pathlib import Path
import subprocess
import tempfile


def check_validators(binary, clickhouse):
    def exported(option):
        return subprocess.check_output([binary, option], text=True)

    docs = (Path(__file__).resolve().parents[3] / "docs/validator-clickhouse.md").read_text()

    def query(marker):
        return docs.split(f"<!-- {marker} -->", 1)[1].split("```sql\n", 1)[1].split("```", 1)[0].strip()

    cycles = query("validator-cycles-clickhouse")
    members = query("validator-members-clickhouse")
    tables = ["validator_events", "validator_elections", "validator_election_participants",
              "validator_cycles", "validator_cycle_members", "validator_complaints", "contract_methods"]
    checks = f"""
SELECT count(),sum(amount),max(query_id),min(tx_lt) FROM validator_events FINAL;
SELECT event_type,isNull(validator_pubkey),isNull(election_id),reason,amount
FROM validator_events FINAL WHERE mc_seqno=200;
SELECT election_id,total_stake,finished,source_mc_seqno FROM validator_elections FINAL;
SELECT count(),sum(stake),uniqExact(stake_holder_address),max(source_mc_seqno)
FROM validator_election_participants FINAL;
{cycles} ORDER BY utime_since;
SELECT utime_since,validator_index,validator.3,stake,source_mc_seqno
FROM ({members}) ORDER BY utime_since,validator_index;
SELECT argMax(stake_held_for,source_mc_seqno),max(max_stake)
FROM validator_cycles FINAL WHERE utime_since=1788220800;
SELECT isNull(adnl_addr),paid,suggested_fine,voted_validators,weight_remaining,approved_percent,is_passed,source_mc_seqno
FROM validator_complaints FINAL;
SELECT length(code_hash),methods FROM contract_methods FINAL;
SELECT marker FROM default.validator_events;
"""
    schema = exported("--validator-schema")
    sql = """
CREATE TABLE default.validator_events (marker UInt8) ENGINE=Memory;
INSERT INTO default.validator_events VALUES (42);
CREATE DATABASE validator_pilot;
USE validator_pilot;
""" + schema + schema
    sql += "\n".join(f"SYSTEM STOP MERGES {table};" for table in tables) + "\n"
    sql += exported("--validator-fixture") + checks
    sql += "\n".join(f"SYSTEM START MERGES {table}; OPTIMIZE TABLE {table} FINAL; SYSTEM STOP MERGES {table};"
                     for table in tables) + "\n" + checks
    sql += exported("--validator-correction") + checks

    big = "900719925474099312345"
    fined = "900719925474099312300"
    weight = "18446744073709551615"
    expected = [
        f"3\t{big}\t{weight}\t9007199254741093",
        "recover_failed\t1\t1\t-17\t0",
        f"1788000000\t{fined}\ttrue\t300",
        "2\t900719925474099312307\t2\t300",
        f"1788220800\t1788000000\t{big}\t(1788286336,1,1,{weight})\t300",
        f"1788286336\t\\N\t\\N\t(1788351872,1,1,{weight})\t300",
        f"1788351872\t\\N\t0\t(1788417408,1,1,{weight})\t300",
        f"1788220800\t0\t{weight}\t{big}\t300",
        f"1788286336\t0\t{weight}\t\\N\t300",
        f"1788351872\t0\t{weight}\t0\t300",
        "32769\t1329227995784915872903807060280344575",
        f"1\t0\t{big}\t[0,1]\t-17\t100\ttrue\t300",
        "44\t[85143,97026]",
        "42",
    ]
    corrected = expected.copy()
    for index in (4, 7):
        corrected[index] = corrected[index].replace(big, "900719925474099312320")
    with tempfile.TemporaryDirectory(prefix="validator-storage-ch-") as directory:
        result = subprocess.run([clickhouse, "local", "--path", directory, "--multiquery"],
                                input=sql, text=True, capture_output=True, timeout=90)
        assert result.returncode == 0, result.stdout + result.stderr
    actual = result.stdout.strip().splitlines()
    assert actual == expected * 2 + corrected, (actual, expected * 2 + corrected)
    print("ClickHouse validators: exact/null amounts, late batches, retry/merge/correction and database isolation: OK")
