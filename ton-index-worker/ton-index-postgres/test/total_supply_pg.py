"""Run the production Gramstat writer against a fresh Unix-socket-only cluster."""

import base64
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile


def run(*args, input=None):
    result = subprocess.run(args, input=input, text=True, capture_output=True, timeout=90)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result


def main():
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        print("initdb requires an unprivileged user")
        return 77
    binary, initdb, pg_ctl, migrate = sys.argv[1:]
    psql = str(Path(initdb).with_name("psql"))
    docs = (Path(__file__).resolve().parents[3] / "docs/gramstat.md").read_text()
    accounts = docs.split("<!-- accounts-postgres -->", 1)[1].split("```sql\n", 1)[1].split("```", 1)[0]
    deployments = docs.split("<!-- deployments-postgres -->", 1)[1].split("```sql\n", 1)[1].split("```", 1)[0]
    jetton_daily = docs.split("<!-- jetton-daily-postgres -->", 1)[1].split("```sql\n", 1)[1].split("```", 1)[0]
    nft_daily = docs.split("<!-- nft-daily-postgres -->", 1)[1].split("```sql\n", 1)[1].split("```", 1)[0]
    nft_owner = docs.split("<!-- nft-owner-postgres -->", 1)[1].split("```sql\n", 1)[1].split("```", 1)[0]
    with tempfile.TemporaryDirectory(prefix="gpg-") as directory:
        root = Path(directory)
        data = root / "data"
        run(initdb, "-D", str(data), "-U", "gramstat_test", "-A", "trust", "--no-locale", "-E", "UTF8")
        options = f"-F -c listen_addresses='' -k {shlex.quote(str(root))}"
        run(pg_ctl, "-D", str(data), "-l", str(root / "postgres.log"), "-o", options, "-w", "start")
        try:
            connection = f"host='{root}' user=gramstat_test dbname=postgres"
            result = run(binary, connection)
            print(result.stdout, end="")

            def sql(query):
                return run(psql, "-XqAt", "-F", "\t", "-v", "ON_ERROR_STOP=1",
                           connection, "-f", "-", input=query).stdout.strip()

            points = sql("SET TIME ZONE 'Pacific/Honolulu';\n"
                         "PREPARE accounts(bigint, date, date) AS " + accounts + "\n"
                         "EXECUTE accounts(5, '2026-09-01', '2026-09-03');\n"
                         "EXECUTE accounts(5, '2026-09-02', '2026-09-03');\n"
                         "EXECUTE accounts(7, '2026-09-01', '2026-09-03');")
            assert points.splitlines() == [
                "2026-09-01\t8\t22\t2\t32",
                "2026-09-02\t9\t22\t1\t32", "2026-09-02\t9\t22\t1\t32"], points

            code1, code2, other = [base64.b64encode(bytes([i]) * 32).decode() for i in range(3)]
            a, b, c, d, e = ["0:" + f"{i:064X}" for i in range(1, 6)]
            master = "-1:" + f"{2:064X}"
            events = [
                (1, 1788220799, a, 10, code1, 0), (1, 1788220799, b, 11, other, 0),
                (2, 1788220800, b, 20, code2, 0), (2, 1788220800, d, 25, None, 0),
                (3, 1788264000, a, 30, code2, 3), (3, 1788264000, master, 31, code1, 0),
                (4, 1788307200, b, 40, code1, 0), (4, 1788307200, c, 45, code1, 0),
                (5, 1788393600, e, 60, code1, 0),
            ]
            values = ",\n".join(
                f"({mc}, {stamp}, '{addr}', {lt}, '{code1}', " +
                (f"'{code}'" if code else "NULL") + f", {out_count})"
                for mc, stamp, addr, lt, code, out_count in events)
            sql("TRUNCATE gramstat.deployments; INSERT INTO gramstat.deployments VALUES " + values + ";")
            points = sql("SET TIME ZONE 'Pacific/Honolulu';\n"
                         "PREPARE deployments(tonhash[], bigint, bigint) AS " + deployments + "\n" +
                         f"EXECUTE deployments(ARRAY['{code1}', '{code2}']::tonhash[], 1788220800, 1788393600);")
            assert points.splitlines() == ["2026-09-01\t2", "2026-09-02\t1"], points

            # Exercise the real 1.3.2 migration and its rerun over an earlier 1.3.2.
            sql("CREATE TABLE ton_db_version (id int PRIMARY KEY, major int, minor int, patch int);"
                "INSERT INTO ton_db_version VALUES (1, 1, 3, 1);")
            for extra in ([], ["--rerun-last-migration"]):
                sql("DROP TABLE gramstat.account_deltas; DROP TABLE gramstat.deployments;")
                sql("DROP TABLE nft_item_states;")
                run(migrate, "--pg", connection, "--no-create-indexes", *extra)
                assert sql("SELECT major || '.' || minor || '.' || patch FROM ton_db_version") == "1.3.2"
                assert sql("SELECT count(*) FROM gramstat.account_deltas") == "0"
                assert sql("SELECT count(*) FROM gramstat.total_supply") == "4"
                assert sql("SELECT count(*) FROM gramstat.deployments") == "0"
                assert sql("SELECT count(*) FROM pg_indexes WHERE schemaname='gramstat' AND tablename='deployments'") == "4"
                assert sql("SELECT count(*) FROM jetton_wallet_states") == "4"
                assert sql("SELECT count(*) FROM pg_indexes WHERE schemaname='public' AND tablename='jetton_wallet_states'") == "1"
                assert sql("SELECT to_regclass('public.jetton_wallet_counts_daily') IS NULL") == "t"
                assert sql("SELECT to_regclass('public.jetton_wallet_counts_daily_day_idx') IS NULL") == "t"
                assert sql("SELECT count(*) FROM nft_item_states") == "0"
                assert sql("SELECT count(*) FROM pg_indexes WHERE schemaname='public' AND tablename='nft_item_states'") == "2"
                assert sql("SELECT to_regclass('public.nft_item_counts_daily') IS NULL") == "t"
                assert sql("SELECT to_regclass('public.nft_item_counts_daily_day_idx') IS NULL") == "t"

            fixture = (Path(__file__).resolve().parents[2] / "tondb-scanner/test/jetton-wallet-history.sql").read_text()
            sql("TRUNCATE jetton_wallet_states; TRUNCATE blocks;\n" + fixture)
            sql("INSERT INTO jetton_wallet_states (account,mc_seqno,gen_utime,last_tx_lt,existing,balance) "
                "SELECT account,mc_seqno,gen_utime,last_tx_lt,existing,balance FROM jetton_wallet_states "
                "ON CONFLICT (account,mc_seqno) DO NOTHING;")
            assert sql("SELECT count(*) FROM jetton_wallet_states") == "11"
            # Optional aggregation is installed explicitly using the documentation,
            # never by the production migration (including a 1.3.2 rerun).
            sql(jetton_daily)
            # The first refresh populates the view. Later refreshes can be concurrent.
            sql("REFRESH MATERIALIZED VIEW jetton_wallet_counts_daily;")
            points = sql("SET TIME ZONE 'Pacific/Honolulu'; SELECT * FROM jetton_wallet_counts_daily ORDER BY day;")
            daily = ["2026-09-01\t2\t2\t1\t0", "2026-09-02\t4\t\t\t1", "2026-09-03\t5\t\t\t1",
                     "2026-09-04\t6\t1\t0\t0", "2026-09-05\t7\t1\t1\t0"]
            assert points.splitlines() == daily, points
            sql("UPDATE jetton_wallet_states SET existing=true,balance=30 WHERE account='0:A' AND mc_seqno=3;")
            sql("REFRESH MATERIALIZED VIEW CONCURRENTLY jetton_wallet_counts_daily;")
            points = sql("SELECT * FROM jetton_wallet_counts_daily ORDER BY day;")
            assert points.splitlines() == [daily[0], "2026-09-02\t4\t2\t2\t0", "2026-09-03\t5\t2\t2\t0", *daily[3:]], points
            print("PostgreSQL jetton history and documented optional MV/concurrent refresh: OK")
            fixture = (Path(__file__).resolve().parents[2] / "tondb-scanner/test/nft-item-history.sql").read_text()
            sql("TRUNCATE nft_item_states; TRUNCATE blocks;\n" + fixture)
            sql("INSERT INTO nft_item_states SELECT * FROM nft_item_states ON CONFLICT (account,mc_seqno) DO NOTHING;")
            assert sql("SELECT count(*) FROM nft_item_states") == "11"
            owners = sql("PREPARE nft_owner(tonaddr) AS " + nft_owner + "\nEXECUTE nft_owner('0:X');")
            assert owners.splitlines() == ["0:A", "0:B"], owners
            sql(nft_daily)
            sql("REFRESH MATERIALIZED VIEW nft_item_counts_daily;")
            points = sql("SET TIME ZONE 'Pacific/Honolulu'; SELECT * FROM nft_item_counts_daily ORDER BY day;")
            daily = ["2026-09-01\t2\t2\t0", "2026-09-02\t4\t\t1", "2026-09-03\t5\t\t1",
                     "2026-09-04\t6\t1\t0", "2026-09-05\t7\t1\t0"]
            assert points.splitlines() == daily, points
            sql("INSERT INTO nft_item_states VALUES ('0:A',3,1788307200,30,true,'0:Z') "
                "ON CONFLICT (account,mc_seqno) DO UPDATE SET existing=EXCLUDED.existing,owner=EXCLUDED.owner;")
            sql("REFRESH MATERIALIZED VIEW CONCURRENTLY nft_item_counts_daily;")
            points = sql("SELECT * FROM nft_item_counts_daily ORDER BY day;")
            assert points.splitlines() == [daily[0], "2026-09-02\t4\t2\t0", "2026-09-03\t5\t2\t0", *daily[3:]], points
            assert sql("SELECT count(*) FROM nft_item_states WHERE existing AND owner IS NULL") == "1"
            print("PostgreSQL NFT owner history, replay and documented optional daily view: OK")
            print("PostgreSQL documented Accounts/deployments SQL, prefix gaps and migration 1.3.2 rerun: OK")
        finally:
            run(pg_ctl, "-D", str(data), "-m", "immediate", "-w", "stop")
    return 0


if __name__ == "__main__":
    sys.exit(main())
