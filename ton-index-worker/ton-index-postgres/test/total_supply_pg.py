"""Run the production Gramstat writer against a fresh Unix-socket-only cluster."""

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

            # Exercise the real 1.3.2 migration and its rerun over an earlier 1.3.2.
            sql("CREATE TABLE ton_db_version (id int PRIMARY KEY, major int, minor int, patch int);"
                "INSERT INTO ton_db_version VALUES (1, 1, 3, 1);")
            for extra in ([], ["--rerun-last-migration"]):
                sql("DROP TABLE gramstat.account_deltas;")
                run(migrate, "--pg", connection, "--no-create-indexes", *extra)
                assert sql("SELECT major || '.' || minor || '.' || patch FROM ton_db_version") == "1.3.2"
                assert sql("SELECT count(*) FROM gramstat.account_deltas") == "0"
                assert sql("SELECT count(*) FROM gramstat.total_supply") == "4"
            print("PostgreSQL documented Accounts SQL, prefix gaps and migration 1.3.2 rerun: OK")
        finally:
            run(pg_ctl, "-D", str(data), "-m", "immediate", "-w", "stop")
    return 0


if __name__ == "__main__":
    sys.exit(main())
