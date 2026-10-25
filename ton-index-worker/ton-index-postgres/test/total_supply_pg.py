"""Run the production Gramstat writer against a fresh Unix-socket-only cluster."""

import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile


def run(*args):
    result = subprocess.run(args, text=True, capture_output=True, timeout=90)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result


def main():
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        print("initdb requires an unprivileged user")
        return 77
    binary, initdb, pg_ctl = sys.argv[1:]
    with tempfile.TemporaryDirectory(prefix="gpg-") as directory:
        root = Path(directory)
        data = root / "data"
        run(initdb, "-D", str(data), "-U", "gramstat_test", "-A", "trust", "--no-locale", "-E", "UTF8")
        options = f"-F -c listen_addresses='' -k {shlex.quote(str(root))}"
        run(pg_ctl, "-D", str(data), "-l", str(root / "postgres.log"), "-o", options, "-w", "start")
        try:
            result = run(binary, f"host='{root}' user=gramstat_test dbname=postgres")
            print(result.stdout, end="")
        finally:
            run(pg_ctl, "-D", str(data), "-m", "immediate", "-w", "stop")
    return 0


if __name__ == "__main__":
    sys.exit(main())
