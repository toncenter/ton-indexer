# PostgreSQL monitoring role

Create the monitoring role manually on the writable primary of both
PostgreSQL clusters: hot and cold. Physical Patroni replication carries the
role and grants to the replicas.

Connect as a PostgreSQL administrator through a direct primary address or a
stable HAProxy/Patroni write endpoint:

```bash
psql "host=<write-endpoint> port=5432 dbname=postgres user=<admin> target_session_attrs=read-write"
```

Create the role and enter its password interactively:

```sql
CREATE ROLE prometheus_monitor
    LOGIN
    INHERIT
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION
    NOBYPASSRLS
    CONNECTION LIMIT 15;

\password prometheus_monitor

-- Required by target_session_attrs=read-write. Write safety comes from grants,
-- not from a read-only session default.
ALTER ROLE prometheus_monitor SET default_transaction_read_only = off;
ALTER ROLE prometheus_monitor SET statement_timeout = '10s';
ALTER ROLE prometheus_monitor SET lock_timeout = '2s';
ALTER ROLE prometheus_monitor SET idle_in_transaction_session_timeout = '30s';

GRANT pg_monitor TO prometheus_monitor;
GRANT CONNECT ON DATABASE postgres TO prometheus_monitor;
GRANT CONNECT ON DATABASE <indexer_database> TO prometheus_monitor;

\connect <indexer_database>

GRANT USAGE ON SCHEMA public TO prometheus_monitor;

-- Required in both hot and cold.
GRANT SELECT ON TABLE
    public.blocks,
    public._ton_indexer_progress
TO prometheus_monitor;
```

Run this additional block only in hot:

```sql
GRANT SELECT ON TABLE
    public._ton_indexer_leader,
    public._ton_hot_cold_split,
    public._classifier_progress,
    public._classifier_tasks,
    public._classifier_failed_traces
TO prometheus_monitor;
```

The cold collector intentionally does not require the hot-only tables. Run
applicable grants after migrations if a listed table does not exist yet.
Neither `SUPERUSER`, `pg_read_all_data`, nor the `REPLICATION` attribute is
required.

If an existing role has `default_transaction_read_only = on`, change it to
`off`. Libpq rejects that session for `target_session_attrs=read-write` even
when the server is a primary. Explicit `SELECT` grants still prevent the role
from changing application data.

## `pg_hba.conf`

Allow local `postgres_exporter` connections on every member:

```text
host  <indexer_database>  prometheus_monitor  127.0.0.1/32  scram-sha-256
host  <indexer_database>  prometheus_monitor  ::1/128       scram-sha-256
```

Allow `sql_exporter` from the network address PostgreSQL sees. With a direct
endpoint this is normally the monitoring host address; with HAProxy it is
normally the HAProxy egress address:

```text
host  <indexer_database>  prometheus_monitor  <sql-exporter-source-ip>/32  scram-sha-256
```

Reload PostgreSQL configuration after changing `pg_hba.conf`:

```sql
SELECT pg_reload_conf();
```

## Verification

```bash
PGPASSWORD='<password>' psql \
  "host=<write-endpoint> port=5432 dbname=<indexer_database> user=prometheus_monitor target_session_attrs=read-write connect_timeout=5 application_name=sql_exporter_preflight" \
  --no-psqlrc --tuples-only --no-align \
  --command "SELECT (NOT pg_is_in_recovery()) AND pg_has_role(current_user, 'pg_monitor', 'member') AND has_table_privilege(current_user, 'public.blocks', 'SELECT');"
```

The expected result is `t`. Prefer a temporary `PGPASSFILE` instead of placing
the real password directly in shell history.
