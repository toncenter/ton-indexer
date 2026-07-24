# TON Indexer monitoring

This directory installs exporters for one hot/cold PostgreSQL and Kvrocks
stack, configures one stack-specific `sql_exporter` on an existing monitoring
host, deploys Prometheus scrape files, and provides Grafana dashboards.

Prometheus and Grafana must already be installed. The playbooks do not install
them or provision Grafana.

## Components

| Component | Installed on | Listen address |
| --- | --- | --- |
| node_exporter 1.11.1 | PostgreSQL and Kvrocks hosts | `0.0.0.0:9100` |
| postgres_exporter 0.19.1 | every PostgreSQL member | `0.0.0.0:9187` |
| kvrocks_exporter 1.0.9 | every Kvrocks member | `0.0.0.0:9122` |
| redis_exporter 1.84.0 | every Sentinel host | `0.0.0.0:9121` |
| sql_exporter 0.24.3 | monitoring host | `127.0.0.1:<sql_exporter_port>` |

The exporters are installed as hardened systemd services with pinned release
checksums. PostgreSQL and Kvrocks credentials are written only to root/exporter
readable files and must be supplied through Ansible Vault.

## Inventory interface

`ansible/inventories/ton_index/` is an example, not a required inventory name.
The directory can be copied or renamed freely:

```bash
cd monitoring/ansible
cp -R inventories/ton_index inventories/my_inventory
```

No playbook derives configuration from `my_inventory`. The logical identifier
is the `monitoring_stack_name` variable in `group_vars/all/main.yml`.

The inventory must expose these fixed functional groups:

- `hot_postgres`
- `cold_postgres`
- `postgresql`, containing the hot and cold groups
- `kvrocks`
- `monitoring`, containing exactly one host

Host counts are arbitrary. The example contains two hot PostgreSQL members,
two cold members, and three Kvrocks/Sentinel members.

`monitoring_stack_name` is used in Prometheus labels, systemd service names,
and installation paths. Use a lowercase identifier containing letters,
numbers, `_`, or `-`, with a maximum length of 18 characters.

## 1. Configure the example stack

Edit:

```text
ansible/inventories/ton_index/hosts.yml
ansible/inventories/ton_index/group_vars/all/main.yml
ansible/inventories/ton_index/group_vars/hot_postgres.yml
ansible/inventories/ton_index/group_vars/cold_postgres.yml
ansible/inventories/ton_index/group_vars/kvrocks.yml
```

At minimum, set:

- SSH user, key, and hostnames;
- `monitoring_stack_name`;
- PostgreSQL database, user, and SSL mode;
- local Kvrocks port in `kvrocks_exporter_address`;
- `sql_exporter_port`;
- hot and cold `sql_exporter_targets`.

Each SQL target may be a direct PostgreSQL primary or a stable HAProxy/Patroni
write endpoint. `target_session_attrs=read-write` ensures that `sql_exporter`
does not silently collect primary-only data from a replica.

If several independently configured stacks use the same monitoring host, each
must have a unique `monitoring_stack_name` and `sql_exporter_port`.

## 2. Create and encrypt secrets

```bash
cd monitoring/ansible

cp inventories/ton_index/group_vars/all/vault.yml.example \
  inventories/ton_index/group_vars/all/vault.yml

ansible-vault encrypt inventories/ton_index/group_vars/all/vault.yml
```

Set these values before encryption:

```yaml
vault_hot_postgres_monitor_password: CHANGE_ME
vault_cold_postgres_monitor_password: CHANGE_ME
vault_kvrocks_password: CHANGE_ME
vault_sentinel_password: ""
```

An empty Sentinel password is supported and causes the password argument/file
to be omitted. Real `vault.yml` files, `.vault-password`, generated Prometheus
bundles, local deployment inventories, and `.DS_Store` files are ignored by
git.

## 3. Create the PostgreSQL monitoring role

Repeat this procedure on the writable primary of both PostgreSQL clusters:
hot and cold. Use the corresponding hot/cold password from Vault. Patroni
physical replication propagates the role and grants to replicas, but it does
not propagate them between the independent hot and cold clusters.

Connect as a PostgreSQL administrator:

```bash
psql "host=<write-endpoint> port=5432 dbname=postgres user=<admin> target_session_attrs=read-write"
```

Create the role and enter its password interactively so it is not stored in
shell or SQL history:

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

ALTER ROLE prometheus_monitor SET default_transaction_read_only = off;
ALTER ROLE prometheus_monitor SET statement_timeout = '10s';
ALTER ROLE prometheus_monitor SET lock_timeout = '2s';
ALTER ROLE prometheus_monitor
    SET idle_in_transaction_session_timeout = '30s';

GRANT pg_monitor TO prometheus_monitor;
GRANT CONNECT ON DATABASE <indexer_database> TO prometheus_monitor;

\connect <indexer_database>

GRANT USAGE ON SCHEMA public TO prometheus_monitor;

-- Required in both hot and cold.
GRANT SELECT ON TABLE
    public.blocks,
    public._ton_indexer_progress
TO prometheus_monitor;
```

Run this additional block only in the hot cluster:

```sql
GRANT SELECT ON TABLE
    public._ton_indexer_leader,
    public._ton_hot_cold_split,
    public._classifier_progress,
    public._classifier_tasks,
    public._classifier_failed_traces
TO prometheus_monitor;
```

`default_transaction_read_only` must be `off` because the SQL exporter uses
`target_session_attrs=read-write` to reject replicas. The role remains unable
to modify application data because it receives only `CONNECT`, `USAGE`,
`pg_monitor`, and explicit `SELECT` grants.

Allow local `postgres_exporter` connections on every PostgreSQL member:

```text
host  <indexer_database>  prometheus_monitor  127.0.0.1/32  scram-sha-256
host  <indexer_database>  prometheus_monitor  ::1/128       scram-sha-256
```

Allow `sql_exporter` from the address PostgreSQL sees. For a direct endpoint
this is normally the monitoring host; for HAProxy it is the HAProxy egress
address. Add the rule to every member that may become primary:

```text
host  <indexer_database>  prometheus_monitor  <sql-exporter-source-ip>/32  scram-sha-256
```

Reload `pg_hba.conf`:

```sql
SELECT pg_reload_conf();
```

If migrations create the operational tables after the role is prepared, rerun
the applicable grants. The complete role setup and a manual preflight command
are also available in [`postgres/README.md`](postgres/README.md). Ansible
validates the role but never creates or alters it.

## 4. Validate the inventory and playbooks

Run from `monitoring/ansible`:

```bash
ansible-inventory -i inventories/ton_index/hosts.yml --graph

ansible-playbook -i inventories/ton_index/hosts.yml \
  playbooks/environment_exporters.yml --syntax-check

ansible-playbook -i inventories/ton_index/hosts.yml \
  playbooks/central_monitoring.yml --syntax-check

ansible-playbook -i inventories/ton_index/hosts.yml \
  playbooks/render_prometheus.yml --syntax-check

ansible-playbook -i inventories/ton_index/hosts.yml \
  playbooks/verify_exporters.yml --syntax-check
```

Use `--ask-vault-pass` or your normal Vault identity/password file on commands
that load the encrypted secrets.

## 5. Deploy exporters

Install node, PostgreSQL, Kvrocks, and Sentinel exporters:

```bash
ansible-playbook -i inventories/ton_index/hosts.yml \
  playbooks/environment_exporters.yml \
  --ask-vault-pass
```

Install the stack-specific SQL exporter on the monitoring host:

```bash
ansible-playbook -i inventories/ton_index/hosts.yml \
  playbooks/central_monitoring.yml \
  --ask-vault-pass
```

For `monitoring_stack_name: ton_index`, this creates:

```text
sql_exporter-ton_index.service
/etc/sql_exporter/ton_index/
127.0.0.1:9399
```

The SQL preflight requires both hot and cold endpoints to be writable
primaries, membership in `pg_monitor`, and the table grants documented in
`postgres/README.md`.

## 6. Render and deploy Prometheus configuration

```bash
ansible-playbook -i inventories/ton_index/hosts.yml \
  playbooks/render_prometheus.yml
```

The playbook:

1. renders `build/prometheus/ton_index/` locally;
2. copies it to `/etc/prometheus/ton-indexer/ton_index/`;
3. adds the explicit scrape include to the existing `prometheus.yml`;
4. validates the complete configuration with `promtool`;
5. restarts Prometheus only when managed files changed;
6. waits for Prometheus readiness.

Override these inventory variables if the installation uses different paths:

```yaml
prometheus_config_file: /etc/prometheus/prometheus.yml
prometheus_promtool_path: /usr/bin/promtool
prometheus_service_name: prometheus
```

See [`prometheus/README.md`](prometheus/README.md) for the generated layout.

## 7. Verify endpoints

```bash
ansible-playbook -i inventories/ton_index/hosts.yml \
  playbooks/verify_exporters.yml
```

Useful service checks:

```bash
systemctl status node_exporter
systemctl status postgres_exporter
systemctl status kvrocks_exporter
systemctl status redis_exporter
systemctl status sql_exporter-ton_index
```

In Prometheus, verify that `up{environment="ton_index"} == 1` for all expected
targets and that both SQL targets report
`ton_postgres_target_is_primary == 1`.

## 8. Import Grafana dashboards

Import the JSON files under `grafana/` and map `DS_PROMETHEUS` to the existing
Prometheus datasource:

- `overview.json`
- `postgresql.json`
- `kvrocks-sentinel.json`
- `pipeline.json`

The dashboards use the generated `environment`, `cluster`, `tier`, `instance`,
and SQL exporter `target` labels.

## Rollout order

1. Prepare the database role and network access.
2. Deploy environment exporters.
3. Deploy the central SQL exporter.
4. Render and deploy the Prometheus bundle.
5. Run endpoint verification.
6. Import or overwrite the Grafana dashboards.
