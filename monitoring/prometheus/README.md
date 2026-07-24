# Prometheus integration

`playbooks/render_prometheus.yml` renders and deploys one bundle for the stack
defined by `monitoring_stack_name`.

For the example `ton_index` inventory, the local and installed layouts are:

```text
monitoring/build/prometheus/ton_index/       /etc/prometheus/ton-indexer/ton_index/
├── scrape_configs.yml                      ├── scrape_configs.yml
├── prometheus.integration.yml              ├── prometheus.integration.yml
└── file_sd/                                └── file_sd/
    ├── node_exporters.json                     ├── node_exporters.json
    ├── postgres_exporters.json                 ├── postgres_exporters.json
    ├── kvrocks_exporters.json                  ├── kvrocks_exporters.json
    ├── sentinel_exporters.json                 ├── sentinel_exporters.json
    └── sql_exporters.json                      └── sql_exporters.json
```

The generated jobs are:

- `node_exporter_<stack>`
- `postgres_exporter_<stack>`
- `kvrocks_exporter_<stack>`
- `sentinel_exporter_<stack>`
- `sql_exporter_<stack>`
- `sql_exporter_internal_<stack>`

All targets receive `environment`, `cluster`, `tier`, `instance`, and
`service` labels. The generated files contain no database or Kvrocks
passwords.

Run from `monitoring/ansible`:

```bash
ansible-playbook -i inventories/ton_index/hosts.yml \
  playbooks/render_prometheus.yml
```

The deploy role adds this explicit top-level include to the existing
Prometheus configuration:

```yaml
scrape_config_files:
  - /etc/prometheus/ton-indexer/ton_index/scrape_configs.yml
```

Explicit paths are used because Prometheus does not accept a wildcard in
`scrape_config_files`. Existing includes belonging to other software or
independently deployed stacks are preserved.

The role validates the complete Prometheus configuration with `promtool`,
restarts the configured service only after a change, and waits for
`http://127.0.0.1:9090/-/ready`.
