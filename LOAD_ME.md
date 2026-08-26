# Labs Setup Runbook

`labs-setup` is an independent `uv` project. Run all commands in this directory and use its own `pyproject.toml`, `uv.lock`, and `.venv`; do not share the parent project's environment.

## Start

```bash
uv sync
uv run jupyter lab
```

The 17 `py:percent` `.py` files are canonical. After changing a source, force the source-to-notebook refresh:

```bash
uv run jupytext --to ipynb --update <source.py>
```

Do not use bidirectional `--sync` for the canonical refresh. Every notebook must contain exactly one papermill `parameters` cell.

## Local Execution

`NETWORK_MODE=local` is the default. Host-side clients connect through ports published only on `127.0.0.1`. Docker/Compose supplies infrastructure only.

Run each infrastructure notebook to completion and health before its matching lab:

```bash
uv run papermill cassandra/cassandra_infra.ipynb cassandra/cassandra_infra.executed.ipynb -p NETWORK_MODE local -p START_FROM_SCRATCH True
uv run papermill cassandra/cassandra_lab.ipynb cassandra/cassandra_lab.executed.ipynb -p NETWORK_MODE local

uv run papermill mongodb/mongodb_infra.ipynb mongodb/mongodb_infra.executed.ipynb -p NETWORK_MODE local -p START_FROM_SCRATCH True
uv run papermill mongodb/mongodb_lab.ipynb mongodb/mongodb_lab.executed.ipynb -p NETWORK_MODE local
uv run papermill mongodb/mongodb_infra_configsvr.ipynb mongodb/mongodb_infra_configsvr.executed.ipynb -p NETWORK_MODE local -p START_FROM_SCRATCH True

uv run papermill redis/redis_infra.ipynb redis/redis_infra.executed.ipynb -p NETWORK_MODE local -p START_FROM_SCRATCH True
uv run papermill redis/redis_lab.ipynb redis/redis_lab.executed.ipynb -p NETWORK_MODE local

uv run papermill neo4j/neo4j_infra.ipynb neo4j/neo4j_infra.executed.ipynb -p NETWORK_MODE local -p START_FROM_SCRATCH True
uv run papermill neo4j/neo4j_lab.ipynb neo4j/neo4j_lab.executed.ipynb -p NETWORK_MODE local

uv run papermill opensearch/opensearch_infra.ipynb opensearch/opensearch_infra.executed.ipynb -p NETWORK_MODE local -p START_FROM_SCRATCH True
uv run papermill opensearch/opensearch_lab.ipynb opensearch/opensearch_lab.executed.ipynb -p NETWORK_MODE local

uv run papermill hadoop/hadoop_infra.ipynb hadoop/hadoop_infra.executed.ipynb -p NETWORK_MODE local -p START_FROM_SCRATCH True
uv run papermill hadoop/hadoop_lab.ipynb hadoop/hadoop_lab.executed.ipynb -p NETWORK_MODE local
uv run papermill hadoop/hive_infra.ipynb hadoop/hive_infra.executed.ipynb -p NETWORK_MODE local -p START_FROM_SCRATCH True
uv run papermill hadoop/hive_lab.ipynb hadoop/hive_lab.executed.ipynb -p NETWORK_MODE local
uv run papermill spark/spark_infra.ipynb spark/spark_infra.executed.ipynb -p NETWORK_MODE local -p START_FROM_SCRATCH True
uv run papermill spark/spark_lab.ipynb spark/spark_lab.executed.ipynb -p NETWORK_MODE local
```

Hive and Spark require healthy Hadoop infrastructure. Run `hadoop_infra.ipynb` before `hive_infra.ipynb` or `spark_infra.ipynb`.

## VPN Execution

Preflight the VPN before running notebooks:

1. Connect WireGuard and run `wg show`.
2. Confirm the host owns `VPN_HOST_IP` (`10.15.20.100` by default).
3. Confirm the CoreDNS student base record and wildcard service record both resolve to that IP:

```bash
nslookup <VPN_CLIENT_ALIAS>.<VPN_DOMAIN> <VPN_DNS_IP>
nslookup cassandra-node-1.<VPN_CLIENT_ALIAS>.<VPN_DOMAIN> <VPN_DNS_IP>
```

CoreDNS must provide `<student_alias>.<VPN_DOMAIN>` and wildcard `<container>.<student_alias>.<VPN_DOMAIN>` records to the same WireGuard client IP. Set `VPN_CLIENT_ALIAS` to the applicable student alias; service clients use the wildcard identity.

Pass all VPN identity parameters to every notebook, preserving Boolean papermill values as unquoted `True`/`False`:

```bash
uv run papermill <module>/<name>_infra.ipynb <module>/<name>_infra.executed.ipynb -p NETWORK_MODE vpn -p VPN_HOST_IP 10.15.20.100 -p VPN_DNS_IP 10.15.20.1 -p VPN_CLIENT_ALIAS mavasbel -p VPN_DOMAIN vpn.itam.mx -p START_FROM_SCRATCH True
uv run papermill <module>/<name>_lab.ipynb <module>/<name>_lab.executed.ipynb -p NETWORK_MODE vpn -p VPN_HOST_IP 10.15.20.100 -p VPN_DNS_IP 10.15.20.1 -p VPN_CLIENT_ALIAS mavasbel -p VPN_DOMAIN vpn.itam.mx
```

`VPN_CLIENT_ALIAS` and `VPN_DOMAIN` are configurable. Use the values assigned to the student and keep each infrastructure/lab pair consistent. Docker-internal service names remain unchanged.

## Cleanup

Use the infra notebook's stop cell, or run this against its generated file:

```bash
docker compose -f <name>.docker-compose.yml down -v
```

Generated Compose files, state, credentials, keys, certificates, and `*.executed.ipynb` outputs are ignored and must not be committed.

## Acceptance Checklist

- `uv sync` succeeds in the submodule environment.
- All 17 notebooks have exactly one parameters cell and match their canonical `py:percent` sources.
- Every `*_infra.ipynb` completes and reports healthy before its matching `*_lab.ipynb`.
- The MongoDB config-server infrastructure notebook completes.
- Hadoop completes before Hive and Spark; both dependent stacks complete successfully.
- All supported infrastructure/lab pairs pass in Local mode and VPN mode.
- Local clients use `127.0.0.1`; VPN base and wildcard service DNS tests resolve to `VPN_HOST_IP`.
- Spark validates all four tracked JAR checksums and performs no runtime Maven resolution or download.
- Cleanup leaves no required generated output in version control.
