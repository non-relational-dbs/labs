# Labs Setup Maintenance Contract

These rules apply to maintainers and automation working inside this independent submodule.

## Project Boundary

- Treat `labs-setup` as its own `uv` project with its own `pyproject.toml`, `uv.lock`, and `.venv`.
- Run `uv sync` separately here. Do not merge this environment with the parent repository.
- Keep every present-state `README.md`, `LOAD_ME.md`, and `AGENTS.md` current, mutually consistent, and written in English.
- Historical plans and research live in the parent repository. Preserve them as historical records; do not rewrite or delete them when updating current submodule documentation.

## Network Contract

- `NETWORK_MODE` accepts exactly lowercase `local` or `vpn`; its default is `local`. Reject every other value.
- Docker and Compose provide infrastructure in both modes. Docker-internal service names remain unchanged for container-to-container communication.
- Local host-side client endpoints use only ports published on `127.0.0.1`.
- VPN host-side client identities use `<container>.<VPN_CLIENT_ALIAS>.<VPN_DOMAIN>` and published ports bind to `VPN_HOST_IP`.
- Defaults are `VPN_CLIENT_ALIAS=mavasbel`, `VPN_DOMAIN=vpn.itam.mx`, `VPN_HOST_IP=10.15.20.100`, and `VPN_DNS_IP=10.15.20.1`. The alias and domain are editable notebook parameters and must remain injectable through papermill.
- CoreDNS must provide each student's base identity `<student_alias>.<VPN_DOMAIN>` and wildcard service resolution for `<container>.<student_alias>.<VPN_DOMAIN>` to the same WireGuard client IP. A notebook's `VPN_CLIENT_ALIAS` selects the applicable student alias.

## Notebook Contract

- Treat each `py:percent` `.py` source as canonical.
- Force source-to-notebook updates with `uv run jupytext --to ipynb --update <source.py>`. Do not use bidirectional `--sync` for this refresh.
- Keep exactly one papermill `parameters` cell in every notebook.
- Preserve intended papermill types, especially unquoted Boolean values such as `True` and `False`.
- Run every `*_infra.ipynb` to completion and health before its matching `*_lab.ipynb`.
- Hive and Spark require healthy Hadoop infrastructure first.
- Acceptance requires complete execution of every supported infrastructure/lab pair in both Local and VPN modes, including MongoDB config-server infrastructure.

## Spark Contract

- Spark is self-contained and must not resolve Maven packages or download JARs at runtime.
- Keep these four versioned JARs tracked and validate their pinned SHA-256 checksums before creating a Spark session:
  - `iceberg-spark-runtime-3.5_2.12-1.6.1.jar`
  - `delta-spark_2.12-3.2.0.jar`
  - `delta-storage-3.2.0.jar`
  - `antlr4-runtime-4.9.3.jar`
- Executors run in Docker with the shipped Python environment and bundled JAR classpath. The host-side driver advertises `host.docker.internal` for executor callbacks.

## Safety

- Never commit generated Compose files, generated state, executed notebooks, credentials, secrets, WireGuard private keys or client configurations, certificates, or student-identifiable data.
- Keep generated outputs and secrets covered by `.gitignore`.
- Do not hand-edit generated Compose files; change the canonical source and regenerate them.
