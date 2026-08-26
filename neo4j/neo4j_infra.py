# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.5
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: non-relational-dbs-labs
# ---

# %% [markdown]
# # Setup

# %% tags=["parameters"]
# Local mode is the portable default; VPN mode publishes services on this host.
NETWORK_MODE = "local"
VPN_HOST_IP = "10.15.20.100"
VPN_DNS_IP = "10.15.20.1"
VPN_DOMAIN = "vpn.itam.mx"
VPN_CLIENT_ALIAS = "mavasbel"
START_FROM_SCRATCH = False

# %%
import re

NETWORK_MODE = NETWORK_MODE.strip().lower()
if NETWORK_MODE not in {"local", "vpn"}:
    raise ValueError("NETWORK_MODE must be 'local' or 'vpn'")
if NETWORK_MODE == "vpn" and not VPN_HOST_IP.startswith("10.15.20."):
    raise ValueError("VPN_HOST_IP must be in the 10.15.20.* subnet")
VPN_CLIENT_ALIAS = VPN_CLIENT_ALIAS.strip().lower()
if not VPN_CLIENT_ALIAS:
    raise ValueError("VPN_CLIENT_ALIAS must not be empty")
if re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", VPN_CLIENT_ALIAS) is None:
    raise ValueError(
        "VPN_CLIENT_ALIAS must contain only lowercase letters, digits, or hyphens "
        "and must not start or end with a hyphen"
    )
VPN_CLIENT_DOMAIN = f"{VPN_CLIENT_ALIAS}.{VPN_DOMAIN}"

# %%
# Resolve module assets from the labs-setup root.
import os
from pathlib import Path

_start = Path.cwd().resolve()
LABS_ROOT = next(
    (
        candidate
        for candidate in (_start, *_start.parents)
        if (candidate / "pyproject.toml").is_file()
        and (candidate / "cassandra").is_dir()
        and (candidate / "mongodb").is_dir()
    ),
    None,
)
if LABS_ROOT is None:
    raise RuntimeError(
        "Could not find the labs-setup root from the current directory"
    )
MODULE_DIR = LABS_ROOT / "neo4j"
os.chdir(MODULE_DIR)
print(f"Working directory: {MODULE_DIR}")

# %%
NEO4J_START_FROM_SCRATCH = START_FROM_SCRATCH
DOCKER_DNS = [VPN_DNS_IP] if NETWORK_MODE == "vpn" else []
HOST_BIND_IP = VPN_HOST_IP if NETWORK_MODE == "vpn" else "127.0.0.1"


def vpn_fqdn(container_name):
    return f"{container_name}.{VPN_CLIENT_DOMAIN}"


NEO4J_SERVICE_NAME = "neo4j"
NEO4J_CONTAINER_NAME = "neo4j-instance"
NEO4J_CLIENT_HOST = (
    "127.0.0.1"
    if NETWORK_MODE == "local"
    else vpn_fqdn(NEO4J_CONTAINER_NAME)
)

# NEO4J_WORKDIR = "/var/lib/neo4j"
NEO4J_PORT = 7687
NEO4J_WEBUI_PORT = 7474

NEO4J_INIT_USER = "neo4j"
NEO4J_INIT_PASSWORD = "password"

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = str(MODULE_DIR.resolve())
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")

mount_path = Path(DOCKER_MOUNTDIR)
mount_path.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# ## Stop neo4j.docker-compose.yml

# %%
import subprocess

if NEO4J_START_FROM_SCRATCH:
    subprocess.run(
        ["docker", "compose", "-f", "neo4j.docker-compose.yml", "down", "-v"],
        check=False,
    )
else:
    print("Preserving existing containers and volumes")


# %%
def clear_bind_directory(path):
    target = Path(path).resolve()
    target.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "docker", "run", "--rm", "--mount",
            f"type=bind,source={target},target=/target",
            "busybox:1.36", "sh", "-c",
            "find /target -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +",
        ],
        check=True,
    )

if NEO4J_START_FROM_SCRATCH:
    clear_bind_directory(DOCKER_MOUNTDIR)

# %% [markdown]
# # Start neo4j.docker-compose.yml

# %%
import os
import yaml
from IPython.display import Markdown, display

neo4j_compose_dict = {
    "name": "neo4j-compose",
    "networks": {
        "neo4j-network": {"name": "neo4j-network", "driver": "bridge"}
    },
    "services": {
        NEO4J_SERVICE_NAME: {
            "image": "neo4j:ubi9",
            "container_name": NEO4J_CONTAINER_NAME,
            "hostname": (
                NEO4J_CONTAINER_NAME
                if NETWORK_MODE == "local"
                else vpn_fqdn(NEO4J_CONTAINER_NAME)
            ),
            "volumes": [
                f"{os.path.join(DOCKER_MOUNTDIR, 'data')}:/data",
                f"{os.path.join(DOCKER_MOUNTDIR, 'neo4j', 'logs')}:/logs",
                f"{os.path.join(DOCKER_MOUNTDIR, 'neo4j', 'import')}:/var/lib/neo4j/import",
                f"{os.path.join(DOCKER_MOUNTDIR, 'plugins')}:/plugins",
            ],
            "environment": {
                "NEO4J_AUTH": f"{NEO4J_INIT_USER}/{NEO4J_INIT_PASSWORD}",
                # NEO4J 5.x Memory Settings (Double underscores represent single underscores in .conf)
                "NEO4J_server_memory_heap_initial__size": "512m",
                "NEO4J_server_memory_heap_max__size": "1G",
                "NEO4J_server_memory_pagecache_size": "512m",
                "NEO4J_dbms_memory_heap_initial_size": "1024m",
                "NEO4J_dbms_memory_heap_max_size": "2G",
                # Plugin Configuration
                "NEO4J_PLUGINS": '["apoc", "graph-data-science"]',
                # Security: Allow APOC, GDS to run procedures
                "NEO4J_dbms_security_procedures_unrestricted": "apoc.*,gds.*",
                "NEO4J_dbms_security_procedures_allowlist": "apoc.*,gds.*",
                "NEO4J_server_config_strict__validation_enabled": "false",
                "NEO4J_server_bolt_enabled": "true",
                "NEO4J_server_default__listen__address": "0.0.0.0",
                "NEO4J_server_bolt_listen__address": f"0.0.0.0:{NEO4J_PORT}",
                "NEO4J_server_bolt_advertised__address": f"{NEO4J_CLIENT_HOST}:{NEO4J_PORT}",
                "NEO4J_server_http_listen__address": f"0.0.0.0:{NEO4J_WEBUI_PORT}",
                "NEO4J_server_http_advertised__address": f"{NEO4J_CLIENT_HOST}:{NEO4J_WEBUI_PORT}",
            },
            "ports": [
                f"{HOST_BIND_IP}:{NEO4J_WEBUI_PORT}:{NEO4J_WEBUI_PORT}",
                f"{HOST_BIND_IP}:{NEO4J_PORT}:{NEO4J_PORT}",
            ],
            "networks": ["neo4j-network"],
            "dns": DOCKER_DNS,
            "restart": "unless-stopped",
            "deploy": {"resources": {"limits": {"cpus": "2.0", "memory": "2048M"}}},
            "healthcheck": {
                "test": [
                    "CMD-SHELL",
                    f"cypher-shell -u {NEO4J_INIT_USER} -p {NEO4J_INIT_PASSWORD} 'RETURN 1' >/dev/null",
                ],
                "interval": "10s",
                "timeout": "10s",
                "retries": 30,
                "start_period": "30s",
            },
        }
    },
}

neo4j_compose_yaml_path = os.path.join(LOCALHOST_WORKDIR, "neo4j.docker-compose.yml")
neo4j_compose_yaml_contents = yaml.dump(
    neo4j_compose_dict, default_flow_style=False, sort_keys=False, indent=4
)
with open(neo4j_compose_yaml_path, "w") as f:
    f.write(neo4j_compose_yaml_contents)

print(f"Successfully created: '{os.path.relpath(neo4j_compose_yaml_path)}'")
display(Markdown(f"```yaml\n{neo4j_compose_yaml_contents}\n```"))

# %%
# !docker compose -f neo4j.docker-compose.yml up -d --wait
