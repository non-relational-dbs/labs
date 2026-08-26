# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.5
#   kernelspec:
#     display_name: non-relational-dbs-labs
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

# %% [markdown]
# Validates the papermill-injected NETWORK_MODE and VPN_CLIENT_ALIAS values and derives VPN_CLIENT_DOMAIN as `<alias>.<VPN_DOMAIN>` for later VPN-mode host naming.

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

# %% [markdown]
# Locates the labs-setup root by walking parent directories for a pyproject.toml alongside the cassandra and mongodb module folders, then changes into the redis module directory.

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
MODULE_DIR = LABS_ROOT / "redis"
os.chdir(MODULE_DIR)
print(f"Working directory: {MODULE_DIR}")

# %% [markdown]
# Defines the Redis cluster topology: six redis-node containers with fixed 172.28.0.* internal IPs, per-mode client hosts via `vpn_fqdn` or 127.0.0.1, data/bus ports, and the shared admin and init credentials.

# %%
REDIS_START_FROM_SCRATCH = START_FROM_SCRATCH
DOCKER_INTERNAL_HOST = "host.docker.internal"
DOCKER_DNS = [VPN_DNS_IP] if NETWORK_MODE == "vpn" else []
HOST_BIND_IP = VPN_HOST_IP if NETWORK_MODE == "vpn" else "127.0.0.1"

REDIS_TOTAL_NODES = 6
REDIS_NODE_NAMES = [f"redis-node-{i+1}" for i in range(REDIS_TOTAL_NODES)]
REDIS_NODE_INTERNAL_IPS = [
    f"172.28.0.{i + 11}" for i in range(REDIS_TOTAL_NODES)
]
def vpn_fqdn(container_name):
    return f"{container_name}.{VPN_CLIENT_DOMAIN}"


REDIS_NODE_CLIENT_HOSTS = [
    "127.0.0.1" if NETWORK_MODE == "local" else vpn_fqdn(name)
    for name in REDIS_NODE_NAMES
]
REDIS_NODE_COMPOSE_HOSTS = [
    name if NETWORK_MODE == "local" else vpn_fqdn(name)
    for name in REDIS_NODE_NAMES
]
REDIS_NODE_PORTS = [6380 + i + 1 for i in range(REDIS_TOTAL_NODES)]
REDIS_NODE_BUS_PORTS = [16380 + i + 1 for i in range(REDIS_TOTAL_NODES)]

REDIS_WORKDIR = "/data"

REDIS_ADMIN_PASSWORD = "redis"
REDIS_DEFAULT_PASSWORD = "redis"
REDIS_INIT_USER = "redis"
REDIS_INIT_PASSWORD = "redis"

# %% [markdown]
# Creates the local bind mount directory (DOCKER_MOUNTDIR) that will back each Redis node's persistent /data volume.

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = str(MODULE_DIR.resolve())
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")

mount_path = Path(DOCKER_MOUNTDIR)
mount_path.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# # Stop redis-cluster.docker-compose.yml

# %%
import subprocess

if REDIS_START_FROM_SCRATCH:
    subprocess.run(
        ["docker", "compose", "-f", "redis-cluster.docker-compose.yml", "down", "-v"],
        check=False,
    )
else:
    print("Preserving existing containers and volumes")


# %% [markdown]
# Defines clear_bind_directory, which empties the bind mount through a throwaway busybox:1.36 container, and runs it on DOCKER_MOUNTDIR when REDIS_START_FROM_SCRATCH is true.

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

if REDIS_START_FROM_SCRATCH:
    clear_bind_directory(DOCKER_MOUNTDIR)

# %% [markdown]
# # Start redis-cluster.docker-compose.yml

# %%
import os
import yaml
from IPython.display import Markdown, display


REDIS_MEMORY_LIMIT = "512mb"
REDIS_CPU_LIMIT = "0.5"

redis_compose_dict = {
    "name": "redis-cluster",
    "services": {},
    "networks": {
        "redis-cluster": {
            "name": "redis-network",
            "driver": "bridge",
            "ipam": {"config": [{"subnet": "172.28.0.0/24"}]},
        }
    },
}

for i in range(REDIS_TOTAL_NODES):
    cluster_announcement = [
        "--cluster-announce-ip",
        REDIS_NODE_INTERNAL_IPS[i],
    ]
    if NETWORK_MODE == "vpn":
        cluster_announcement.extend(
            [
                "--cluster-announce-hostname",
                vpn_fqdn(REDIS_NODE_NAMES[i]),
                "--cluster-preferred-endpoint-type",
                "hostname",
            ]
        )

    redis_compose_dict["services"][REDIS_NODE_NAMES[i]] = {
        "image": "redis:8.4.0-bookworm",
        "container_name": REDIS_NODE_NAMES[i],
        "hostname": REDIS_NODE_COMPOSE_HOSTS[i],
        "command": [
            "redis-server",
            *["--cluster-enabled", "yes"],
            *["--requirepass", f"{REDIS_DEFAULT_PASSWORD}"],
            *["--masterauth", f"{REDIS_ADMIN_PASSWORD}"],
            *[
                "--user",
                f"{REDIS_INIT_USER}",
                "on",
                f">{REDIS_INIT_PASSWORD}",
                "~*",
                "+@all",
            ],
            *["--cluster-node-timeout", "15000"],
            *["--cluster-config-file", "nodes.conf"],
            *["--cluster-node-timeout", "5000"],
            *["--appendonly", "yes"],
            *["--cluster-replica-validity-factor", "0"],
            *["--port", f"{REDIS_NODE_PORTS[i]}"],
            *["--bind", "0.0.0.0"],
            *["--protected-mode", "no"],
            *cluster_announcement,
            *["--cluster-announce-port", f"{REDIS_NODE_PORTS[i]}"],
            *["--cluster-announce-bus-port", f"{REDIS_NODE_BUS_PORTS[i]}"],
        ],
        "volumes": [
            f"{os.path.join(DOCKER_MOUNTDIR, REDIS_NODE_NAMES[i])}:{REDIS_WORKDIR}"
        ],
        "networks": {
            "redis-cluster": {"ipv4_address": REDIS_NODE_INTERNAL_IPS[i]}
        },
        "ports": [
            f"{HOST_BIND_IP}:{REDIS_NODE_PORTS[i]}:{REDIS_NODE_PORTS[i]}",
            f"{HOST_BIND_IP}:{REDIS_NODE_BUS_PORTS[i]}:{REDIS_NODE_BUS_PORTS[i]}",
        ],
        "dns": DOCKER_DNS,
        "deploy": {
            "resources": {
                "limits": {"cpus": REDIS_CPU_LIMIT, "memory": REDIS_MEMORY_LIMIT}
            }
        },
        "depends_on": {
            REDIS_NODE_NAMES[j]: {"condition": "service_started"} for j in range(0, i)
        },
        "healthcheck": {
            "test": [
                "CMD",
                "redis-cli",
                "-a",
                REDIS_ADMIN_PASSWORD,
                "-p",
                f"{REDIS_NODE_PORTS[i]}",
                "ping",
            ],
            "interval": "10s",
            "timeout": "10s",
            "retries": 10,
            "start_period": "20s",
        },
    }

redis_compose_yaml_path = os.path.join(
    LOCALHOST_WORKDIR, "redis-cluster.docker-compose.yml"
)
redis_compose_yaml_contents = yaml.dump(
    redis_compose_dict, default_flow_style=False, sort_keys=False, indent=4
)

with open(redis_compose_yaml_path, "w") as f:
    f.write(redis_compose_yaml_contents)

print(f"Successfully created: '{os.path.relpath(redis_compose_yaml_path)}'")
display(Markdown(f"```yaml\n{redis_compose_yaml_contents}\n```"))

# %% [markdown]
# Brings the six-node Redis cluster up in detached mode with `docker compose up -d --wait`, blocking until every node's redis-cli ping healthcheck passes.

# %%
# !docker compose -f redis-cluster.docker-compose.yml up -d --wait

# %% [markdown]
# Bootstraps the cluster with `redis-cli --cluster create` across the six node endpoints (three primaries, three replicas), then polls CLUSTER INFO until cluster_state is ok and all six nodes are known.

# %%
nodes_ports = " ".join(
    f"{REDIS_NODE_INTERNAL_IPS[i]}:{REDIS_NODE_PORTS[i]}"
    for i in range(REDIS_TOTAL_NODES)
)

cluster_bootstrap = subprocess.run(
    [
        "docker", "exec", "redis-node-1", "redis-cli",
        "-a", REDIS_ADMIN_PASSWORD,
        "--cluster", "create", *nodes_ports.split(),
        "--cluster-replicas", "1", "--cluster-yes",
    ],
    capture_output=True,
    text=True,
    timeout=120,
)
print(cluster_bootstrap.stdout)
assert cluster_bootstrap.returncode == 0, cluster_bootstrap.stderr

import time

cluster_info_command = [
    "docker", "exec", "redis-node-1", "redis-cli",
    "-a", REDIS_ADMIN_PASSWORD, "-p", str(REDIS_NODE_PORTS[0]),
    "cluster", "info",
]
for _ in range(30):
    cluster_info = subprocess.run(
        cluster_info_command,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    if "cluster_state:ok" in cluster_info:
        break
    time.sleep(2)
assert "cluster_state:ok" in cluster_info, cluster_info
assert "cluster_known_nodes:6" in cluster_info, cluster_info
print(cluster_info)
