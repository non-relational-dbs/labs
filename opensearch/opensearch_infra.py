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
# Validates the papermill-injected NETWORK_MODE and VPN_CLIENT_ALIAS values, rejecting anything other than lowercase local or vpn mode, and derives the student's VPN_CLIENT_DOMAIN.

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
# Locates the labs-setup project root by walking up from the current directory until a pyproject.toml with the cassandra and mongodb module directories is found, then changes into the opensearch module directory.

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
MODULE_DIR = LABS_ROOT / "opensearch"
os.chdir(MODULE_DIR)
print(f"Working directory: {MODULE_DIR}")

# %% [markdown]
# Builds the cluster topology settings: three opensearch-node containers, their REST and performance-analyzer ports, local hostnames or VPN FQDNs, the Dashboards service name, and the initial admin password.

# %%
OPENSEARCH_START_FROM_SCRATCH = START_FROM_SCRATCH
DOCKER_INTERNAL_HOST = "host.docker.internal"
DOCKER_DNS = [VPN_DNS_IP] if NETWORK_MODE == "vpn" else []
HOST_BIND_IP = VPN_HOST_IP if NETWORK_MODE == "vpn" else "127.0.0.1"

OPENSEARCH_DASHBOARD_PORT = 5601

OPENSEARCH_CLUSTER_NAME = "opensearch-cluster"
OPENSEARCH_TOTAL_NODES = 3

OPENSEARCH_NODE_NAMES = [
    f"opensearch-node-{i+1}" for i in range(OPENSEARCH_TOTAL_NODES)
]


def vpn_fqdn(container_name):
    return f"{container_name}.{VPN_CLIENT_DOMAIN}"


OPENSEARCH_NODE_REST_API_PORTS = [9200 + (i + 1) for i in range(OPENSEARCH_TOTAL_NODES)]
OPENSEARCH_NODE_PERF_ANA_PORTS = [
    16280 + (i + 1) for i in range(OPENSEARCH_TOTAL_NODES)
]
OPENSEARCH_NODE_HOSTNAMES = [
    name if NETWORK_MODE == "local" else vpn_fqdn(name)
    for name in OPENSEARCH_NODE_NAMES
]
OPENSEARCH_NODE_INTERNAL_ENDPOINTS = [
    f"https://{name}:{port}"
    for name, port in zip(OPENSEARCH_NODE_NAMES, OPENSEARCH_NODE_REST_API_PORTS)
]
OPENSEARCH_DASHBOARDS_NAME = "opensearch-dashboards"
OPENSEARCH_DASHBOARDS_HOSTNAME = (
    OPENSEARCH_DASHBOARDS_NAME
    if NETWORK_MODE == "local"
    else vpn_fqdn(OPENSEARCH_DASHBOARDS_NAME)
)

OPENSEARCH_WORKDIR = "/usr/share/opensearch/data"

OPENSEARCH_INITIAL_ADMIN_PASSWORD = "OpenSearchP455"
# OPENSEARCH_DEFAULT_PASSWORD = "opensearch"
# OPENSEARCH_INIT_USER = "opensearch"
# OPENSEARCH_INIT_PASSWORD = "opensearch"

# %% [markdown]
# Creates the bind-mount base directory under the opensearch module folder that will hold the per-node data volumes.

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = str(MODULE_DIR.resolve())
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")

mount_path = Path(DOCKER_MOUNTDIR)
mount_path.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# # Stop opensearch-cluster.docker-compose.yml

# %%
import subprocess

if OPENSEARCH_START_FROM_SCRATCH:
    subprocess.run(
        ["docker", "compose", "-f", "opensearch-cluster.docker-compose.yml", "down", "-v"],
        check=False,
    )
else:
    print("Preserving existing containers and volumes")


# %% [markdown]
# Defines clear_bind_directory, which empties the bind-mount directory using a throwaway busybox container when START_FROM_SCRATCH is enabled.

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

if OPENSEARCH_START_FROM_SCRATCH:
    clear_bind_directory(DOCKER_MOUNTDIR)

# %% [markdown]
# # Start opensearch-cluster.docker-compose.yml

# %%
import os
import yaml
from IPython.display import Markdown, display

node_cpus = "2.0"
node_memory = "2G"
node_start_heap = "1G"
node_max_heap = "1G"

opensearch_compose_dict = {
    "name": "opensearch-cluster",
    "services": {},
    "networks": {
        "opensearch-cluster": {"name": "opensearch-network", "driver": "bridge"}
    },
}
opensearch_hosts_json = '["' + '","'.join(OPENSEARCH_NODE_INTERNAL_ENDPOINTS) + '"]'

for i in range(OPENSEARCH_TOTAL_NODES):

    node_mount_dir = os.path.join(
        DOCKER_MOUNTDIR,
        OPENSEARCH_NODE_NAMES[i],
    )
    Path(node_mount_dir).mkdir(parents=True, exist_ok=True)
    with open(
        os.path.join(node_mount_dir, "performance-analyzer.properties"), "w"
    ) as f:
        f.write(
            f"""# Performance Analyzer Configuration for {OPENSEARCH_NODE_NAMES[i]}
webservice-bind-host = 0.0.0.0
webservice-listener-port = {OPENSEARCH_NODE_PERF_ANA_PORTS[i]}
metrics-location = /dev/shm/performanceanalyzer/
cleanup-metrics-db-files = true
https-enabled = false
"""
        )

    opensearch_compose_dict["services"][OPENSEARCH_NODE_NAMES[i]] = {
        "image": "opensearchproject/opensearch:3.4.0",
        "container_name": OPENSEARCH_NODE_NAMES[i],
        "environment": [
            f"cluster.name={OPENSEARCH_CLUSTER_NAME}",
            f"node.name={OPENSEARCH_NODE_NAMES[i]}",
            f"discovery.seed_hosts={','.join(OPENSEARCH_NODE_NAMES)}",
            f"cluster.initial_cluster_manager_nodes={','.join(OPENSEARCH_NODE_NAMES)}",
            "cluster.routing.allocation.disk.threshold_enabled=false",
            f"http.port={OPENSEARCH_NODE_REST_API_PORTS[i]}",
            "bootstrap.memory_lock=true",
            f"OPENSEARCH_JAVA_OPTS=-Xms{node_start_heap} -Xmx{node_max_heap}",
            f"OPENSEARCH_INITIAL_ADMIN_PASSWORD={OPENSEARCH_INITIAL_ADMIN_PASSWORD}",
            f"OPENSEARCH_HOSTS={opensearch_hosts_json}",
        ],
        "ulimits": {
            "memlock": {"soft": -1, "hard": -1},
            "nofile": {"soft": 65536, "hard": 65536},
        },
        "volumes": [
            f"{os.path.join(node_mount_dir, 'data')}:{OPENSEARCH_WORKDIR}",
            f"{os.path.join(node_mount_dir, 'performance-analyzer.properties')}:/usr/share/opensearch/config/opensearch-performance-analyzer/performance-analyzer.properties",
        ],
        "networks": ["opensearch-cluster"],
        "hostname": OPENSEARCH_NODE_HOSTNAMES[i],
        "ports": [
            f"{HOST_BIND_IP}:{OPENSEARCH_NODE_REST_API_PORTS[i]}:{OPENSEARCH_NODE_REST_API_PORTS[i]}",
            f"{HOST_BIND_IP}:{OPENSEARCH_NODE_PERF_ANA_PORTS[i]}:{OPENSEARCH_NODE_PERF_ANA_PORTS[i]}",
        ],
        "extra_hosts": [
            f"{DOCKER_INTERNAL_HOST}:host-gateway",
        ],
        "dns": DOCKER_DNS,
        "deploy": {"resources": {"limits": {"cpus": node_cpus, "memory": node_memory}}},
        "depends_on": {
            OPENSEARCH_NODE_NAMES[j]: {"condition": "service_started"}
            for j in range(0, i)
        },
        "healthcheck": {
            "test": [
                "CMD-SHELL",
                f"curl -sf -k -u admin:{OPENSEARCH_INITIAL_ADMIN_PASSWORD} "
                f'https://localhost:{OPENSEARCH_NODE_REST_API_PORTS[i]}/_cluster/health | grep -qv \'"status":"red"\'',
            ],
            "interval": "10s",
            "timeout": "10s",
            "retries": 10,
            "start_period": "30s",
        },
    }

opensearch_compose_dict["services"][OPENSEARCH_DASHBOARDS_NAME] = {
    "image": "opensearchproject/opensearch-dashboards:3.4.0",
    "container_name": OPENSEARCH_DASHBOARDS_NAME,
    "hostname": OPENSEARCH_DASHBOARDS_HOSTNAME,
    "environment": [
        f"OPENSEARCH_HOSTS={opensearch_hosts_json}",
        f"OPENSEARCH_JAVA_OPTS=-Xms{node_start_heap} -Xmx{node_max_heap}",
    ],
    "networks": ["opensearch-cluster"],
    "ports": [
        f"{HOST_BIND_IP}:{OPENSEARCH_DASHBOARD_PORT}:{OPENSEARCH_DASHBOARD_PORT}",
    ],
    "dns": DOCKER_DNS,
    "deploy": {"resources": {"limits": {"cpus": node_cpus, "memory": node_memory}}},
    "depends_on": {
        OPENSEARCH_NODE_NAMES[j]: {"condition": "service_healthy"}
        for j in range(OPENSEARCH_TOTAL_NODES)
    },
}


opensearch_compose_yaml_path = os.path.join(
    LOCALHOST_WORKDIR, "opensearch-cluster.docker-compose.yml"
)
opensearch_compose_yaml_contents = yaml.dump(
    opensearch_compose_dict, default_flow_style=False, sort_keys=False, indent=4
)

with open(opensearch_compose_yaml_path, "w") as f:
    f.write(opensearch_compose_yaml_contents)

print(f"Successfully created: '{os.path.relpath(opensearch_compose_yaml_path)}'")
display(Markdown(f"```yaml\n{opensearch_compose_yaml_contents}\n```"))

# %% [markdown]
# Starts the generated opensearch-cluster.docker-compose.yml stack in detached mode and waits until every node's _cluster/health healthcheck and Dashboards become healthy.

# %%
# !docker compose -f opensearch-cluster.docker-compose.yml up -d --wait
