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
#     name: python3
# ---

# %% [markdown]
# # Setup

# %% tags=["parameters"]
# Safe default: papermill validates structure without external side effects.
DRY_RUN = False

# %%
# Universal papermill dry-run guard.
if DRY_RUN:
    try:
        _dry_run_shell = get_ipython()
    except NameError:
        print("DRY RUN: no notebook side effects were executed")
        raise SystemExit(0)

    if _dry_run_shell is None:
        print("DRY RUN: no notebook side effects were executed")
        raise SystemExit(0)

    from IPython.core.interactiveshell import ExecutionInfo, ExecutionResult

    async def _dry_run_cell_async(
        raw_cell,
        store_history=False,
        silent=False,
        shell_futures=True,
        *,
        transformed_cell=None,
        preprocessing_exc_tuple=None,
        cell_id=None,
        cell_meta=None,
    ):
        print("DRY RUN: skipped executable cell")
        info = ExecutionInfo(
            raw_cell,
            store_history,
            silent,
            shell_futures,
            cell_id,
            cell_meta,
            transformed_cell,
        )
        return ExecutionResult(info)

    _dry_run_shell.run_cell_async = _dry_run_cell_async
    print("DRY RUN: notebook loaded; subsequent executable cells will be skipped")



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

# %%
OPENSEARCH_START_FROM_SCRATCH = False
DOCKER_INTERNAL_HOST = "host.docker.internal"
DOCKER_DNS = ["10.15.20.1"]
VPN_HOST_IP = "10.15.20.2"

OPENSEARCH_DASHBOARD_PORT = 5601

OPENSEARCH_CLUSTER_NAME = "opensearch-cluster.mavasbel.vpn.itam.mx"
OPENSEARCH_TOTAL_NODES = 3

OPENSEARCH_NODE_IPS = ["10.15.20.2"] * OPENSEARCH_TOTAL_NODES
OPENSEARCH_NODE_NAMES = [
    f"opensearch-node-{i+1}" for i in range(OPENSEARCH_TOTAL_NODES)
]
OPENSEARCH_NODE_REST_API_PORTS = [9200 + (i + 1) for i in range(OPENSEARCH_TOTAL_NODES)]
OPENSEARCH_NODE_PERF_ANA_PORTS = [
    16280 + (i + 1) for i in range(OPENSEARCH_TOTAL_NODES)
]
OPENSEARCH_NODE_HOSTNAMES = [
    f"{OPENSEARCH_NODE_NAMES[j]}.mavasbel.vpn.itam.mx"
    for j in range(OPENSEARCH_TOTAL_NODES)
]
OPENSEARCH_NODE_HTTP_HOSTNAMES = [
    f"https://{OPENSEARCH_NODE_HOSTNAMES[j]}:{OPENSEARCH_NODE_REST_API_PORTS[j]}"
    for j in range(OPENSEARCH_TOTAL_NODES)
]

OPENSEARCH_WORKDIR = "/usr/share/opensearch/data"

OPENSEARCH_INITIAL_ADMIN_PASSWORD = "OpenSearchP455"
# OPENSEARCH_DEFAULT_PASSWORD = "opensearch"
# OPENSEARCH_INIT_USER = "opensearch"
# OPENSEARCH_INIT_PASSWORD = "opensearch"

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = f"{os.path.join(os.path.relpath(Path.cwd()))}"
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")

mount_path = Path(DOCKER_MOUNTDIR)
mount_path.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# # Stop opensearch-cluster.docker-compose.yml

# %%
if OPENSEARCH_START_FROM_SCRATCH:
    # !docker compose -f opensearch-cluster.docker-compose.yml down -v
else:
    print("Preserving existing containers and volumes")


# %%
import shutil

if OPENSEARCH_START_FROM_SCRATCH:
    shutil.rmtree(DOCKER_MOUNTDIR, ignore_errors=True)
    Path(DOCKER_MOUNTDIR).mkdir(parents=True, exist_ok=True)

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
    "networks": {"opensearch-cluster": {"driver": "bridge"}},
}

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
            f"http.port={OPENSEARCH_NODE_REST_API_PORTS[i]}",
            "bootstrap.memory_lock=true",
            f"OPENSEARCH_JAVA_OPTS=-Xms{node_start_heap} -Xmx{node_max_heap}",
            f"OPENSEARCH_INITIAL_ADMIN_PASSWORD={OPENSEARCH_INITIAL_ADMIN_PASSWORD}",
            f'OPENSEARCH_HOSTS=["{'","'.join(OPENSEARCH_NODE_HTTP_HOSTNAMES)}"]',
        ],
        "ulimits": {
            "memlock": {"soft": -1, "hard": -1},
            "nofile": {"soft": 65536, "hard": 65536},
        },
        "volumes": [
            f"{os.path.join(node_mount_dir, "data")}:{OPENSEARCH_WORKDIR}",
            f"{os.path.join(node_mount_dir, "performance-analyzer.properties")}:/usr/share/opensearch/config/opensearch-performance-analyzer/performance-analyzer.properties",
        ],
        "networks": ["opensearch-cluster"],
        "hostname": OPENSEARCH_NODE_HOSTNAMES[i],
        "ports": [
            f"{VPN_HOST_IP}:{OPENSEARCH_NODE_REST_API_PORTS[i]}:{OPENSEARCH_NODE_REST_API_PORTS[i]}",
            f"{VPN_HOST_IP}:{OPENSEARCH_NODE_PERF_ANA_PORTS[i]}:{OPENSEARCH_NODE_PERF_ANA_PORTS[i]}",
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
                f"curl -s -k -u admin:${{OPENSEARCH_INITIAL_ADMIN_PASSWORD}} "
                f'https://localhost:{OPENSEARCH_NODE_REST_API_PORTS[i]}/_cluster/health | grep -qv \'"status":"red"\'',
            ],
            "interval": "10s",
            "timeout": "10s",
            "retries": 10,
            "start_period": "30s",
        },
    }

opensearch_compose_dict["services"]["opensearch-dashboards"] = {
    "image": "opensearchproject/opensearch-dashboards:3.4.0",
    "container_name": "opensearch-dashboards",
    "environment": [
        f'OPENSEARCH_HOSTS=["{'","'.join(OPENSEARCH_NODE_HTTP_HOSTNAMES)}"]',
        f"OPENSEARCH_JAVA_OPTS=-Xms{node_start_heap} -Xmx{node_max_heap}",
    ],
    "networks": ["opensearch-cluster"],
    "ports": [
        f"{VPN_HOST_IP}:{OPENSEARCH_DASHBOARD_PORT}:{OPENSEARCH_DASHBOARD_PORT}",
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

# %%
# !docker compose -f opensearch-cluster.docker-compose.yml up -d --wait
