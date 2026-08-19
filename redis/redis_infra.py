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
MODULE_DIR = LABS_ROOT / "redis"
os.chdir(MODULE_DIR)
print(f"Working directory: {MODULE_DIR}")

# %%
REDIS_START_FROM_SCRATCH = False
DOCKER_INTERNAL_HOST = "host.docker.internal"
DOCKER_DNS = ["10.15.20.1"]
VPN_HOST_IP = "10.15.20.2"

REDIS_TOTAL_NODES = 6
REDIS_NODE_IPS = ["10.15.20.2"] * REDIS_TOTAL_NODES
REDIS_NODE_NAMES = [f"redis-node-{i+1}" for i in range(REDIS_TOTAL_NODES)]
REDIS_NODE_HOSTNAMES = [
    f"{REDIS_NODE_NAMES[i]}.mavasbel.vpn.itam.mx" for i in range(REDIS_TOTAL_NODES)
]
REDIS_NODE_PORTS = [f"{6380 + i + 1}" for i in range(REDIS_TOTAL_NODES)]
REDIS_NODE_BUS_PORTS = [f"{16380 + i + 1}" for i in range(REDIS_TOTAL_NODES)]

REDIS_WORKDIR = "/data"

REDIS_ADMIN_PASSWORD = "redis"
REDIS_DEFAULT_PASSWORD = "redis"
REDIS_INIT_USER = "redis"
REDIS_INIT_PASSWORD = "redis"

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = f"{os.path.join(os.path.relpath(Path.cwd()))}"
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")

mount_path = Path(DOCKER_MOUNTDIR)
mount_path.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# # Stop redis-cluster.docker-compose.yml

# %%
if REDIS_START_FROM_SCRATCH:
    # !docker compose -f redis-cluster.docker-compose.yml down -v
else:
    print("Preserving existing containers and volumes")


# %%
import shutil

if REDIS_START_FROM_SCRATCH:
    shutil.rmtree(DOCKER_MOUNTDIR, ignore_errors=True)
    Path(DOCKER_MOUNTDIR).mkdir(parents=True, exist_ok=True)

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
            "driver": "bridge",
            # "ipam": {
            #     "config": [{"subnet": "173.20.0.0/16"}]
            # }
        }
    },
}

for i in range(REDIS_TOTAL_NODES):
    redis_compose_dict["services"][REDIS_NODE_NAMES[i]] = {
        "image": "redis:8.4.0-bookworm",
        "container_name": REDIS_NODE_NAMES[i],
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
            *["--port", REDIS_NODE_PORTS[i]],
            *["--bind", "0.0.0.0"],
            *["--protected-mode", "no"],
            *["--cluster-announce-ip", REDIS_NODE_IPS[i]],
            *["--cluster-announce-port", REDIS_NODE_PORTS[i]],
            *["--cluster-announce-bus-port", REDIS_NODE_BUS_PORTS[i]],
        ],
        "volumes": [
            f"{os.path.join(DOCKER_MOUNTDIR, REDIS_NODE_NAMES[i])}:{REDIS_WORKDIR}"
        ],
        "networks": ["redis-cluster"],
        # f"hostname": f"{REDIS_NODE_HOSTNAMES[i]}",
        "ports": [
            f"{VPN_HOST_IP}:{REDIS_NODE_PORTS[i]}:{REDIS_NODE_PORTS[i]}",
            f"{VPN_HOST_IP}:{REDIS_NODE_BUS_PORTS[i]}:{REDIS_NODE_BUS_PORTS[i]}",
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
            "test": ["CMD", "redis-cli", "-p", f"{REDIS_NODE_PORTS[i]}", "ping"],
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

# %%
# !docker compose -f redis-cluster.docker-compose.yml up -d --wait

# %%
nodes_ports = ' '.join([f"{REDIS_NODE_IPS[i]}:{REDIS_NODE_PORTS[i]}" for i in range(REDIS_TOTAL_NODES)])

# !docker exec redis-node-1 redis-cli -a {REDIS_ADMIN_PASSWORD} --cluster create {nodes_ports} --cluster-replicas 1 --cluster-yes
