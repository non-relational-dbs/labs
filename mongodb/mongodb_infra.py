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
# Docker is the portable default; VPN mode publishes services on this host.
NETWORK_MODE = "docker"
VPN_HOST_IP = "10.15.20.100"
VPN_DNS_IP = "10.15.20.1"
START_FROM_SCRATCH = False

# %%
NETWORK_MODE = NETWORK_MODE.strip().lower()
if NETWORK_MODE not in {"docker", "vpn"}:
    raise ValueError("NETWORK_MODE must be 'docker' or 'vpn'")
if NETWORK_MODE == "vpn" and not VPN_HOST_IP.startswith("10.15.20."):
    raise ValueError("VPN_HOST_IP must be in the 10.15.20.* subnet")

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
MODULE_DIR = LABS_ROOT / "mongodb"
os.chdir(MODULE_DIR)
print(f"Working directory: {MODULE_DIR}")

# %%
MONGODB_START_FROM_SCRATCH = START_FROM_SCRATCH
DOCKER_INTERNAL_HOST = "host.docker.internal"
DOCKER_DNS = [VPN_DNS_IP] if NETWORK_MODE == "vpn" else []
HOST_BIND_IP = VPN_HOST_IP if NETWORK_MODE == "vpn" else "127.0.0.1"
BOOTSTRAP_HOST = VPN_HOST_IP if NETWORK_MODE == "vpn" else "127.0.0.1"

MONGODB_NODES_DOMAIN = "mavasbel.vpn.itam.mx"
MONGODB_REPLICA_SET = "replica_set_0"
MONGODB_TOTAL_NODES = 3

MONGODB_NODE_NAMES = [f"mongodb-node-{i + 1}" for i in range(MONGODB_TOTAL_NODES)]
MONGODB_NODE_HOSTNAMES = [
    MONGODB_NODE_NAMES[i] if NETWORK_MODE == "docker" else VPN_HOST_IP
    for i in range(MONGODB_TOTAL_NODES)
]
MONGODB_NODE_PORTS = [27010 + (i + 1) for i in range(0, MONGODB_TOTAL_NODES)]

MONGODB_WORKDIR = "/data/db"

MONGO_INITDB_ROOT_USERNAME = "admin"
MONGO_INITDB_ROOT_PASSWORD = "admin"
MONGO_INITDB_DATABASE = "admin"

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = str(MODULE_DIR.resolve())
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")
MONGODB_LOCAL_CLUSTER_KEY_PATH = os.path.join(DOCKER_MOUNTDIR, "mongo-keyfile")

mount_path = Path(DOCKER_MOUNTDIR)
mount_path.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# # Stop mongodb-cluster.docker-compose.yml

# %%
import subprocess

if MONGODB_START_FROM_SCRATCH:
    subprocess.run(
        ["docker", "compose", "-f", "mongodb-cluster.docker-compose.yml", "down", "-v"],
        check=False,
    )
else:
    print("Preserving existing containers and volumes")


# %%
import shutil
import stat

def clear_bind_directory(path):
    target = Path(path).resolve()
    target.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--mount",
            f"type=bind,source={target},target=/target",
            "mongo:7.0",
            "bash",
            "-c",
            "find /target -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +",
        ],
        check=True,
    )

if MONGODB_START_FROM_SCRATCH:
    if os.path.exists(MONGODB_LOCAL_CLUSTER_KEY_PATH):
        os.chmod(MONGODB_LOCAL_CLUSTER_KEY_PATH, stat.S_IWRITE)
        os.remove(MONGODB_LOCAL_CLUSTER_KEY_PATH)
    clear_bind_directory(DOCKER_MOUNTDIR)

# %% [markdown]
# # Start mongodb-cluster.docker-compose.yml

# %%
import os
import stat
import yaml
import base64
import secrets
from IPython.display import Markdown, display

if not os.path.exists(MONGODB_LOCAL_CLUSTER_KEY_PATH):
    with open(MONGODB_LOCAL_CLUSTER_KEY_PATH, "w") as f:
        raw_data = secrets.token_bytes(756)
        f.write(base64.b64encode(raw_data).decode("utf-8"))

mongodb_compose_dict = {
    "name": "mongodb-cluster",
    "services": {},
    "networks": {"mongodb-network": {"external": True, "name": "mongodb-network"}},
}

for i in range(MONGODB_TOTAL_NODES):
    mongodb_compose_dict["services"][MONGODB_NODE_NAMES[i]] = {
        "image": "mongo:7.0",
        "container_name": MONGODB_NODE_NAMES[i],
        "hostname": MONGODB_NODE_HOSTNAMES[i],
        "command": [
            "bash",
            "-c",
            " && ".join(
                [
                    "chown 999:999 /data/configdb/keyfile",
                    "chmod 400 /data/configdb/keyfile",
                    " ".join(
                        [
                            "exec",
                            "docker-entrypoint.sh",
                            "mongod",
                            "--replSet",
                            MONGODB_REPLICA_SET,
                            "--keyFile",
                            "/data/configdb/keyfile",
                            "--bind_ip_all",
                            "--port",
                            f"{MONGODB_NODE_PORTS[i]}",
                        ]
                    ),
                ]
            ),
        ],
        "environment": [
            f"MONGO_INITDB_ROOT_USERNAME={MONGO_INITDB_ROOT_USERNAME}",
            f"MONGO_INITDB_ROOT_PASSWORD={MONGO_INITDB_ROOT_PASSWORD}",
            f"MONGO_INITDB_DATABASE={MONGO_INITDB_DATABASE}",
        ],
        "volumes": [
            f"{os.path.join(DOCKER_MOUNTDIR, MONGODB_NODE_NAMES[i])}:/data/db",
            f"{os.path.join(DOCKER_MOUNTDIR, 'mongo-keyfile')}:/data/configdb/keyfile",
        ],
        "networks": ["mongodb-network"],
        "ports": [
            f"{HOST_BIND_IP}:{MONGODB_NODE_PORTS[i]}:{MONGODB_NODE_PORTS[i]}"
        ],
        "extra_hosts": [f"{DOCKER_INTERNAL_HOST}:host-gateway"],
        # + [
        #     f"{MONGODB_NODE_HOSTNAMES[j]}:host-gateway"
        #     for j in range(MONGODB_TOTAL_NODES)
        # ],
        "dns": DOCKER_DNS,
        "deploy": {"resources": {"limits": {"cpus": "1.0", "memory": "1024M"}}},
        "healthcheck": {
            "test": [
                "CMD",
                "mongosh",
                "--port",
                f"{MONGODB_NODE_PORTS[i]}",
                "--quiet",
                "--eval",
                "db.adminCommand('ping')",
            ],
            "interval": "10s",
            "timeout": "10s",
            "retries": 30,
            "start_period": "30s",
        },
        "depends_on": {
            MONGODB_NODE_NAMES[j]: {"condition": "service_started"} for j in range(0, i)
        },
    }
    # mongodb_compose_dict["services"]["coredns"] = {
    #     "image": "coredns/coredns:1.13.2",
    #     "container_name": "coredns",
    #     "restart": "always",
    #     "network_mode": "host",
    #     "hostname": "coredns",
    #     "volumes": [
    #         f"{os.path.join(DOCKER_MOUNTDIR, 'coredns', 'Corefile')}:/Corefile",
    #     ],
    #     "command": "-conf /Corefile"
    # }

mongodb_compose_yaml_path = os.path.join(
    LOCALHOST_WORKDIR, "mongodb-cluster.docker-compose.yml"
)
mongodb_compose_yaml_contents = yaml.dump(
    mongodb_compose_dict, default_flow_style=False, sort_keys=False, indent=4
)
with open(mongodb_compose_yaml_path, "w") as f:
    f.write(mongodb_compose_yaml_contents)

(print(f"Successfully created: '{os.path.relpath(mongodb_compose_yaml_path)}'"),)
display(Markdown(f"```yaml\n{mongodb_compose_yaml_contents}\n```"))

# %%
# !docker compose -f mongodb-cluster.docker-compose.yml up -d --wait

# %%
import time
from pymongo import MongoClient
from pymongo.errors import OperationFailure

MONGODB_PRIMARY_SELECTION_TIMEOUT_SECONDS = 30
client_options = {"directConnection": True, "serverSelectionTimeoutMS": 5000}

init_client = MongoClient(
    f"mongodb://{MONGO_INITDB_ROOT_USERNAME}:{MONGO_INITDB_ROOT_PASSWORD}@{BOOTSTRAP_HOST}:{MONGODB_NODE_PORTS[0]}/",
    **client_options,
)
try:
    print(f"🚀 Initializing Replica Set: '{MONGODB_REPLICA_SET}'")
    init_client.admin.command(
        "replSetInitiate",
        {
            "_id": MONGODB_REPLICA_SET,
            "members": [
                {
                    "_id": i,
                    "host": f"{MONGODB_NODE_HOSTNAMES[i]}:{MONGODB_NODE_PORTS[i]}",
                }
                for i in range(MONGODB_TOTAL_NODES)
            ],
        },
    )
    print("✅ Initiation command accepted.")
except OperationFailure as e:
    if "already initialized" in str(e).lower():
        print("⚠️ Cluster is already initiated. Verifying health...")
    else:
        raise

start_time = time.time()
primary_found = False
print(f"⏳ Waiting for Primary (Timeout: {MONGODB_PRIMARY_SELECTION_TIMEOUT_SECONDS}s)")
while time.time() - start_time < MONGODB_PRIMARY_SELECTION_TIMEOUT_SECONDS:
    for i in range(MONGODB_TOTAL_NODES):
        try:
            with MongoClient(
                f"mongodb://{BOOTSTRAP_HOST}:{MONGODB_NODE_PORTS[i]}/",
                **client_options,
            ) as node_check:
                res = node_check.admin.command("hello")
                if res.get("isWritablePrimary") or res.get("ismaster"):
                    primary_found = True
                    elapsed = round(time.time() - start_time, 2)
                    print(
                        f"\n🌟 Primary Elected: {res.get('me')} (Found at {MONGODB_NODE_HOSTNAMES[i]}:{MONGODB_NODE_PORTS[i]} in {elapsed}s)"
                    )
                    break
        except Exception:
            continue
    if primary_found:
        break
    print(f"Still electing... [{int(time.time() - start_time)}s]", end="\r")
    time.sleep(0.05)

if not primary_found:
    status = init_client.admin.command("replSetGetStatus")
    print("\n❌ Timeout reached. Current Node States:")
    for m in status.get("members", []):
        print(f" - {m['name']}: {m['stateStr']}")
    raise TimeoutError("Replica Set failed to elect a Primary.")
else:
    status = init_client.admin.command("replSetGetStatus")
    print(f"\nCluster '{MONGODB_REPLICA_SET}' Status Summary:")
    for m in status["members"]:
        icon = "🟢" if m["health"] == 1 else "🔴"
        print(f"{icon} {m['name']:<35} | {m['stateStr']:<10}")

# %%
# docker exec -it mongodb-node-1 mongosh --port 27011 -u "admin" -p "admin" --authenticationDatabase "admin"
# docker exec -it mongodb-node-2 mongosh --port 27012 -u "admin" -p "admin" --authenticationDatabase "admin"
# docker exec -it mongodb-node-3 mongosh --port 27013 -u "admin" -p "admin" --authenticationDatabase "admin"

# # !docker exec -i mongodb-node-1 mongosh --port 27011 -u "admin" -p "admin" --authenticationDatabase "admin" --quiet --eval "db.estudiantes.find({ promedio: { $gt: 9.5 } }).explain('allPlansExecution')"
# # !docker exec -i mongodb-node-2 mongosh --port 27012 -u "admin" -p "admin" --authenticationDatabase "admin" --quiet --eval "db.estudiantes.find({ promedio: { $gt: 9.5 } }).explain('allPlansExecution')"
# # !docker exec -i mongodb-node-3 mongosh --port 27013 -u "admin" -p "admin" --authenticationDatabase "admin" --quiet --eval "db.estudiantes.find({ promedio: { $gt: 9.5 } }).explain('allPlansExecution')"
