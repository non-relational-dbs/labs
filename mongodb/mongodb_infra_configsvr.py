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
# # Config Servers – Setup
#
# This notebook deploys **only the config servers** (`configsvr` role) that form the
# `config_rs` replica set.

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

# --- CONFIG SERVER CONFIGURATION ---
MONGODB_CONFIG_SVR_NODES = 3
MONGODB_STARTING_PORT = 27110
# ------------------------------------

MONGO_INITDB_ROOT_USERNAME = "admin"
MONGO_INITDB_ROOT_PASSWORD = "admin"
MONGO_INITDB_DATABASE = "admin"

# --- DYNAMIC NAME & PORT GENERATION ---
MONGODB_CONFIG_SVR_NAMES = [
    f"config-server-{i+1}" for i in range(MONGODB_CONFIG_SVR_NODES)
]
MONGODB_CONFIG_SVR_PORTS = [
    MONGODB_STARTING_PORT + (i + 1) for i in range(MONGODB_CONFIG_SVR_NODES)
]
MONGODB_CONFIG_SVR_HOSTNAMES = [
    name if NETWORK_MODE == "docker" else VPN_HOST_IP
    for name in MONGODB_CONFIG_SVR_NAMES
]

print(
    "Config servers:",
    *list(
        zip(
            MONGODB_CONFIG_SVR_NAMES,
            MONGODB_CONFIG_SVR_HOSTNAMES,
            MONGODB_CONFIG_SVR_PORTS,
        )
    ),
    sep="\n",
)

import subprocess

network_exists = subprocess.run(
    ["docker", "network", "inspect", "mongodb-network"],
    capture_output=True,
    text=True,
).returncode == 0
if not network_exists:
    subprocess.run(
        ["docker", "network", "create", "mongodb-network"], check=True
    )

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = str(MODULE_DIR.resolve())
LOCALHOST_CONFIG_DIR = os.path.abspath("config")
LOCALHOST_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "configsvr-mount")
MONGODB_LOCAL_CLUSTER_KEY_PATH = os.path.join(LOCALHOST_WORKDIR, "mongo-keyfile")

mount_path = Path(LOCALHOST_MOUNTDIR)
mount_path.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# # Stop mongodb-configsvr.docker-compose.yml

# %%
if MONGODB_START_FROM_SCRATCH:
    subprocess.run(
        ["docker", "compose", "-f", "mongodb-configsvr.docker-compose.yml", "down", "-v"],
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
            "mongo:8.2.3",
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
    clear_bind_directory(LOCALHOST_CONFIG_DIR)
    clear_bind_directory(LOCALHOST_MOUNTDIR)

# %% [markdown]
# # Start mongodb-configsvr.docker-compose.yml

# %%
import yaml
import base64
import secrets
from IPython.display import Markdown, display

# --- Generate keyfile if it doesn't exist ---
if not os.path.exists(MONGODB_LOCAL_CLUSTER_KEY_PATH):
    with open(MONGODB_LOCAL_CLUSTER_KEY_PATH, "w") as f:
        raw_data = secrets.token_bytes(756)
        f.write(base64.b64encode(raw_data).decode("utf-8"))

# %%
# --- Generate mongod.conf and mongod-bootstrap.sh for each config server ---
#
# mongod-bootstrap.sh implements the two-phase startup that docker-entrypoint.sh
# cannot handle for configsvr nodes:
#   Phase 1 – plain standalone (no configsvr/keyFile/replSet) to create the admin user
#   Phase 2 – exec real mongod with the full config

MONGO_REPLICA_SET_NAME = "config_rs"
MONGO_BOOTSTRAP_SCRIPT = """\
#!/bin/bash
set -Eeuo pipefail

# --- Step 1: Secure the keyfile ---
cp -f /etc/mongo/config/mongo-keyfile /data/keyfile
chmod 400 /data/keyfile
chown 999:999 /data/keyfile

# --- Step 2: Skip init if already initialized ---
for marker in /data/db/WiredTiger /data/db/storage.bson /data/db/journal; do
    if [ -e "$marker" ]; then
        echo "[bootstrap] Data directory already initialized – skipping user creation."
        exec mongod --config /etc/mongo/config/mongod.conf
    fi
done

# --- Step 3: Phase 1 – plain standalone, no configsvr/keyFile/replSet ---
echo "[bootstrap] Starting temporary standalone mongod for user initialisation..."
mongod \\
    --dbpath /data/db \\
    --bind_ip 127.0.0.1 \\
    --port 27017 \\
    --fork \\
    --logpath /tmp/mongod-init.log

echo "[bootstrap] Waiting for temporary mongod to accept connections..."
until mongosh --quiet --eval "db.adminCommand('ping')" > /dev/null 2>&1; do
    sleep 1
done

echo "[bootstrap] Creating admin user..."
mongosh admin --quiet --eval "
db.createUser({
    user: '$MONGO_INITDB_ROOT_USERNAME',
    pwd:  '$MONGO_INITDB_ROOT_PASSWORD',
    roles: [{ role: 'root', db: '$MONGO_INITDB_DATABASE' }]
});
"

echo "[bootstrap] Shutting down temporary mongod..."
mongod --dbpath /data/db --shutdown

echo "[bootstrap] Init complete – starting production mongod."

# --- Step 4: Phase 2 – real mongod with keyFile, configsvr, replSet ---
exec mongod --config /etc/mongo/config/mongod.conf
"""

os.makedirs(LOCALHOST_CONFIG_DIR, exist_ok=True)
for i, node_name in enumerate(MONGODB_CONFIG_SVR_NAMES):
    node_config_dir = os.path.join(LOCALHOST_CONFIG_DIR, node_name)
    os.makedirs(node_config_dir, exist_ok=True)

    # Keyfile
    shutil.copy(
        MONGODB_LOCAL_CLUSTER_KEY_PATH, os.path.join(node_config_dir, "mongo-keyfile")
    )

    # mongod.conf — storage.dbPath explicit so bootstrap finds the right directory
    config_dict = {
        "storage": {"dbPath": "/data/db"},
        "net": {"bindIp": "0.0.0.0", "port": MONGODB_CONFIG_SVR_PORTS[i]},
        "security": {"keyFile": "/data/keyfile"},
        "sharding": {"clusterRole": "configsvr"},
        "replication": {"replSetName": MONGO_REPLICA_SET_NAME},
    }
    with open(
        os.path.join(node_config_dir, "mongod.conf"),
        "w",
        encoding="utf-8",
        newline="\n",
    ) as f:
        yaml.dump(config_dict, f, default_flow_style=False)

    # mongod-bootstrap.sh
    bootstrap_path = os.path.join(node_config_dir, "mongod-bootstrap.sh")
    with open(bootstrap_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(MONGO_BOOTSTRAP_SCRIPT)

print("✅ Configuration files generated in 'config/' directory.")

# %%
# --- Copy config files into mount directories ---
for node_name in MONGODB_CONFIG_SVR_NAMES:
    src_config_dir = os.path.join(LOCALHOST_CONFIG_DIR, node_name)
    dest_config_dir = os.path.join(LOCALHOST_MOUNTDIR, node_name, "config")
    dest_data_dir = os.path.join(LOCALHOST_MOUNTDIR, node_name, "data")

    if MONGODB_START_FROM_SCRATCH and os.path.exists(dest_config_dir):
        shutil.rmtree(dest_config_dir)
    if MONGODB_START_FROM_SCRATCH and os.path.exists(dest_data_dir):
        shutil.rmtree(dest_data_dir)
    os.makedirs(dest_config_dir, exist_ok=True)
    os.makedirs(dest_data_dir, exist_ok=True)

    shutil.copytree(src_config_dir, dest_config_dir, dirs_exist_ok=True)

print("✅ Configuration copied to mount directories.")

# %%
# --- Generate Docker Compose file ---
configsvr_compose_dict = {
    "name": "mongodb-configsvr",
    "services": {},
    "networks": {"mongodb-network": {"external": True, "name": "mongodb-network"}},
}

# The bootstrap script handles keyfile copy + two-phase init + real mongod exec.
# MONGO_INITDB_ROOT_* env vars are read directly by the script via shell expansion.
cmd_script = "bash /etc/mongo/config/mongod-bootstrap.sh"

for i, node_name in enumerate(MONGODB_CONFIG_SVR_NAMES):
    hostname = MONGODB_CONFIG_SVR_HOSTNAMES[i]
    port = MONGODB_CONFIG_SVR_PORTS[i]

    service_def = {
        "image": "mongo:7.0",
        "container_name": node_name,
        "hostname": hostname,
        "command": ["bash", "-c", cmd_script],
        "environment": [
            f"MONGO_INITDB_ROOT_USERNAME={MONGO_INITDB_ROOT_USERNAME}",
            f"MONGO_INITDB_ROOT_PASSWORD={MONGO_INITDB_ROOT_PASSWORD}",
            f"MONGO_INITDB_DATABASE={MONGO_INITDB_DATABASE}",
        ],
        "volumes": [
            f"{os.path.join(LOCALHOST_MOUNTDIR, node_name, 'data')}:/data/db",
            f"{os.path.join(LOCALHOST_MOUNTDIR, node_name, 'config')}:/etc/mongo/config",
        ],
        "networks": ["mongodb-network"],
        "ports": [f"{HOST_BIND_IP}:{port}:{port}"],
        "extra_hosts": [f"{DOCKER_INTERNAL_HOST}:host-gateway"],
        "dns": DOCKER_DNS,
        "deploy": {"resources": {"limits": {"cpus": "1.0", "memory": "512M"}}},
        "healthcheck": {
            "test": [
                "CMD",
                "mongosh",
                "--port",
                f"{port}",
                "--quiet",
                "--eval",
                "db.adminCommand('ping')",
            ],
            "interval": "10s",
            "timeout": "10s",
            "retries": 10,
            "start_period": "30s",  # allow time for the two-phase init
        },
        "depends_on": {},
    }

    # Each server depends on the previous one having started
    if i > 0:
        service_def["depends_on"][MONGODB_CONFIG_SVR_NAMES[i - 1]] = {
            "condition": "service_started"
        }

    configsvr_compose_dict["services"][node_name] = service_def

compose_yaml_path = os.path.join(
    LOCALHOST_WORKDIR, "mongodb-configsvr.docker-compose.yml"
)
with open(compose_yaml_path, "w") as f:
    yaml.dump(
        configsvr_compose_dict, f, default_flow_style=False, sort_keys=False, indent=4
    )

print(f"Successfully created: '{os.path.relpath(compose_yaml_path)}'")
display(
    Markdown(
        f"```yaml\n{yaml.dump(configsvr_compose_dict, default_flow_style=False, sort_keys=False, indent=4)}\n```"
    )
)

# %%
# !docker compose -f mongodb-configsvr.docker-compose.yml up -d --wait

# %% [markdown]
# # Initialise the `config_rs` Replica Set
#
# At this point the containers are healthy. The bootstrap script has created the
# admin user during Phase 1, so we can connect with credentials.
#
# We connect to `localhost` on each published port but list the full Docker-internal
# hostnames in the `replSetInitiate` member config so inter-container replication
# works correctly inside the Docker network.

# %%
import time
from pymongo import MongoClient
from pymongo.errors import OperationFailure

client_options = {"directConnection": True, "serverSelectionTimeoutMS": 20000}

auth_uri = (
    f"mongodb://{MONGO_INITDB_ROOT_USERNAME}:{MONGO_INITDB_ROOT_PASSWORD}"
    f"@{BOOTSTRAP_HOST}:{MONGODB_CONFIG_SVR_PORTS[0]}/?authSource=admin"
)

replica_set_config = {
    "_id": MONGO_REPLICA_SET_NAME,
    "configsvr": True,
    "members": [
        {
            "_id": i,
            "host": f"{MONGODB_CONFIG_SVR_HOSTNAMES[i]}:{MONGODB_CONFIG_SVR_PORTS[i]}",
        }
        for i in range(MONGODB_CONFIG_SVR_NODES)
    ],
}

print(
    f"🚀 Initiating replica set '{MONGO_REPLICA_SET_NAME}' on localhost:{MONGODB_CONFIG_SVR_PORTS[0]} ..."
)
for m in replica_set_config["members"]:
    print(f"   _id={m['_id']}  host={m['host']}")

try:
    with MongoClient(auth_uri, **client_options) as client:
        client.admin.command("replSetInitiate", replica_set_config)
        print("✅ replSetInitiate accepted.")
except OperationFailure as e:
    if "already initialized" in str(e).lower():
        print(
            f"⚠️  Replica set '{MONGO_REPLICA_SET_NAME}' is already initiated. Skipping..."
        )
    else:
        raise

# %%
import time

PRIMARY_ELECTION_TIMEOUT = 30

primary = None
start_time = time.time()
with MongoClient(auth_uri, **client_options) as client:
    while primary is None and time.time() < start_time + PRIMARY_ELECTION_TIMEOUT:
        status = client.admin.command("replSetGetStatus")
        primary = next(
            (m for m in status.get("members", []) if m["stateStr"] == "PRIMARY"),
            None,
        )
        if primary is None:
            print("⏳ No PRIMARY yet, retrying in 1 s...")
            time.sleep(1)
        else:
            print(f"Replica Set '{MONGO_REPLICA_SET_NAME}' Status Summary:")
            for m in status["members"]:
                icon = "🟢" if m["health"] == 1 else "🔴"
                print(f"{icon} {m['name']:<35} | {m['stateStr']:<10}")

if primary is None:
    raise TimeoutError(f"No PRIMARY elected within {PRIMARY_ELECTION_TIMEOUT}s.")
