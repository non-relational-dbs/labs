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

# %% [markdown]
# Validates the injected network parameters and derives `VPN_CLIENT_DOMAIN`: `NETWORK_MODE` must be lowercase `local` or `vpn`, VPN values must stay in the `10.15.20.*` subnet, and the client alias must be a valid single DNS label.

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
# Locates the `labs-setup` project root by searching upward for the directory that contains both `pyproject.toml` and the lab module folders, then changes the working directory into the `redis` module so relative asset paths resolve consistently.

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
# Defines the six-node Redis cluster topology: container names, internal bridge IPs, mode-aware client hosts (`127.0.0.1` in local mode, `<container>.<VPN_CLIENT_DOMAIN>` in VPN mode), data/bus port ranges, and the shared credentials.

# %%
DOCKER_INTERNAL_HOST = "host.docker.internal"

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
REDIS_NODE_PORTS = [6380 + i + 1 for i in range(REDIS_TOTAL_NODES)]
REDIS_NODE_BUS_PORTS = [16380 + i + 1 for i in range(REDIS_TOTAL_NODES)]

REDIS_WORKDIR = "/data"

REDIS_ADMIN_PASSWORD = "redis"
REDIS_DEFAULT_PASSWORD = "redis"
REDIS_INIT_USER = "redis"
REDIS_INIT_PASSWORD = "redis"

# %% [markdown]
# Builds the host-side mount directory path and creates it on disk so the cluster nodes have a writable shared location for their data.

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = f"{os.path.join(os.path.relpath(Path.cwd()))}"
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")

mount_path = Path(DOCKER_MOUNTDIR)
mount_path.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# # Session creation

# %%
import pprint
from redis.cluster import RedisCluster, ClusterNode
from redis.cluster import LoadBalancingStrategy
from redis import Redis

redis_nodes = [
    ClusterNode(REDIS_NODE_CLIENT_HOSTS[i], REDIS_NODE_PORTS[i])
    for i in range(0, REDIS_TOTAL_NODES)
]
pprint.pprint(f"🔗 Connecting to: {redis_nodes}")
redis_endpoints = ",".join(f"{node.host}:{node.port}" for node in redis_nodes)
print(
    f"🔗 Connection String: redis://{REDIS_INIT_USER}:{REDIS_INIT_PASSWORD}:{redis_endpoints}"
)

def redis_address_remap(address):
    host, port = address
    if NETWORK_MODE == "local" and host in REDIS_NODE_INTERNAL_IPS:
        return "127.0.0.1", port
    return address

redis_cluster = RedisCluster(
    startup_nodes=redis_nodes,
    username=REDIS_INIT_USER,
    password=REDIS_INIT_PASSWORD,
    decode_responses=True,
    load_balancing_strategy=LoadBalancingStrategy.RANDOM_REPLICA,
    require_full_coverage=True,
    address_remap=redis_address_remap if NETWORK_MODE == "local" else None,
)
cluster_details = redis_cluster.cluster_info()
cluster_status = cluster_details["cluster_state"]
cluster_nodes = Redis(
    host=redis_nodes[0].host,
    port=redis_nodes[0].port,
    username=REDIS_INIT_USER,
    password=REDIS_INIT_PASSWORD,
    decode_responses=True,
).execute_command("CLUSTER NODES")
nodes_count = len(cluster_nodes)
primaries = list(redis_cluster.get_primaries())
discovered_endpoints = {
    (node.host, node.port) for node in redis_cluster.get_nodes()
}
expected_endpoints = set(zip(REDIS_NODE_CLIENT_HOSTS, REDIS_NODE_PORTS))
assert cluster_status == "ok", cluster_details
assert int(cluster_details["cluster_slots_assigned"]) == 16384, cluster_details
assert nodes_count == REDIS_TOTAL_NODES, nodes_count
assert len(primaries) == 3, primaries
assert len(discovered_endpoints) == len(primaries), discovered_endpoints
assert discovered_endpoints.issubset(expected_endpoints), discovered_endpoints
validation_key = "network-mode-validation"
validation_value = "ok"
assert redis_cluster.set(validation_key, validation_value)
assert redis_cluster.get(validation_key) == validation_value
print("✅ Cluster connected")
print(f"🟢 Cluster State: {cluster_status.upper()}")
print(f"🌐 Nodes Discovered: {nodes_count}; primaries={len(primaries)}")

# %% [markdown]
# Configures pandas and IPython display options so later DataFrames render with full rows, columns, and width in the notebook output.

# %%
import pandas as pd
from IPython.display import display

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option('display.max_colwidth', None)
pd.options.display.html.use_mathjax = True
pd.options.display.html.border = 1
pd.options.display.html.table_schema = False
pd.options.display.expand_frame_repr = True

# %% [markdown]
# Opens a direct `Redis` connection to each of the six nodes, collects `CLUSTER INFO` and full node configuration from every one, and displays both as transposed DataFrames for side-by-side inspection.

# %%
from redis import Redis
import pandas as pd

cluster_info = []
config_get = []
for i in range(REDIS_TOTAL_NODES):
    redis = Redis(
        host=REDIS_NODE_CLIENT_HOSTS[i],
        port=REDIS_NODE_PORTS[i],
        username=REDIS_INIT_USER,
        password=REDIS_INIT_PASSWORD,
        decode_responses=True,
    )
    cluster_info.append(redis.execute_command("CLUSTER INFO"))
    config_get.append(redis.config_get())

display(pd.DataFrame(cluster_info).transpose())
display(pd.DataFrame(config_get).transpose())

# %% [markdown]
# Runs `CLUSTER NODES` through the cluster client and renders the full topology (nodes, roles, slots, connection state) as a DataFrame table.

# %%
import pprint
import pandas as pd
from IPython.display import display

try:
    print("🛰️ Cluster Nodes View")
    display(
        pd.DataFrame(redis_cluster.execute_command("CLUSTER NODES"))
        .transpose()
        .sort_index()
        .transpose()
        .sort_index()
    )
except Exception as e:
    pprint.pprint(f"Error checking survivors: {e}")

# %% [markdown]
# Demonstrates cluster load distribution: resets command statistics on all nodes, performs 1000 `GET` reads over 10 keys through the cluster client, then reads each node's `commandstats` to show which primaries and replicas actually processed the requests.

# %%
import uuid
from typing import cast
from redis import Redis

# 1. Reset statistics on all nodes
print("Emptying stats on all nodes...")
# for node in redis_cluster.get_nodes():
for node in redis_nodes:
    redis = Redis(
        host=node.host,
        port=node.port,
        username=REDIS_INIT_USER,
        password=REDIS_INIT_PASSWORD,
        decode_responses=True,
    )
    redis.config_resetstat()

# 2. Perform a burst of reads on multiple keys
print("Performing 1000 reads across 10 keys...")
test_keys = [f"{uuid.uuid4()}" for i in range(10)]
for k in test_keys:
    redis_cluster.set(k, str(uuid.uuid4()))  # Ensure keys exist
for i in range(1000):
    redis_cluster.get(test_keys[i % REDIS_TOTAL_NODES])

# 3. Check who actually processed the 'GET' command
print("\n📊 ACTUAL Execution Stats (From Redis Engines)")
for node in redis_cluster.get_nodes():
    node = cast(ClusterNode, node)
    redis = Redis(
        host=node.host,
        port=node.port,
        username=REDIS_INIT_USER,
        password=REDIS_INIT_PASSWORD,
        decode_responses=True,
    )
    role = redis.info("replication")["role"]
    stats = redis.info("commandstats")
    get_calls = stats.get("cmdstat_get", {}).get("calls", 0)

    if get_calls > 0:
        print(f"✅ Node {node.port} ({role.upper()}): {get_calls} GETs handled")
    else:
        print(f"❌ Node {node.port} ({role.upper()}): 0 GETs handled")

# %% [markdown]
# Cleans up the lab data by flushing every key on all primary nodes, then closes the cluster client to release its connection pools.

# %%
# Flushes all in every primary node
for node in redis_cluster.get_primaries():
    node = cast(ClusterNode, node)
    redis = Redis(
        host=node.host,
        port=node.port,
        username=REDIS_INIT_USER,
        password=REDIS_INIT_PASSWORD,
        decode_responses=True,
    )
    redis.flushall()

redis_cluster.close()

# %% [markdown]
# Optional (fully commented out) monitoring snippet: polls `CLUSTER NODES` every two seconds so you can kill the current master by hand and watch failover events appear in real time.

# %%
# import time
# from redis import Redis

# idx = 4

# master_node = Redis(
#     host=REDIS_NODE_CLIENT_HOSTS[idx],
#     port=REDIS_NODE_PORTS[idx],
#     username=REDIS_INIT_USER,
#     password=REDIS_INIT_PASSWORD,
#     decode_responses=True,
# )

# print("👀 Monitoring Cluster for Failover Events... (Kill your master now)")
# while True:
#     nodes = master_node.execute_command("CLUSTER NODES")
#     # Look for the 'fail' flag or 'failover' state
#     for line in str(nodes).split("\n"):
#         if "fail" in line or "handshake" in line:
#             print(f"⏰ {time.strftime('%H:%M:%S')} | Event Detected: {line}...")
#             break
#     time.sleep(2)

# %% [markdown]
# Optional (fully commented out) manual-failover snippet: connects to a replica and issues `CLUSTER FAILOVER TAKEOVER` to promote it to master, then re-initializes the cluster client's node view.

# %%
# from redis import Redis

# idx = 4

# replica = Redis(
#     host=REDIS_NODE_CLIENT_HOSTS[idx],
#     port=REDIS_NODE_PORTS[idx],
#     username=REDIS_INIT_USER,
#     password=REDIS_INIT_PASSWORD,
#     decode_responses=True,
# )

# try:
#     print(
#         f"🚀 Attempting foredis_clustered takeover on port {REDIS_NODE_PORTS[idx]}..."
#     )
#     # 'TAKEOVER' is the aggressive version of failover
#     replica.execute_command("CLUSTER FAILOVER TAKEOVER")
#     print("✅ Success! The replica has promoted itself to MASTER.")
#     redis_cluster.nodes_manager.initialize()
#     print("✅ Success! The cluster has been initialized.")
# except Exception as e:
#     print(f"❌ Manual takeover failed: {e}")
#     print("Check if the Replica can actually 'see' the other Masters.")
