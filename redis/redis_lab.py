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
# Docker is the portable default; VPN mode publishes services on this host.
NETWORK_MODE = "docker"
VPN_HOST_IP = "10.15.20.100"
VPN_DNS_IP = "10.15.20.1"

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
MODULE_DIR = LABS_ROOT / "redis"
os.chdir(MODULE_DIR)
print(f"Working directory: {MODULE_DIR}")

# %%
DOCKER_INTERNAL_HOST = "host.docker.internal"

REDIS_TOTAL_NODES = 6
REDIS_NODE_NAMES = [f"redis-node-{i+1}" for i in range(REDIS_TOTAL_NODES)]
REDIS_NODE_HOSTNAMES = [
    REDIS_NODE_NAMES[i] if NETWORK_MODE == "docker" else VPN_HOST_IP
    for i in range(REDIS_TOTAL_NODES)
]
REDIS_NODE_IPS = REDIS_NODE_HOSTNAMES
REDIS_NODE_PORTS = [6380 + i + 1 for i in range(REDIS_TOTAL_NODES)]
REDIS_NODE_BUS_PORTS = [16380 + i + 1 for i in range(REDIS_TOTAL_NODES)]

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
# # Session creation

# %%
import pprint
from redis.cluster import RedisCluster, ClusterNode
from redis.cluster import LoadBalancingStrategy
from redis import Redis

redis_nodes = [
    ClusterNode(f"{REDIS_NODE_HOSTNAMES[i]}", REDIS_NODE_PORTS[i])
    for i in range(0, REDIS_TOTAL_NODES)
]
pprint.pprint(f"🔗 Connecting to: {redis_nodes}")
redis_endpoints = ",".join(f"{node.host}:{node.port}" for node in redis_nodes)
print(
    f"🔗 Connection String: redis://{REDIS_INIT_USER}:{REDIS_INIT_PASSWORD}:{redis_endpoints}"
)

redis_cluster = RedisCluster(
    startup_nodes=redis_nodes,
    username=REDIS_INIT_USER,
    password=REDIS_INIT_PASSWORD,
    decode_responses=True,
    load_balancing_strategy=LoadBalancingStrategy.RANDOM_REPLICA,
    require_full_coverage=True,
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
assert cluster_status == "ok", cluster_details
assert int(cluster_details["cluster_slots_assigned"]) == 16384, cluster_details
assert nodes_count == REDIS_TOTAL_NODES, nodes_count
assert len(primaries) == 3, primaries
print("✅ Cluster connected")
print(f"🟢 Cluster State: {cluster_status.upper()}")
print(f"🌐 Nodes Discovered: {nodes_count}; primaries={len(primaries)}")

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

# %%
from redis import Redis
import pandas as pd

cluster_info = []
config_get = []
for i in range(REDIS_TOTAL_NODES):
    redis = Redis(
        host=REDIS_NODE_HOSTNAMES[i],
        port=REDIS_NODE_PORTS[i],
        username=REDIS_INIT_USER,
        password=REDIS_INIT_PASSWORD,
        decode_responses=True,
    )
    cluster_info.append(redis.execute_command("CLUSTER INFO"))
    config_get.append(redis.config_get())

display(pd.DataFrame(cluster_info).transpose())
display(pd.DataFrame(config_get).transpose())

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

# %%
# import time
# from redis import Redis

# idx = 4

# master_node = Redis(
#     host=REDIS_NODE_HOSTNAMES[idx],
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

# %%
# from redis import Redis

# idx = 4

# replica = Redis(
#     host=REDIS_NODE_HOSTNAMES[idx],
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
