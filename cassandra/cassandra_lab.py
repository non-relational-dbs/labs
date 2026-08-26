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
MODULE_DIR = LABS_ROOT / "cassandra"
os.chdir(MODULE_DIR)
print(f"Working directory: {MODULE_DIR}")

# %%
CASSANDRA_START_FROM_SCRATCH = False
DOCKER_INTERNAL_HOST = "host.docker.internal"

CASSANDRA_CLUSTER_NAME = "cassandra-cluster"
CASSANDRA_TOTAL_NODES = 3

CASSANDRA_NODE_NAMES = [f"cassandra-node-{i+1}" for i in range(CASSANDRA_TOTAL_NODES)]
def vpn_fqdn(container_name):
    return f"{container_name}.{VPN_CLIENT_DOMAIN}"


CASSANDRA_NODE_CLIENT_HOSTS = [
    "127.0.0.1" if NETWORK_MODE == "local" else vpn_fqdn(name)
    for name in CASSANDRA_NODE_NAMES
]
CASSANDRA_NODE_GOSSIP_PORTS = [7000 + (i + 1) for i in range(CASSANDRA_TOTAL_NODES)]
CASSANDRA_NODE_RPC_PORTS = [9040 + (i + 1) for i in range(CASSANDRA_TOTAL_NODES)]
CASSANDRA_NODE_SSL_GOSSIP_PORTS = [7500 + (i + 1) for i in range(CASSANDRA_TOTAL_NODES)]
CASSANDRA_NODE_JMX_PORTS = [7200 + (i + 1) for i in range(0, CASSANDRA_TOTAL_NODES)]

CASSANDRA_CA_CERT_PASSWORD = "cassandra_cluster_ca_cert_passowrd"
CASSANDRA_NODE_CERT_PASSWORD = "cassandra_cluster_cert_passowrd"
CASSANDRA_INIT_USER = "cassandra"
CASSANDRA_INIT_PASSWORD = "cassandra"

CASSANDRA_WORKDIR = "/var/lib/cassandra"

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = f"{os.path.join(os.path.relpath(Path.cwd()))}"
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")
CASSANDRA_LOCALHOST_CLUSTER_CA_CERTDIR = os.path.join(LOCALHOST_WORKDIR, "cluster_certs")

mount_path = Path(DOCKER_MOUNTDIR)
mount_path.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("CQLENG_ALLOW_SCHEMA_MANAGEMENT", "True")

# %% [markdown]
# # Session creation

# %%
from cassandra.cluster import Cluster
from cassandra.connection import DefaultEndPoint, DefaultEndPointFactory
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory


class ClientEndPointFactory(DefaultEndPointFactory):
    def __init__(self, hosts_by_port):
        super().__init__()
        self.hosts_by_port = hosts_by_port

    def create(self, row):
        port = row.get("native_port")
        if port not in self.hosts_by_port:
            return super().create(row)
        return DefaultEndPoint(self.hosts_by_port[port], port)


cassandra_nodes = [
    DefaultEndPoint(CASSANDRA_NODE_CLIENT_HOSTS[j], CASSANDRA_NODE_RPC_PORTS[j])
    for j in range(CASSANDRA_TOTAL_NODES)
]
cassandra_endpoints = [
    f"{cassandra_node.address}:{cassandra_node.port}"
    for cassandra_node in cassandra_nodes
]
print(f"🔗 Connecting to: {cassandra_endpoints}")
print(f"JDBC URL: jdbc:cassandra://{','.join(cassandra_endpoints)}")

auth_provider = PlainTextAuthProvider(
    username=CASSANDRA_INIT_USER, password=CASSANDRA_INIT_PASSWORD
)
endpoint_factory = ClientEndPointFactory(
    dict(zip(CASSANDRA_NODE_RPC_PORTS, CASSANDRA_NODE_CLIENT_HOSTS))
)
cluster = Cluster(
    contact_points=cassandra_nodes,
    auth_provider=auth_provider,
    endpoint_factory=endpoint_factory,
)

session = cluster.connect()
session.row_factory = dict_factory
print(f"✅ Connected to cluster: {cluster.metadata.cluster_name}")
print(f"🌐 Nodes found: {len(cluster.metadata.all_hosts())}")
assert cluster.metadata.cluster_name == CASSANDRA_CLUSTER_NAME
assert len(cluster.metadata.all_hosts()) == CASSANDRA_TOTAL_NODES
discovered_endpoints = {
    (host.endpoint.address, host.endpoint.port)
    for host in cluster.metadata.all_hosts()
}
expected_endpoints = set(zip(CASSANDRA_NODE_CLIENT_HOSTS, CASSANDRA_NODE_RPC_PORTS))
assert discovered_endpoints == expected_endpoints, discovered_endpoints
assert all(host.is_up for host in cluster.metadata.all_hosts())

# %%
import pprint
import pandas as pd
from IPython.display import display

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option('display.max_colwidth', None)
pd.options.display.html.use_mathjax = True
pd.options.display.html.border = 1
pd.options.display.html.table_schema = False
pd.options.display.expand_frame_repr = True

display(
    pd.DataFrame(
        [cassandra_host.__dict__ for cassandra_host in cluster.metadata.all_hosts()]
    )
    .sort_index()
    .transpose()
)

# %%
# from cassandra.cluster import ResultSet

# peers_v2: ResultSet = session.execute("SELECT * FROM system.peers_v2")
# locals: ResultSet = session.execute("SELECT * FROM system.local")

# for peer_v2 in peers_v2:
#     print(" | ".join([f"{col}: {peer_v2[col]}" for col in peers_v2.column_names]))
# print("=" * 100)
# for local in locals:
#     print(" | ".join([f"{col}: {local[col]}" for col in locals.column_names]))
# print("=" * 100)

# %% [markdown]
# ### Keyspace

# %%
keyspace_name = "generic_analytics"
if CASSANDRA_START_FROM_SCRATCH:
    session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_name}")

# %%
keyspace_name = "generic_analytics"
session.execute(
    f"""
CREATE KEYSPACE IF NOT EXISTS {keyspace_name} 
WITH replication = {{
    'class': 'NetworkTopologyStrategy', 
    'dc1': {CASSANDRA_TOTAL_NODES}
}}
"""
)
session.set_keyspace(keyspace_name)

# Create Table
# Partition Key: city (distributes data)
# Clustering Column: user_id (sorts data within city)
session.execute(
    """
CREATE TABLE IF NOT EXISTS user_metrics (
    city text,
    user_id uuid,
    username text,
    session_duration int,
    last_access timestamp,
    PRIMARY KEY (city, user_id)
);
"""
)

# %% [markdown]
# ### Insert

# %%
from faker import Faker

fake = Faker()

# %%
import uuid
from datetime import datetime
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, BatchType

# 1. Prepare your statement outside the loop
query = """
INSERT INTO user_metrics (city, user_id, username, session_duration, last_access)
VALUES (?, ?, ?, ?, ?)
"""
prepared = session.prepare(query)
# prepared.consistency_level = ConsistencyLevel.QUORUM
# statement = SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM)

# 2. Create the Batch object
# LOGGED ensures atomicity but adds disk overhead, UNLOGGED is faster
batch_records = 200
batch = BatchStatement(batch_type=BatchType.LOGGED)
batch.consistency_level = ConsistencyLevel.QUORUM
print(f"Preparing batch of {batch_records} records...")
for _ in range(batch_records):
    batch.add(
        prepared,
        (
            fake.city(),
            uuid.uuid4(),
            fake.user_name(),
            fake.random_int(min=1, max=3600),
            datetime.now(),
        ),
    )

# 3. Execute the entire batch at once
session.execute(batch)
print("Batch successfully committed to the cluster.")

# %% [markdown]
# ### Query

# %%
from cassandra.cluster import ResultSet
from typing import cast

count_row = cast(
    ResultSet, session.execute("SELECT count(*) FROM user_metrics")
).one()
row_count = next(iter(count_row.values()))
assert row_count >= batch_records, row_count
print(f"Rows stored: {row_count}")

# %%
from cassandra.cluster import ResultSet

rows: ResultSet = session.execute("SELECT * FROM user_metrics LIMIT 100")
for row in rows.current_rows:
    print(row)

# %%
from cassandra.cluster import ResultSet

rows: ResultSet = session.execute("SELECT DISTINCT city FROM user_metrics", [])
for row in rows.current_rows:
    print(row)

# %% [markdown]
# ### Find nodes storing random data

# %%
import random
import pprint

random_token = random.randint(-9223372036854775808, 9223372036854775807)
query = "SELECT * FROM user_metrics WHERE token(city) >= %s LIMIT 1"
row: dict = cast(ResultSet, session.execute(query, [random_token])).one()
print(f"Random user_metric: {pprint.pformat(row)}")

prepared = session.prepare("SELECT * FROM user_metrics WHERE city=? AND user_id=?")
bound = prepared.bind([row['city'], row['user_id']])
routing_key = bound.routing_key
nodes = cluster.metadata.get_replicas(keyspace_name, routing_key)
assert len(nodes) == CASSANDRA_TOTAL_NODES, nodes

print(f"Nodes storing '{row['city']}':")
for node in nodes:
    print(f" - Host: {node.address}, Gossip Port: {node.broadcast_port}")

# %% [markdown]
# ### ORM-like

# %%
from typing import cast
from cassandra.cqlengine import columns
from cassandra.cqlengine.query import ModelQuerySet
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table, create_keyspace_network_topology
from cassandra.cqlengine import connection

# 1. Connect the engine
connection.set_session(session)


# 2. Define your "Generic" Model
class UserMetrics(Model):
    __table_name__ = "user_metrics"

    # FIRST primary_key=True is the Partition Key
    # city = columns.Text(primary_key=True)
    city = columns.Text(primary_key=True, partition_key=True)

    # SECOND primary_key=True is the Clustering Key
    user_id = columns.UUID(primary_key=True, default=uuid.uuid4)

    # Attributes (Data)
    username = columns.Text(index=True)
    session_duration = columns.Integer()
    last_access = columns.DateTime()


# 3. Create Keyspace and Sync Table (Equivalent to CREATE TABLE)
create_keyspace_network_topology(keyspace_name, {"dc1": CASSANDRA_TOTAL_NODES})
sync_table(UserMetrics)

# %%
# 4. Use it like an ORM
# Create UserMetrics
new_metric: UserMetrics = UserMetrics.create(
    city=fake.city(),
    username=fake.name(),
    session_duration=120,
    last_access=datetime.now(),
)
new_metric.save()
print(f"Saved user_metris: {new_metric}")


# 5. Query UserMetrics
user_metrics = (
    cast(ModelQuerySet, UserMetrics.objects())
    .filter(session_duration__gte=120)
    .allow_filtering() # Non-recommended, used for non-primary key queries
)
for user_metric in user_metrics:
    print(f"Query user_metris: {user_metric}")

# %% [markdown]
# ### Insert/Create

# %%
from typing import cast

cast(ModelQuerySet, UserMetrics.ttl(86400)).create(
    city=fake.city(),
    username=fake.user_name(),
    session_duration=fake.random_int(10, 1000),
    last_access=fake.date_time(),
)

# %%
from typing import cast
from cassandra.cqlengine.query import BatchQuery
from cassandra.cqlengine.query import ModelQuerySet

with BatchQuery() as b:
    for _ in range(100):
        cast(ModelQuerySet, UserMetrics.batch(b)).create(
            city=fake.city(),
            username=fake.user_name(),
            session_duration=fake.random_int(10, 1000),
            last_access=fake.date_time(),
        )

# %% [markdown]
# ### Delete

# %%
user_metrics = (
    cast(ModelQuerySet, UserMetrics.objects())
    .filter(session_duration__gte=120)
    .allow_filtering() # Non-recommended, used for non-primary key queries
)
for user_metric in user_metrics:
    user_metric.delete()
    print(f"Delete user_metris: {user_metric}")

session.shutdown()
cluster.shutdown()

# %%
# from cassandra.cqlengine import connection

# # This isn't a compaction, but it forces RAM data to disk so the background compaction process can see it.
# connection.execute(f"ALTER TABLE {keyspace_name}.user_metrics WITH gc_grace_seconds = 10")
