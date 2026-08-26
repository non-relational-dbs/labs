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

# %% [markdown]
# Validates and normalizes the papermill-injected network parameters, rejecting any NETWORK_MODE other than lowercase `local` or `vpn` or an invalid VPN_CLIENT_ALIAS, and composing VPN_CLIENT_DOMAIN for later hostname construction.

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
# Locates the labs-setup project root by walking parent directories until it finds a `pyproject.toml` alongside the `cassandra` and `mongodb` folders, then changes the working directory to the mongodb module.

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

# %% [markdown]
# Defines the lab's view of the running replica set: `MONGODB_REPLICA_SET` with three nodes, ports 27011 through 27013, client hosts resolved per NETWORK_MODE via `vpn_fqdn`, and the admin credentials used throughout the notebook.

# %%
DOCKER_INTERNAL_HOST = "host.docker.internal"

MONGODB_REPLICA_SET = "replica_set_0"
MONGODB_TOTAL_NODES = 3

MONGODB_NODE_NAMES = [f"mongodb-node-{i + 1}" for i in range(MONGODB_TOTAL_NODES)]
def vpn_fqdn(container_name):
    return f"{container_name}.{VPN_CLIENT_DOMAIN}"


MONGODB_NODE_CLIENT_HOSTS = [
    "127.0.0.1" if NETWORK_MODE == "local" else vpn_fqdn(name)
    for name in MONGODB_NODE_NAMES
]
MONGODB_NODE_PORTS = [27010 + (i + 1) for i in range(0, MONGODB_TOTAL_NODES)]

MONGODB_WORKDIR = "/data/db"

MONGO_INITDB_ROOT_USERNAME = "admin"
MONGO_INITDB_ROOT_PASSWORD = "admin"
MONGO_INITDB_DATABASE = "admin"

# %% [markdown]
# Prepares the local `mount/` working directory relative to the current path and computes MONGODB_LOCAL_CLUSTER_KEY_PATH, mirroring the layout used by the infrastructure notebook.

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = f"{os.path.join(os.path.relpath(Path.cwd()))}"
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")
MONGODB_LOCAL_CLUSTER_KEY_PATH = os.path.join(DOCKER_MOUNTDIR, "mongo-keyfile")

mount_path = Path(DOCKER_MOUNTDIR)
mount_path.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# ### Create session

# %%
from pymongo import MongoClient

nodes_ports = list(zip(MONGODB_NODE_CLIENT_HOSTS, MONGODB_NODE_PORTS))

if NETWORK_MODE == "local":
    primary_endpoint = None
    for host, port in nodes_ports:
        direct_uri = (
            f"mongodb://{MONGO_INITDB_ROOT_USERNAME}:{MONGO_INITDB_ROOT_PASSWORD}"
            f"@{host}:{port}/?authSource=admin"
        )
        try:
            with MongoClient(
                direct_uri,
                directConnection=True,
                serverSelectionTimeoutMS=5000,
            ) as node_client:
                hello = node_client.admin.command("hello")
                if hello.get("isWritablePrimary") or hello.get("ismaster"):
                    primary_endpoint = (host, port)
                    break
        except Exception:
            continue
    if primary_endpoint is None:
        raise TimeoutError("Could not find the MongoDB primary through published ports")
    primary_host, primary_port = primary_endpoint
    connection_string = (
        f"mongodb://{MONGO_INITDB_ROOT_USERNAME}:{MONGO_INITDB_ROOT_PASSWORD}"
        f"@{primary_host}:{primary_port}/"
        "?directConnection=true&authSource=admin&w=majority"
    )
else:
    seed_list = ",".join(f"{host}:{port}" for host, port in nodes_ports)
    connection_string = (
        f"mongodb://{MONGO_INITDB_ROOT_USERNAME}:{MONGO_INITDB_ROOT_PASSWORD}@"
        f"{seed_list}/"
        f"?replicaSet={MONGODB_REPLICA_SET}&authSource=admin&w=majority"
    )
print(f"Connection URL: {connection_string}")

client = MongoClient(connection_string, serverSelectionTimeoutMS=20000)
assert client.admin.command("ping")["ok"] == 1
replica_status = client.admin.command("replSetGetStatus")
assert len(replica_status["members"]) == MONGODB_TOTAL_NODES
assert sum(member["stateStr"] == "PRIMARY" for member in replica_status["members"]) == 1
assert sum(member["stateStr"] == "SECONDARY" for member in replica_status["members"]) == 2

db = client["db"]
users_collection = db["users"]
users_collection.drop()

# %% [markdown]
# ### Insert

# %%
from faker import Faker

fake = Faker()

# %% [markdown]
# Builds a batch of 10000 realistic user documents with Faker and random (nested profile, tags, login_count, last_login), inserts them into `users_collection` with `insert_many`, and asserts the resulting document count matches the batch size.

# %%
# # %%timeit -n 2 -r 2
# -n 1: run only 2 loop
# -r 1: repeat only 2 time

import random

print("Generating batch...")

users_batch = [
    {
        "name": (
            fake.unique.name() if random.random() > 0.5 else fake.unique.name().upper()
        ),
        "email": fake.ascii_free_email(),
        "profile": {
            "job": fake.job(),
            "company": fake.company(),
            "location": {
                "lat": float(fake.latitude()),
                "lng": float(fake.longitude()),
            },
        },
        "tags": [fake.word() for _ in range(random.randint(2, 5))],
        "login_count": random.randint(1, 1000),
        "last_login": fake.date_time_this_year().isoformat(),
        "active": fake.boolean(chance_of_getting_true=75),
    }
    for _ in range(10000)
]
print("Inserting batch...")
users_collection.insert_many(users_batch)
assert users_collection.count_documents({}) == len(users_batch)

# %% [markdown]
# ### Query

# %%
query = {"active": True, "login_count": {"$gt": 500}}
results = users_collection.find(query)
print(f"Found {users_collection.count_documents(query)} highly active users.")

# %% [markdown]
# Runs a projected `find` for documents tagged `work`, returning only name, email, and profile.job while excluding _id, limited to the first 100 matches.

# %%
projection = {"name": 1, "email": 1, "profile.job": 1, "_id": 0}
cursor = users_collection.find({"tags": "work"}, projection).limit(100)
for user in cursor:
    print(user)

# %% [markdown]
# Executes a five-stage MongoDB aggregation pipeline ($match, $group, $sort, $project, $limit) that averages `login_count` per `profile.job` and returns the top 100 professions by average logins.

# %%
pipeline = [
    {"$match": {"active": True}},  # Stage 1: Filter only active users
    {  # Stage 2: Group by the nested 'job' field
        "$group": {
            "_id": "$profile.job",
            "avg_logins": {"$avg": "$login_count"},
            "user_count": {"$sum": 1},
        }
    },
    {"$sort": {"avg_logins": -1}},  # Stage 3: Sort by average logins descending
    {
        "$project": {
            "_id": 0,  # Hide the original _id
            "job_title": "$_id",  # Rename _id to job_title
            "stats": {  # Create a nested object for stats
                "average": "$avg_logins",
                "total_users": "$user_count",
            },
        }
    },
    {"$limit": 100},  # Stage 4: Limit to top 100 most active professions
]
results = list(users_collection.aggregate(pipeline))
for res in results:
    print(res)

# %% [markdown]
# Counts users located in the Northern Hemisphere by querying the nested `profile.location.lat` field for values greater than zero.

# %%
northern_users = users_collection.count_documents({"profile.location.lat": {"$gt": 0}})
print(f"Users in Northern Hemisphere: {northern_users}")

# %% [markdown]
# Demonstrates collation-aware sorting: sorts user names ascending with locale `en` and strength 2 so ordering is case-insensitive rather than the default byte-wise Z-A-a-z order.

# %%
# Standard Sort (Z-A-a-z) vs. Collation Sort (A-a-B-b...)
cursor = (
    users_collection.find({})
    .sort("name", 1)
    .collation({"locale": "en", "strength": 2})
    .limit(100)
)

for user in cursor:
    print(user["name"])

# %% [markdown]
# ### Update

# %%
# 1. Get a single user to test with
target_user = users_collection.find_one({"active": True})
user_id = target_user["_id"]
initial_logins = target_user.get("login_count", 0)

print(f"User: {target_user['name']}")
print(f"Initial login count: {initial_logins}")

# 2. Increment the login counter for JUST this user
users_collection.update_one({"_id": user_id}, {"$inc": {"login_count": 1}})

# 3. Query again to see the change
updated_user = users_collection.find_one({"_id": user_id})
new_logins = updated_user.get("login_count", 0)

print(f"Updated login count: {new_logins}")
print(f"Change confirmed: {new_logins == initial_logins + 1}")
assert new_logins == initial_logins + 1

# %% [markdown]
# Uses `find_one_and_update` with `ReturnDocument.AFTER` to increment login_count atomically while returning the updated document in a single round trip.

# %%
from pymongo import ReturnDocument

# This performs the update and returns the NEW version of the document immediately
updated_doc = users_collection.find_one_and_update(
    {"_id": user_id}, {"$inc": {"login_count": 1}}, return_document=ReturnDocument.AFTER
)

print(f"New count from single-step operation: {updated_doc['login_count']}")

# %% [markdown]
# Performs a bulk `update_many` that flags every document whose profile.job matches the case-insensitive regex for engineer with is_technical set to true, reporting result.modified_count.

# %%
query = {"profile.job": {"$regex": ".*engineer.*", "$options": "i"}}
update = {"$set": {"is_technical": True}}
result = users_collection.update_many(query, update)
print(f"Updated {result.modified_count} engineers.")

# %% [markdown]
# Deactivates the account with email example@user.com via `update_one`, setting active to false if that document exists.

# %%
query = {"email": "example@user.com"}
new_values = {"$set": {"active": False}}
users_collection.update_one(query, new_values)

# %% [markdown]
# ### Delete

# %%
delete_result = users_collection.delete_many({})
assert delete_result.deleted_count == len(users_batch)
print(f"Deleted {delete_result.deleted_count} documents.")

# %% [markdown]
# Removes the `users` collection entirely from `db`, complementing the earlier document-level `delete_many` with collection-level cleanup.

# %%
db.drop_collection(users_collection)
print("Deleted users collection.")

# %% [markdown]
# ### Explain

# %%
import random
import pprint

db = client["universidad"]
students_collection = db["estudiantes"]

# 1. Limpiar y Poblar
print("Generando datos...")
students_collection.drop()
students = [
    {"nombre": f"{fake.name()}", "promedio": round(random.uniform(5, 10), 2)}
    for i in range(100000)
]
students_collection.insert_many(students)

# 2. Análisis sin índice
print("\n--- Find sin índice ---")
explain_find_no_idx = students_collection.find({"promedio": {"$gt": 9.9}}).explain()
pprint.pprint(explain_find_no_idx)
# stats_no_idx = explain_find_no_idx.get("executionStats", {})
# pprint.pprint(
#     {
#         "Stage": stats_no_idx.get("executionStages", {}).get("stage"),
#         "Docs Examinados": stats_no_idx.get("totalDocsExamined"),
#         "Execution Millis": stats_no_idx.get("executionTimeMillis"),
#     }
# )

# 3. Crear Índice
students_collection.create_index([("promedio", 1)])

# 4. Análisis con índice
print("\n--- Find con índice ---")
explain_find_idx = students_collection.find({"promedio": {"$gt": 9.9}}).explain()
pprint.pprint(explain_find_idx)

def contains_stage(value, stage):
    if isinstance(value, dict):
        return value.get("stage") == stage or any(
            contains_stage(item, stage) for item in value.values()
        )
    if isinstance(value, list):
        return any(contains_stage(item, stage) for item in value)
    return False

assert contains_stage(explain_find_no_idx["queryPlanner"]["winningPlan"], "COLLSCAN")
assert contains_stage(explain_find_idx["queryPlanner"]["winningPlan"], "IXSCAN")
# stats_idx = explain_find_idx.get("executionStats", {})
# Nota: Cuando hay índice, el 'stage' suele estar dentro de 'inputStage'
# input_stage = stats_idx.get("executionStages", {}).get("inputStage", {})
# pprint.pprint(
#     {
#         "Stage": "IXSCAN + FETCH",
#         "Docs Examinados": stats_idx.get("totalDocsExamined"),
#         "Execution Millis": stats_idx.get("executionTimeMillis"),
#     }
# )

# %% [markdown]
# Shows the alternative explain form by wrapping a find on `estudiantes` in a database command with verbosity executionStats, printing the resulting executionStats, then dropping the universidad database and closing the client.

# %%
# Otra forma es definiendo un comando con explain que envuelve al find
query_explain = {
    "explain": {"find": "estudiantes", "filter": {"promedio": {"$gt": 9.5}}},
    "verbosity": "executionStats",
}

# Ejecutamos el comando directamente en la base de datos
stats = db.command(query_explain)

pprint.pprint(stats.get("executionStats", {}))
client.drop_database("universidad")
client.close()
