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
DOCKER_INTERNAL_HOST = "host.docker.internal"

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

LOCALHOST_WORKDIR = f"{os.path.join(os.path.relpath(Path.cwd()))}"
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")
MONGODB_LOCAL_CLUSTER_KEY_PATH = os.path.join(DOCKER_MOUNTDIR, "mongo-keyfile")

mount_path = Path(DOCKER_MOUNTDIR)
mount_path.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# ### Create session

# %%
from pymongo import MongoClient

nodes_ports = [
    f"{MONGODB_NODE_HOSTNAMES[i]}:{MONGODB_NODE_PORTS[i]}"
    for i in range(MONGODB_TOTAL_NODES)
]
connection_string = (
    f"mongodb://{MONGO_INITDB_ROOT_USERNAME}:{MONGO_INITDB_ROOT_PASSWORD}@"
    f"{','.join(nodes_ports)}/"
    f"?replicaSet={MONGODB_REPLICA_SET}&authSource=admin&w=majority"
)
print(f"Connection URL: {connection_string}")

client = MongoClient(connection_string, serverSelectionTimeoutMS=20000)
assert client.admin.command("ping")["ok"] == 1
replica_status = client.admin.command("replSetGetStatus")
assert len(replica_status["members"]) == MONGODB_TOTAL_NODES
assert sum(member["stateStr"] == "PRIMARY" for member in replica_status["members"]) == 1

db = client["db"]
users_collection = db["users"]
users_collection.drop()

# %% [markdown]
# ### Insert

# %%
from faker import Faker

fake = Faker()

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

# %%
projection = {"name": 1, "email": 1, "profile.job": 1, "_id": 0}
cursor = users_collection.find({"tags": "work"}, projection).limit(100)
for user in cursor:
    print(user)

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

# %%
northern_users = users_collection.count_documents({"profile.location.lat": {"$gt": 0}})
print(f"Users in Northern Hemisphere: {northern_users}")

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

# %%
from pymongo import ReturnDocument

# This performs the update and returns the NEW version of the document immediately
updated_doc = users_collection.find_one_and_update(
    {"_id": user_id}, {"$inc": {"login_count": 1}}, return_document=ReturnDocument.AFTER
)

print(f"New count from single-step operation: {updated_doc['login_count']}")

# %%
query = {"profile.job": {"$regex": ".*engineer.*", "$options": "i"}}
update = {"$set": {"is_technical": True}}
result = users_collection.update_many(query, update)
print(f"Updated {result.modified_count} engineers.")

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
