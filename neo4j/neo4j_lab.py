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
MODULE_DIR = LABS_ROOT / "neo4j"
os.chdir(MODULE_DIR)
print(f"Working directory: {MODULE_DIR}")

# %%
NEO4J_START_FROM_SCRATCH = False

# NEO4J_WORKDIR = "/var/lib/neo4j"
NEO4J_PORT = 7687
NEO4J_WEBUI_PORT = 7474

NEO4J_INIT_USER = "neo4j"
NEO4J_INIT_PASSWORD = "password"

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = f"{os.path.join(os.path.relpath(Path.cwd()))}"
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")

mount_path = Path(DOCKER_MOUNTDIR)
mount_path.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# ### Create session

# %%
from neo4j import GraphDatabase

n4j_uri = f"bolt://localhost:{NEO4J_PORT}"
n4j_auth = (NEO4J_INIT_USER, NEO4J_INIT_PASSWORD)

driver = GraphDatabase.driver(n4j_uri, auth=n4j_auth)
session = driver.session()

# %% [markdown]
# ### Insert nodes

# %%
from faker import Faker

fake = Faker()

# %%
total_persons = 10

with session.begin_transaction() as transaction:
    transaction.run(
        """
        UNWIND $batch AS user
        MERGE (p:Person {id: user.id})
        SET p.name = user.name, 
            p.email = user.email, 
            p.job = user.job, 
            p.city = user.city,
            p.joined = date(user.joined)
        """,
        batch=[
            {
                "id": i,
                "name": fake.unique.first_name(),
                "email": fake.email(),
                "job": fake.job(),
                "city": fake.city(),
                "joined": fake.date_this_decade().isoformat(),
            }
            for i in range(total_persons)
        ],
    )
    print("Successfully inserted nodes")

# %% [markdown]
# ### Insert relationships

# %%
import random

total_relationships = 14

with session.begin_transaction() as transaction:
    user_ids = [
        record["id"] for record in transaction.run("MATCH (p:Person) RETURN p.id AS id")
    ]
    if len(user_ids) < 2:
        print("Not enough users to create friendships.")
    else:
        relationships = set()
        relationships_data = []
        for _ in range(total_relationships):
            ids = tuple(sorted(random.sample(user_ids, 2)))
            relationships.add(ids)
        for relationship in relationships:
            relationships_data.append(
                {
                    "id_a": relationship[0],
                    "id_b": relationship[1],
                    "since": random.randint(2010, 2026),
                    "str_ab": round(random.uniform(0.1, 1.0), 2),
                    "str_ba": round(random.uniform(0.1, 1.0), 2),
                }
            )
        transaction.run(
            """
            UNWIND $data AS row
            MATCH (a:Person {id: row.id_a})
            MATCH (b:Person {id: row.id_b})
            MERGE (a)-[r1:knows]->(b)
            SET r1.since = row.since,
                r1.strength = row.str_ab
            MERGE (b)-[r2:knows]->(a)
            SET r2.since = row.since,
                r2.strength = row.str_ba
            """,
            data=relationships_data,
        )
        print("Successfully added relationships")

# %% [markdown]
# ### Delete nodes

# %%
with session.begin_transaction() as transaction:

    # transaction.run("MATCH (a:Person)<-[r:knows]-(b:Person) DELETE r")
    transaction.run("MATCH (p:Person) DETACH DELETE p")

    print("Successfully deleted persons")
