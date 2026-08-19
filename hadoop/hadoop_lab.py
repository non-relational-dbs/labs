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
#

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
MODULE_DIR = LABS_ROOT / "hadoop"
os.chdir(MODULE_DIR)
print(f"Working directory: {MODULE_DIR}")

# %%
HADOOP_START_FROM_SCRATCH = False
HADOOP_DATA_RECORDS = 100_000
HADOOP_VPN_DOMAIN = "mavasbel.vpn.itam.mx"
DOCKER_INTERNAL_HOST = "host.docker.internal"
DOCKER_DNS = ["10.15.20.1"]

HADOOP_NAMENODE_HOSTNAME = f"namenode.{HADOOP_VPN_DOMAIN}"
HADOOP_NAMENODE_IP = "10.15.20.2"
HADOOP_NAMENODE_PORT = 8020
HADOOP_NAMENODE_WEBUI_PORT = 9870

HADOOP_RESOURCEMANAGER_HOSTNAME = f"resourcemanager.{HADOOP_VPN_DOMAIN}"
HADOOP_RESOURCEMANAGER_IP = "10.15.20.2"
HADOOP_RESOURCEMANAGER_WEBUI_PORT = 8088
HADOOP_RESOURCEMANAGER_RPC_APP_MANAGER_PORT = 8032
HADOOP_RESOURCEMANAGER_TRACKER_PORT = 8031
HADOOP_RESOURCEMANAGER_SCHEDULER_PORT = 8030
HADOOP_RESOURCEMANAGER_ADMIN_PORT = 8033

HADOOP_MAPRED_JOB_HISTORY_PORT = 10020
HADOOP_MAPRED_LOG_SERVER_PORT = 19888

HADOOP_REPLICATION = 2
HADOOP_NUM_WORKERS = 2

HADOOP_DATANODE_IPS = ["10.15.20.2"] * HADOOP_REPLICATION
HADOOP_DATANODE_NAMES = [f"datanode-{i+1}" for i in range(HADOOP_NUM_WORKERS)]
HADOOP_DATANODE_HOSTNAMES = [
    f"{HADOOP_DATANODE_NAMES[i]}.{HADOOP_VPN_DOMAIN}" for i in range(HADOOP_NUM_WORKERS)
]
HADOOP_DATANODE_WEBUI_PORTS = [9864 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]
HADOOP_DATANODE_TRANSFER_PORTS = [9866 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]
HADOOP_DATANODE_IPC_PORTS = [6867 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]

HADOOP_NODEMANAGER_IPS = ["10.15.20.2"] * HADOOP_NUM_WORKERS
HADOOP_NODEMANAGER_NAMES = [f"nodemanager-{i+1}" for i in range(HADOOP_NUM_WORKERS)]
HADOOP_NODEMANAGER_HOSTNAMES = [
    f"{HADOOP_NODEMANAGER_NAMES[i]}.{HADOOP_VPN_DOMAIN}"
    for i in range(HADOOP_NUM_WORKERS)
]
HADOOP_NODEMANAGER_WEBUI_PORTS = [8050 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]
HADOOP_NODEMANAGER_RPC_PORTS = [8051 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]

HADOOP_WORKDIR = "/opt/hadoop/work-dir"
HADOOP_NAMENODE_NAMEDIR = "/opt/hadoop/dfs/name"
HADOOP_DATANODE_DATADIR = "/opt/hadoop/dfs/data"

HADOOP_HDFS_DATADIR = "/opt/hadoop/work-dir"

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = f"{os.path.join(os.path.relpath(Path.cwd()))}"
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")

Path(DOCKER_MOUNTDIR).mkdir(parents=True, exist_ok=True)

# %%
import csv
import random

from faker import Faker
from tqdm import tqdm


def generate_data(path, records):
    Faker.seed(2026)
    random.seed(2026)
    fake = Faker()
    print(f"Generating {records:,} deterministic records...")
    with open(path, "w", newline="", encoding="utf-8") as data_file:
        writer = csv.writer(data_file)
        writer.writerow(["ts", "id", "user", "amount", "category", "country"])
        for _ in tqdm(range(records), desc="Progress", unit="rows"):
            writer.writerow(
                [
                    fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S"),
                    fake.uuid4(),
                    fake.name(),
                    round(random.uniform(10.50, 10_000.00), 2),
                    fake.bs(),
                    fake.country(),
                ]
            )


dataset_source_path = os.path.join(LOCALHOST_WORKDIR, "data.csv")
if HADOOP_START_FROM_SCRATCH or not os.path.exists(dataset_source_path):
    generate_data(dataset_source_path, HADOOP_DATA_RECORDS)
    print(f"Created {dataset_source_path}")


# %%
import shutil

dataset_source_path = os.path.join(LOCALHOST_WORKDIR, "data.csv")
dataset_dest_path = os.path.join(DOCKER_MOUNTDIR, "namenode", "work-dir", "data.csv")
if HADOOP_START_FROM_SCRATCH or not os.path.exists(dataset_dest_path):
    shutil.copy(dataset_source_path, dataset_dest_path)

# %% [markdown]
# ### Create HDFS input directory and clear previous output
#

# %%
# !docker exec namenode hdfs dfs -mkdir -p {HADOOP_HDFS_DATADIR}/input
# !docker exec namenode hdfs dfs -rm -r -f {HADOOP_HDFS_DATADIR}/output
print("HDFS environment initialized.")

# %% [markdown]
# ### Upload from the container's mount point to HDFS
#

# %%
# !docker exec namenode hdfs dfs -put -f {HADOOP_WORKDIR}/data.csv {HADOOP_HDFS_DATADIR}/input/
# !docker exec namenode hdfs dfs -ls {HADOOP_HDFS_DATADIR}/input

# %% [markdown]
# ### Check block locations and replication across datanodes
#

# %%
# !docker exec namenode hdfs fsck {HADOOP_HDFS_DATADIR}/input/data.csv -files -blocks -locations

# %% [markdown]
# ### Generate mapper and reducer scripts
#

# %%
import os

mapper_file_contents = """#!/usr/bin/env python
import sys

# Standard for Hadoop Streaming: read from STDIN
for line in sys.stdin:
    line = line.strip()
    # Split the CSV line
    parts = line.split(',')
    
    # Check if we have enough columns and skip the header
    if len(parts) >= 4 and parts[0] != "ts":
        category = parts[4]
        amount = parts[3]
        
        # Output: category [TAB] amount
        # Hadoop will sort these by the key (category) before the Reducer sees them
        print("%s\\t%s" % (category, amount))
"""

with open(os.path.join(DOCKER_MOUNTDIR,"resourcemanager", "work-dir",'mapper.py'), 'w') as mapper_file:
    mapper_file.write(mapper_file_contents)
print("Mapper script created")


reducer_file_contents = """#!/usr/bin/env python
import sys

current_category = None
current_sum = 0.0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
        
    try:
        category, amount = line.split('\\t')
        amount = float(amount)
    except ValueError:
        continue

    # Logic: If the category changes, print the total for the previous one
    if current_category == category:
        current_sum += amount
    else:
        if current_category:
            print("%s\\t%.2f" % (current_category, current_sum))
        current_category = category
        current_sum = amount

# Don't forget the last category!
if current_category:
    print("%s\\t%.2f" % (current_category, current_sum))
"""

with open(os.path.join(DOCKER_MOUNTDIR,"resourcemanager", "work-dir",'reducer.py'), 'w') as reducer_file:
    reducer_file.write(reducer_file_contents)
print("Reducer script created")

# !docker exec resourcemanager ls -l {HADOOP_WORKDIR}

# %% [markdown]
# ### Count directly in namenode for validation
#

# %%
shutil.copy(
    os.path.join(DOCKER_MOUNTDIR, "resourcemanager", "work-dir", "mapper.py"),
    os.path.join(DOCKER_MOUNTDIR, "namenode", "work-dir", "mapper.py"),
)
shutil.copy(
    os.path.join(
        DOCKER_MOUNTDIR, "resourcemanager", "work-dir", "reducer.py"
    ),
    os.path.join(DOCKER_MOUNTDIR, "namenode", "work-dir", "reducer.py"),
)
# !docker exec namenode bash -c "cat {HADOOP_WORKDIR}/data.csv | python {HADOOP_WORKDIR}/mapper.py | sort | python {HADOOP_WORKDIR}/reducer.py"

# %% [markdown]
# # Hadoop map reduce
#

# %%
# 1. Ensure the output directory is clean
# !docker exec namenode hdfs dfs -rm -r -f {HADOOP_HDFS_DATADIR}/output

# 2. Submit the job from the ResourceManager to Nodemanagers
# !docker exec resourcemanager yarn jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar \
#     -D mapred.reduce.tasks=2 \
#     -D mapreduce.map.memory.mb=1024 \
#     -D mapreduce.reduce.memory.mb=1024 \
#     -files {HADOOP_WORKDIR}/mapper.py,{HADOOP_WORKDIR}/reducer.py \
#     -mapper "python mapper.py" \
#     -reducer "python reducer.py" \
#     -input {HADOOP_HDFS_DATADIR}/input/data.csv \
#     -output {HADOOP_HDFS_DATADIR}/output

# 3. Show output file
# !docker exec namenode hdfs dfs -ls {HADOOP_HDFS_DATADIR}/output

# %%
# 4. Merge and sort output
# !docker exec namenode hdfs dfs -getmerge {HADOOP_HDFS_DATADIR}/output {HADOOP_WORKDIR}/output.csv
# !docker exec namenode bash -c "cat {HADOOP_WORKDIR}/output.csv | sort > {HADOOP_WORKDIR}/output_sorted.csv"
