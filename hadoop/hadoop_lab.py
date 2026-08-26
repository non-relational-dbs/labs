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
#

# %% tags=["parameters"]
# Local mode is the portable default; VPN mode publishes services on this host.
NETWORK_MODE = "local"
VPN_HOST_IP = "10.15.20.100"
VPN_DNS_IP = "10.15.20.1"
VPN_DOMAIN = "vpn.itam.mx"
VPN_CLIENT_ALIAS = "mavasbel"

# %%
NETWORK_MODE = NETWORK_MODE.strip().lower()
VPN_CLIENT_ALIAS = VPN_CLIENT_ALIAS.strip().lower()
if NETWORK_MODE not in {"local", "vpn"}:
    raise ValueError("NETWORK_MODE must be 'local' or 'vpn'")
if NETWORK_MODE == "vpn" and not VPN_HOST_IP.startswith("10.15.20."):
    raise ValueError("VPN_HOST_IP must be in the 10.15.20.* subnet")
if (
    not VPN_CLIENT_ALIAS
    or VPN_CLIENT_ALIAS.startswith("-")
    or VPN_CLIENT_ALIAS.endswith("-")
    or not VPN_CLIENT_ALIAS.isascii()
    or not VPN_CLIENT_ALIAS.replace("-", "").isalnum()
):
    raise ValueError(
        "VPN_CLIENT_ALIAS must contain only lowercase letters, digits, and internal hyphens"
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
MODULE_DIR = LABS_ROOT / "hadoop"
os.chdir(MODULE_DIR)
print(f"Working directory: {MODULE_DIR}")

# %%
HADOOP_START_FROM_SCRATCH = False
HADOOP_DATA_RECORDS = 100_000
DOCKER_INTERNAL_HOST = "host.docker.internal"

HADOOP_NAMENODE_HOSTNAME = "namenode"
HADOOP_NAMENODE_CLIENT_HOST = (
    "127.0.0.1"
    if NETWORK_MODE == "local"
    else f"{HADOOP_NAMENODE_HOSTNAME}.{VPN_CLIENT_DOMAIN}"
)
HADOOP_NAMENODE_IP = HADOOP_NAMENODE_CLIENT_HOST
HADOOP_NAMENODE_PORT = 8020
HADOOP_NAMENODE_WEBUI_PORT = 9870

HADOOP_RESOURCEMANAGER_HOSTNAME = "resourcemanager"
HADOOP_RESOURCEMANAGER_CLIENT_HOST = (
    "127.0.0.1"
    if NETWORK_MODE == "local"
    else f"{HADOOP_RESOURCEMANAGER_HOSTNAME}.{VPN_CLIENT_DOMAIN}"
)
HADOOP_RESOURCEMANAGER_IP = HADOOP_RESOURCEMANAGER_CLIENT_HOST
HADOOP_RESOURCEMANAGER_WEBUI_PORT = 8088
HADOOP_RESOURCEMANAGER_RPC_APP_MANAGER_PORT = 8032
HADOOP_RESOURCEMANAGER_TRACKER_PORT = 8031
HADOOP_RESOURCEMANAGER_SCHEDULER_PORT = 8030
HADOOP_RESOURCEMANAGER_ADMIN_PORT = 8033

HADOOP_MAPRED_JOB_HISTORY_PORT = 10020
HADOOP_MAPRED_LOG_SERVER_PORT = 19888

HADOOP_REPLICATION = 2
HADOOP_NUM_WORKERS = 2

HADOOP_DATANODE_NAMES = [f"datanode-{i+1}" for i in range(HADOOP_NUM_WORKERS)]
HADOOP_DATANODE_HOSTNAMES = HADOOP_DATANODE_NAMES
HADOOP_DATANODE_IPS = [
    f"{name}.{VPN_CLIENT_DOMAIN}" if NETWORK_MODE == "vpn" else "127.0.0.1"
    for name in HADOOP_DATANODE_NAMES
]
HADOOP_DATANODE_WEBUI_PORTS = [9864 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]
HADOOP_DATANODE_TRANSFER_PORTS = [9866 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]
HADOOP_DATANODE_IPC_PORTS = [6867 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]

HADOOP_NODEMANAGER_NAMES = [f"nodemanager-{i+1}" for i in range(HADOOP_NUM_WORKERS)]
HADOOP_NODEMANAGER_HOSTNAMES = HADOOP_NODEMANAGER_NAMES
HADOOP_NODEMANAGER_IPS = [
    f"{name}.{VPN_CLIENT_DOMAIN}" if NETWORK_MODE == "vpn" else "127.0.0.1"
    for name in HADOOP_NODEMANAGER_NAMES
]
HADOOP_NODEMANAGER_WEBUI_PORTS = [8050 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]
HADOOP_NODEMANAGER_RPC_PORTS = [8051 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]

HADOOP_WORKDIR = "/opt/hadoop/work-dir"
HADOOP_NAMENODE_NAMEDIR = "/opt/hadoop/dfs/name"
HADOOP_DATANODE_DATADIR = "/opt/hadoop/dfs/data"

HADOOP_HDFS_DATADIR = "/opt/hadoop/work-dir"

# %%
import json
from urllib.request import urlopen

with urlopen(
    f"http://{HADOOP_NAMENODE_CLIENT_HOST}:{HADOOP_NAMENODE_WEBUI_PORT}/jmx"
    "?qry=Hadoop:service=NameNode,name=NameNodeInfo",
    timeout=15,
) as response:
    namenode_jmx = json.load(response)
assert namenode_jmx["beans"], namenode_jmx

with urlopen(
    f"http://{HADOOP_RESOURCEMANAGER_CLIENT_HOST}:8088/ws/v1/cluster/info",
    timeout=15,
) as response:
    yarn_info = json.load(response)["clusterInfo"]
assert yarn_info["state"] == "STARTED", yarn_info
print(
    f"Validated {NETWORK_MODE} data path to NameNode and ResourceManager at "
    f"{HADOOP_NAMENODE_CLIENT_HOST}"
)

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = f"{os.path.join(os.path.relpath(Path.cwd()))}"
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")

Path(DOCKER_MOUNTDIR).mkdir(parents=True, exist_ok=True)

import requests

namenode_jmx = requests.get(
    f"http://{HADOOP_NAMENODE_CLIENT_HOST}:{HADOOP_NAMENODE_WEBUI_PORT}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo",
    timeout=30,
)
namenode_jmx.raise_for_status()
assert namenode_jmx.json()["beans"], namenode_jmx.text
yarn_metrics = requests.get(
    f"http://{HADOOP_RESOURCEMANAGER_CLIENT_HOST}:{HADOOP_RESOURCEMANAGER_WEBUI_PORT}/ws/v1/cluster/metrics",
    timeout=30,
)
yarn_metrics.raise_for_status()
metrics = yarn_metrics.json()["clusterMetrics"]
assert metrics["activeNodes"] == HADOOP_NUM_WORKERS, metrics

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
import subprocess

mapreduce_job = subprocess.run(
    [
        "docker", "exec", "resourcemanager", "yarn", "jar",
        "/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar",
        "-D", "mapred.reduce.tasks=2",
        "-D", "mapreduce.map.memory.mb=1024",
        "-D", "mapreduce.reduce.memory.mb=1024",
        "-files", f"{HADOOP_WORKDIR}/mapper.py,{HADOOP_WORKDIR}/reducer.py",
        "-mapper", "python mapper.py",
        "-reducer", "python reducer.py",
        "-input", f"{HADOOP_HDFS_DATADIR}/input/data.csv",
        "-output", f"{HADOOP_HDFS_DATADIR}/output",
    ],
    capture_output=True,
    text=True,
    timeout=300,
)
print(mapreduce_job.stdout)
assert mapreduce_job.returncode == 0, mapreduce_job.stderr[-4000:]

# 3. Show output file
# !docker exec namenode hdfs dfs -ls {HADOOP_HDFS_DATADIR}/output

# %%
# 4. Merge and sort output
# !docker exec namenode hdfs dfs -getmerge {HADOOP_HDFS_DATADIR}/output {HADOOP_WORKDIR}/output.csv
# !docker exec namenode bash -c "cat {HADOOP_WORKDIR}/output.csv | sort > {HADOOP_WORKDIR}/output_sorted.csv"

# %%
import subprocess

output = subprocess.run(
    ["docker", "exec", "namenode", "cat", f"{HADOOP_WORKDIR}/output_sorted.csv"],
    check=True,
    capture_output=True,
    text=True,
).stdout.strip()
assert output, "MapReduce output is empty"
assert "\t" in output, output[:200]
print(f"Validated MapReduce output with {len(output.splitlines())} categories")
