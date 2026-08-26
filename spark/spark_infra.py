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
START_FROM_SCRATCH = False

# %% [markdown]
# Validates the papermill-injected NETWORK_MODE and VPN_CLIENT_ALIAS values and derives VPN_CLIENT_DOMAIN as `<alias>.<VPN_DOMAIN>` for later VPN-mode host naming.

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

# %% [markdown]
# Locates the labs-setup root by walking parent directories for a pyproject.toml alongside the cassandra and mongodb module folders, then changes into the spark module directory.

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
MODULE_DIR = LABS_ROOT / "spark"
os.chdir(MODULE_DIR)
print(f"Working directory: {MODULE_DIR}")

# %% [markdown]
# Defines the Spark cluster naming and addressing: spark-master, three spark-worker containers, the spark-jupyter service with their Docker image tags, ports, per-mode compose/client hostnames via VPN_CLIENT_DOMAIN, and the shared-workspace path.

# %%
SPARK_START_FROM_SCRATCH = START_FROM_SCRATCH
DOCKER_INTERNAL_HOST = "host.docker.internal"
SPARK_LOCAL_HDFS_HOST = "namenode.lvh.me"
DOCKER_DNS = [VPN_DNS_IP] if NETWORK_MODE == "vpn" else []
HOST_BIND_IP = VPN_HOST_IP if NETWORK_MODE == "vpn" else "127.0.0.1"

SPARK_DOCKER_BASE = "spark:3.5.7-scala2.12-java17-python3-ubuntu"
SPARK_JUPYTER_LAB_DOCKER_TAG = "spark-jupyter:3.5.7-scala2.12-java17-python3-ubuntu"
SPARK_JOB_VENV_DOCKER_TAG = "spark-job-venv:3.5.7-scala2.12-java17-python3-ubuntu"
SPARK_JOB_VENV_BUILD_DIR = "/opt/spark/venv-build"
SPARK_EXECUTOR_ENV_DIR = "/opt/spark/executor-env"

SPARK_MASTER_NAME = "spark-master"
SPARK_MASTER_HOSTNAME = "spark-master-internal"
SPARK_MASTER_COMPOSE_HOST = (
    SPARK_MASTER_HOSTNAME
    if NETWORK_MODE == "local"
    else f"{SPARK_MASTER_NAME}.{VPN_CLIENT_DOMAIN}"
)
SPARK_MASTER_CLIENT_HOST = (
    "127.0.0.1"
    if NETWORK_MODE == "local"
    else f"{SPARK_MASTER_NAME}.{VPN_CLIENT_DOMAIN}"
)
SPARK_MASTER_WUBUI_PORT = 6080
SPARK_MASTER_PORT = 6077

SPARK_TOTAL_WORKERS = 3
SPARK_WORKER_NAMES = [f"spark-worker-{i+1}" for i in range(SPARK_TOTAL_WORKERS)]
SPARK_WORKER_HOSTNAMES = SPARK_WORKER_NAMES
SPARK_WORKER_COMPOSE_HOSTS = [
    name if NETWORK_MODE == "local" else f"{name}.{VPN_CLIENT_DOMAIN}"
    for name in SPARK_WORKER_NAMES
]
SPARK_WORKER_IPS = [
    f"{name}.{VPN_CLIENT_DOMAIN}" if NETWORK_MODE == "vpn" else "127.0.0.1"
    for name in SPARK_WORKER_NAMES
]
SPARK_WORKER_WEBUI_PORTS = [6080 + (i + 1) for i in range(SPARK_TOTAL_WORKERS)]

SPARK_WORKDIR = "/opt/spark/work-dir"

JUPYTER_LAB_NAME = "spark-jupyter"
JUPYTER_LAB_HOSTNAME = JUPYTER_LAB_NAME
JUPYTER_LAB_COMPOSE_HOST = (
    JUPYTER_LAB_HOSTNAME
    if NETWORK_MODE == "local"
    else f"{JUPYTER_LAB_NAME}.{VPN_CLIENT_DOMAIN}"
)
JUPYTER_LAB_CLIENT_HOST = (
    "127.0.0.1"
    if NETWORK_MODE == "local"
    else f"{JUPYTER_LAB_NAME}.{VPN_CLIENT_DOMAIN}"
)
JUPYTER_LAB_PORT = 6888
JUPYTER_LAB_MONITOR_PORT = 4040
JUPYTER_LAB_TOKEN = ""

SPARK_SHARED_WORKSPACE = "shared-workspace"
SPARK_SHARED_WORKSPACE_DIR = f"/opt/spark/{SPARK_SHARED_WORKSPACE}"

# %% [markdown]
# Creates the local bind mount directory (DOCKER_MOUNTDIR) that backs the Spark containers' persistent volumes.

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = str(MODULE_DIR.resolve())
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")

path = Path(LOCALHOST_WORKDIR)
path.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# # Stop spark-cluster.docker-compose.yml

# %%
import subprocess

if SPARK_START_FROM_SCRATCH:
    subprocess.run(
        ["docker", "compose", "-f", "spark-cluster.docker-compose.yml", "down", "-v"],
        check=False,
    )
else:
    print("Preserving existing containers and volumes")


# %% [markdown]
# Defines clear_bind_directory, which empties a bind mount through a throwaway busybox:1.36 container, and uses it to wipe the master, worker, and shared-workspace mounts when SPARK_START_FROM_SCRATCH is true.

# %%
def clear_bind_directory(path):
    target = Path(path).resolve()
    target.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "docker", "run", "--rm", "--mount",
            f"type=bind,source={target},target=/target",
            "busybox:1.36", "sh", "-c",
            "find /target -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +",
        ],
        check=True,
    )

if SPARK_START_FROM_SCRATCH :
    # shutil.rmtree(os.path.join(DOCKER_MOUNTDIR, SPARK_SHARED_WORKSPACE, "spark-warehouse"), ignore_errors=True)
    # shutil.rmtree(
    #     os.path.join(DOCKER_MOUNTDIR, SPARK_SHARED_WORKSPACE, "iceberg-warehouse"), ignore_errors=True
    # )
    clear_bind_directory(os.path.join(DOCKER_MOUNTDIR, SPARK_MASTER_NAME))
    for spark_worker_name in SPARK_WORKER_NAMES:
        clear_bind_directory(os.path.join(DOCKER_MOUNTDIR, spark_worker_name))
    clear_bind_directory(os.path.join(DOCKER_MOUNTDIR, SPARK_SHARED_WORKSPACE))
    
    Path(os.path.join(DOCKER_MOUNTDIR, JUPYTER_LAB_NAME)).mkdir(parents=True, exist_ok=True)

# %% [markdown]
# ### Build spark-jupyter

# %%
import os
from IPython.display import Markdown, display

dockerfile_spark_jupyter_python_packages = (
    "pyspark==3.5.7 delta-spark==3.2.0 jupyterlab pandas pyarrow"
)

dockerfile_spark_jupyter_name = "dockerfile.spark-jupyter"

# language=dockerfile
dockerfile_spark_jupyter_contents = f""" 

# Use the official Spark image as the base
FROM apache/{SPARK_DOCKER_BASE}

# Switch to root to install software
USER root

# Set the working directory
WORKDIR {SPARK_WORKDIR}

# Expose the Jupyter port
EXPOSE 8888

# Install Python dependencies
RUN apt-get update && apt-get install -y python3-venv
RUN python3 -m pip install --no-cache-dir {dockerfile_spark_jupyter_python_packages}

# Set the default command to launch Jupyter Lab
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=$$JUPYTER_LAB_TOKEN"]
"""

with open(
    os.path.join(LOCALHOST_WORKDIR, dockerfile_spark_jupyter_name), "w"
) as spark_compose_yaml_file:
    spark_compose_yaml_file.write(dockerfile_spark_jupyter_contents.strip())

print(
    f"Successfully created: '{os.path.relpath(os.path.join(LOCALHOST_WORKDIR,dockerfile_spark_jupyter_name))}'"
)
display(Markdown(f"```dockerfile\n{dockerfile_spark_jupyter_contents}\n```"))

# %% [markdown]
# Builds the spark-jupyter image from dockerfile.spark-jupyter, tagging it as SPARK_JUPYTER_LAB_DOCKER_TAG.

# %%
# !docker build -t {SPARK_JUPYTER_LAB_DOCKER_TAG} -f dockerfile.spark-jupyter .

# %% [markdown]
# ### Build spark-job-venv

# %%
import os
from IPython.display import Markdown, display

dockerfile_spark_job_venv_name = "dockerfile.spark-job-venv"
dockerfile_spark_job_venv_contents = f"""
# Use the previously generated spark-jupyter image as the base
FROM {SPARK_JUPYTER_LAB_DOCKER_TAG}

# Create virtual env for spark jobs
RUN mkdir -p {SPARK_JOB_VENV_BUILD_DIR} && \\
        cd {SPARK_JOB_VENV_BUILD_DIR} && \\
        python3 -m venv --copies spark_job_env && \\
        {SPARK_JOB_VENV_BUILD_DIR}/spark_job_env/bin/pip install venv-pack pandas pyarrow faker faker-commerce mimesis && \\
        {SPARK_JOB_VENV_BUILD_DIR}/spark_job_env/bin/venv-pack -p spark_job_env -o spark_job_env.tar.gz
"""

with open(os.path.join(LOCALHOST_WORKDIR, dockerfile_spark_job_venv_name), "w") as spark_compose_yaml_file:
    spark_compose_yaml_file.write(dockerfile_spark_job_venv_contents.strip())

print(f"Successfully created: '{os.path.relpath(os.path.join(LOCALHOST_WORKDIR,dockerfile_spark_jupyter_name))}'")
display(Markdown(f"```dockerfile\n{dockerfile_spark_job_venv_contents}\n```"))

# %% [markdown]
# Ensures the mount directory exists, then builds the spark-job-venv image, creates a throwaway container from it, copies the packed spark_job_env.tar.gz into the shared workspace, and removes the container.

# %%
from pathlib import Path
path = Path(DOCKER_MOUNTDIR)
path.mkdir(parents=True, exist_ok=True)

# !docker build -t {SPARK_JOB_VENV_DOCKER_TAG} -f dockerfile.spark-job-venv .
# !docker create --name spark-job-venv {SPARK_JOB_VENV_DOCKER_TAG}
# !docker cp spark-job-venv:{SPARK_JOB_VENV_BUILD_DIR}/spark_job_env.tar.gz "{DOCKER_MOUNTDIR}/{SPARK_SHARED_WORKSPACE}/spark_job_env.tar.gz"
# !docker rm spark-job-venv

# %% [markdown]
# # Build Spark images and create spark-cluster.docker-compose.yml

# %%
import os
import yaml
from IPython.display import Markdown, display

SPARK_VSCODE_SERVER_DIR = os.path.join(LOCALHOST_WORKDIR, "vscode_server")
SPARK_MOUNT_JARS = [
    f"{os.path.join(LOCALHOST_WORKDIR, 'jars')}:/opt/spark/course-jars:ro"
]

spark_compose_dict = {
    "name": "spark-cluster",
    "networks": {
        "spark-cluster": {"external": True, "name": "hadoop-network"},
    },
    "services": {
        SPARK_MASTER_NAME: {
            "image": f"apache/{SPARK_DOCKER_BASE}",
            "container_name": SPARK_MASTER_NAME,
            "user": "root",
            "command": f'bash -c "/opt/spark/bin/spark-class org.apache.spark.deploy.$$SPARK_MODE.$${{SPARK_MODE^}} --host {SPARK_MASTER_HOSTNAME} --port $$SPARK_MASTER_PORT --webui-port $$SPARK_MASTER_WEBUI_PORT"',
            "environment": [
                "PYSPARK_PYTHON=python3",
                "SPARK_MODE=master",
                f"SPARK_MASTER_PORT={SPARK_MASTER_PORT}",
                f"SPARK_MASTER_WEBUI_PORT={SPARK_MASTER_WUBUI_PORT}",
                "SPARK_DAEMON_MEMORY=1G",
            ],
            "volumes": [
                f"{os.path.join(DOCKER_MOUNTDIR,SPARK_SHARED_WORKSPACE)}:{SPARK_SHARED_WORKSPACE_DIR}",
                f"{os.path.join(DOCKER_MOUNTDIR,SPARK_MASTER_NAME)}:{SPARK_WORKDIR}"
            ]
            + SPARK_MOUNT_JARS,
            "networks": {
                "spark-cluster": {"aliases": [SPARK_MASTER_HOSTNAME]},
            },
            "hostname": SPARK_MASTER_COMPOSE_HOST,
            "ports": [
                f"{HOST_BIND_IP}:{SPARK_MASTER_WUBUI_PORT}:{SPARK_MASTER_WUBUI_PORT}",
                f"{HOST_BIND_IP}:{SPARK_MASTER_PORT}:{SPARK_MASTER_PORT}",
            ],
            "extra_hosts": [
                f"{DOCKER_INTERNAL_HOST}:host-gateway",
            ],
            "dns": DOCKER_DNS,
            "deploy": {"resources": {"limits": {"cpus": "2.0", "memory": "1G"}}},
            "healthcheck": {
                "test": [
                    "CMD",
                    "curl",
                    "-f",
                    f"http://{SPARK_MASTER_HOSTNAME}:{SPARK_MASTER_WUBUI_PORT}",
                ],
                "interval": "10s",
                "timeout": "10s",
                "retries": 10,
                "start_period": "10s",
            },
        },
        "spark-jupyter": {
            "image": SPARK_JUPYTER_LAB_DOCKER_TAG,
            "container_name": "spark-jupyter",
            "user": "root",
            "command": [
                "bash",
                "-c",
                " ".join(
                    [
                        "jupyter lab",
                        "--ip=0.0.0.0",
                        f"--port={JUPYTER_LAB_PORT}",
                        "--no-browser",
                        "--allow-root",
                        f"--NotebookApp.token='{JUPYTER_LAB_TOKEN}'",
                        "--NotebookApp.password=''",
                        "--NotebookApp.allow_origin='*'",
                        "--ServerApp.disable_check_xsrf=True",
                        f"--ServerApp.root_dir={SPARK_WORKDIR}",
                    ]
                ),
            ],
            "environment": [
                "PYSPARK_PYTHON=python3",
                # f"JUPYTER_LAB_PORT={JUPYTER_LAB_PORT}",
                # f"JUPYTER_LAB_TOKEN={JUPYTER_LAB_TOKEN}",
                "SPARK_EXECUTOR_MEMORY=1536M",
            ],
            "volumes": [
                f"{os.path.join(DOCKER_MOUNTDIR,SPARK_SHARED_WORKSPACE)}:{SPARK_SHARED_WORKSPACE_DIR}",
                f"{os.path.join(DOCKER_MOUNTDIR,JUPYTER_LAB_NAME)}:{SPARK_WORKDIR}",
                f"{SPARK_VSCODE_SERVER_DIR}:/root/.vscode-server",
            ]
            + SPARK_MOUNT_JARS,
            "networks": ["spark-cluster"],
            "hostname": JUPYTER_LAB_COMPOSE_HOST,
            "ports": [
                f"{HOST_BIND_IP}:{JUPYTER_LAB_PORT}:{JUPYTER_LAB_PORT}",
                f"{HOST_BIND_IP}:{JUPYTER_LAB_MONITOR_PORT}:{JUPYTER_LAB_MONITOR_PORT}",
            ],
            "extra_hosts": [
                f"{DOCKER_INTERNAL_HOST}:host-gateway",
            ],
            "dns": DOCKER_DNS,
            "deploy": {"resources": {"limits": {"cpus": "2.0", "memory": "1G"}}},
            "healthcheck": {
                "test": [
                    "CMD",
                    "curl",
                    "-f",
                    f"http://{JUPYTER_LAB_HOSTNAME}:{JUPYTER_LAB_PORT}",
                ],
                "interval": "10s",
                "timeout": "10s",
                "retries": 10,
                "start_period": "10s",
            },
        },
    },
}

for i in range(SPARK_TOTAL_WORKERS):

    spark_compose_dict["services"][SPARK_WORKER_NAMES[i]] = {
        "image": f"apache/{SPARK_DOCKER_BASE}",
        "container_name": SPARK_WORKER_NAMES[i],
        "user": "root",
        "command": f'bash -c "rm -rf {SPARK_EXECUTOR_ENV_DIR} && mkdir -p {SPARK_EXECUTOR_ENV_DIR} && tar -xzf {SPARK_SHARED_WORKSPACE_DIR}/spark_job_env.tar.gz -C {SPARK_EXECUTOR_ENV_DIR} && /opt/spark/bin/spark-class org.apache.spark.deploy.$$SPARK_MODE.$${{SPARK_MODE^}} $$SPARK_MASTER_URL --host {SPARK_WORKER_HOSTNAMES[i]} --webui-port $$SPARK_WORKER_WEBUI_PORT"',
        "environment": [
            "PYSPARK_PYTHON=python3",
            "SPARK_MODE=worker",
            "SPARK_WORKER_CORES=2",
            "SPARK_DAEMON_MEMORY=512M",
            "SPARK_WORKER_MEMORY=2048M",
            f"SPARK_WORKER_WEBUI_PORT={SPARK_WORKER_WEBUI_PORTS[i]}",
            f"SPARK_MASTER_URL=spark://{SPARK_MASTER_HOSTNAME}:{SPARK_MASTER_PORT}",
        ],
        "volumes": [
            f"{os.path.join(DOCKER_MOUNTDIR,SPARK_SHARED_WORKSPACE)}:{SPARK_SHARED_WORKSPACE_DIR}",
            f"{os.path.join(DOCKER_MOUNTDIR,SPARK_WORKER_NAMES[i])}:{SPARK_WORKDIR}"
        ]
        + SPARK_MOUNT_JARS,
        "networks": ["spark-cluster"],
        "hostname": SPARK_WORKER_COMPOSE_HOSTS[i],
        "ports": [
            f"{HOST_BIND_IP}:{SPARK_WORKER_WEBUI_PORTS[i]}:{SPARK_WORKER_WEBUI_PORTS[i]}"
        ],
        "extra_hosts": [
            f"{DOCKER_INTERNAL_HOST}:host-gateway",
        ],
        "dns": DOCKER_DNS,
        "deploy": {"resources": {"limits": {"cpus": "4.0", "memory": "1.5G"}}},
        "depends_on": {
            "spark-master": {"condition": "service_healthy"},
            "spark-jupyter": {"condition": "service_healthy"},
        }
        | {
            SPARK_WORKER_NAMES[j]: {"condition": "service_started"} for j in range(0, i)
        },
        "healthcheck": {
            "test": [
                "CMD",
                "curl",
                "-f",
                f"http://{SPARK_WORKER_HOSTNAMES[i]}:{SPARK_WORKER_WEBUI_PORTS[i]}",
            ],
            "interval": "10s",
            "timeout": "10s",
            "retries": 10,
            "start_period": "10s",
        },
    }

# 3. Dump the dictionary to a YAML file
spark_compose_yaml_path = os.path.join(
    LOCALHOST_WORKDIR, "spark-cluster.docker-compose.yml"
)
spark_compose_yaml_contents = yaml.dump(
    spark_compose_dict, default_flow_style=False, sort_keys=False, indent=4
)
with open(spark_compose_yaml_path, "w") as spark_compose_yaml_file:
    spark_compose_yaml_file.write(spark_compose_yaml_contents)

print(f"Successfully created: '{os.path.relpath(spark_compose_yaml_path)}'")
display(Markdown(f"```yaml\n{spark_compose_yaml_contents}\n```"))

# %% [markdown]
# Starts the full Spark cluster (spark-master, the three workers, and spark-jupyter) in detached mode with `docker compose up -d --wait`, blocking until every healthcheck passes.

# %%
# !docker compose -f spark-cluster.docker-compose.yml up -d --wait
