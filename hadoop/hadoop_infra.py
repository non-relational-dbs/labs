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
START_FROM_SCRATCH = False

# %% [markdown]
# Validates and normalizes the injected parameters: lowercases NETWORK_MODE and VPN_CLIENT_ALIAS, rejects unsupported network modes and malformed aliases, and derives VPN_CLIENT_DOMAIN from them.

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
# Walks up the directory tree from the current working directory to locate the labs-setup root (identified by pyproject.toml plus the cassandra and mongodb module folders), then changes into the hadoop module directory.

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

# %% [markdown]
# Defines the full Hadoop cluster topology: NameNode, ResourceManager, DataNode, and NodeManager hostnames, compose-network aliases, client IPs, RPC and Web UI ports, replication factor, worker counts, and HDFS storage directories for both local and VPN modes.

# %%
HADOOP_START_FROM_SCRATCH = START_FROM_SCRATCH
DOCKER_INTERNAL_HOST = "host.docker.internal"
DOCKER_DNS = [VPN_DNS_IP] if NETWORK_MODE == "vpn" else []
HOST_BIND_IP = VPN_HOST_IP if NETWORK_MODE == "vpn" else "127.0.0.1"

HADOOP_NAMENODE_HOSTNAME = "namenode"
HADOOP_NAMENODE_COMPOSE_HOST = (
    HADOOP_NAMENODE_HOSTNAME
    if NETWORK_MODE == "local"
    else f"{HADOOP_NAMENODE_HOSTNAME}.{VPN_CLIENT_DOMAIN}"
)
HADOOP_NAMENODE_IP = (
    f"{HADOOP_NAMENODE_HOSTNAME}.{VPN_CLIENT_DOMAIN}"
    if NETWORK_MODE == "vpn"
    else "127.0.0.1"
)
HADOOP_NAMENODE_PORT = 8020
HADOOP_NAMENODE_WEBUI_PORT = 9870
HADOOP_NAMENODE_DISTRIBUTED_HOST = (
    f"{HADOOP_NAMENODE_HOSTNAME}.{VPN_CLIENT_DOMAIN}"
    if NETWORK_MODE == "vpn"
    else f"{HADOOP_NAMENODE_HOSTNAME}.lvh.me"
)

HADOOP_RESOURCEMANAGER_HOSTNAME = "resourcemanager"
HADOOP_RESOURCEMANAGER_COMPOSE_HOST = (
    HADOOP_RESOURCEMANAGER_HOSTNAME
    if NETWORK_MODE == "local"
    else f"{HADOOP_RESOURCEMANAGER_HOSTNAME}.{VPN_CLIENT_DOMAIN}"
)
HADOOP_RESOURCEMANAGER_IP = (
    f"{HADOOP_RESOURCEMANAGER_HOSTNAME}.{VPN_CLIENT_DOMAIN}"
    if NETWORK_MODE == "vpn"
    else "127.0.0.1"
)
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
HADOOP_DATANODE_HOSTNAMES = [
    f"{name}.{VPN_CLIENT_DOMAIN}" if NETWORK_MODE == "vpn" else f"{name}.lvh.me"
    for name in HADOOP_DATANODE_NAMES
]
HADOOP_DATANODE_COMPOSE_HOSTS = [
    name if NETWORK_MODE == "local" else f"{name}.{VPN_CLIENT_DOMAIN}"
    for name in HADOOP_DATANODE_NAMES
]
HADOOP_DATANODE_IPS = [
    f"{name}.{VPN_CLIENT_DOMAIN}" if NETWORK_MODE == "vpn" else "127.0.0.1"
    for name in HADOOP_DATANODE_NAMES
]
HADOOP_DATANODE_WEBUI_PORTS = [9864 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]
HADOOP_DATANODE_TRANSFER_PORTS = [9866 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]
HADOOP_DATANODE_IPC_PORTS = [6867 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]

HADOOP_NODEMANAGER_NAMES = [f"nodemanager-{i+1}" for i in range(HADOOP_NUM_WORKERS)]
HADOOP_NODEMANAGER_HOSTNAMES = [
    HADOOP_NODEMANAGER_NAMES[i] for i in range(HADOOP_NUM_WORKERS)
]
HADOOP_NODEMANAGER_COMPOSE_HOSTS = [
    name if NETWORK_MODE == "local" else f"{name}.{VPN_CLIENT_DOMAIN}"
    for name in HADOOP_NODEMANAGER_NAMES
]
HADOOP_NODEMANAGER_IPS = [
    f"{name}.{VPN_CLIENT_DOMAIN}" if NETWORK_MODE == "vpn" else "127.0.0.1"
    for name in HADOOP_NODEMANAGER_NAMES
]
HADOOP_NODEMANAGER_WEBUI_PORTS = [8050 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]
HADOOP_NODEMANAGER_RPC_PORTS = [8051 + (i * 10) for i in range(HADOOP_NUM_WORKERS)]
HADOOP_NODEMANAGER_JOB_CLIENT_START_PORTS = [
    41000 + (i * 100) for i in range(HADOOP_NUM_WORKERS)
]
HADOOP_NODEMANAGER_JOB_CLIENT_END_PORTS = [
    41000 + ((i + 1) * 100) - 1 for i in range(HADOOP_NUM_WORKERS)
]
HADOOP_NODEMANAGER_JOB_LOCALIZER_PORTS = [8040 + i for i in range(HADOOP_NUM_WORKERS)]

HADOOP_WORKDIR = "/opt/hadoop/work-dir"
HADOOP_NAMENODE_NAMEDIR = "/opt/hadoop/dfs/name"
HADOOP_DATANODE_DATADIR = "/opt/hadoop/dfs/data"

HADOOP_HDFS_DATADIR = "/opt/hadoop/work-dir"

# %% [markdown]
# Creates the local bind-mount directory (DOCKER_MOUNTDIR) under the hadoop module folder that will hold persistent state for every Hadoop container.

# %%
import os
from pathlib import Path

LOCALHOST_WORKDIR = str(MODULE_DIR.resolve())
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")

Path(DOCKER_MOUNTDIR).mkdir(parents=True, exist_ok=True)

# %% [markdown]
# # Stop hadoop-cluster.docker-compose.yml
#

# %%
import subprocess

if HADOOP_START_FROM_SCRATCH:
    subprocess.run(
        ["docker", "compose", "-f", "hadoop-cluster.docker-compose.yml", "down", "-v"],
        check=False,
    )
else:
    print("Preserving existing containers and volumes")


# %% [markdown]
# # Delete prev mountpoints
#

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

if HADOOP_START_FROM_SCRATCH:
    clear_bind_directory(DOCKER_MOUNTDIR)

# %% [markdown]
# # Auxiliary function
#

# %%
import re


def extract_mb(val_str):
    """
    Extrae el valor numérico de un string de memoria y lo convierte siempre a Megabytes (MB).
    Soporta formatos mezclados como '512', '256M', '2G', '-Xmx2g', '-Xms512000k', etc.
    """
    if not val_str:
        return 0

    # Convierte a string por seguridad y busca un número seguido opcionalmente por una letra (K, M, G)
    # Ejemplo en "-Xmx2g": match.group(1) será "2" y match.group(2) será "g"
    match = re.search(r"(\d+)\s*([kKmMgG]?)", str(val_str))

    if not match:
        return 0

    value = int(match.group(1))
    unit = match.group(2).upper()  # Pasamos a mayúscula para simplificar

    # Hacemos la conversión a Megabytes según la unidad detectada
    if unit == "G":
        return value * 1024  # De Gigabytes a Megabytes
    elif unit == "K":
        return value // 1024  # De Kilobytes a Megabytes (división entera)
    else:
        # Si la unidad es 'M' o no tiene unidad (string vacío),
        # Hadoop y Docker asumen Megabytes por defecto.
        return value


# %% [markdown]
# # Create hadoop-cluster.docker-compose.yml
#

# %%
import os
import yaml
from IPython.display import Markdown, display

# Configs can be valited in the running containers with
# hdfs getconf -confKey {key}
# or printing
# Four principal servicios
# # cat $HADOOP_HOME/etc/hadoop/core-site.xml       # fs.defaultFS, trash, security, proxyuser
# # cat $HADOOP_HOME/etc/hadoop/hdfs-site.xml       # replication, block size, permissions, namenode dirs
# # cat $HADOOP_HOME/etc/hadoop/yarn-site.xml       # resourcemanager, nodemanager, memory
# # cat $HADOOP_HOME/etc/hadoop/mapred-site.xml     # framework, classpath, JVM opts
# Daemons env vars
# # cat $HADOOP_HOME/etc/hadoop/hadoop-env.sh       # JAVA_HOME, HADOOP_HEAPSIZE, HADOOP_LOG_DIR
# # cat $HADOOP_HOME/etc/hadoop/yarn-env.sh         # YARN_HEAPSIZE, YARN_LOG_DIR
# # cat $HADOOP_HOME/etc/hadoop/mapred-env.sh       # HADOOP_JOB_HISTORYSERVER_HEAPSIZE
# Workers / topology
# # cat $HADOOP_HOME/etc/hadoop/workers             # DataNodes/NodeManagers
# # cat $HADOOP_HOME/etc/hadoop/masters             # SecondaryNameNode
# Logging
# # cat $HADOOP_HOME/etc/hadoop/log4j.properties    # Log levels per component
# Scheduler capacity
# # cat $HADOOP_HOME/etc/hadoop/capacity-scheduler.xml
# All of them
# for f in $HADOOP_HOME/etc/hadoop/*.xml; do
#     echo ""; echo "════════ $f ════════"; cat $f
# done

env_content = {
    # ==========================================
    # VARIABLES DE ENTORNO Y HEAP (MEMORIA DEMONIOS)
    # ==========================================
    "HADOOP_HOME": "/opt/hadoop",
    "HADOOP_WORKDIR": HADOOP_WORKDIR,
    "HADOOP_HEAPSIZE_MAX": "512",
    "HDFS_NAMENODE_OPTS": "-Xmx512m",
    "HDFS_DATANODE_OPTS": "-Xmx512m",  # Heap estricto para el DataNode
    "YARN_HEAPSIZE": "512",
    # ==========================================
    # CORE-SITE.XML (CONFIGURACIONES GENERALES)
    # ==========================================
    "CORE-SITE.XML_fs.defaultFS": f"hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}",
    "CORE-SITE.XML_fs.trash.interval": "1440",  # Tiempo de vida de la papelera en minutos (1440 = 24h)
    "CORE-SITE.XML_fs.trash.checkpoint.interval": "60",  # Cada 60 minutos crea un nuevo checkpoint en .Trash
    "CORE-SITE.XML_hadoop.proxyuser.hive.hosts": "*",
    "CORE-SITE.XML_hadoop.proxyuser.hive.groups": "*",
    "CORE-SITE.XML_hadoop.security.authentication": "simple",  # Sin Kerberos
    # [NUEVOS PARÁMETROS CORE]
    "CORE-SITE.XML_hadoop.tmp.dir": "/opt/hadoop/tmp",  # Directorio temporal seguro (evita /tmp del SO)
    "CORE-SITE.XML_io.file.buffer.size": "131072",  # Búfer de I/O aumentado a 128KB para mejor rendimiento
    # ==========================================
    # HDFS-SITE.XML (NAMENODE Y DATANODES)
    # ==========================================
    "HDFS-SITE.XML_dfs.replication": HADOOP_REPLICATION,
    "HDFS-SITE.XML_dfs.permissions.enabled": "true",  # Aplica permisos POSIX
    "HDFS-SITE.XML_dfs.namenode.name.dir": HADOOP_NAMENODE_NAMEDIR,
    "HDFS-SITE.XML_dfs.namenode.name.dir.perm": "775",
    "HDFS-SITE.XML_dfs.namenode.datanode.registration.ip-hostname-check": "false",  # Para que no intente resolucion DNS inversa
    "HDFS-SITE.XML_dfs.datanode.data.dir": HADOOP_DATANODE_DATADIR,
    "HDFS-SITE.XML_dfs.datanode.use.datanode.hostname": "true",
    # [NUEVOS PARÁMETROS HDFS]
    "HDFS-SITE.XML_dfs.blocksize": "134217728",  # Tamaño de bloque en bytes (134217728 = 128 MB)
    "HDFS-SITE.XML_dfs.namenode.handler.count": "20",  # Número de hilos del NameNode para peticiones
    "HDFS-SITE.XML_dfs.datanode.failed.volumes.tolerated": "0",  # Discos que pueden fallar antes de apagar el nodo (0 es ideal para 1 solo disco)
    # ==========================================
    # YARN-SITE.XML (RESOURCEMANAGER Y NODEMANAGER)
    # ==========================================
    "YARN-SITE.XML_yarn.resourcemanager.hostname": HADOOP_RESOURCEMANAGER_HOSTNAME,
    "YARN-SITE.XML_yarn.resourcemanager.resource-tracker.address": f"{HADOOP_RESOURCEMANAGER_HOSTNAME}:{HADOOP_RESOURCEMANAGER_TRACKER_PORT}",
    "YARN-SITE.XML_yarn.nodemanager.aux-services": "mapreduce_shuffle",
    "YARN-SITE.XML_yarn.nodemanager.vmem-check-enabled": "true",
    "YARN-SITE.XML_yarn.nodemanager.resource.memory-mb": "1792",  # Memoria total que YARN puede repartir en un nodo
    "YARN-SITE.XML_yarn.scheduler.maximum-allocation-mb": "1536",  # Tamaño máximo de un solo contenedor
    "YARN-SITE.XML_yarn.scheduler.minimum-allocation-mb": "128",  # Tamaño mínimo de asignación
    "YARN-SITE.XML_yarn.log-aggregation-enable": "true",
    "YARN-SITE.XML_yarn.log.server.url": f"http://{HADOOP_RESOURCEMANAGER_HOSTNAME}:{HADOOP_MAPRED_LOG_SERVER_PORT}/jobhistory/logs",
    # [NUEVOS PARÁMETROS YARN]
    "YARN-SITE.XML_yarn.resourcemanager.scheduler.class": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler",  # Programador de colas
    "YARN-SITE.XML_yarn.nodemanager.resource.cpu-vcores": "2",  # Número de vCores que el nodo puede prestar (Ajustar según tu CPU)
    "YARN-SITE.XML_yarn.nodemanager.local-dirs": "/opt/hadoop/data/yarn/local",  # Dónde guardan los contenedores sus temporales
    # "YARN-SITE.XML_yarn.resourcemanager.nodes.include-path": "/opt/hadoop/etc/hadoop/includes.txt", # (Descomentar si usas archivos de allowlist)
    # "YARN-SITE.XML_yarn.resourcemanager.nodes.exclude-path": "/opt/hadoop/etc/hadoop/excludes.txt", # (Descomentar si usas archivos de blocklist)
    # ==========================================
    # MAPRED-SITE.XML (MAPREDUCE)
    # ==========================================
    "MAPRED-SITE.XML_mapreduce.framework.name": "yarn",
    "MAPRED-SITE.XML_mapreduce.jobhistory.address": f"{HADOOP_RESOURCEMANAGER_HOSTNAME}:{HADOOP_MAPRED_JOB_HISTORY_PORT}",
    "MAPRED-SITE.XML_mapreduce.jobhistory.webapp.address": f"{HADOOP_RESOURCEMANAGER_HOSTNAME}:{HADOOP_MAPRED_LOG_SERVER_PORT}",
    "MAPRED-SITE.XML_mapreduce.application.classpath": ":".join(
        [
            "/opt/hadoop/share/hadoop/mapreduce/*",
            "/opt/hadoop/share/hadoop/common/*",
            "/opt/hadoop/share/hadoop/common/lib/*",
            "/opt/hadoop/share/hadoop/hdfs/*",
            "/opt/hadoop/share/hadoop/hdfs/lib/*",
            "/opt/hadoop/share/hadoop/yarn/*",
            "/opt/hadoop/share/hadoop/yarn/lib/*",
        ]
    ),
}
# Path("envs").mkdir(parents=True, exist_ok=True)
# with open("envs/common.env", "w") as f:
#     for key, value in env_content.items():
#         f.write(f"{key}={value}\n")

# Definimos el margen de seguridad para Docker y SO (Overhead)
DOCKER_OVERHEAD_MB = 128

# Extraemos las variables del env_content
global_heap = extract_mb(env_content.get("HADOOP_HEAPSIZE_MAX", "512"))
namenode_heap = extract_mb(env_content.get("HDFS_NAMENODE_OPTS")) or global_heap
datanode_heap = extract_mb(env_content.get("HDFS_DATANODE_OPTS")) or global_heap
yarn_heap = extract_mb(env_content.get("YARN_HEAPSIZE", "512"))
# Memoria extra exclusiva del NodeManager (los contenedores)
yarn_nodemanager_vault = extract_mb(
    env_content.get("YARN-SITE.XML_yarn.nodemanager.resource.memory-mb", "2048")
)

# Calculamos los límites reales de Docker (como strings listos para el Compose)
# Limit = Memoria del Demonio + Overhead
NAMENODE_DOCKER_LIMIT = f"{namenode_heap + DOCKER_OVERHEAD_MB}M"
DATANODE_DOCKER_LIMIT = f"{datanode_heap + DOCKER_OVERHEAD_MB}M"
RESOURCEMANAGER_DOCKER_LIMIT = f"{yarn_heap + DOCKER_OVERHEAD_MB}M"
NODEMANAGER_DOCKER_LIMIT = f"{yarn_heap + yarn_nodemanager_vault + DOCKER_OVERHEAD_MB}M"  # El NodeManager es la suma (Memoria del Demonio + Memoria para Trabajos + Overhead)

hadoop_compose_file_name = "hadoop-cluster.docker-compose.yml"
namenode_ports = [
    f"{HOST_BIND_IP}:{HADOOP_NAMENODE_PORT}:{HADOOP_NAMENODE_PORT}",
    f"{HOST_BIND_IP}:{HADOOP_NAMENODE_WEBUI_PORT}:{HADOOP_NAMENODE_WEBUI_PORT}",
]

hadoop_compose_dict = {
    "name": "hadoop-cluster",
    "networks": {
        "hadoop-cluster": {"name": "hadoop-network", "driver": "bridge"},
        # "hadoop-namenode": {"driver": "bridge"},
        # "hadoop-datanodes": {"driver": "bridge"},
        # "hadoop-resourcemanager": {"driver": "bridge"},
        # "hadoop-nodemanagers": {"driver": "bridge"},
    },
    "services": {
        "namenode": {
            "image": "apache/hadoop:3.4.3",
            "container_name": "namenode",
            "command": [
                "bash",
                "-c",
                " ".join(
                    [
                        f"sudo chown -R $(id -u hadoop):$(id -g hadoop) {HADOOP_WORKDIR} &&",
                        f"sudo mkdir -p {HADOOP_NAMENODE_NAMEDIR} &&",
                        f"sudo chown -R $(id -u hadoop):$(id -g hadoop) {HADOOP_NAMENODE_NAMEDIR} &&",
                        f"if [ ! -d {HADOOP_NAMENODE_NAMEDIR}/current ]; then hdfs namenode -Ddfs.namenode.name.dir={HADOOP_NAMENODE_NAMEDIR} -format -force; fi &&",
                        "hdfs",
                        "namenode",
                        f"-Dfs.defaultFS=hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}",
                        # f"-Ddfs.replication={HADOOP_REPLICATION}",
                        # f"-Ddfs.namenode.name.dir={HADOOP_NAMENODE_NAMEDIR}",
                        # "-Ddfs.namenode.name.dir.perm=775",
                        # "-Ddfs.permissions.enabled=false",
                        # f"-Ddfs.namenode.rpc-address=0.0.0.0:{HADOOP_NAMENODE_PORT}",
                        # f"-Ddfs.namenode.http-address=0.0.0.0:{HADOOP_NAMENODE_WEBUI_PORT}",
                    ]
                ),
            ],
            # "env_file": ["envs/common.env"],
            "environment": env_content.copy()
            | {
                "HDFS-SITE.XML_dfs.namenode.rpc-address": f"0.0.0.0:{HADOOP_NAMENODE_PORT}",
                "HDFS-SITE.XML_dfs.namenode.http-address": f"0.0.0.0:{HADOOP_NAMENODE_WEBUI_PORT}",
            },
            "volumes": [
                f"{os.path.join(DOCKER_MOUNTDIR, 'namenode', 'work-dir')}:{HADOOP_WORKDIR}",
                f"{os.path.join(DOCKER_MOUNTDIR, 'namenode', 'name-dir')}:{HADOOP_NAMENODE_NAMEDIR}",
            ],
            "networks": {
                "hadoop-cluster": {"aliases": [HADOOP_NAMENODE_DISTRIBUTED_HOST]}
            },
            "hostname": HADOOP_NAMENODE_COMPOSE_HOST,
            "ports": namenode_ports,
            "extra_hosts": [
                f"{DOCKER_INTERNAL_HOST}:host-gateway",
            ],
            "dns": DOCKER_DNS,
            "deploy": {
                "resources": {
                    "limits": {"cpus": "1.0", "memory": NAMENODE_DOCKER_LIMIT}
                }
            },
            "healthcheck": {
                "test": [
                    "CMD-SHELL",
                    f"bash -c '</dev/tcp/127.0.0.1/{HADOOP_NAMENODE_WEBUI_PORT}' 2>/dev/null || exit 1",
                ],
                "interval": "5s",  # Docker hará la prueba cada 5 segundos
                "timeout": "5s",  # Si el NameNode tarda más de 5s en responder, cuenta como fallo
                "retries": 30,  # Si falla 3 veces seguidas, el contenedor se marca como "unhealthy"
                "start_period": "5s",  # Tiempo de gracia inicial (Hadoop necesita tiempo para formatear y arrancar la JVM)
            },
        },
        "resourcemanager": {
            "image": "apache/hadoop:3.4.3",
            "container_name": "resourcemanager",
            "command": [
                "bash",
                "-c",
                " ".join(
                    [
                        f"sudo chown -R $(id -u hadoop):$(id -g hadoop) {HADOOP_WORKDIR} &&",
                        f"mapred --daemon start historyserver &&",
                        "yarn",
                        "resourcemanager",
                        # f"-Dfs.defaultFS=hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}",
                        # f"-Dyarn.resourcemanager.hostname={HADOOP_RESOURCEMANAGER_HOSTNAME}",
                        # f"-Dyarn.resourcemanager.webapp.address=0.0.0.0:{HADOOP_RESOURCEMANAGER_WEBUI_PORT}",
                        # f"-Dyarn.resourcemanager.address=0.0.0.0:{HADOOP_RESOURCEMANAGER_RPC_APP_MANAGER_PORT}",
                        # f"-Dyarn.resourcemanager.scheduler.address=0.0.0.0:{HADOOP_RESOURCEMANAGER_SCHEDULER_PORT}",
                        # f"-Dyarn.resourcemanager.resource-tracker.address=0.0.0.0:{HADOOP_RESOURCEMANAGER_TRACKER_PORT}",
                        # f"-Dyarn.resourcemanager.admin.address=0.0.0.0:{HADOOP_RESOURCEMANAGER_ADMIN_PORT}",
                    ]
                ),
            ],
            # "env_file": ["envs/common.env"],
            "environment": env_content.copy()
            | {
                "YARN-SITE.XML_yarn.resourcemanager.bind-host": "0.0.0.0",
                "YARN-SITE.XML_yarn.resourcemanager.webapp.address": f"{HADOOP_RESOURCEMANAGER_HOSTNAME}:{HADOOP_RESOURCEMANAGER_WEBUI_PORT}",
                "YARN-SITE.XML_yarn.resourcemanager.address": f"{HADOOP_RESOURCEMANAGER_HOSTNAME}:{HADOOP_RESOURCEMANAGER_RPC_APP_MANAGER_PORT}",
                "YARN-SITE.XML_yarn.resourcemanager.scheduler.address": f"{HADOOP_RESOURCEMANAGER_HOSTNAME}:{HADOOP_RESOURCEMANAGER_SCHEDULER_PORT}",
                "YARN-SITE.XML_yarn.resourcemanager.resource-tracker.address": f"{HADOOP_RESOURCEMANAGER_HOSTNAME}:{HADOOP_RESOURCEMANAGER_TRACKER_PORT}",
                "YARN-SITE.XML_yarn.resourcemanager.admin.address": f"{HADOOP_RESOURCEMANAGER_HOSTNAME}:{HADOOP_RESOURCEMANAGER_ADMIN_PORT}",
            },
            "volumes": [
                f"{os.path.join(DOCKER_MOUNTDIR, 'resourcemanager', 'work-dir')}:{HADOOP_WORKDIR}",
            ],
            "networks": ["hadoop-cluster"],
            "hostname": HADOOP_RESOURCEMANAGER_COMPOSE_HOST,
            "ports": [
                f"{HOST_BIND_IP}:{HADOOP_RESOURCEMANAGER_WEBUI_PORT}:{HADOOP_RESOURCEMANAGER_WEBUI_PORT}",
                f"{HOST_BIND_IP}:{HADOOP_RESOURCEMANAGER_RPC_APP_MANAGER_PORT}:{HADOOP_RESOURCEMANAGER_RPC_APP_MANAGER_PORT}",
                f"{HOST_BIND_IP}:{HADOOP_RESOURCEMANAGER_SCHEDULER_PORT}:{HADOOP_RESOURCEMANAGER_SCHEDULER_PORT}",
                f"{HOST_BIND_IP}:{HADOOP_RESOURCEMANAGER_TRACKER_PORT}:{HADOOP_RESOURCEMANAGER_TRACKER_PORT}",
                f"{HOST_BIND_IP}:{HADOOP_RESOURCEMANAGER_ADMIN_PORT}:{HADOOP_RESOURCEMANAGER_ADMIN_PORT}",
                f"{HOST_BIND_IP}:{HADOOP_MAPRED_JOB_HISTORY_PORT}:{HADOOP_MAPRED_JOB_HISTORY_PORT}",
                f"{HOST_BIND_IP}:{HADOOP_MAPRED_LOG_SERVER_PORT}:{HADOOP_MAPRED_LOG_SERVER_PORT}",
            ],
            "extra_hosts": [
                f"{DOCKER_INTERNAL_HOST}:host-gateway",
            ],
            "dns": DOCKER_DNS,
            "depends_on": {"namenode": {"condition": "service_healthy"}},
            "deploy": {
                "resources": {
                    "limits": {"cpus": "2.0", "memory": RESOURCEMANAGER_DOCKER_LIMIT}
                }
            },
            "healthcheck": {
                "test": [
                    "CMD-SHELL",
                    f"bash -c '</dev/tcp/127.0.0.1/{HADOOP_RESOURCEMANAGER_WEBUI_PORT}' 2>/dev/null || exit 1",
                ],
                "interval": "5s",
                "timeout": "5s",
                "retries": 30,
                "start_period": "5s",  # Un poco menos que el NN porque no tiene que formatear discos
            },
        },
    },
}


for i in range(0, HADOOP_NUM_WORKERS):

    # Programmatically add DataNodes
    hadoop_compose_dict["services"][HADOOP_DATANODE_NAMES[i]] = {
        "image": "apache/hadoop:3.4.3",
        "container_name": HADOOP_DATANODE_NAMES[i],
        "command": [
            "bash",
            "-c",
            " ".join(
                [
                    f"sudo chown -R $(id -u hadoop):$(id -g hadoop) {HADOOP_WORKDIR} &&",
                    f"sudo mkdir -p {HADOOP_DATANODE_DATADIR} &&",
                    f"sudo chown -R $(id -u hadoop):$(id -g hadoop) {HADOOP_DATANODE_DATADIR} &&",
                    "hdfs",
                    "datanode",
                    # f"-Dfs.defaultFS=hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}",
                    # f"-Ddfs.datanode.data.dir={HADOOP_DATANODE_DATADIR}",
                    # "-Ddfs.datanode.data.dir.perm=775",
                    # "-Ddfs.permissions.enabled=false",
                    # f"-Ddfs.datanode.address=0.0.0.0:{HADOOP_DATANODE_TRANSFER_PORTS[i]}",
                    # f"-Ddfs.datanode.http.address=0.0.0.0:{HADOOP_DATANODE_WEBUI_PORTS[i]}",
                    # f"-Ddfs.datanode.ipc.address=0.0.0.0:{HADOOP_DATANODE_IPC_PORTS[i]}",
                    # "-Ddfs.datanode.use.datanode.hostname=true",
                ]
            ),
        ],
        # "env_file": ["envs/common.env"],
        "environment": env_content.copy()
        | {
            "HDFS-SITE.XML_dfs.datanode.address": f"0.0.0.0:{HADOOP_DATANODE_TRANSFER_PORTS[i]}",
            "HDFS-SITE.XML_dfs.datanode.http.address": f"0.0.0.0:{HADOOP_DATANODE_WEBUI_PORTS[i]}",
            "HDFS-SITE.XML_dfs.datanode.ipc.address": f"0.0.0.0:{HADOOP_DATANODE_IPC_PORTS[i]}",
            "HDFS-SITE.XML_dfs.datanode.hostname": HADOOP_DATANODE_HOSTNAMES[i],
        },
        "volumes": [
            f"{os.path.join(DOCKER_MOUNTDIR, HADOOP_DATANODE_NAMES[i], 'work-dir')}:{HADOOP_WORKDIR}",
            f"{os.path.join(DOCKER_MOUNTDIR, HADOOP_DATANODE_NAMES[i], 'data-dir')}:{HADOOP_DATANODE_DATADIR}",
        ],
        "networks": {
            "hadoop-cluster": {"aliases": [HADOOP_DATANODE_HOSTNAMES[i]]}
        },
        "hostname": HADOOP_DATANODE_COMPOSE_HOSTS[i],
        "ports": [
            f"{HOST_BIND_IP}:{HADOOP_DATANODE_WEBUI_PORTS[i]}:{HADOOP_DATANODE_WEBUI_PORTS[i]}",
            f"{HOST_BIND_IP}:{HADOOP_DATANODE_TRANSFER_PORTS[i]}:{HADOOP_DATANODE_TRANSFER_PORTS[i]}",
            f"{HOST_BIND_IP}:{HADOOP_DATANODE_IPC_PORTS[i]}:{HADOOP_DATANODE_IPC_PORTS[i]}",
        ],
        "extra_hosts": [
            f"{DOCKER_INTERNAL_HOST}:host-gateway",
        ],
        "dns": DOCKER_DNS,
        "deploy": {
            "resources": {"limits": {"cpus": "1.0", "memory": DATANODE_DOCKER_LIMIT}}
        },
        "depends_on": {
            "namenode": {"condition": "service_healthy"},
            # "resourcemanager": {"condition": "service_healthy"},
        }
        | {
            HADOOP_DATANODE_NAMES[j]: {"condition": "service_started"}
            for j in range(0, i)
        },
    }

    # Programmatically add NodeManagers
    hadoop_compose_dict["services"][HADOOP_NODEMANAGER_NAMES[i]] = {
        "image": "apache/hadoop:3.4.3",
        "container_name": HADOOP_NODEMANAGER_NAMES[i],
        "command": [
            "bash",
            "-c",
            " ".join(
                [
                    f"sudo chown -R $(id -u hadoop):$(id -g hadoop) {HADOOP_WORKDIR} &&",
                    "yarn",
                    "nodemanager",
                    # f"-Dyarn.resourcemanager.hostname={HADOOP_RESOURCEMANAGER_HOSTNAME}",
                    # f"-Dyarn.nodemanager.aux-services=mapreduce_shuffle",
                    # f"-Dyarn.resourcemanager.resource-tracker.address={HADOOP_RESOURCEMANAGER_HOSTNAME}:{HADOOP_RESOURCEMANAGER_TRACKER_PORT}",
                    # f"-Dyarn.nodemanager.address=0.0.0.0:{HADOOP_NODEMANAGER_RPC_PORTS[i]}",
                    # f"-Dyarn.nodemanager.webapp.address=0.0.0.0:{HADOOP_NODEMANAGER_WEBUI_PORTS[i]}",
                ]
            ),
        ],
        # "env_file": ["envs/common.env"],
        "environment": env_content.copy()
        | {
            "YARN-SITE.XML_yarn.nodemanager.hostname": HADOOP_NODEMANAGER_HOSTNAMES[i],
            "YARN-SITE.XML_yarn.nodemanager.address": f"0.0.0.0:{HADOOP_NODEMANAGER_RPC_PORTS[i]}",
            "YARN-SITE.XML_yarn.nodemanager.webapp.address": f"0.0.0.0:{HADOOP_NODEMANAGER_WEBUI_PORTS[i]}",
            # "YARN-SITE.XML_yarn.nodemanager.localizer.port": f"{HADOOP_NODEMANAGER_JOB_LOCALIZER_PORTS[i]}",
            # "YARN-SITE.XML_yarn.app.mapreduce.am.job.client.port-range": f"{HADOOP_NODEMANAGER_JOB_CLIENT_START_PORTS[i]}-{HADOOP_NODEMANAGER_JOB_CLIENT_END_PORTS[i]}",
            # "TEZ-SITE.XML_tez.am.client.am.port-range": f"{HADOOP_NODEMANAGER_JOB_CLIENT_START_PORTS[i]}-{HADOOP_NODEMANAGER_JOB_CLIENT_END_PORTS[i]}",
            # "TEZ-SITE.XML_tez.am.hostname": "0.0.0.0",
            # "TEZ-SITE.XML_tez.am.hostname": HADOOP_NODEMANAGER_HOSTNAMES[i],
            # "TEZ-SITE.XML_tez.am.launch.cmd-opts": " ".join(
            #     [
            #         f"-Djava.net.preferIPv4Stack=true",
            #         f"-Dtez.am.client.am.port-range={HADOOP_NODEMANAGER_JOB_CLIENT_START_PORTS[i]}-{HADOOP_NODEMANAGER_JOB_CLIENT_END_PORTS[i]}",
            #         f"-Dhadoop.rpc.protection=authentication",
            #     ]
            # ),
            # "HADOOP_OPTS": " ".join(
            #     [
            #         f"-Dyarn.app.mapreduce.am.job.client.port-range={HADOOP_NODEMANAGER_JOB_CLIENT_START_PORTS[i]}-{HADOOP_NODEMANAGER_JOB_CLIENT_END_PORTS[i]}",
            #         f"-Dtez.am.client.am.port-range={HADOOP_NODEMANAGER_JOB_CLIENT_START_PORTS[i]}-{HADOOP_NODEMANAGER_JOB_CLIENT_END_PORTS[i]}",
            #     ]
            # ),
        },
        "volumes": [
            f"{os.path.join(DOCKER_MOUNTDIR, HADOOP_NODEMANAGER_NAMES[i], 'work-dir')}:{HADOOP_WORKDIR}",
        ],
        "networks": ["hadoop-cluster"],
        "hostname": HADOOP_NODEMANAGER_COMPOSE_HOSTS[i],
        "ports": [
            f"{HOST_BIND_IP}:{HADOOP_NODEMANAGER_WEBUI_PORTS[i]}:{HADOOP_NODEMANAGER_WEBUI_PORTS[i]}",
            f"{HOST_BIND_IP}:{HADOOP_NODEMANAGER_RPC_PORTS[i]}:{HADOOP_NODEMANAGER_RPC_PORTS[i]}",
            # (
            #     f"{HADOOP_NODEMANAGER_JOB_CLIENT_START_PORTS[i]}-{HADOOP_NODEMANAGER_JOB_CLIENT_END_PORTS[i]}"
            #     f":"
            #     f"{HADOOP_NODEMANAGER_JOB_CLIENT_START_PORTS[i]}-{HADOOP_NODEMANAGER_JOB_CLIENT_END_PORTS[i]}"
            # ),
            # f"{HADOOP_NODEMANAGER_JOB_LOCALIZER_PORTS[i]}:{HADOOP_NODEMANAGER_JOB_LOCALIZER_PORTS[i]}",
            # "50000-50005:50000-50005",
        ],
        "extra_hosts": [
            f"{DOCKER_INTERNAL_HOST}:host-gateway",
        ],
        "dns": DOCKER_DNS,
        "deploy": {
            "resources": {"limits": {"cpus": "2.0", "memory": NODEMANAGER_DOCKER_LIMIT}}
        },
        "depends_on": {
            "namenode": {"condition": "service_healthy"},
            "resourcemanager": {"condition": "service_healthy"},
        }
        | {
            HADOOP_DATANODE_NAMES[j]: {"condition": "service_started"}
            for j in range(HADOOP_NUM_WORKERS)
        }
        | {
            HADOOP_NODEMANAGER_NAMES[j]: {"condition": "service_started"}
            for j in range(0, i)
        },
    }


hadoop_compose_yaml_contents = yaml.dump(
    hadoop_compose_dict, default_flow_style=False, sort_keys=False, indent=4
)
with open(
    os.path.join(LOCALHOST_WORKDIR, hadoop_compose_file_name), "w"
) as hadoop_compose_file:
    hadoop_compose_file.write(hadoop_compose_yaml_contents)
print(
    f"Successfully created: {os.path.relpath(os.path.join(LOCALHOST_WORKDIR,hadoop_compose_file_name))}"
)
display(Markdown(f"```yaml\n{hadoop_compose_yaml_contents}\n```"))

# %% [markdown]
# # Start hadoop-cluster.docker-compose.yml
#

# %%
# !docker compose -f hadoop-cluster.docker-compose.yml up -d --wait
