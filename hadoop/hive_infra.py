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
HIVE_START_FROM_SCRATCH = START_FROM_SCRATCH
DOCKER_INTERNAL_HOST = "host.docker.internal"
DOCKER_DNS = [VPN_DNS_IP] if NETWORK_MODE == "vpn" else []
HOST_BIND_IP = VPN_HOST_IP if NETWORK_MODE == "vpn" else "127.0.0.1"

POSTGRES_USER = "hive"
POSTGRES_PASSWORD = "hive"
POSTGRES_DB = "metastore"

HIVE_DB_CONTAINER_NAME = "hive-metastore-db"
HIVE_SCHEMA_INIT_CONTAINER_NAME = "hive-schema-init"
HIVE_METASTORE_CONTAINER_NAME = "hive-metastore"
HIVE_SERVER2_CONTAINER_NAME = "hive-server2"

HIVE_DB_HOSTNAME = HIVE_DB_CONTAINER_NAME
HIVE_SCHEMA_INIT_HOSTNAME = HIVE_SCHEMA_INIT_CONTAINER_NAME
HIVE_METASTORE_HOSTNAME = HIVE_METASTORE_CONTAINER_NAME
HIVE_SERVER2_HOSTNAME = HIVE_SERVER2_CONTAINER_NAME
HIVE_DB_COMPOSE_HOST = (
    HIVE_DB_HOSTNAME
    if NETWORK_MODE == "local"
    else f"{HIVE_DB_CONTAINER_NAME}.{VPN_CLIENT_DOMAIN}"
)
HIVE_SCHEMA_INIT_COMPOSE_HOST = (
    HIVE_SCHEMA_INIT_HOSTNAME
    if NETWORK_MODE == "local"
    else f"{HIVE_SCHEMA_INIT_CONTAINER_NAME}.{VPN_CLIENT_DOMAIN}"
)
HIVE_METASTORE_COMPOSE_HOST = (
    HIVE_METASTORE_HOSTNAME
    if NETWORK_MODE == "local"
    else f"{HIVE_METASTORE_CONTAINER_NAME}.{VPN_CLIENT_DOMAIN}"
)
HIVE_SERVER2_COMPOSE_HOST = (
    HIVE_SERVER2_HOSTNAME
    if NETWORK_MODE == "local"
    else f"{HIVE_SERVER2_CONTAINER_NAME}.{VPN_CLIENT_DOMAIN}"
)
HIVE_DB_CLIENT_HOST = (
    "127.0.0.1"
    if NETWORK_MODE == "local"
    else f"{HIVE_DB_CONTAINER_NAME}.{VPN_CLIENT_DOMAIN}"
)
HIVE_SCHEMA_INIT_CLIENT_HOST = (
    "127.0.0.1"
    if NETWORK_MODE == "local"
    else f"{HIVE_SCHEMA_INIT_CONTAINER_NAME}.{VPN_CLIENT_DOMAIN}"
)
HIVE_METASTORE_CLIENT_HOST = (
    "127.0.0.1"
    if NETWORK_MODE == "local"
    else f"{HIVE_METASTORE_CONTAINER_NAME}.{VPN_CLIENT_DOMAIN}"
)
HIVE_SERVER2_CLIENT_HOST = (
    "127.0.0.1"
    if NETWORK_MODE == "local"
    else f"{HIVE_SERVER2_CONTAINER_NAME}.{VPN_CLIENT_DOMAIN}"
)

HIVE_DB_INTERNAL_PORT = 15432
HIVE_METASTORE_INTERNAL_PORT = 9083
HIVE_SERVER2_INTERNAL_PORT = 10000
HIVE_SERVER2_UI_INTERNAL_PORT = 10002

HIVE_DB_EXTERNAL_PORT = 15432
HIVE_METASTORE_EXTERNAL_PORT = 9083
HIVE_SERVER2_EXTERNAL_PORT = 10000
HIVE_SERVER2_UI_EXTERNAL_PORT = 10002

HIVE_USERDIR = "/user/hive"
HIVE_DATADIR = f"{HIVE_USERDIR}/warehouse"

HADOOP_RESOURCEMANAGER_WEBUI_PORT = 8088
HADOOP_RESOURCEMANAGER_RPC_APP_MANAGER_PORT = 8032
HADOOP_RESOURCEMANAGER_TRACKER_PORT = 8031
HADOOP_RESOURCEMANAGER_SCHEDULER_PORT = 8030
HADOOP_RESOURCEMANAGER_ADMIN_PORT = 8033

HADOOP_NAMENODE_HOSTNAME = "namenode"
HADOOP_NAMENODE_PORT = 8020

HADOOP_RESOURCEMANAGER_HOSTNAME = "resourcemanager"
HADOOP_RESOURCEMANAGER_PORT = 8032

APACHE_HIVE_IMAGE = "apache/hive:4.0.1"
POSTGRES_IMAGE = "postgres:18"

# %%
import os
from pathlib import Path

LOCAL_WORKDIR = f"{os.path.join(os.path.relpath(Path.cwd()))}"
DOCKER_MOUNTDIR = os.path.join(LOCAL_WORKDIR, "mount")

mount_path = Path(DOCKER_MOUNTDIR)
mount_path.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# # Stop hive-cluster.docker-compose.yml

# %%
import subprocess

if HIVE_START_FROM_SCRATCH:
    subprocess.run(
        ["docker", "compose", "-f", "hive-cluster.docker-compose.yml", "down", "-v"],
        check=False,
    )
else:
    print("Preserving existing containers and volumes")


# %%
import shutil
import stat

os.makedirs(os.path.join(DOCKER_MOUNTDIR), exist_ok=True)
if HIVE_START_FROM_SCRATCH:
    for container in [
        HIVE_DB_CONTAINER_NAME,
        HIVE_METASTORE_CONTAINER_NAME,
        HIVE_SERVER2_CONTAINER_NAME,
        HIVE_SCHEMA_INIT_CONTAINER_NAME,
    ]:
        if os.path.exists(os.path.join(DOCKER_MOUNTDIR, container)):

            def on_rm_error(func, path, exc_info):
                os.chmod(path, stat.S_IWRITE)
                os.unlink(path)

            shutil.rmtree(os.path.join(DOCKER_MOUNTDIR, container), onerror=on_rm_error)
        os.makedirs(os.path.join(DOCKER_MOUNTDIR, container), exist_ok=True)

# %% [markdown]
# # Create all config files

# %%
core_site_xml_content = f"""<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}</value>
    </property>
</configuration>
"""

for container in [HIVE_METASTORE_CONTAINER_NAME, HIVE_SERVER2_CONTAINER_NAME]:
    dest_dir = os.path.join(DOCKER_MOUNTDIR, container, "hive_custom_conf")
    os.makedirs(dest_dir, exist_ok=True)
    with open(os.path.join(dest_dir, "core-site.xml"), "w") as f:
        f.write(core_site_xml_content)

# %%
hive_site_xml_content = f"""<?xml version="1.0"?>
<configuration>
    <property>
        <name>hive.users.in.admin.role</name>
        <value>hive,root,hadoop</value>
    </property>
    <property>
        <name>hive.server2.thrift.bind.host</name>
        <value>0.0.0.0</value>
    </property>
    <property>
        <name>hive.server2.webui.host</name>
        <value>0.0.0.0</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:postgresql://{HIVE_DB_HOSTNAME}:{HIVE_DB_EXTERNAL_PORT}/{POSTGRES_DB}</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>org.postgresql.Driver</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>{POSTGRES_USER}</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>{POSTGRES_PASSWORD}</value>
    </property>

    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}</value>
    </property>
    <property>
        <name>dfs.datanode.use.datanode.hostname</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://{HIVE_METASTORE_HOSTNAME}:{HIVE_METASTORE_EXTERNAL_PORT}</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>{HADOOP_RESOURCEMANAGER_HOSTNAME}:{HADOOP_RESOURCEMANAGER_RPC_APP_MANAGER_PORT}</value>
    </property>
    <property>
        <name>yarn.resourcemanager.scheduler.address</name>
        <value>{HADOOP_RESOURCEMANAGER_HOSTNAME}:{HADOOP_RESOURCEMANAGER_SCHEDULER_PORT}</value>
    </property>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>

    <property>
        <name>hive.server2.enable.doAs</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.server2.thrift.port</name>
        <value>10000</value>
    </property>
    <property>
        <name>hive.server2.transport.mode</name>
        <value>binary</value>
    </property>
    <property>
        <name>hive.plan.serialization.format</name>
        <value>javaXML</value>
    </property>
    <property>
        <name>hive.exec.submit.local.task.via.child</name>
        <value>false</value>
    </property>

    <property>
        <name>hive.metastore.event.db.listener.timetolive</name>
        <value>0s</value>
    </property>
    <property>
        <name>hive.notification.event.poll.interval</name>
        <value>0s</value>
    </property>

    <property>
        <name>tez.lib.uris</name>
        <value>hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}/apps/dist-lib/dist-lib.tar.gz</value>
    </property>
    <property>
        <name>tez.use.cluster.hadoop-libs</name>
        <value>true</value> 
    </property>
    <property>
        <name>tez.ignore.lib.uris</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.execution.engine</name>
        <value>tez</value> 
    </property>
    <property>
        <name>tez.local.mode</name>
        <value>false</value>
    </property>
    <property>
        <name>tez.runtime.optimize.local.fetch</name>
        <value>true</value>
    </property>

    <property>
        <name>hive.exec.scratchdir</name>
        <value>hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}/tmp/hive/scratch</value>
    </property>
    <property>
        <name>hive.exec.stagingdir</name>
        <value>hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}/tmp/hive/staging</value>
    </property>
    <property>
        <name>hive.exec.local.scratchdir</name>
        <value>/tmp/hive/local</value>
    </property>
    <property>
        <name>hive.start.cleanup.scratchdir</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.fetch.task.conversion</name>
        <value>more</value>
    </property>
    <property>
        <name>hive.exec.mode.local.auto</name>
        <value>true</value>
    </property>
</configuration>
"""

for container in [HIVE_METASTORE_CONTAINER_NAME, HIVE_SERVER2_CONTAINER_NAME]:
    dest_dir = os.path.join(DOCKER_MOUNTDIR, container, "hive_custom_conf")
    os.makedirs(dest_dir, exist_ok=True)
    with open(os.path.join(dest_dir, "hive-site.xml"), "w") as f:
        f.write(hive_site_xml_content)

# %%
tez_site_content = f"""<?xml version="1.0"?>
<configuration>
    <property>
        <name>tez.lib.uris</name>
        <value>hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}/apps/dist-lib/dist-lib.tar.gz</value>
    </property>
    <property>
        <name>tez.use.cluster.hadoop-libs</name>
        <value>true</value> 
    </property>
    <property>
        <name>tez.ignore.lib.uris</name>
        <value>false</value>
    </property>
    <property>
        <name>tez.local.mode</name>
        <value>false</value>
    </property>
    <property>
        <name>tez.runtime.optimize.local.fetch</name>
        <value>true</value>
    </property>
    <property>
        <name>tez.cluster.additional.classpath.prefix</name>
        <value>/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/yarn/*:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hive/lib/*:/opt/hive/conf</value>
    </property>
    <property>
        <name>tez.am.hostname</name>
        <value>0.0.0.0</value> 
    </property>
    <property>
        <name>tez.am.launch.cmd-opts</name>
        <value>-Djava.net.preferIPv4Stack=true -Dhadoop.rpc.protection=authentication</value>
    </property>

    <property>
        <name>hive.exec.scratchdir</name>
        <value>hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}/tmp/hive/scratch</value>
    </property>
    <property>
        <name>hive.exec.stagingdir</name>
        <value>hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}/tmp/hive/staging</value>
    </property>
    <property>
        <name>hive.exec.local.scratchdir</name>
        <value>/tmp/hive/local</value>
    </property>
    <property>
        <name>hive.start.cleanup.scratchdir</name>
        <value>true</value>
    </property>
    
    <property>
        <name>tez.history.logging.service.class</name>
        <value>org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService</value>
    </property>

    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>hive.exec.submit.local.task.via.child</name>
        <value>false</value>
    </property>
</configuration>
"""

for container in [HIVE_METASTORE_CONTAINER_NAME, HIVE_SERVER2_CONTAINER_NAME]:
    dest_dir = os.path.join(DOCKER_MOUNTDIR, container, "hive_custom_conf")
    os.makedirs(dest_dir, exist_ok=True)
    with open(os.path.join(dest_dir, "tez-site.xml"), "w") as f:
        f.write(tez_site_content)

# %%
import os

# 1. Definimos el contenido del archivo de logs
log4j_properties_content = """#
log4j.rootLogger=INFO, console

# Configuración del appender (salida estándar)
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n

# Silenciar componentes específicos que son muy ruidosos
log4j.logger.org.apache.hadoop=INFO
log4j.logger.org.apache.hive=INFO
log4j.logger.org.apache.tez=INFO
log4j.logger.org.apache.parquet=INFO
log4j.logger.org.apache.iceberg=INFO
"""

# 2. Iteramos sobre los contenedores para escribir el archivo
for container in [HIVE_METASTORE_CONTAINER_NAME, HIVE_SERVER2_CONTAINER_NAME]:
    dest_dir = os.path.join(DOCKER_MOUNTDIR, container, "hive_custom_conf")
    os.makedirs(dest_dir, exist_ok=True)
    with open(os.path.join(dest_dir, "log4j.properties"), "w") as f:
        f.write(log4j_properties_content)

# %% [markdown]
# # Start hive-cluster.docker-compose.yml

# %%
import yaml
from IPython.display import Markdown, display

compose_filename = os.path.join(LOCAL_WORKDIR, "hive-cluster.docker-compose.yml")

# Usamos EXTERNAL_PORT para respetar tu VPN/DNS
metastore_jdbc_opts = f" ".join(
    [
        f"-Dhive.metastore.db.type=postgres",
        f"-Dmetastore.db.type=postgres",
        f"-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver",
        f"-Djavax.jdo.option.ConnectionURL=jdbc:postgresql://{HIVE_DB_HOSTNAME}:{HIVE_DB_EXTERNAL_PORT}/{POSTGRES_DB}",
        f"-Djavax.jdo.option.ConnectionUserName={POSTGRES_USER}",
        f"-Djavax.jdo.option.ConnectionPassword={POSTGRES_PASSWORD}",
    ]
)

hdfs_opts = f" ".join(
    [
        f"-Dfs.defaultFS=hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}",
        f"-Dhive.metastore.warehouse.dir=hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}{HIVE_DATADIR}/managed",
        "-Dmapreduce.framework.name=yarn",
        f"-Dyarn.resourcemanager.address={HADOOP_RESOURCEMANAGER_HOSTNAME}:{HADOOP_RESOURCEMANAGER_RPC_APP_MANAGER_PORT}",
        f"-Dmapreduce.application.classpath=/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/share/hadoop/mapreduce/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/yarn/*:/opt/hadoop/share/hadoop/yarn/lib/*",
    ]
)

hive_compose_dict = {
    "name": "hive-cluster",
    # "networks": {"hive-cluster": {"driver": "bridge"}},
    # "networks": {
    #     "hadoop-cluster": {"external": True, "name": "hadoop-cluster_hadoop-cluster"}
    # },
    "networks": {
        "hadoop-cluster": {
            "external": True,
            "name": "hadoop-network",
        }
    },
    "services": {
        HIVE_DB_CONTAINER_NAME: {
            "image": POSTGRES_IMAGE,
            "container_name": HIVE_DB_CONTAINER_NAME,
            "hostname": HIVE_DB_COMPOSE_HOST,
            "environment": {
                "POSTGRES_USER": POSTGRES_USER,
                "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
                "POSTGRES_DB": POSTGRES_DB,
                "PGPORT": str(HIVE_DB_INTERNAL_PORT),
            },
            "volumes": [f".\\mount\\{HIVE_DB_CONTAINER_NAME}:/var/lib/postgresql"],
            "networks": ["hadoop-cluster"],
            "ports": [
                f"{HOST_BIND_IP}:{HIVE_DB_EXTERNAL_PORT}:{HIVE_DB_INTERNAL_PORT}"
            ],
            "extra_hosts": [f"{DOCKER_INTERNAL_HOST}:host-gateway"],
            "dns": DOCKER_DNS,
            "deploy": {"resources": {"limits": {"cpus": "1.0", "memory": "512M"}}},
            "healthcheck": {
                "test": [
                    "CMD-SHELL",
                    f"pg_isready -h 127.0.0.1 -p {HIVE_DB_INTERNAL_PORT} -U {POSTGRES_USER} -d {POSTGRES_DB}",
                ],
                "interval": "5s",
                "timeout": "5s",
                "retries": 20,
                "start_period": "15s",
            },
        },
        HIVE_SCHEMA_INIT_CONTAINER_NAME: {
            "image": APACHE_HIVE_IMAGE,
            "container_name": HIVE_SCHEMA_INIT_CONTAINER_NAME,
            "hostname": HIVE_SCHEMA_INIT_COMPOSE_HOST,
            "depends_on": {HIVE_DB_CONTAINER_NAME: {"condition": "service_healthy"}},
            "environment": {
                "DB_DRIVER": "postgres",
                "SERVICE_OPTS": f"{metastore_jdbc_opts}",
                # "HADOOP_CLASSPATH": "/hive_custom_conf:/opt/hive/lib/postgresql-42.7.10.jar:/opt/hive/lib/*:/opt/tez/*:/opt/tez/lib/*",
                "HADOOP_CLASSPATH": "/hive_custom_conf:/opt/hive/lib/postgresql-42.7.10.jar",
            },
            "volumes": [
                "./postgresql-42.7.10.jar:/opt/hive/lib/postgresql-42.7.10.jar"
            ],
            "networks": ["hadoop-cluster"],
            "extra_hosts": [f"{DOCKER_INTERNAL_HOST}:host-gateway"],
            "dns": DOCKER_DNS,
            "deploy": {"resources": {"limits": {"cpus": "2.0", "memory": "512M"}}},
        },
        HIVE_METASTORE_CONTAINER_NAME: {
            "image": APACHE_HIVE_IMAGE,
            "container_name": HIVE_METASTORE_CONTAINER_NAME,
            "hostname": HIVE_METASTORE_COMPOSE_HOST,
            "depends_on": {
                HIVE_SCHEMA_INIT_CONTAINER_NAME: {
                    "condition": "service_completed_successfully"
                }
            },
            "environment": {
                "DB_DRIVER": "postgres",
                "IS_RESUME": "true",
                "SERVICE_NAME": "metastore",
                "HIVE_CUSTOM_CONF_DIR": "/hive_custom_conf",
                "TEZ_CONF_DIR": "/hive_custom_conf",
                "HADOOP_CONF_DIR": "/hive_custom_conf",
                "SERVICE_OPTS": f"{metastore_jdbc_opts} {hdfs_opts}",
                # "HADOOP_CLASSPATH": "/hive_custom_conf:/opt/hive/lib/postgresql-42.7.10.jar:/opt/hive/lib/*:/opt/tez/*:/opt/tez/lib/*",
                "HADOOP_CLASSPATH": "/hive_custom_conf:/opt/hive/lib/postgresql-42.7.10.jar:/opt/tez/*:/opt/tez/lib/*",
            },
            "volumes": [
                "./postgresql-42.7.10.jar:/opt/hive/lib/postgresql-42.7.10.jar",
                f"./mount/{HIVE_METASTORE_CONTAINER_NAME}/hive_custom_conf:/hive_custom_conf",
                f"./mount/{HIVE_METASTORE_CONTAINER_NAME}/hive_custom_conf/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml",
            ],
            "networks": ["hadoop-cluster"],
            "ports": [
                f"{HOST_BIND_IP}:{HIVE_METASTORE_EXTERNAL_PORT}:{HIVE_METASTORE_INTERNAL_PORT}"
            ],
            "extra_hosts": [f"{DOCKER_INTERNAL_HOST}:host-gateway"],
            "dns": DOCKER_DNS,
            "deploy": {"resources": {"limits": {"cpus": "1.0", "memory": "512M"}}},
            "healthcheck": {
                "test": [
                    "CMD-SHELL",
                    f"bash -c '</dev/tcp/127.0.0.1/{HIVE_METASTORE_INTERNAL_PORT}'",
                ],
                "interval": "5s",
                "timeout": "5s",
                "retries": 20,
                "start_period": "15s",
            },
        },
        HIVE_SERVER2_CONTAINER_NAME: {
            "image": APACHE_HIVE_IMAGE,
            "container_name": HIVE_SERVER2_CONTAINER_NAME,
            "hostname": HIVE_SERVER2_COMPOSE_HOST,
            "depends_on": {
                HIVE_METASTORE_CONTAINER_NAME: {"condition": "service_healthy"}
            },
            "environment": {
                "DB_DRIVER": "postgres",
                "IS_RESUME": "true",
                "SERVICE_NAME": "hiveserver2",
                "HIVE_CUSTOM_CONF_DIR": "/hive_custom_conf",
                "TEZ_CONF_DIR": "/hive_custom_conf",
                "HADOOP_CONF_DIR": "/hive_custom_conf",
                "SERVICE_OPTS": f"-Dhive.metastore.uris=thrift://{HIVE_METASTORE_HOSTNAME}:{HIVE_METASTORE_EXTERNAL_PORT} {hdfs_opts}",
                # "HADOOP_CLASSPATH": "/hive_custom_conf:/opt/hive/lib/postgresql-42.7.10.jar:/opt/hive/lib/*:/opt/tez/*:/opt/tez/lib/*",
                "HADOOP_CLASSPATH": "/hive_custom_conf:/opt/hive/lib/postgresql-42.7.10.jar:/opt/tez/*:/opt/tez/lib/*",
            },
            "volumes": [
                "./postgresql-42.7.10.jar:/opt/hive/lib/postgresql-42.7.10.jar",
                f"./mount/{HIVE_SERVER2_CONTAINER_NAME}/hive_custom_conf:/hive_custom_conf",
                f"./mount/{HIVE_SERVER2_CONTAINER_NAME}/hive_custom_conf/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml",
            ],
            "networks": ["hadoop-cluster"],
            "ports": [
                f"{HOST_BIND_IP}:{HIVE_SERVER2_EXTERNAL_PORT}:{HIVE_SERVER2_INTERNAL_PORT}",
                f"{HOST_BIND_IP}:{HIVE_SERVER2_UI_EXTERNAL_PORT}:{HIVE_SERVER2_UI_INTERNAL_PORT}",
            ],
            "extra_hosts": [f"{DOCKER_INTERNAL_HOST}:host-gateway"],
            "dns": DOCKER_DNS,
            "deploy": {"resources": {"limits": {"cpus": "1.0", "memory": "768M"}}},
            "healthcheck": {
                "test": [
                    "CMD-SHELL",
                    f"bash -c '</dev/tcp/127.0.0.1/{HIVE_SERVER2_INTERNAL_PORT}'",
                ],
                "interval": "5s",
                "timeout": "5s",
                "retries": 30,
                "start_period": "15s",
            },
        },
    },
}

yaml_contents = yaml.dump(
    hive_compose_dict, default_flow_style=False, sort_keys=False, indent=4
)
with open(compose_filename, "w") as f:
    f.write(yaml_contents)

# %%
# # !docker exec namenode hdfs dfsadmin -safemode leave

# %%
if HIVE_START_FROM_SCRATCH:
    for hdfs_path in [
        "/tmp/hive/*",
        f"{HIVE_DATADIR}/managed/*",
        f"{HIVE_DATADIR}/external/*",
    ]:
        subprocess.run(
            [
                "docker", "exec", "-u", "hadoop", "namenode", "bash", "-c",
                f"hdfs dfs -rm -r -f -skipTrash {hdfs_path}",
            ],
            check=False,
        )

# !docker exec -u root namenode groupadd hive
# !docker exec -u root namenode useradd -m -g hive hive
# !docker exec -u root namenode usermod -aG sudo hive
# !docker exec -u root namenode usermod -aG hadoop hive
# !docker exec -u root namenode bash -c "grep -qFx 'hive ALL=(ALL) NOPASSWD:ALL' /etc/sudoers || echo 'hive ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers"
# # !docker exec -u root namenode bash -c "echo 'hive ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers"
# !docker exec -u hadoop namenode hdfs dfs -mkdir -p /tmp/hive
# !docker exec -u hadoop namenode hdfs dfs -chown -R hive:hive /tmp/hive
# !docker exec -u hadoop namenode hdfs dfs -chmod -R 777 /tmp
# !docker exec -u hadoop namenode hdfs dfs -mkdir -p {HIVE_DATADIR}/managed
# !docker exec -u hadoop namenode hdfs dfs -mkdir -p {HIVE_DATADIR}/external
# !docker exec -u hadoop namenode hdfs dfs -chown -R hive:hive {HIVE_USERDIR}
# !docker exec -u hadoop namenode hdfs dfs -chmod -R 777 {HIVE_USERDIR}

# %% vscode={"languageId": "markdown"}
# !docker compose -f hive-cluster.docker-compose.yml up -d --wait

# %% [markdown]
# # Add .beeline dir to hive $HOME

# %%
# !docker exec -u root hive-server2 bash -c "mkdir -p /home/hive/.beeline && chown -R hive:hive /home/hive"

# %% [markdown]
# # Load dist-lib (tez, common-collections) to HDFS

# %%
import os

print("--- CONSOLIDANDO Y EMPAQUETANDO DISTRIBUTED CACHE LIB ---")

# Script que se ejecutará DENTRO de hive-server2 para armar el paquete unificado
build_tar_script = """#!/bin/bash
set -e

echo "1. Creando directorio unificado /tmp/dist-lib..."
rm -rf /tmp/dist-lib/* /tmp/dist-lib.tar.gz
mkdir -p /tmp/dist-lib/conf
mkdir -p /tmp/dist-lib/lib

echo "2. Copiando Tez nativo..."
cp -r /opt/tez/* /tmp/dist-lib/

echo "3. Inyectando commons-collections y commons-lang de Hive..."
cp /opt/hive/lib/commons-collections-*.jar /opt/hive/lib/commons-lang-*.jar /tmp/dist-lib/lib/ 2>/dev/null || true

echo "4. Generando log4j seguro..."
cat << 'EOF' > /tmp/dist-lib/conf/tez-container-log4j.properties
tez.root.logger=INFO,CLA
log4j.rootLogger=${tez.root.logger}
log4j.appender.CLA=org.apache.log4j.ConsoleAppender
log4j.appender.CLA.Target=System.out
log4j.appender.CLA.layout=org.apache.log4j.PatternLayout
log4j.appender.CLA.layout.ConversionPattern=%d{ISO8601} [%p] [%t] |%c{2}|: %m%n
EOF

echo "5. Eliminando SLF4J duplicados para evitar conflictos..."
rm -f /tmp/dist-lib/lib/slf4j-*.jar

echo "6. Comprimiendo el paquete final..."
cd /tmp/dist-lib
# El asterisco asegura que no haya carpetas intermedias, tal como YARN lo espera
tar -czf /tmp/dist-lib.tar.gz *

echo "Paquete construido con éxito."
"""

with open("build_dist_lib_tar.sh", "w", newline='\n') as f:
    f.write(build_tar_script)

# Ejecutamos la construcción dentro de hive-server2
print("Construyendo el tarball dentro de hive-server2...")
# !docker cp build_dist_lib_tar.sh hive-server2:/tmp/build_dist_lib_tar.sh
# !docker exec -u root hive-server2 bash /tmp/build_dist_lib_tar.sh

# Extraemos SOLO el tarball ya terminado y lo pasamos al namenode
print("Transfiriendo el tarball terminado al Namenode...")
# !docker cp hive-server2:/tmp/dist-lib.tar.gz ./dist-lib.tar.gz
# !docker cp ./dist-lib.tar.gz namenode:/tmp/dist-lib.tar.gz

# Subimos a HDFS desde el namenode a la nueva ruta
print("Subiendo a HDFS en /apps/dist-lib ...")

# !docker exec namenode hdfs dfs -mkdir -p /apps/dist-lib
# !docker exec namenode hdfs dfs -rm -r -f /apps/dist-lib/*
# !docker exec namenode hdfs dfs -put /tmp/dist-lib.tar.gz /apps/dist-lib/dist-lib.tar.gz
# !docker exec namenode hdfs dfs -chmod -R 777 /apps/dist-lib

print("✅ ¡ÉXITO! Distributed Cache unificado y subido a HDFS en /apps/dist-lib.")

# 4. Limpieza ultra rápida
os.remove("./dist-lib.tar.gz")
os.remove("./build_dist_lib_tar.sh")
print("Archivos temporales locales limpios.")
