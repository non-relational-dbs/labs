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
START_FROM_SCRATCH = False

# %%
NETWORK_MODE = NETWORK_MODE.strip().lower()
if NETWORK_MODE not in {"docker", "vpn"}:
    raise ValueError("NETWORK_MODE must be 'docker' or 'vpn'")
if NETWORK_MODE == "vpn" and not VPN_HOST_IP.startswith("10.15.20."):
    raise ValueError("VPN_HOST_IP must be in the 10.15.20.* subnet")

# %%
# Resolve module assets from the labs-setup root.
import os
import subprocess
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
CASSANDRA_START_FROM_SCRATCH = START_FROM_SCRATCH
DOCKER_INTERNAL_HOST = "host.docker.internal"
DOCKER_DNS = [VPN_DNS_IP] if NETWORK_MODE == "vpn" else []
HOST_BIND_IP = VPN_HOST_IP if NETWORK_MODE == "vpn" else "127.0.0.1"

CASSANDRA_CLUSTER_NAME = "cassandra-cluster"
CASSANDRA_TOTAL_NODES = 3

CASSANDRA_NODE_NAMES = [f"cassandra-node-{i+1}" for i in range(CASSANDRA_TOTAL_NODES)]
CASSANDRA_NODE_HOSTNAMES = [
    CASSANDRA_NODE_NAMES[i] if NETWORK_MODE == "docker" else VPN_HOST_IP
    for i in range(CASSANDRA_TOTAL_NODES)
]
CASSANDRA_NODE_IPS = CASSANDRA_NODE_HOSTNAMES
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

LOCALHOST_WORKDIR = str(MODULE_DIR.resolve())
DOCKER_MOUNTDIR = os.path.join(LOCALHOST_WORKDIR, "mount")
CASSANDRA_LOCALHOST_CLUSTER_CA_CERTDIR = os.path.join(LOCALHOST_WORKDIR, "cluster_certs")

mount_path = Path(DOCKER_MOUNTDIR)
mount_path.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# # Stop cassandra-cluster.docker-compose.yml

# %%
if CASSANDRA_START_FROM_SCRATCH:
    subprocess.run(
        ["docker", "compose", "-f", "cassandra-cluster.docker-compose.yml", "down", "-v"],
        check=False,
    )
else:
    print("Preserving existing containers and volumes")


# %%
import shutil

if CASSANDRA_START_FROM_SCRATCH:
    shutil.rmtree(DOCKER_MOUNTDIR, ignore_errors=True)
    shutil.rmtree(CASSANDRA_LOCALHOST_CLUSTER_CA_CERTDIR, ignore_errors=True)
    Path(DOCKER_MOUNTDIR).mkdir(parents=True, exist_ok=True)

# %% [markdown]
# # Cluster keystore generation

# %% [markdown]
# ### Create cluster CA certificate

# %%
ca_keystore_path = os.path.join(
    CASSANDRA_LOCALHOST_CLUSTER_CA_CERTDIR, "ca.keystore"
)
if not os.path.isfile(ca_keystore_path):
    Path(CASSANDRA_LOCALHOST_CLUSTER_CA_CERTDIR).mkdir(
        parents=True, exist_ok=True
    )
    ca_cert_gen_command = " && ".join(
        [
            # 1. Generate Private Key & Keystore
            f"keytool -genkeypair -alias ca-{CASSANDRA_CLUSTER_NAME} -keyalg RSA -keysize 4096 -ext bc:c -validity 3650 "
            f"-keystore /certs/ca.keystore -storepass {CASSANDRA_CA_CERT_PASSWORD} -keypass {CASSANDRA_CA_CERT_PASSWORD} "
            f"-dname 'CN=ca-{CASSANDRA_CLUSTER_NAME}, O=Uni'",
            # 2. Export CA cert
            f"keytool -export -alias ca-{CASSANDRA_CLUSTER_NAME} -file /certs/ca.crt "
            f"-keystore /certs/ca.keystore -storepass {CASSANDRA_CA_CERT_PASSWORD}",
        ]
    )
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--mount",
            f"type=bind,source={CASSANDRA_LOCALHOST_CLUSTER_CA_CERTDIR},target=/certs",
            "cassandra:5.0",
            "bash",
            "-c",
            ca_cert_gen_command,
        ],
        check=True,
    )
    assert os.path.isfile(ca_keystore_path), ca_keystore_path
    print("✅ Cassandra cluster CA certificate generated")

# %% [markdown]
# ### Create cassandra.yaml and cluster nodes certificates

# %%
import yaml

for i in range(0, CASSANDRA_TOTAL_NODES):

    if not os.path.exists(
        os.path.join(
            os.path.join(DOCKER_MOUNTDIR, CASSANDRA_NODE_NAMES[i]), "cassandra.yaml"
        )
    ):
        with open(
            os.path.join(LOCALHOST_WORKDIR, "cassandra.yaml"), "r"
        ) as cassandra_config_source_file:
            cassandra_config = yaml.load(
                cassandra_config_source_file, Loader=yaml.FullLoader
            )
            cassandra_config["cluster_name"] = CASSANDRA_CLUSTER_NAME
            cassandra_config["native_transport_port"] = CASSANDRA_NODE_RPC_PORTS[i]
            cassandra_config["authenticator"] = "PasswordAuthenticator"
            cassandra_config["authorizer"] = "CassandraAuthorizer"
            cassandra_config["server_encryption_options"] = (
                cassandra_config["server_encryption_options"] or {}
            )
            cassandra_config["server_encryption_options"].update(
                {
                    "internode_encryption": "all",
                    "enabled": True,
                    "keystore": "/etc/cassandra/certs/keystore.jks",
                    "keystore_password": CASSANDRA_NODE_CERT_PASSWORD,
                    "truststore": "/etc/cassandra/certs/truststore.jks",
                    "truststore_password": CASSANDRA_NODE_CERT_PASSWORD,
                    "require_client_auth": True,
                }
            )
            cassandra_config["client_encryption_options"] = (
                cassandra_config["client_encryption_options"] or {}
            )
            cassandra_config["client_encryption_options"].update(
                {
                    "enabled": False,
                }
            )
            Path(os.path.join(DOCKER_MOUNTDIR, CASSANDRA_NODE_NAMES[i])).mkdir(
                parents=True, exist_ok=True
            )
            with open(
                os.path.join(
                    os.path.join(DOCKER_MOUNTDIR, CASSANDRA_NODE_NAMES[i]),
                    "cassandra.yaml",
                ),
                "w",
            ) as node_cassandra_yaml_file:
                yaml.dump(
                    cassandra_config,
                    node_cassandra_yaml_file,
                    default_flow_style=False,
                    sort_keys=False,
                )
                
    if not os.path.exists(
        os.path.join(
            os.path.join(DOCKER_MOUNTDIR, CASSANDRA_NODE_NAMES[i]), "cassandra-rackdc.properties"
        )
    ):
        Path(os.path.join(DOCKER_MOUNTDIR, CASSANDRA_NODE_NAMES[i])).mkdir(
                parents=True, exist_ok=True
            )
        shutil.copyfile(
            os.path.join(LOCALHOST_WORKDIR, "cassandra-rackdc.properties"),
            os.path.join(
                os.path.join(DOCKER_MOUNTDIR, CASSANDRA_NODE_NAMES[i]),
                "cassandra-rackdc.properties",
            ),
        )

    node_certdir = os.path.join(DOCKER_MOUNTDIR, CASSANDRA_NODE_NAMES[i], "certs")
    node_keystore_path = os.path.join(node_certdir, "keystore.jks")
    if not os.path.isfile(node_keystore_path):
        Path(node_certdir).mkdir(parents=True, exist_ok=True)
        node_cmd = " && ".join(
            [
                # 1. Generate Private Key & Keystore
                f"keytool -genkeypair -alias {CASSANDRA_NODE_NAMES[i]} -keyalg RSA -keysize 4096 -validity 365 "
                f"-keystore /certs/keystore.jks -storepass {CASSANDRA_NODE_CERT_PASSWORD} -keypass {CASSANDRA_NODE_CERT_PASSWORD} "
                f"-dname 'CN={CASSANDRA_NODE_NAMES[i]}, OU=Lab, O=Uni, C=US'",
                # 2. Generate CSR
                f"keytool -certreq -alias {CASSANDRA_NODE_NAMES[i]} -file /certs/{CASSANDRA_NODE_NAMES[i]}.csr "
                f"-keystore /certs/keystore.jks -storepass {CASSANDRA_NODE_CERT_PASSWORD}",
                # 3. Sign the CSR with the CA Master (generating the .crt)
                f"keytool -gencert -alias ca-{CASSANDRA_CLUSTER_NAME} -infile /certs/{CASSANDRA_NODE_NAMES[i]}.csr -outfile /certs/{CASSANDRA_NODE_NAMES[i]}.crt "
                f"-keystore /cacerts/ca.keystore -storepass {CASSANDRA_CA_CERT_PASSWORD} -validity 365",
                # 4. Import CA into Keystore (to complete the chain)
                f"keytool -import -alias ca-{CASSANDRA_CLUSTER_NAME} -file /cacerts/ca.crt "
                f"-keystore /certs/keystore.jks -storepass {CASSANDRA_NODE_CERT_PASSWORD} -noprompt",
                # 5. Import the Signed Certificate back into Keystore
                f"keytool -import -alias {CASSANDRA_NODE_NAMES[i]} -file /certs/{CASSANDRA_NODE_NAMES[i]}.crt "
                f"-keystore /certs/keystore.jks -storepass {CASSANDRA_NODE_CERT_PASSWORD}",
                # 6. Create the Truststore (contains only the CA cert)
                f"keytool -import -alias ca-{CASSANDRA_CLUSTER_NAME} -file /cacerts/ca.crt "
                f"-keystore /certs/truststore.jks -storepass {CASSANDRA_NODE_CERT_PASSWORD} -noprompt",
                # 7. Save Private Key as independent file
                # f"openssl pkcs12 -in /certs/keystore.jks -nodes -nocerts -out /certs/{CASSANDRA_NODE_NAMES[i]}.key -passin pass:{CASSANDRA_NODE_CERT_PASSWORD}",
            ]
        )
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--mount",
                f"type=bind,source={node_certdir},target=/certs",
                "--mount",
                f"type=bind,source={CASSANDRA_LOCALHOST_CLUSTER_CA_CERTDIR},target=/cacerts,readonly",
                "--entrypoint",
                "bash",
                "cassandra:5.0",
                "-c",
                node_cmd,
            ],
            check=True,
        )
        assert os.path.isfile(node_keystore_path), node_keystore_path

# %% [markdown]
# # Start cassandra-cluster.docker-compose.yml

# %%
import os
import yaml
from IPython.display import Markdown, display

node_cpus = "2.0"
node_memory = "1G"
node_start_heap = "256M"
node_max_heap = "768M"
jmx_pass_path = os.path.join(DOCKER_MOUNTDIR, "jmxremote.password")
with open(jmx_pass_path, "w") as f:
    f.write(f"{CASSANDRA_INIT_USER} {CASSANDRA_INIT_USER}")

cassandra_compose_dict = {
    "name": "cassandra-cluster",
    "services": {},
    "networks": {
        "cassandra-cluster": {"name": "cassandra-network", "driver": "bridge"}
    },
}
cassandra_seeds = ",".join(
    f"{CASSANDRA_NODE_IPS[j]}:{CASSANDRA_NODE_GOSSIP_PORTS[j]}"
    for j in range(CASSANDRA_TOTAL_NODES)
)

for i in range(0, CASSANDRA_TOTAL_NODES):

    jvm_opts = " ".join(
        [
            f"-Xms{node_start_heap}",
            f"-Xmx{node_max_heap}",
            f"-Dcassandra.native_transport_port={CASSANDRA_NODE_RPC_PORTS[i]}",
            f"-Dcassandra.storage_port={CASSANDRA_NODE_GOSSIP_PORTS[i]}",
            f"-Dcassandra.ssl_storage_port={CASSANDRA_NODE_SSL_GOSSIP_PORTS[i]}",
            f"-Dcassandra.broadcast_rpc_address={CASSANDRA_NODE_HOSTNAMES[i]}",
            f"-Dcassandra.broadcast_address={CASSANDRA_NODE_HOSTNAMES[i]}",
            "-Dcom.sun.management.jmxremote.authenticate=false",
            "-Dcom.sun.management.jmxremote.ssl=false",
            f"-Dcassandra.jmx.remote.port={CASSANDRA_NODE_JMX_PORTS[i]}",
        ]
    )

    cassandra_compose_dict["services"][CASSANDRA_NODE_NAMES[i]] = {
        "image": "cassandra:5.0",
        "container_name": CASSANDRA_NODE_NAMES[i],
        "environment": [
            "CASSANDRA_USER=cassandra",  # Forces ownership check
            f"MAX_HEAP_SIZE={node_max_heap}",
            f"HEAP_NEWSIZE={node_start_heap}",
            f"CASSANDRA_CLUSTER_NAME={CASSANDRA_CLUSTER_NAME}",
            f"CASSANDRA_SEEDS={cassandra_seeds}",
            "CASSANDRA_LISTEN_ADDRESS=auto",
            "CASSANDRA_RPC_ADDRESS=0.0.0.0",  # Listen on all interfaces
            "CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch",
            f"CASSANDRA_NATIVE_TRANSPORT_PORT={CASSANDRA_NODE_RPC_PORTS[i]}",
            f"CASSANDRA_STORAGE_PORT={CASSANDRA_NODE_GOSSIP_PORTS[i]}",
            f"CASSANDRA_SSL_STORAGE_PORT={CASSANDRA_NODE_SSL_GOSSIP_PORTS[i]}",
            f"JVM_OPTS={jvm_opts}",
            f"CASSANDRA_BROADCAST_RPC_ADDRESS={CASSANDRA_NODE_IPS[i]}",
            f"CASSANDRA_BROADCAST_ADDRESS={CASSANDRA_NODE_IPS[i]}",
            "LOCAL_JMX=no",  # Setting to 'no' allows remote JMX
        ],
        "volumes": [
            f"{os.path.join(DOCKER_MOUNTDIR, CASSANDRA_NODE_NAMES[i])}:{CASSANDRA_WORKDIR}",
            f"{os.path.join(DOCKER_MOUNTDIR, CASSANDRA_NODE_NAMES[i], 'certs')}:/etc/cassandra/certs",
            f"{os.path.join(DOCKER_MOUNTDIR, CASSANDRA_NODE_NAMES[i], 'cassandra.yaml')}:/etc/cassandra/cassandra.yaml",
            f"{os.path.join(DOCKER_MOUNTDIR, CASSANDRA_NODE_NAMES[i], 'cassandra-rackdc.properties')}:/etc/cassandra/cassandra-rackdc.properties",
            f"{os.path.join(DOCKER_MOUNTDIR, 'jmxremote.password')}:/etc/cassandra/jmxremote.password",
        ],
        "networks": ["cassandra-cluster"],
        # "hostname": f"{CASSANDRA_NODE_HOSTNAMES[i]}",
        "ports": [
            f"{HOST_BIND_IP}:{CASSANDRA_NODE_RPC_PORTS[i]}:{CASSANDRA_NODE_RPC_PORTS[i]}",
            f"{HOST_BIND_IP}:{CASSANDRA_NODE_GOSSIP_PORTS[i]}:{CASSANDRA_NODE_GOSSIP_PORTS[i]}",
            f"{HOST_BIND_IP}:{CASSANDRA_NODE_SSL_GOSSIP_PORTS[i]}:{CASSANDRA_NODE_SSL_GOSSIP_PORTS[i]}",
            f"{HOST_BIND_IP}:{CASSANDRA_NODE_JMX_PORTS[i]}:{CASSANDRA_NODE_JMX_PORTS[i]}",
        ],
        "extra_hosts": [
            f"{DOCKER_INTERNAL_HOST}:host-gateway",
        ],
        "dns": DOCKER_DNS,
        "deploy": {"resources": {"limits": {"cpus": node_cpus, "memory": node_memory}}},
        "healthcheck": {
            "test": [
                "CMD-SHELL",
                f"env JVM_OPTS= nodetool -u {CASSANDRA_INIT_USER} -pw {CASSANDRA_INIT_PASSWORD} status | grep UN || exit 1",
            ],
            "interval": "5s",
            "timeout": "10s",
            "retries": 24,
            "start_period": "20s",
        },
        "depends_on": {
            CASSANDRA_NODE_NAMES[j]: {"condition": "service_started"}
            for j in range(0, i)
        },
    }

cassandra_compose_yaml_path = os.path.join(
    LOCALHOST_WORKDIR, "cassandra-cluster.docker-compose.yml"
)
cassandra_compose_yaml_contents = yaml.dump(
    cassandra_compose_dict, default_flow_style=False, sort_keys=False, indent=4
)
with open(cassandra_compose_yaml_path, "w") as f:
    f.write(cassandra_compose_yaml_contents)

print(f"Successfully created: '{os.path.relpath(cassandra_compose_yaml_path)}'")
display(Markdown(f"```yaml\n{cassandra_compose_yaml_contents}\n```"))

# %%
# !docker compose -f cassandra-cluster.docker-compose.yml up -d --wait

# %%
# # !docker exec -it cassandra-node-1 env CQLSH_PORT=9041 cqlsh -u cassandra -p cassandra
# # !docker exec -it cassandra-node-2 env CQLSH_PORT=9042 cqlsh -u cassandra -p cassandra
# # !docker exec -it cassandra-node-3 env CQLSH_PORT=9043 cqlsh -u cassandra -p cassandra

# # !docker exec cassandra-node-1 env JVM_OPTS="" nodetool -u cassandra -pw cassandra status
# # !docker exec cassandra-node-2 env JVM_OPTS="" nodetool -u cassandra -pw cassandra status
# # !docker exec cassandra-node-3 env JVM_OPTS="" nodetool -u cassandra -pw cassandra status

# # !docker exec cassandra-node-1 env JVM_OPTS="" nodetool -u cassandra -pw cassandra describecluster
# # !docker exec cassandra-node-2 env JVM_OPTS="" nodetool -u cassandra -pw cassandra describecluster
# # !docker exec cassandra-node-3 env JVM_OPTS="" nodetool -u cassandra -pw cassandra describecluster

# # !docker exec cassandra-node-1 env JVM_OPTS="" nodetool -u cassandra -pw cassandra repair --full system_auth
# # !docker exec cassandra-node-2 env JVM_OPTS="" nodetool -u cassandra -pw cassandra repair --full system_auth
# # !docker exec cassandra-node-3 env JVM_OPTS="" nodetool -u cassandra -pw cassandra repair --full system_auth

# # !docker exec -it cassandra-node-1 env CQLSH_PORT=9041 cqlsh -u cassandra -p cassandra -e "TRACING ON; SELECT * FROM generic_analytics.user_metrics LIMIT 1;"
# # !docker exec -it cassandra-node-2 env CQLSH_PORT=9042 cqlsh -u cassandra -p cassandra -e "TRACING ON; SELECT * FROM generic_analytics.user_metrics LIMIT 1"
# # !docker exec -it cassandra-node-3 env CQLSH_PORT=9043 cqlsh -u cassandra -p cassandra -e "TRACING ON; SELECT * FROM generic_analytics.user_metrics LIMIT 1;"
