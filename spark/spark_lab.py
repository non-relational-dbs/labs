# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.5
#   kernelspec:
#     display_name: Python 3 (ipykernel)
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
# Validates the injected network parameters and derives `VPN_CLIENT_DOMAIN`: `NETWORK_MODE` must be lowercase `local` or `vpn`, VPN values must stay in the `10.15.20.*` subnet, and the client alias must be a valid single DNS label.

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
# Locates the `labs-setup` project root by searching upward for the directory that contains both `pyproject.toml` and the lab module folders, then changes the working directory into the `spark` module so relative asset paths resolve consistently.

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
# Declares the Spark cluster topology and mode-aware endpoints: image tags, master/worker names, client hosts (`127.0.0.1` in local mode, `*.VPN_CLIENT_DOMAIN` in VPN mode), Web UI ports, driver callback host (`host.docker.internal`, since executors run in Docker while this notebook driver runs on the host), executor environment paths, and the shared workspace mount.

# %%
DOCKER_INTERNAL_HOST = "host.docker.internal"
SPARK_LOCAL_HDFS_HOST = "namenode.lvh.me"

SPARK_DOCKER_BASE = "spark:3.5.7-scala2.12-java17-python3-ubuntu"
SPARK_JUPYTER_LAB_DOCKER_TAG = "spark-jupyter:3.5.7-scala2.12-java17-python3-ubuntu"
SPARK_JOB_VENV_DOCKER_TAG = "spark-job-venv:3.5.7-scala2.12-java17-python3-ubuntu"
SPARK_JOB_VENV_BUILD_DIR = "/opt/spark/venv-build"
SPARK_EXECUTOR_ENV_DIR = "/opt/spark/executor-env"

SPARK_MASTER_NAME = "spark-master"
SPARK_MASTER_HOSTNAME = SPARK_MASTER_NAME
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
SPARK_WORKER_IPS = [
    f"{name}.{VPN_CLIENT_DOMAIN}" if NETWORK_MODE == "vpn" else "127.0.0.1"
    for name in SPARK_WORKER_NAMES
]
SPARK_WORKER_WEBUI_PORTS = [6080 + (i + 1) for i in range(SPARK_TOTAL_WORKERS)]

SPARK_WORKDIR = "/opt/spark/work-dir"

JUPYTER_LAB_NAME = "spark-jupyter"
JUPYTER_LAB_HOSTNAME = JUPYTER_LAB_NAME
JUPYTER_LAB_CLIENT_HOST = (
    "127.0.0.1"
    if NETWORK_MODE == "local"
    else f"{JUPYTER_LAB_NAME}.{VPN_CLIENT_DOMAIN}"
)
# Executors run in Docker while this notebook driver runs on the host.
SPARK_DRIVER_HOST = DOCKER_INTERNAL_HOST
JUPYTER_LAB_PORT = 6888
JUPYTER_LAB_MONITOR_PORT = 4040
JUPYTER_LAB_TOKEN = ""

SPARK_SHARED_WORKSPACE = "shared-workspace"
SPARK_SHARED_WORKSPACE_DIR = f"/opt/spark/{SPARK_SHARED_WORKSPACE}"

# %% [markdown]
# Derives the mode-aware HDFS URI: local mode reaches the NameNode through the split-horizon name `namenode.lvh.me`, VPN mode through `namenode.<VPN_CLIENT_DOMAIN>`, both on RPC port 8020.

# %%
HADOOP_NAMENODE_HOSTNAME = "namenode"
HADOOP_NAMENODE_PORT = 8020
SPARK_HDFS_HOST = (
    SPARK_LOCAL_HDFS_HOST
    if NETWORK_MODE == "local"
    else f"{HADOOP_NAMENODE_HOSTNAME}.{VPN_CLIENT_DOMAIN}"
)
SPARK_HDFS_URI = f"hdfs://{SPARK_HDFS_HOST}:{HADOOP_NAMENODE_PORT}"

# %% [markdown]
# Creates the host-side shared workspace directory that is bind-mounted into the Spark containers and defines the `SPARK_DATADIR` HDFS path where the lab datasets will be written.

# %%
import os
from pathlib import Path

SPARK_HOST_SHARED_WORKSPACE = MODULE_DIR / "mount" / SPARK_SHARED_WORKSPACE
SPARK_HOST_SHARED_WORKSPACE.mkdir(parents=True, exist_ok=True)
SPARK_DATADIR = f"{SPARK_HDFS_URI}/spark-lab/data"

# %% [markdown]
# ##### Cleaning Spark context

# %%
import pyspark
from pyspark import SparkContext

sc = SparkContext._active_spark_context
print(f"    PySpark Version: {pyspark.__version__}")

if sc is not None:
    sc.stop()
    print("🧹 Ghost SparkContext cleaned up.")
else:
    print("✨ No existing SparkContext to clean.")

# %% [markdown]
# # Spark session

# %%
import sys
import hashlib
from pyspark.sql import SparkSession
from datetime import datetime

os.environ["HADOOP_USER_NAME"] = "hadoop"
java_security_option = "-Djava.security.manager=allow"
java_tool_options = os.environ.get("JAVA_TOOL_OPTIONS", "")
if java_security_option not in java_tool_options.split():
    os.environ["JAVA_TOOL_OPTIONS"] = f"{java_tool_options} {java_security_option}".strip()

SPARK_DRIVER_PORT = 4050
SPARK_BLOCK_MANAGER_PORT = 4051
SPARK_JOB_ARCHIVE = SPARK_HOST_SHARED_WORKSPACE / "spark_job_env.tar.gz"
SPARK_JARS_DIR = MODULE_DIR / "jars"
SPARK_JARS = {
    "iceberg-spark-runtime-3.5_2.12-1.6.1.jar": "87e7184f31ef0caac415bbdfcf1bc4943346a58b98d747dc83434f7139e12acb",
    "delta-spark_2.12-3.2.0.jar": "51d473537d1bc10c81f48b03d8e2a6b604e1b421a70835ec12e917a4245a31d5",
    "delta-storage-3.2.0.jar": "58aab63eba7736fea9e03eafb0dde6704a34a70f570c1a69ab8e4012c25a95d4",
    "antlr4-runtime-4.9.3.jar": "131a6594969bc4f321d652ea2a33bc0e378ca312685ef87791b2c60b29d01ea5",
}
for jar_name, expected_sha256 in SPARK_JARS.items():
    jar_path = SPARK_JARS_DIR / jar_name
    if not jar_path.is_file():
        raise FileNotFoundError(f"Required versioned Spark JAR is missing: {jar_path}")
    actual_sha256 = hashlib.sha256(jar_path.read_bytes()).hexdigest()
    if actual_sha256 != expected_sha256:
        raise ValueError(
            f"Spark JAR checksum mismatch for {jar_path}: "
            f"expected {expected_sha256}, got {actual_sha256}"
        )

SPARK_DRIVER_CLASSPATH = os.pathsep.join(
    str(SPARK_JARS_DIR / jar_name) for jar_name in SPARK_JARS
)
SPARK_EXECUTOR_CLASSPATH = ":".join(
    f"/opt/spark/course-jars/{jar_name}" for jar_name in SPARK_JARS
)
SPARK_ICEBERG_WAREHOUSE = f"{SPARK_HDFS_URI}/spark-lab/iceberg-warehouse"
assert SPARK_JOB_ARCHIVE.is_file(), SPARK_JOB_ARCHIVE
builder = (
    SparkSession.builder.master(f"spark://{SPARK_MASTER_CLIENT_HOST}:{SPARK_MASTER_PORT}")
    .appName(f"SparkLab_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')}")
    .config("spark.driver.host", SPARK_DRIVER_HOST)
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.driver.port", SPARK_DRIVER_PORT)
    .config("spark.blockManager.port", SPARK_BLOCK_MANAGER_PORT)
    .config("spark.driver.memory", "512m")
    .config("spark.driver.maxResultSize", "256m")
    .config("spark.task.maxDirectResultSize", "64m")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .config("spark.driver.extraClassPath", SPARK_DRIVER_CLASSPATH)
    .config("spark.executor.extraClassPath", SPARK_EXECUTOR_CLASSPATH)
    .config(
        "spark.executorEnv.PYSPARK_PYTHON",
        f"{SPARK_EXECUTOR_ENV_DIR}/bin/python3",
    )
    .config("spark.executor.memory", "1G")
    .config(
        "spark.executorEnv.PYTHONPATH",
        f"{SPARK_EXECUTOR_ENV_DIR}/lib/python{'.'.join(str(n) for n in sys.version_info[:2])}/site-packages",
    )
    .config(
        "spark.sql.catalog.local.warehouse",
        SPARK_ICEBERG_WAREHOUSE,
    )
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config(
        "spark.sql.warehouse.dir",
        f"{SPARK_HDFS_URI}/spark-lab/spark-warehouse",
    )
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,"
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .enableHiveSupport()
)
spark = builder.getOrCreate()

print(f"Spark Version: {spark.version}")
print(f"Scala Version: {spark._jvm.scala.util.Properties.versionString()}")
print("✅ Spark Session is now active.")

# %% [markdown]
# # Data generation

# %%
total_rows = 10000
partitions = 10


def batch_generator(ids):
    import socket
    import random
    from faker import Faker

    node_name = socket.gethostname()
    faker = Faker()
    for _ in ids:
        yield (
            faker.uuid4(),
            node_name,
            faker.date_time(),
            faker.first_name(),
            faker.last_name(),
            faker.email(),
            faker.basic_phone_number(),
            random.random() * 1000.0,
        )

df_column_names = [
    "id",
    "worker",
    "timestamp",
    "first_name",
    "last_name",
    "email",
    "phone",
    "amount",
]
df_column_types = spark.createDataFrame(
    list(batch_generator(range(1))), schema=df_column_names
).schema
print(f"✅ batch_generator schema: {df_column_types}")

# %% [markdown]
# Generates 10,000 rows distributed over 10 partitions with a Faker-based batch generator executed on the workers, writes them as CSV to HDFS, and displays per-worker/per-partition row counts to prove the work was truly distributed.

# %%
from pyspark.sql import functions as F
from IPython.display import Markdown, display

df = spark.createDataFrame(
    list(batch_generator(range(total_rows))), df_column_names
).repartition(partitions)
df.write.mode("overwrite").csv(f"{SPARK_DATADIR}/faker.csv")
print(f"✅ Created {SPARK_DATADIR}/faker.csv")

partition_stats = (
    df.withColumn("partition_id", F.spark_partition_id())
    .groupBy("worker", "partition_id")
    .count()
    .orderBy("worker", "partition_id")
)
# partition_stats.show()
display(partition_stats.toPandas())


# %% [markdown]
# Regenerates the same dataset through the RDD API (`parallelize` + `mapPartitions`) and writes it as Parquet to HDFS, again displaying per-partition statistics for comparison with the DataFrame path.

# %%
from IPython.display import Markdown, display

rdd = spark.sparkContext.parallelize(range(total_rows), partitions).mapPartitions(
    batch_generator
)
df = rdd.toDF(df_column_names)
df.write.mode("overwrite").parquet(f"{SPARK_DATADIR}/faker.parquet")
print(f"✅ Created {SPARK_DATADIR}/faker.parquet")

partition_stats = (
    df.withColumn("partition_id", F.spark_partition_id())
    .groupBy("worker", "partition_id")
    .count()
    .orderBy("worker", "partition_id")
)
# partition_stats.show()
display(partition_stats.toPandas())

# %% [markdown]
# Demonstrates a vectorized pandas UDF: the generator runs as Arrow-batched Pandas DataFrames inside `spark.range`, producing the dataset in a third way, which is then written to Parquet under `/tmp` in HDFS with partition stats displayed.

# %%
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(df_column_types)
def generate_batch_vectorized(batch_ser: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(list(batch_generator(batch_ser)))


df: DataFrame = (
    spark.range(total_rows, numPartitions=partitions)
    .withColumn("data", generate_batch_vectorized("id"))
    .select("data.*")
)
# df.write.mode("overwrite").parquet(f"{SPARK_DATADIR}/faker_vectorized.parquet")
# df.coalesce(1).write.mode("overwrite").parquet(f"{SPARK_DATADIR}/faker_vectorized.parquet")
# pdf = df.toPandas()
# pdf.to_parquet(f"{SPARK_DATADIR}/faker_vectorized.parquet", index=False)
df.write.mode("overwrite").parquet(f"{SPARK_HDFS_URI}/tmp/faker_vectorized.parquet")
print(f"✅ Created {SPARK_HDFS_URI}/tmp/faker_vectorized.parquet")


partition_stats = (
    df.withColumn("partition_id", F.spark_partition_id())
    .groupBy("worker", "partition_id")
    .count()
    .orderBy("worker", "partition_id")
)
# partition_stats.show()
display(partition_stats.toPandas())

# %% [markdown]
# Reads the vectorized Parquet output back from HDFS, asserts the row count matches, and displays the 10 most recent records plus the 10 most frequent first names as verification tables.

# %%
from IPython.display import Markdown, display
from pyspark.sql import functions as F

# Read it back and check the schema/count

# df_verify = spark.read.parquet(f"{SPARK_DATADIR}/faker_vectorized.parquet").repartition(partitions)
# pdf_verify = pd.read_parquet(f"{SPARK_DATADIR}/faker_vectorized.parquet")
# df_verify = spark.createDataFrame(pdf_verify).repartition(partitions)
df_verify = spark.read.parquet(f"{SPARK_HDFS_URI}/tmp/faker_vectorized.parquet").repartition(partitions)
verified_rows = df_verify.count()
assert verified_rows == total_rows, verified_rows
print(f"Generated rows: {verified_rows}")

print("\nFirst 10 by timestamp desc:")
# df_verify.sort(F.col("timestamp").desc()).show(10)
display(df_verify.sort(F.col("timestamp").desc()).limit(10).toPandas())

print("\nFirst 10 by count(first_name) desc:")
# df_verify.groupBy("first_name").count().sort(F.col("count").desc()).show(10)
display(
    df_verify.groupBy("first_name")
    .count()
    .sort(F.col("count").desc())
    .limit(10)
    .toPandas()
)

# %% [markdown]
# Registers the verified DataFrame as a temporary view and runs a Spark SQL aggregation computing total amount and record count per first name, ordered by frequency.

# %%
from IPython.display import Markdown, display

df_verify.createOrReplaceTempView("df_verify")
df_sparkql = spark.sql("""
    SELECT 
        first_name, 
        SUM(amount) as total_amount,
        COUNT(*) as first_name_count
    FROM df_verify
    GROUP BY first_name
    ORDER BY first_name_count DESC
""")
display(df_sparkql.toPandas())

# %% [markdown]
# # Delta Lake and Iceberg validation

# %%
delta_path = f"{SPARK_HDFS_URI}/tmp/delta-products"
df_verify.limit(100).write.format("delta").mode("overwrite").save(delta_path)
assert spark.read.format("delta").load(delta_path).count() == 100

spark.sql("DROP TABLE IF EXISTS local.default.course_products")
spark.sql(
    "CREATE TABLE local.default.course_products "
    "USING iceberg AS SELECT * FROM df_verify LIMIT 100"
)
assert spark.table("local.default.course_products").count() == 100
print("✅ Delta Lake and Iceberg writes validated")

# %% [markdown]
# Queries the Parquet file directly with Spark SQL (no temporary view) using the same aggregation, then stops the Spark session to release the cluster.

# %%
from IPython.display import Markdown, display

df_sparkql = spark.sql(f"""
    SELECT 
        first_name, 
        SUM(amount) as total_amount,
        COUNT(*) as first_name_count
    FROM parquet.`{SPARK_HDFS_URI}/tmp/faker_vectorized.parquet`
    GROUP BY first_name
    ORDER BY first_name_count DESC
""")
display(df_sparkql.toPandas())

spark.stop()
