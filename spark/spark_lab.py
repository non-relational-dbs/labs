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
MODULE_DIR = LABS_ROOT / "spark"
os.chdir(MODULE_DIR)
print(f"Working directory: {MODULE_DIR}")

# %%
DOCKER_INTERNAL_HOST = "host.docker.internal"
SPARK_VPN_DOMAIN = "mavasbel.vpn.itam.mx"

SPARK_DOCKER_BASE = "spark:3.5.7-scala2.12-java17-python3-ubuntu"
SPARK_JUPYTER_LAB_DOCKER_TAG = "spark-jupyter:3.5.7-scala2.12-java17-python3-ubuntu"
SPARK_JOB_VENV_DOCKER_TAG = "spark-job-venv:3.5.7-scala2.12-java17-python3-ubuntu"
SPARK_JOB_VENV_BUILD_DIR = "/opt/spark/venv-build"

SPARK_MASTER_NAME = "spark-master"
SPARK_MASTER_HOSTNAME = (
    SPARK_MASTER_NAME if NETWORK_MODE == "docker" else VPN_HOST_IP
)
SPARK_MASTER_WUBUI_PORT = 6080
SPARK_MASTER_PORT = 6077

SPARK_TOTAL_WORKERS = 3
SPARK_WORKER_NAMES = [f"spark-worker-{i+1}" for i in range(SPARK_TOTAL_WORKERS)]
SPARK_WORKER_HOSTNAMES = [
    SPARK_WORKER_NAMES[i] if NETWORK_MODE == "docker" else VPN_HOST_IP
    for i in range(SPARK_TOTAL_WORKERS)
]
SPARK_WORKER_IPS = SPARK_WORKER_HOSTNAMES
SPARK_WORKER_WEBUI_PORTS = [6080 + (i + 1) for i in range(SPARK_TOTAL_WORKERS)]

SPARK_WORKDIR = "/opt/spark/work-dir"

JUPYTER_LAB_NAME = "spark-jupyter"
JUPYTER_LAB_HOSTNAME = (
    "labs-spark-runner" if NETWORK_MODE == "docker" else VPN_HOST_IP
)
JUPYTER_LAB_PORT = 6888
JUPYTER_LAB_MONITOR_PORT = 4040
JUPYTER_LAB_TOKEN = ""

SPARK_SHARED_WORKSPACE = "shared-workspace"
SPARK_SHARED_WORKSPACE_DIR = f"/opt/spark/{SPARK_SHARED_WORKSPACE}"

# %%
HADOOP_NAMENODE_HOSTNAME = "namenode" if NETWORK_MODE == "docker" else VPN_HOST_IP
HADOOP_NAMENODE_IP = HADOOP_NAMENODE_HOSTNAME
HADOOP_NAMENODE_PORT = 8020

# %%
import os
from pathlib import Path

SPARK_DATADIR = Path(os.path.join(os.path.abspath(Path.cwd()), "data"))
SPARK_DATADIR.mkdir(parents=True, exist_ok=True)

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
from pyspark.sql import SparkSession
from datetime import datetime
from delta import configure_spark_with_delta_pip

os.environ["HADOOP_USER_NAME"] = "hadoop"

SPARK_DRIVER_PORT = 4050
SPARK_BLOCK_MANAGER_PORT = 4051
SPARK_JOB_ARCHIVE = MODULE_DIR / "mount" / JUPYTER_LAB_NAME / "spark_job_env.tar.gz"
SPARK_JOB_ARCHIVE_URI = SPARK_JOB_ARCHIVE.resolve().as_uri()
SPARK_ICEBERG_JAR_URI = (
    MODULE_DIR / "jars" / "iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
).resolve().as_uri()
SPARK_ICEBERG_WAREHOUSE = (
    f"file://{SPARK_SHARED_WORKSPACE_DIR}/iceberg-warehouse"
    if NETWORK_MODE == "docker"
    else f"hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}/iceberg-warehouse"
)
assert SPARK_JOB_ARCHIVE.is_file(), SPARK_JOB_ARCHIVE
builder = (
    SparkSession.builder.master(f"spark://{SPARK_MASTER_HOSTNAME}:{SPARK_MASTER_PORT}")
    .appName(f"SparkLab_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')}")
    .config("spark.archives", f"{SPARK_JOB_ARCHIVE_URI}#environment")
    .config("spark.driver.host", f"{JUPYTER_LAB_HOSTNAME}")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.driver.port", SPARK_DRIVER_PORT)
    .config("spark.blockManager.port", SPARK_BLOCK_MANAGER_PORT)
    .config("spark.driver.memory", "512m")
    .config(
        "spark.jars",
        SPARK_ICEBERG_JAR_URI,
    )
    .config("spark.executorEnv.PYSPARK_PYTHON", "./environment/bin/python3")
    .config("spark.executor.memory", "1G")
    .config(
        "spark.executorEnv.PYTHONPATH",
        f"./environment/lib/python{'.'.join(str(n) for n in sys.version_info[:2])}/site-packages",
    )
    .config(
        "spark.sql.catalog.local.warehouse",
        SPARK_ICEBERG_WAREHOUSE,
    )
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.warehouse.dir", os.path.abspath("spark-warehouse"))
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
spark = configure_spark_with_delta_pip(builder).getOrCreate()

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
df.write.mode("overwrite").parquet(f"hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}/tmp/faker_vectorized.parquet")
print(f"✅ Created hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}/tmp/faker_vectorized.parquet")


partition_stats = (
    df.withColumn("partition_id", F.spark_partition_id())
    .groupBy("worker", "partition_id")
    .count()
    .orderBy("worker", "partition_id")
)
# partition_stats.show()
display(partition_stats.toPandas())

# %%
from IPython.display import Markdown, display
from pyspark.sql import functions as F

# Read it back and check the schema/count

# df_verify = spark.read.parquet(f"{SPARK_DATADIR}/faker_vectorized.parquet").repartition(partitions)
# pdf_verify = pd.read_parquet(f"{SPARK_DATADIR}/faker_vectorized.parquet")
# df_verify = spark.createDataFrame(pdf_verify).repartition(partitions)
df_verify = spark.read.parquet(f"hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}/tmp/faker_vectorized.parquet").repartition(partitions)
verified_rows = df_verify.count()
assert verified_rows == total_rows, verified_rows
print(f"Generated rows: {verified_rows}")

print("\nFirst 10 by timestamp desc:")
# df_verify.sort(F.col("timestamp").desc()).show(10)
display(df_verify.sort(F.col("timestamp").desc()).toPandas())

print("\nFirst 10 by count(first_name) desc:")
# df_verify.groupBy("first_name").count().sort(F.col("count").desc()).show(10)
display(df_verify.groupBy("first_name").count().sort(F.col("count").desc()).toPandas())

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
delta_path = f"hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}/tmp/delta-products"
df_verify.limit(100).write.format("delta").mode("overwrite").save(delta_path)
assert spark.read.format("delta").load(delta_path).count() == 100

spark.sql("DROP TABLE IF EXISTS local.default.course_products")
spark.sql(
    "CREATE TABLE local.default.course_products "
    "USING iceberg AS SELECT * FROM df_verify LIMIT 100"
)
assert spark.table("local.default.course_products").count() == 100
print("✅ Delta Lake and Iceberg writes validated")

# %%
from IPython.display import Markdown, display

df_sparkql = spark.sql(f"""
    SELECT 
        first_name, 
        SUM(amount) as total_amount,
        COUNT(*) as first_name_count
    FROM parquet.`hdfs://{HADOOP_NAMENODE_HOSTNAME}:{HADOOP_NAMENODE_PORT}/tmp/faker_vectorized.parquet`
    GROUP BY first_name
    ORDER BY first_name_count DESC
""")
display(df_sparkql.toPandas())

spark.stop()
