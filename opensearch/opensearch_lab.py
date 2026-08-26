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
# # OpenSearch: indexing, search, and analytics
#
# This lab validates the three-node OpenSearch cluster and performs real index,
# document, search, aggregation, and update operations. Run
# `opensearch_infra.ipynb` first and wait for all three nodes to become healthy.
#

# %% tags=["parameters"]
# Local mode is the portable default; VPN mode publishes services on this host.
NETWORK_MODE = "local"
VPN_HOST_IP = "10.15.20.100"
VPN_DNS_IP = "10.15.20.1"
VPN_DOMAIN = "vpn.itam.mx"
VPN_CLIENT_ALIAS = "mavasbel"

# %% [markdown]
# Validates the papermill-injected NETWORK_MODE and VPN_CLIENT_ALIAS values, accepting only lowercase local or vpn mode, and derives the student's VPN_CLIENT_DOMAIN for VPN addressing.

# %%
import re

NETWORK_MODE = NETWORK_MODE.strip().lower()
if NETWORK_MODE not in {"local", "vpn"}:
    raise ValueError("NETWORK_MODE must be 'local' or 'vpn'")
if NETWORK_MODE == "vpn" and not VPN_HOST_IP.startswith("10.15.20."):
    raise ValueError("VPN_HOST_IP must be in the 10.15.20.* subnet")
VPN_CLIENT_ALIAS = VPN_CLIENT_ALIAS.strip().lower()
if not VPN_CLIENT_ALIAS:
    raise ValueError("VPN_CLIENT_ALIAS must not be empty")
if re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", VPN_CLIENT_ALIAS) is None:
    raise ValueError(
        "VPN_CLIENT_ALIAS must contain only lowercase letters, digits, or hyphens "
        "and must not start or end with a hyphen"
    )
VPN_CLIENT_DOMAIN = f"{VPN_CLIENT_ALIAS}.{VPN_DOMAIN}"

# %% [markdown]
# Locates the labs-setup project root by walking up from the current directory until a pyproject.toml with the cassandra and mongodb module directories is found, then changes into the opensearch module directory.

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
MODULE_DIR = LABS_ROOT / "opensearch"
os.chdir(MODULE_DIR)
print(f"Working directory: {MODULE_DIR}")


# %% [markdown]
# Defines the three-node cluster endpoints on ports 9201-9203, the admin credentials used by opensearchpy, the course-products index name, and whether DELETE_INDEX_AT_END removes it during cleanup.

# %%
OPENSEARCH_NODE_NAMES = [f"opensearch-node-{i}" for i in range(1, 4)]
OPENSEARCH_PORTS = [9201, 9202, 9203]


def vpn_fqdn(container_name):
    return f"{container_name}.{VPN_CLIENT_DOMAIN}"


OPENSEARCH_HOSTS = [
    ("127.0.0.1" if NETWORK_MODE == "local" else vpn_fqdn(name), port)
    for name, port in zip(OPENSEARCH_NODE_NAMES, OPENSEARCH_PORTS)
]
OPENSEARCH_USERNAME = "admin"
OPENSEARCH_PASSWORD = "OpenSearchP455"
OPENSEARCH_INDEX = "course-products"
DELETE_INDEX_AT_END = False


# %% [markdown]
# ## 1. Connect securely to the lab cluster
#
# The local cluster uses OpenSearch's generated demonstration certificate. TLS is
# enabled, while certificate verification is disabled only for this isolated
# teaching environment.
#

# %%
from opensearchpy import OpenSearch, helpers

client = OpenSearch(
    hosts=[{"host": host, "port": port} for host, port in OPENSEARCH_HOSTS],
    http_auth=(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD),
    http_compress=True,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False,
    timeout=30,
)

cluster_health = client.cluster.health()
node_info = client.nodes.info()
node_count = len(node_info["nodes"])
assert node_count == 3, f"Expected 3 nodes, found {node_count}"
assert cluster_health["status"] in {"yellow", "green"}, cluster_health
print(
    f"Cluster {cluster_health['cluster_name']}: "
    f"status={cluster_health['status']}, nodes={node_count}"
)


# %% [markdown]
# ## 2. Create an index with explicit mappings
#
# The index uses three primary shards so data can be distributed across the
# three-node cluster. A replica protects each primary shard.
#

# %%
if client.indices.exists(index=OPENSEARCH_INDEX):
    client.indices.delete(index=OPENSEARCH_INDEX)

index_definition = {
    "settings": {
        "index": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
        }
    },
    "mappings": {
        "properties": {
            "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "description": {"type": "text"},
            "category": {"type": "keyword"},
            "price": {"type": "double"},
            "stock": {"type": "integer"},
            "created_at": {"type": "date"},
        }
    },
}

created = client.indices.create(index=OPENSEARCH_INDEX, body=index_definition)
assert created["acknowledged"] is True
print(f"Created index: {OPENSEARCH_INDEX}")


# %% [markdown]
# ## 3. Bulk-index a concrete product catalog
#

# %%
products = [
    {
        "id": "p-001",
        "name": "Mechanical Keyboard",
        "description": "Compact keyboard with tactile switches",
        "category": "peripherals",
        "price": 129.90,
        "stock": 18,
        "created_at": "2026-08-01T09:00:00Z",
    },
    {
        "id": "p-002",
        "name": "Wireless Mouse",
        "description": "Ergonomic mouse for long work sessions",
        "category": "peripherals",
        "price": 59.50,
        "stock": 42,
        "created_at": "2026-08-02T09:00:00Z",
    },
    {
        "id": "p-003",
        "name": "USB-C Dock",
        "description": "Dock with display, network, and charging ports",
        "category": "connectivity",
        "price": 189.00,
        "stock": 10,
        "created_at": "2026-08-03T09:00:00Z",
    },
    {
        "id": "p-004",
        "name": "27-inch Monitor",
        "description": "High resolution monitor for data analysis",
        "category": "displays",
        "price": 349.99,
        "stock": 7,
        "created_at": "2026-08-04T09:00:00Z",
    },
    {
        "id": "p-005",
        "name": "Portable Monitor",
        "description": "Lightweight secondary monitor with USB-C",
        "category": "displays",
        "price": 229.00,
        "stock": 13,
        "created_at": "2026-08-05T09:00:00Z",
    },
    {
        "id": "p-006",
        "name": "Network Adapter",
        "description": "USB-C adapter for wired gigabit networks",
        "category": "connectivity",
        "price": 39.90,
        "stock": 30,
        "created_at": "2026-08-06T09:00:00Z",
    },
]

actions = [
    {
        "_op_type": "index",
        "_index": OPENSEARCH_INDEX,
        "_id": product["id"],
        "_source": {key: value for key, value in product.items() if key != "id"},
    }
    for product in products
]
indexed_count, errors = helpers.bulk(client, actions, refresh=True)
assert not errors
assert indexed_count == len(products)
assert client.count(index=OPENSEARCH_INDEX)["count"] == len(products)
print(f"Indexed and refreshed {indexed_count} products")


# %% [markdown]
# ## 4. Full-text search
#
# Search the analyzed `name` and `description` fields. The assertions turn the
# example into a reproducible smoke test rather than a visual-only demonstration.
#

# %%
search_response = client.search(
    index=OPENSEARCH_INDEX,
    body={
        "query": {
            "multi_match": {
                "query": "monitor data",
                "fields": ["name^2", "description"],
            }
        },
        "sort": [{"_score": "desc"}, {"price": "asc"}],
    },
)

hits = search_response["hits"]["hits"]
assert len(hits) == 2, hits
for hit in hits:
    print(f"{hit['_id']}: score={hit['_score']:.3f} | {hit['_source']['name']}")


# %% [markdown]
# ## 5. Aggregate by category and average price
#

# %%
aggregation_response = client.search(
    index=OPENSEARCH_INDEX,
    body={
        "size": 0,
        "aggs": {
            "by_category": {
                "terms": {"field": "category"},
                "aggs": {"average_price": {"avg": {"field": "price"}}},
            }
        },
    },
)

buckets = aggregation_response["aggregations"]["by_category"]["buckets"]
category_counts = {bucket["key"]: bucket["doc_count"] for bucket in buckets}
assert category_counts == {"connectivity": 2, "displays": 2, "peripherals": 2}
for bucket in buckets:
    print(
        f"{bucket['key']}: count={bucket['doc_count']}, "
        f"average_price={bucket['average_price']['value']:.2f}"
    )


# %% [markdown]
# ## 6. Update and verify one document
#

# %%
client.update(
    index=OPENSEARCH_INDEX,
    id="p-004",
    body={"doc": {"stock": 5}},
    refresh=True,
)
updated = client.get(index=OPENSEARCH_INDEX, id="p-004")
assert updated["_source"]["stock"] == 5
print(f"Updated p-004 stock to {updated['_source']['stock']}")


# %% [markdown]
# ## 7. Inspect shard distribution
#
# The result confirms that the index's primary and replica shards are allocated
# across the cluster rather than remaining on one node.
#

# %%
import time

for _ in range(30):
    index_health = client.cluster.health(index=OPENSEARCH_INDEX)
    if index_health["status"] == "green" and index_health["active_shards"] == 6:
        break
    time.sleep(2)
assert index_health["status"] == "green", index_health
assert index_health["active_shards"] == 6, index_health

shards = client.cat.shards(index=OPENSEARCH_INDEX, format="json")
started = [shard for shard in shards if shard["state"] == "STARTED"]
started_primaries = [shard for shard in started if shard["prirep"] == "p"]
nodes = {shard["node"] for shard in started}
assert len(started_primaries) == 3, started_primaries
assert len(started) == 6, started
assert len(nodes) >= 2, nodes
for shard in shards:
    print(
        f"shard={shard['shard']} type={shard['prirep']} "
        f"state={shard['state']} node={shard['node']}"
    )


# %% [markdown]
# ## 8. Optional cleanup
#

# %%
if DELETE_INDEX_AT_END:
    deleted = client.indices.delete(index=OPENSEARCH_INDEX)
    assert deleted["acknowledged"] is True
    print(f"Deleted index: {OPENSEARCH_INDEX}")
else:
    print(f"Index retained for exploration: {OPENSEARCH_INDEX}")

client.close()

