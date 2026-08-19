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
DOCKER_DNS = ["10.15.20.1"]
VPN_HOST_IP = "10.15.20.2"

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
# ## Stop neo4j.docker-compose.yml

# %%
if NEO4J_START_FROM_SCRATCH:
    # !docker compose -f neo4j.docker-compose.yml down -v
else:
    print("Preserving existing containers and volumes")


# %%
import shutil

if NEO4J_START_FROM_SCRATCH:
    shutil.rmtree(DOCKER_MOUNTDIR)
    Path(DOCKER_MOUNTDIR).mkdir(parents=True, exist_ok=True)

# %% [markdown]
# # Start neo4j.docker-compose.yml

# %%
import os
import yaml
from IPython.display import Markdown, display

neo4j_compose_dict = {
    "name": "neo4j-compose",
    "services": {
        "neo4j": {
            "image": "neo4j:ubi9",
            "container_name": "neo4j-instance",
            "volumes": [
                f"{os.path.join(DOCKER_MOUNTDIR,"data")}:/data",
                f"{os.path.join(DOCKER_MOUNTDIR,"neo4j","logs")}:/logs",
                f"{os.path.join(DOCKER_MOUNTDIR,"neo4j","import")}:/var/lib/neo4j/import",
                f"{os.path.join(DOCKER_MOUNTDIR,"plugins")}:/plugins",
            ],
            "environment": {
                "NEO4J_AUTH": f"{NEO4J_INIT_USER}/{NEO4J_INIT_PASSWORD}",
                # NEO4J 5.x Memory Settings (Double underscores represent single underscores in .conf)
                "NEO4J_server_memory_heap_initial__size": "512m",
                "NEO4J_server_memory_heap_max__size": "1G",
                "NEO4J_server_memory_pagecache_size": "512m",
                "NEO4J_dbms_memory_heap_initial_size": "1024m",
                "NEO4J_dbms_memory_heap_max_size": "2G",
                # Plugin Configuration
                "NEO4J_PLUGINS": '["apoc", "graph-data-science"]',
                # Security: Allow APOC, GDS to run procedures
                "NEO4J_dbms_security_procedures_unrestricted": "apoc.*,gds.*",
                "NEO4J_dbms_security_procedures_allowlist": "apoc.*,gds.*",
                "NEO4J_server_config_strict__validation_enabled": "false",
                "NEO4J_server_bolt_enabled": "true",
            },
            "ports": [
                f"{VPN_HOST_IP}:{NEO4J_WEBUI_PORT}:{NEO4J_WEBUI_PORT}",
                f"{VPN_HOST_IP}:{NEO4J_PORT}:{NEO4J_PORT}",
            ],
            "dns": DOCKER_DNS,
            "restart": "unless-stopped",
            "deploy": {"resources": {"limits": {"cpus": "2.0", "memory": "2048M"}}},
        }
    },
}

neo4j_compose_yaml_path = os.path.join(LOCALHOST_WORKDIR, "neo4j.docker-compose.yml")
neo4j_compose_yaml_contents = yaml.dump(
    neo4j_compose_dict, default_flow_style=False, sort_keys=False, indent=4
)
with open(neo4j_compose_yaml_path, "w") as f:
    f.write(neo4j_compose_yaml_contents)

print(f"Successfully created: '{os.path.relpath(neo4j_compose_yaml_path)}'")
display(Markdown(f"```yaml\n{neo4j_compose_yaml_contents}\n```"))

# %%
# !docker compose -f neo4j.docker-compose.yml up -d --wait
