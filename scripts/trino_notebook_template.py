"""
Notebook-style template for running Trino SQL queries.
Run with a Python kernel that has `trino` installed.
"""

# %%
import os
import logging
from typing import List, Any, Optional

import trino
from trino.auth import BasicAuthentication
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


# %%
def get_trino_config() -> dict:
    """Build Trino connection config from environment variables."""
    host = os.getenv("TRINO_HOST", "localhost")
    port = int(os.getenv("TRINO_PORT", "9080"))
    user = os.getenv("TRINO_USER", "admin")
    password = os.getenv("TRINO_PASSWORD")
    catalog = os.getenv("TRINO_CATALOG", "iceberg")
    schema = os.getenv("SCHEMA", "default")
    http_scheme = os.getenv("TRINO_HTTP_SCHEME", "http")

    auth = BasicAuthentication(user, password) if password else None
    return {
        "host": host,
        "port": port,
        "user": user,
        "catalog": catalog,
        "schema": schema,
        "http_scheme": http_scheme,
        "auth": auth,
    }


# %%
def run_trino_sql(sql: str, fetch: bool = True) -> Optional[List[Any]]:
    """Execute Trino SQL and optionally return rows."""
    config = get_trino_config()
    logger.info(
        "Connecting to Trino host=%s port=%s catalog=%s schema=%s",
        config["host"],
        config["port"],
        config["catalog"],
        config["schema"],
    )
    conn = trino.dbapi.connect(**config)
    cur = conn.cursor()
    logger.info("Executing SQL:\n%s", sql.strip())
    cur.execute(sql)
    if fetch:
        rows = cur.fetchall()
        logger.info("Fetched %s rows", len(rows))
        return rows
    return None


# %%
# Example: Create a schema and table, insert data, then read it back.
catalog = os.getenv("TRINO_CATALOG", "iceberg")
schema = os.getenv("SCHEMA", "default")
target = f"{catalog}.{schema}"

run_trino_sql(
    f"CREATE SCHEMA IF NOT EXISTS {target} WITH (location = 'file:/iceberg/warehouse/{schema}')",
    fetch=False,
)

run_trino_sql(
    f"""
    CREATE TABLE IF NOT EXISTS {target}.notebook_demo (
        id bigint,
        name varchar,
        created_at timestamp(6) with time zone
    )
    WITH (
        format = 'PARQUET'
    )
    """,
    fetch=False,
)

run_trino_sql(
    f"""
    INSERT INTO {target}.notebook_demo (id, name, created_at)
    VALUES
        (1, 'Notebook User 1', current_timestamp),
        (2, 'Notebook User 2', current_timestamp)
    """,
    fetch=False,
)

rows = run_trino_sql(f"SELECT * FROM {target}.notebook_demo ORDER BY id")
for row in rows or []:
    print(row)

# %%
