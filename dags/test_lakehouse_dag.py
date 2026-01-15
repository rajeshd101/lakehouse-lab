import os
import logging
import time
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from trino.dbapi import connect
from airflow.models import Variable, Param
from airflow.exceptions import AirflowSkipException, AirflowException

default_args = {
    'owner': 'Rajesh Daggupati',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


logger = logging.getLogger(__name__)


def _configure_logging():
    verbose = os.getenv("DAG_VERBOSE_LOGGING", "").strip().lower() in {"1", "true", "yes", "y"}
    if verbose:
        logger.setLevel(logging.DEBUG)
    return verbose


def _redact(value):
    if value:
        return "*****"
    return None


def run_trino_sql(sql, fetch=False):
    host = os.getenv("TRINO_HOST", "localhost")
    port = int(os.getenv("TRINO_PORT", "8080"))
    user = os.getenv("TRINO_USER", "admin")
    password = os.getenv("TRINO_PASSWORD") or None
    catalog = os.getenv("TRINO_CATALOG", "iceberg")
    schema = os.getenv("SCHEMA", "default")
    http_scheme = os.getenv("TRINO_HTTP_SCHEME", "http")

    verbose = _configure_logging()
    logger.info("Preparing Trino connection")
    logger.info(
        "Connection config host=%s port=%s user=%s catalog=%s schema=%s http_scheme=%s password=%s",
        host,
        port,
        user,
        catalog,
        schema,
        http_scheme,
        _redact(password),
    )
    if verbose:
        logger.debug("SQL to execute:\n%s", sql.strip())

    auth = None
    if password:
        from trino.auth import BasicAuthentication
        auth = BasicAuthentication(user, password)

    start = time.monotonic()
    conn = None
    cur = None
    try:
        conn = connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema,
            http_scheme=http_scheme,
            auth=auth,
        )
        cur = conn.cursor()
        logger.info("Executing SQL")
        cur.execute(sql)
        if fetch:
            rows = cur.fetchall()
            logger.info("Fetched %s rows", len(rows))
            return rows
        return None
    except Exception:
        logger.exception("Trino query failed")
        raise
    finally:
        elapsed = time.monotonic() - start
        logger.info("Trino query finished in %.3fs", elapsed)


with DAG(
    'test_lakehouse_dag',
    default_args=default_args,
    description='A simple DAG to test Trino Iceberg integration',
    schedule=None,
    catchup=False,
) as dag:
    catalog = os.getenv("TRINO_CATALOG", "iceberg")
    schema = os.getenv("SCHEMA", "default")
    target = f"{catalog}.{schema}"

    create_schema = PythonOperator(
        task_id='create_iceberg_schema',
        python_callable=run_trino_sql,
        op_kwargs={"sql": f"CREATE SCHEMA IF NOT EXISTS {target}"},
    )

    create_table = PythonOperator(
        task_id='create_iceberg_table',
        python_callable=run_trino_sql,
        op_kwargs={"sql": """
            CREATE TABLE IF NOT EXISTS {target}.test_table (
                id bigint,
                name varchar,
                created_at timestamp(6) with time zone
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['day(created_at)']
            )
        """.format(target=target)},
    )

    insert_data = PythonOperator(
        task_id='insert_sample_data',
        python_callable=run_trino_sql,
        op_kwargs={"sql": """
            INSERT INTO {target}.test_table (id, name, created_at)
            VALUES 
                (1, 'Test User 1', current_timestamp),
                (2, 'Test User 2', current_timestamp)
        """.format(target=target)},
    )

    query_data = PythonOperator(
        task_id='query_sample_data',
        python_callable=run_trino_sql,
        op_kwargs={"sql": f"SELECT * FROM {target}.test_table", "fetch": True},
    )

    create_schema >> create_table >> insert_data >> query_data
