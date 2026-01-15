import os
from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from trino.dbapi import connect
from datetime import datetime, timedelta
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


def run_trino_sql(sql, fetch=False):
    host = os.getenv("TRINO_HOST", "localhost")
    port = int(os.getenv("TRINO_PORT", "8080"))
    user = os.getenv("TRINO_USER", "admin")
    password = os.getenv("TRINO_PASSWORD") or None
    catalog = os.getenv("TRINO_CATALOG", "iceberg")
    schema = os.getenv("SCHEMA", "default")
    http_scheme = os.getenv("TRINO_HTTP_SCHEME", "http")

    auth = None
    if password:
        from trino.auth import BasicAuthentication
        auth = BasicAuthentication(user, password)

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
    cur.execute(sql)
    if fetch:
        return cur.fetchall()
    return None


with DAG(
    'test_lakehouse_dag',
    default_args=default_args,
    description='A simple DAG to test Trino Iceberg integration',
    schedule=None,
    catchup=False,
) as dag:

    create_schema = PythonOperator(
        task_id='create_iceberg_schema',
        python_callable=run_trino_sql,
        op_kwargs={"sql": "CREATE SCHEMA IF NOT EXISTS iceberg.default"},
    )

    create_table = PythonOperator(
        task_id='create_iceberg_table',
        python_callable=run_trino_sql,
        op_kwargs={"sql": """
            CREATE TABLE IF NOT EXISTS iceberg.default.test_table (
                id bigint,
                name varchar,
                created_at timestamp(6) with time zone
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['day(created_at)']
            )
        """},
    )

    insert_data = PythonOperator(
        task_id='insert_sample_data',
        python_callable=run_trino_sql,
        op_kwargs={"sql": """
            INSERT INTO iceberg.default.test_table (id, name, created_at)
            VALUES 
                (1, 'Test User 1', current_timestamp),
                (2, 'Test User 2', current_timestamp)
        """},
    )

    query_data = PythonOperator(
        task_id='query_sample_data',
        python_callable=run_trino_sql,
        op_kwargs={"sql": "SELECT * FROM iceberg.default.test_table", "fetch": True},
    )

    create_schema >> create_table >> insert_data >> query_data
