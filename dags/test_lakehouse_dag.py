from airflow.models.dag import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.standard.operators.python import PythonOperator
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

with DAG(
    'test_lakehouse_dag',
    default_args=default_args,
    description='A simple DAG to test Trino Iceberg integration',
    schedule_interval=None,
    catchup=False,
) as dag:

    create_schema = TrinoOperator(
        task_id='create_iceberg_schema',
        trino_conn_id='trino_default',
        sql="CREATE SCHEMA IF NOT EXISTS iceberg.default",
    )

    create_table = TrinoOperator(
        task_id='create_iceberg_table',
        trino_conn_id='trino_default',
        sql="""
            CREATE TABLE IF NOT EXISTS iceberg.default.test_table (
                id bigint,
                name varchar,
                created_at timestamp(6) with time zone
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['day(created_at)']
            )
        """,
    )

    insert_data = TrinoOperator(
        task_id='insert_sample_data',
        trino_conn_id='trino_default',
        sql="""
            INSERT INTO iceberg.default.test_table (id, name, created_at)
            VALUES 
                (1, 'Test User 1', current_timestamp),
                (2, 'Test User 2', current_timestamp)
        """,
    )

    query_data = TrinoOperator(
        task_id='query_sample_data',
        trino_conn_id='trino_default',
        sql="SELECT * FROM iceberg.default.test_table",
        handler=list,
    )

    create_schema >> create_table >> insert_data >> query_data
