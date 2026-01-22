from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Note: These paths should match the volume mounts in docker-compose.yml
DBT_PROJECT_DIR = "/usr/local/airflow/dbt_project"
DBT_PROFILES_DIR = "/usr/local/airflow/dbt_project"

with DAG(
    'dbt_run_weather_pipeline',
    default_args=default_args,
    description='A DAG to run dbt models for the weather pipeline',
    schedule=timedelta(days=1),
    catchup=False,
) as dag:

    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt deps',
    )

    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select silver_weather',
    )

    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select gold_weather_daily',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test',
    )

    dbt_deps >> dbt_run_silver >> dbt_run_gold >> dbt_test
