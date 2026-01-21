import os
import json
import logging
import requests
import time
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from trino.dbapi import connect

default_args = {
    'owner': 'Rajesh Daggupati',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

logger = logging.getLogger(__name__)

def get_trino_conn():
    host = os.getenv("TRINO_HOST", "trino")
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

    return connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema,
        http_scheme=http_scheme,
        auth=auth,
    )

def _create_table_if_not_exists(**kwargs):
    # Establish Trino connection
    conn = get_trino_conn()
    cur = conn.cursor()

    # Create table with hour partitioning and explicit location
    sql = """
    CREATE TABLE IF NOT EXISTS iceberg.default.weather_raw (
        id VARCHAR,
        station_name VARCHAR,
        observation_time TIMESTAMP(6) WITH TIME ZONE,
        ingested_at TIMESTAMP(6) WITH TIME ZONE,
        raw_metadata VARCHAR
    )
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['hour(observation_time)'],
        location = 'file:/iceberg/warehouse/weather_raw'
    )
    """
    logger.info("Execute: %s", sql)
    cur.execute(sql)
    logger.info("Table iceberg.default.weather_raw ensured.")
    cur.close()
    conn.close()

def _fetch_and_load(start_time=None, end_time=None, **kwargs):
    # 1. Fetch data from ECCC API
    # Using the dag run's data interval to fetch specific window for backfill support
    # Ensure times are formatted correctly (Airflow passes ISO strings usually if templated, or Pendulum objs)
    # ECCC expects: YYYY-MM-DDTHH:MM:SSZ
    
    url = "https://api.weather.gc.ca/collections/swob-realtime/items?f=json&limit=100"
    
    if start_time and end_time:
        # If passed as string from template, they might be ISO.
        # Clean up if needed.
        time_filter = f"{start_time}/{end_time}"
        url += f"&datetime={time_filter}"
        
    logger.info("Fetching data from %s", url)
    
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    
    features = data.get('features', [])
    logger.info("Fetched %d records", len(features))
    
    if not features:
        logger.info("No data to insert.")
        return

    # 2. Prepare Insert
    conn = get_trino_conn()
    cur = conn.cursor()
    
    # We construct a parameterized batch insert or single generic insert.
    # Trino python client supports executemany but for simplicity and debugging, 
    # we'll build a values string carefully or loop.
    # Given volume is low (100 items), looping or constructing one big query is fine.
    # Let's construct a VALUES list.
    
    values_list = []
    
    for feat in features:
        props = feat.get('properties', {})
        
        # Extract fields
        # ID
        obs_id = props.get('id', 'unknown')
        
        # Station Name (stn_nam-value)
        # Some properties have -value suffix
        station_name = props.get('stn_nam-value', 'Unknown')
        
        # Observation Time (date_tm-value) -> e.g., 2025-01-15T23:00:00.000Z
        obs_time_str = props.get('date_tm-value')
        
        # Escape single quotes for SQL
        safe_id = obs_id.replace("'", "''")
        safe_station = station_name.replace("'", "''")
        
        # Store full raw JSON
        raw_json = json.dumps(feat).replace("'", "''")
        
        # Format: (id, station_name, observation_time, ingested_at, raw_metadata)
        # Trino TIMESTAMP expected format: TIMESTAMP '2025-01-15 23:00:00.000 Z' or ISO
        
        # Compute hour-level timestamp for partitioning
        if obs_time_str:
            obs_time_dt = datetime.strptime(obs_time_str, "%Y-%m-%dT%H:%M:%S%z")
            obs_time_hour_str = obs_time_dt.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S%z")
        else:
            obs_time_hour_str = None

        val_str = f"""(
            '{safe_id}',
            '{safe_station}',
            TIMESTAMP '{obs_time_str}',
            current_timestamp,
            '{raw_json}'
        )"""
        values_list.append(val_str)

    if values_list:
        # Batch into chunks if needed, but 100 is small enough for one go usually.
        # However, URL length limits might exist. Let's do chunks of 20.
        chunk_size = 20
        for i in range(0, len(values_list), chunk_size):
            chunk = values_list[i : i + chunk_size]
            values_str = ",\n".join(chunk)
            
            sql = f"""
            INSERT INTO iceberg.default.weather_raw 
            (id, station_name, observation_time, ingested_at, raw_metadata)
            VALUES {values_str}
            """
            
            logger.info("Inserting chunk %d-%d", i, i+len(chunk))
            try:
                cur.execute(sql)
            except Exception as e:
                logger.error("Failed to insert chunk: %s", e)
                raise

    cur.close()
    conn.close()

with DAG(
    'eccc_weather_extraction',
    default_args=default_args,
    description='Fetches ECCC weather data and loads to Trino/Iceberg',
    schedule='@hourly',
    catchup=False,
    tags=['weather', 'eccc', 'lakehouse']
) as dag:

    create_table_task = PythonOperator(
        task_id='create_table_if_not_exists',
        python_callable=_create_table_if_not_exists,
    )

    fetch_and_load_task = PythonOperator(
        task_id='fetch_and_load_weather_data',
        python_callable=_fetch_and_load,
        op_kwargs={
            "start_time": "{{ dag_run.conf.get('start_time') if dag_run and dag_run.conf and dag_run.conf.get('start_time') else data_interval_start.strftime('%Y-%m-%dT%H:%M:%SZ') }}",
            "end_time": "{{ dag_run.conf.get('end_time') if dag_run and dag_run.conf and dag_run.conf.get('end_time') else data_interval_end.strftime('%Y-%m-%dT%H:%M:%SZ') }}",
        }
    )

    create_table_task >> fetch_and_load_task
