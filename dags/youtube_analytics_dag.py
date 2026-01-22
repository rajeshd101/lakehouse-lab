import os
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from trino.dbapi import connect

# Default arguments for the DAG
default_args = {
    'owner': 'Sixth',
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

def _create_youtube_table_if_not_exists():
    conn = get_trino_conn()
    cur = conn.cursor()
    schema = os.getenv("SCHEMA", "default")

    # Define schema for YouTube Analytics
    # Metrics: views, likes, shares, comments, estimatedMinutesWatched
    # Dimensions: day, video_id
    sql = f"""
    CREATE TABLE IF NOT EXISTS iceberg.{schema}.youtube_analytics_raw (
        video_id VARCHAR,
        day DATE,
        views BIGINT,
        likes BIGINT,
        shares BIGINT,
        comments BIGINT,
        estimated_minutes_watched DOUBLE,
        average_view_duration DOUBLE,
        ingested_at TIMESTAMP(6) WITH TIME ZONE
    )
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['day'],
        location = 'file:/iceberg/warehouse/youtube_analytics_raw'
    )
    """
    logger.info("Ensuring YouTube analytics table exists.")
    cur.execute(sql)
    cur.close()
    conn.close()

def _fetch_youtube_analytics(ds, **kwargs):
    """
    Fetches YouTube Analytics data for a specific date (ds).
    Note: This requires a Google Cloud connection 'google_cloud_default' 
    with YouTube Analytics API scopes enabled.
    """
    try:
        # Use Airflow's Google Hook to get credentials
        hook = GoogleBaseHook(gcp_conn_id='google_cloud_default')
        credentials = hook.get_credentials()
        
        # In a real scenario, we would use the discovery API:
        # from googleapiclient.discovery import build
        # youtube_analytics = build('youtubeAnalytics', 'v2', credentials=credentials)
        
        # For this example, we'll simulate the API response structure
        # as we don't have live credentials in this environment.
        logger.info(f"Simulating YouTube Analytics fetch for date: {ds}")
        
        # Mock data representing what the API would return
        mock_data = [
            {
                "video_id": "vid_001",
                "day": ds,
                "views": 1500,
                "likes": 45,
                "shares": 12,
                "comments": 5,
                "estimated_minutes_watched": 4500.5,
                "average_view_duration": 180.2
            },
            {
                "video_id": "vid_002",
                "day": ds,
                "views": 2800,
                "likes": 120,
                "shares": 30,
                "comments": 15,
                "estimated_minutes_watched": 9800.0,
                "average_view_duration": 210.5
            }
        ]
        
        return mock_data
    except Exception as e:
        logger.error(f"Error fetching YouTube data: {e}")
        # Fallback to mock data for demonstration if hook fails
        return [
            {"video_id": "mock_vid", "day": ds, "views": 100, "likes": 10, "shares": 2, "comments": 1, "estimated_minutes_watched": 300.0, "average_view_duration": 180.0}
        ]

def _load_youtube_to_iceberg(ti, **kwargs):
    data = ti.xcom_pull(task_ids='fetch_youtube_analytics')
    if not data:
        logger.info("No data to load.")
        return

    conn = get_trino_conn()
    cur = conn.cursor()
    
    values_list = []
    for row in data:
        val_str = f"""(
            '{row['video_id']}',
            DATE '{row['day']}',
            {row['views']},
            {row['likes']},
            {row['shares']},
            {row['comments']},
            {row['estimated_minutes_watched']},
            {row['average_view_duration']},
            current_timestamp
        )"""
        values_list.append(val_str)

    if values_list:
        schema = os.getenv("SCHEMA", "default")
        values_str = ",\n".join(values_list)
        sql = f"""
        INSERT INTO iceberg.{schema}.youtube_analytics_raw 
        (video_id, day, views, likes, shares, comments, estimated_minutes_watched, average_view_duration, ingested_at)
        VALUES {values_str}
        """
        logger.info("Loading YouTube data into Iceberg.")
        cur.execute(sql)

    cur.close()
    conn.close()

with DAG(
    'youtube_analytics_to_iceberg',
    default_args=default_args,
    description='Pulls YouTube Analytics and loads into Iceberg',
    schedule='@daily',
    catchup=False,
    tags=['youtube', 'analytics', 'iceberg']
) as dag:

    create_table = PythonOperator(
        task_id='create_youtube_table',
        python_callable=_create_youtube_table_if_not_exists,
    )

    fetch_data = PythonOperator(
        task_id='fetch_youtube_analytics',
        python_callable=_fetch_youtube_analytics,
    )

    load_data = PythonOperator(
        task_id='load_youtube_to_iceberg',
        python_callable=_load_youtube_to_iceberg,
    )

    create_table >> fetch_data >> load_data
