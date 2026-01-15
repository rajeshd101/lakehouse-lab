"""
Unified Multi-Cloud Data Engineering Library - All utilities in one credential-free file
Supports: AWS S3, Google Cloud Storage, Azure Blob Storage, Trino, Snowflake, Glue
Import and use: from data_eng_lib import *
"""

import trino
try:
    import snowflake.connector
    from snowflake.snowpark import Session
    HAS_SNOWFLAKE = True
except ImportError:
    HAS_SNOWFLAKE = False
import boto3
import time
import os
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv
from typing import Dict, Any, Optional, List, Tuple
from google.cloud import storage as gcs
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureNamedKeyCredential

# =============================================================================
# TRINO UTILITIES
# =============================================================================

def execute_trino_query(query: str, config: Dict[str, Any]) -> List:
    """Execute Trino query using provided config."""
    conn = trino.dbapi.connect(**config)
    print(query)
    cursor = conn.cursor()
    print("Executing query for the first time...")
    cursor.execute(query)
    return cursor.fetchall()

def run_trino_query_dq_check(query: str, config: Dict[str, Any]) -> None:
    """Run Trino query with data quality checks."""
    results = execute_trino_query(query, config)
    if len(results) == 0:
        raise ValueError('The query returned no results!')
    for result in results:
        for column in result:
            if isinstance(column, bool):
                assert column is True, "Data quality check failed"

# =============================================================================
# SNOWFLAKE UTILITIES
# =============================================================================

def execute_snowflake_query(query: str, connection_params: Dict[str, Any]) -> List:
    """Execute Snowflake query using provided connection params."""
    if not HAS_SNOWFLAKE:
        raise ImportError("snowflake-connector-python is not installed. Please install it to use this function.")
    conn = snowflake.connector.connect(**connection_params)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    finally:
        cursor.close()
        conn.close()

def get_snowpark_session(connection_params: Dict[str, Any], schema: str = 'bootcamp') -> Any:
    """Create Snowpark session."""
    if not HAS_SNOWFLAKE:
        raise ImportError("snowflake-snowpark-python is not installed. Please install it to use this function.")
    params = connection_params.copy()
    params['schema'] = schema
    return Session.builder.configs(params).create()

def run_snowflake_query_dq_check(query: str, connection_params: Dict[str, Any]) -> None:
    """Run Snowflake query with data quality checks."""
    results = execute_snowflake_query(query, connection_params)
    if len(results) == 0:
        raise ValueError('The query returned no results!')
    for result in results:
        for column in result:
            if isinstance(column, bool):
                assert column is True, "Data quality check failed"

# =============================================================================
# AWS S3 UTILITIES
# =============================================================================

def upload_to_s3(local_file: str, bucket: str, s3_file: str, config: Optional[Dict] = None) -> Optional[str]:
    """Upload file to S3 using provided config or default credentials."""
    s3 = boto3.client('s3', **config) if config else boto3.client('s3')
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print(f"Upload Successful: {local_file} to s3://{bucket}/{s3_file}")
        return f's3://{bucket}/{s3_file}'
    except FileNotFoundError:
        print("The file was not found")
        return None
    except NoCredentialsError:
        print("AWS credentials not available")
        return None
    except ClientError as e:
        print(f"S3 Client error: {e}")
        return None

def download_from_s3(s3_file: str, local_file: str, bucket: str, config: Optional[Dict] = None) -> bool:
    """Download file from S3."""
    s3 = boto3.client('s3', **config) if config else boto3.client('s3')
    try:
        s3.download_file(bucket, s3_file, local_file)
        print(f"Download Successful: s3://{bucket}/{s3_file} to {local_file}")
        return True
    except Exception as e:
        print(f"S3 Download error: {e}")
        return False

# =============================================================================
# GOOGLE CLOUD STORAGE UTILITIES
# =============================================================================

def upload_to_gcs(local_file: str, bucket_name: str, gcs_file: str, config: Optional[Dict] = None) -> Optional[str]:
    """Upload file to Google Cloud Storage."""
    try:
        client = gcs.Client(**config) if config else gcs.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_file)
        blob.upload_from_filename(local_file)
        print(f"Upload Successful: {local_file} to gs://{bucket_name}/{gcs_file}")
        return f'gs://{bucket_name}/{gcs_file}'
    except Exception as e:
        print(f"GCS Upload error: {e}")
        return None

def download_from_gcs(gcs_file: str, local_file: str, bucket_name: str, config: Optional[Dict] = None) -> bool:
    """Download file from Google Cloud Storage."""
    try:
        client = gcs.Client(**config) if config else gcs.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_file)
        blob.download_to_filename(local_file)
        print(f"Download Successful: gs://{bucket_name}/{gcs_file} to {local_file}")
        return True
    except Exception as e:
        print(f"GCS Download error: {e}")
        return False

# =============================================================================
# AZURE BLOB STORAGE UTILITIES
# =============================================================================

def upload_to_azure(local_file: str, container: str, blob_name: str, config: Optional[Dict] = None) -> Optional[str]:
    """Upload file to Azure Blob Storage."""
    try:
        account_url = config.get('account_url') if config else os.getenv('AZURE_STORAGE_ACCOUNT_URL')
        account_key = config.get('account_key') if config else os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        
        blob_service_client = BlobServiceClient(account_url=account_url, credential=AzureNamedKeyCredential(name="accountkey", key=account_key))
        container_client = blob_service_client.get_container_client(container)
        with open(local_file, "rb") as data:
            blob_client = container_client.upload_blob(name=blob_name, data=data, overwrite=True)
        print(f"Upload Successful: {local_file} to az://{account_url}/{container}/{blob_name}")
        return f'az://{account_url}/{container}/{blob_name}'
    except Exception as e:
        print(f"Azure Upload error: {e}")
        return None

def download_from_azure(blob_name: str, local_file: str, container: str, config: Optional[Dict] = None) -> bool:
    """Download file from Azure Blob Storage."""
    try:
        account_url = config.get('account_url') if config else os.getenv('AZURE_STORAGE_ACCOUNT_URL')
        account_key = config.get('account_key') if config else os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        
        blob_service_client = BlobServiceClient(account_url=account_url, credential=AzureNamedKeyCredential(name="accountkey", key=account_key))
        container_client = blob_service_client.get_container_client(container)
        blob_client = container_client.get_blob_client(blob_name)
        with open(local_file, "wb") as download_file:
            download_stream = blob_client.download_blob()
            download_file.write(download_stream.readall())
        print(f"Download Successful: az://{account_url}/{container}/{blob_name} to {local_file}")
        return True
    except Exception as e:
        print(f"Azure Download error: {e}")
        return False

# =============================================================================
# MULTI-CLOUD UPLOAD HELPER
# =============================================================================

def upload_to_cloud(local_file: str, cloud: str, bucket_container: str, remote_file: str, config: Optional[Dict] = None) -> Optional[str]:
    """Unified upload to any cloud storage (s3://, gs://, az://)."""
    uri = f"{cloud}://{bucket_container}/{remote_file}"
    
    if cloud.lower() == 's3':
        return upload_to_s3(local_file, bucket_container, remote_file, config)
    elif cloud.lower() == 'gs':
        return upload_to_gcs(local_file, bucket_container, remote_file, config)
    elif cloud.lower() == 'az':
        return upload_to_azure(local_file, bucket_container, remote_file, config)
    else:
        raise ValueError(f"Unsupported cloud: {cloud}. Use 's3', 'gs', or 'az'")

# =============================================================================
# GLUE CATALOG UTILITIES
# =============================================================================

def query_glue_table_partitions(table_name: str, partition_path: str, config: Optional[Dict] = None) -> bool:
    """Query AWS Glue Data Catalog to check if partition exists."""
    glue_client = boto3.client('glue', **config) if config else boto3.client('glue')
    
    try:
        database_name, table_name_only = table_name.split('.')
        partition_values = {}
        if '=' in partition_path:
            key, value = partition_path.split('=', 1)
            partition_values[key] = value

        table_response = glue_client.get_table(DatabaseName=database_name, Name=table_name_only)
        partition_keys = table_response['Table'].get('PartitionKeys', [])
        
        if not partition_keys:
            print(f"Table {table_name} is not partitioned")
            return True

        partition_value_list = [partition_values.get(pk['Name']) for pk in partition_keys 
                              if pk['Name'] in partition_values]
        
        if len(partition_value_list) == len([pk for pk in partition_keys if pk['Name'] in partition_values]):
            glue_client.get_partition(DatabaseName=database_name, TableName=table_name_only, PartitionValues=partition_value_list)
            return True
        return False
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            return False
        raise

def poke_glue_partition(table: str, partition: str, config: Optional[Dict] = None, timeout: int = 3600) -> bool:
    """Poll until Glue partition appears."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if query_glue_table_partitions(table, partition, config):
            print(f"Partition {partition} for table {table} is now available")
            return True
        print(f'Partition {partition} for table {table} not found. Retrying in 60s...')
        time.sleep(60)
    print(f"Timeout waiting for partition {partition}")
    return False

# =============================================================================
# GLUE JOB MANAGEMENT (AWS Only)
# =============================================================================

def check_job_status(glue_client: Any, job_name: str, job_run_id: str) -> Dict:
    """Check Glue job run status."""
    response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
    return response['JobRun']

def create_glue_job(
    job_name: str, script_path: str, arguments: Dict[str, str], config: Optional[Dict] = None,
    s3_bucket: Optional[str] = None, description: str = 'Transform CSV data to Parquet format'
) -> str:
    """Create and run AWS Glue job with Iceberg support."""
    
    aws_region = config.get('region_name', 'us-west-2') if config else 'us-west-2'
    script_s3_path = upload_to_s3(script_path, s3_bucket, f'jobscripts/{os.path.basename(script_path)}', config)
    if not script_s3_path:
        raise ValueError('Uploading script to S3 failed!')

    glue_client = boto3.client("glue", **config) if config else boto3.client("glue")
    
    # Check if job exists, create if not
    try:
        glue_client.get_job(JobName=job_name)
        print(f"Job '{job_name}' exists. Running with new parameters.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print(f"Creating new job '{job_name}'")
            glue_client.create_job(
                Name=job_name,
                Description=description,
                Role='AWSGlueServiceRole', # Placeholder role
                Command={
                    'Name': 'glueetl',
                    'ScriptLocation': script_s3_path,
                    'PythonVersion': '3'
                },
                DefaultArguments={
                    '--job-language': 'python',
                    '--TempDir': f's3://{s3_bucket}/temporary/',
                    **arguments
                },
                GlueVersion='4.0'
            )
        else:
            raise
    
    run_response = glue_client.start_job_run(JobName=job_name, Arguments=arguments)
    print(f"Started Glue job run: {run_response['JobRunId']}")
    return job_name

# =============================================================================
# AWS SECRETS MANAGER
# =============================================================================

def get_secret(secret_name: str, config: Optional[Dict] = None, region_name: str = 'us-west-2') -> str:
    """Retrieve secret from AWS Secrets Manager."""
    client = boto3.client('secretsmanager', region_name=region_name, **config) if config else \
             boto3.client('secretsmanager', region_name=region_name)
    full_secret_name = f'airflow/variables/{secret_name}'
    
    try:
        response = client.get_secret_value(SecretId=full_secret_name)
        return response['SecretString']
    except ClientError as e:
        print(f"Failed to retrieve secret {secret_name}: {e.response['Error']['Code']}")
        raise

# =============================================================================
# CONFIG LOADER
# =============================================================================

def load_configs_from_env(prefix: str = "") -> Dict[str, Dict]:
    """Load all cloud configs from environment variables."""
    load_dotenv()
    
    configs = {}
    
    # AWS Config
    configs['aws'] = {
        'aws_access_key_id': os.getenv(f"{prefix}AWS_ACCESS_KEY_ID"),
        'aws_secret_access_key': os.getenv(f"{prefix}AWS_SECRET_ACCESS_KEY"),
        'region_name': os.getenv(f"{prefix}AWS_REGION", 'us-west-2')
    }
    
    # Google Cloud Config (uses service account JSON path or ADC)
    configs['gcs'] = {
        'credentials': os.getenv(f"{prefix}GOOGLE_APPLICATION_CREDENTIALS")
    } if os.getenv(f"{prefix}GOOGLE_APPLICATION_CREDENTIALS") else {}
    
    # Azure Config
    configs['azure'] = {
        'account_url': os.getenv(f"{prefix}AZURE_STORAGE_ACCOUNT_URL"),
        'account_key': os.getenv(f"{prefix}AZURE_STORAGE_ACCOUNT_KEY")
    }
    
    # Trino Config
    configs['trino'] = {
        'host': os.getenv(f"{prefix}TRINO_HOST"),
        'port': int(os.getenv(f"{prefix}TRINO_PORT", 443)),
        'user': os.getenv(f"{prefix}TRINO_USER"),
        'http_scheme': 'https',
        'catalog': os.getenv(f"{prefix}TRINO_CATALOG", 'academy'),
        'auth': trino.auth.BasicAuthentication(
            os.getenv(f"{prefix}TRINO_USER"), 
            os.getenv(f"{prefix}TRINO_PASSWORD")
        )
    }
    
    # Snowflake Config
    configs['snowflake'] = {
        "account": os.getenv(f"{prefix}SNOWFLAKE_ACCOUNT"),
        "user": os.getenv(f"{prefix}SNOWFLAKE_USER"), 
        "password": os.getenv(f"{prefix}SNOWFLAKE_PASSWORD"),
        "role": os.getenv(f"{prefix}SNOWFLAKE_ROLE"),
        'warehouse': os.getenv(f"{prefix}SNOWFLAKE_WAREHOUSE"),
        'database': os.getenv(f"{prefix}SNOWFLAKE_DATABASE")
    }
    
    return {k: {kk: vv for kk, vv in v.items() if vv} for k, v in configs.items()}

# =============================================================================
# USAGE EXAMPLES
# =============================================================================
if __name__ == "__main__":
    configs = load_configs_from_env()
    
    # Multi-cloud upload
    # upload_to_cloud("file.txt", "s3", "my-bucket", "data/file.txt", configs['aws'])
    # upload_to_cloud("file.txt", "gs", "my-gcs-bucket", "data/file.txt", configs['gcs'])
    # upload_to_cloud("file.txt", "az", "my-container", "data/file.txt", configs['azure'])
    
    # Database queries
    # run_trino_query_dq_check("SELECT TRUE as check", configs['trino'])
    # print(execute_snowflake_query("SELECT CURRENT_DATE()", configs['snowflake'])[0])
    
    print("✅ Multi-Cloud Data Engineering Library loaded successfully!")
    print("Usage: from data_eng_lib import *")
