# Connection Templates for Cloud Storage & Snowflake

This document provides templates for setting up Airflow connections for the integrated storage providers.

## 1. Amazon S3 (AWS)
- **Conn Id**: `aws_default`
- **Conn Type**: `Amazon Web Services`
- **Extra**:
```json
{
  "aws_access_key_id": "YOUR_ACCESS_KEY",
  "aws_secret_access_key": "YOUR_SECRET_KEY",
  "region_name": "us-east-1"
}
```

## 2. Google Cloud Storage (GCS)
- **Conn Id**: `google_cloud_default`
- **Conn Type**: `Google Cloud`
- **Keyfile JSON**: Paste your service account JSON key here.

## 3. Azure Blob Storage
- **Conn Id**: `azure_default`
- **Conn Type**: `Azure Blob Storage`
- **Extra**:
```json
{
  "connection_string": "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
}
```

## 4. Snowflake
- **Conn Id**: `snowflake_default`
- **Conn Type**: `Snowflake`
- **Host**: `your_account.snowflakecomputing.com`
- **Schema**: `YOUR_SCHEMA`
- **Login**: `YOUR_USER`
- **Password**: `YOUR_PASSWORD`
- **Extra**:
```json
{
  "account": "your_account",
  "warehouse": "COMPUTE_WH",
  "database": "YOUR_DB",
  "region": "us-west-2"
}
