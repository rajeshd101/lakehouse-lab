# Airflow + dbt Project

This is a clean project structure for building an Airflow and dbt integration from scratch.

## Project Structure
- `dags/`: Airflow DAG definitions.
- `include/`: Shared utilities, scripts, and connection templates.
- `plugins/`: Airflow plugins.
- `Dockerfile`: Custom Docker image definition.
- `docker-compose.yml`: Infrastructure orchestration.
- `requirements.txt`: Python dependencies (Integrated with AWS, GCP, Azure, and Snowflake).
- `packages.txt`: System dependencies.

## Integrated Storage Providers
The project is pre-configured with dependencies for:
- **Amazon S3**: `apache-airflow-providers-amazon`, `boto3`
- **Google Cloud Storage**: `apache-airflow-providers-google`, `google-cloud-storage`
- **Azure Blob Storage**: `apache-airflow-providers-microsoft-azure`, `azure-storage-blob`
- **Snowflake**: `apache-airflow-providers-snowflake`, `snowflake-connector-python`, `snowflake-snowpark-python`

See `include/connection_templates.md` for setup instructions.
