# Lakehouse Lab: Airflow, Trino, Iceberg, and Spark
**Author:** Rajesh Daggupati

This project provides a complete, containerized Lakehouse environment for data engineering, featuring Airflow for orchestration, Trino for federated SQL queries, Apache Iceberg for table management, and Spark for distributed processing.

## 🚀 Quick Start

### Prerequisites
- **Docker** and **Docker Compose** installed.
- At least **8GB of RAM** allocated to Docker.
- **Cloud Credentials:** If you plan to connect to Snowflake, AWS, Azure, GCP, or Databricks, copy `.env.example` to `.env` and fill in your credentials.

### Cloud Connection Setup
1. **Copy the environment template:**
   ```bash
   cp .env.example .env
   ```
2. **Configure your providers:**
   - **Iceberg:** Set `ENABLE_ICEBERG=true` (default: true).
   - **SQL Server:** Set `ENABLE_SQLSERVER=true` (default: true).
   - **Oracle:** Set `ENABLE_ORACLE=true` (default: true).
   - **Snowflake:** Set `ENABLE_SNOWFLAKE=true` and fill in `SNOWFLAKE_*` variables.
   - **AWS (S3/Glue):** Fill in `AWS_*` variables for S3 storage and Glue Metastore access.
   - **GCP (GCS):** Provide the path to your service account JSON in `GOOGLE_APPLICATION_CREDENTIALS`.
   - **Azure:** Fill in `AZURE_STORAGE_*` variables for Blob Storage integration.
   - **Databricks:** Set `ENABLE_DATABRICKS=true` and fill in `DATABRICKS_*` variables.

> **Note:** Catalogs are only loaded if their respective `ENABLE_*` flag is set to `true` in the `.env` file. If a flag is set to `false`, the corresponding catalog configuration will be removed from Trino during startup.

### Running the Project

#### 🐧 Linux / macOS / WSL2
1. **Clone and Enter:**
   ```bash
   git clone https://github.com/rajeshd101/lakehouse-lab.git
   cd lakehouse-lab
   ```
2. **Start Stack:**
   ```bash
   docker-compose up -d
   ```
3. **Verify:**
   ```bash
   docker-compose ps
   ```

#### 🪟 Windows (PowerShell / CMD)
1. **Clone and Enter:**
   ```powershell
   git clone https://github.com/rajeshd101/lakehouse-lab.git
   cd lakehouse-lab
   ```
2. **Start Stack:**
   ```powershell
   docker-compose up -d
   ```
3. **Verify:**
   ```powershell
   docker-compose ps
   ```

## 🛠 Service Access & Credentials

| Service | URL / Port | Credentials |
| :--- | :--- | :--- |
| **Airflow Webserver** | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` |
| **Trino UI** | [http://localhost:9080](http://localhost:9080) | `admin` (no password) |
| **Spark Master UI** | [http://localhost:4040](http://localhost:4040) | N/A |
| **Query UI (SQL Editor)** | [http://localhost:5001](http://localhost:5001) | N/A |
| **SQL Server** | `localhost:1433` | `sa` / `Sqlserver!Password123` |
| **Oracle DB** | `localhost:1521` | `system` / `Oracle!Password123` |

## 📂 Project Structure
- `dags/`: Airflow DAG definitions (e.g., ECCC weather extraction).
- `include/`: Shared utilities, including the `data_eng_lib.py` multi-cloud library.
- `trino/catalog/`: Configuration files for Trino connectors (Iceberg, SQL Server, Oracle).
- `DWH/`: Local persistent storage for the Iceberg warehouse and database data.
- `dbt_project/`: dbt models and configurations for data transformations.
- `tools/`: Local CLI tools, including a Trino CLI wrapper.

## 🏗 Architecture Overview
This lab implements a **Medallion Architecture** (Bronze/Silver/Gold) using:
- **Ingestion:** Airflow DAGs fetch data from APIs (like ECCC Weather) or relational sources (SQL Server/Oracle).
- **Storage:** Data is stored in **Apache Iceberg** format on a local filesystem, managed by a **Hive Metastore**.
- **Querying:** **Trino** acts as the central gateway, allowing you to join data across Iceberg, SQL Server, and Oracle in a single SQL query.
- **Transformation:** **dbt** runs on top of Trino to transform raw data into analytics-ready tables.

### Detailed Architecture Diagram

```text
                                     +---------------------------------------------------+
                                     |              Airflow Orchestrator                 |
                                     |       (DAGs, Scheduler, Worker, Webserver)        |
                                     +---+-------------------+-------------------+-------+
                                         |                   |                   |
                  (1) INGEST             | (2) PROCESS       | (3) TRANSFORM     | (4) MONITOR
                  +----------------------+                   |                   +----------------------+
                  |                                          |                                          |
                  v                                          v                                          v
      +-----------+-----------+             +----------------+----------------+             +-----------+-----------+
      | External Data Sources |             |         Spark Engine            |             |     dbt (on Trino)    |
      | (APIs, S3, GCS, Azure)|             |    (Distributed Processing)     |             |  (Data Transformations)|
      +-----------+-----------+             +----------------+----------------+             +-----------+-----------+
                  |                                          |                                          |
                  | (Raw Data)                               | (Iceberg API / Thrift)                   | (SQL / JDBC)
                  v                                          v                                          v
      +-----------+------------------------------------------+------------------------------------------+-----------+
      |                                             Trino Gateway                                                   |
      |                                       (Federated Query Engine)                                              |
      +-----------+------------------------------------------+------------------------------------------+-----------+
                  |                                          |                                          |
                  | (JDBC Connector)                         | (Iceberg Connector)                      | (JDBC Connector)
                  v                                          v                                          v
      +-----------+-----------+             +----------------+----------------+             +-----------+-----------+
      |   SQL Server Catalog  |             |        Iceberg Catalog          |             |     Oracle Catalog    |
      |  (Relational Source)  |             |       (Lakehouse Tables)        |             |  (Relational Source)  |
      +-----------+-----------+             +----------------+----------------+             +-----------+-----------+
                  |                                          |                                          |
                  | (Port 1433)                              | (Port 9083)                              | (Port 1521)
                  v                                          v                                          v
      +-----------+-----------+             +----------------+----------------+             +-----------+-----------+
      |  SQL Server Container |             |        Hive Metastore           |             |    Oracle Container   |
      |     (mssql-server)    |             |     (Metadata Management)       |             |      (oracle-free)    |
      +-----------+-----------+             +----------------+----------------+             +-----------+-----------+
                  |                                          |                                          |
                  | (Data Persistence)                       | (Metadata Persistence)                   | (Data Persistence)
                  v                                          v                                          v
      +-----------+-----------+             +----------------+----------------+             +-----------+-----------+
      |   Local Data Volume   |             |     Local Iceberg Warehouse     |             |   Local Data Volume   |
      |   (./DWH/sqlserver)   |             |        (./DWH/iceberg)          |             |     (./DWH/oracle)    |
      +-----------------------+             +---------------------------------+             +-----------------------+

      +-------------------------------------------------------------------------------------------------------------+
      |                                              User Access Layer                                              |
      +-----------------------+-----------------------+-----------------------+-----------------------+-------------+
      |     Query UI (5001)   |     Trino UI (9080)   |    Airflow UI (8080)  |     Spark UI (4040)   |   dbt Docs  |
      |   (Custom SQL Editor) |   (Query Monitoring)  |   (DAG Management)    |   (Job Monitoring)    | (Lineage/MD)|
      +-----------------------+-----------------------+-----------------------+-----------------------+-------------+
```

## 📊 Using dbt for Transformations
dbt is used to manage transformations within the Trino catalogs.

### Running dbt on Windows (PowerShell)
To run dbt from your host machine, you must load the environment variables from the root `.env` file. A helper script is provided in the `dbt_project` directory.

1. **Navigate to the dbt project:**
   ```powershell
   cd dbt_project
   ```
2. **Load environment variables and run dbt:**
   ```powershell
   # Load variables for the current session
   . .\load_env.ps1
   
   # Run dbt commands
   dbt debug
   dbt run
   ```
   *Note: The `load_env.ps1` script automatically handles mapping `trino:8080` (internal) to `localhost:9080` (external) for local development.*

### Running dbt on Linux / macOS / WSL2
1. **Navigate to the dbt project:**
   ```bash
   cd dbt_project
   ```
2. **Source the environment variables:**
   ```bash
   source dbt.env
   dbt debug
   ```

### Direct Database Connections (Optional)
While Trino provides a unified federated layer, you can also connect dbt directly to the underlying databases. This requires installing additional adapters:
```bash
pip install dbt-sqlserver dbt-oracle dbt-spark[session]
```

Example commands for direct connections:
```powershell
# SQL Server Direct
dbt debug --target sqlserver_direct

# Oracle Direct
dbt debug --target oracle_direct

# Iceberg/Spark Direct
dbt debug --target spark_direct
```
*Note: Direct connections may require local drivers (e.g., ODBC for SQL Server) to be installed on your host machine.*

## 🔗 Federated Query Examples
Trino allows you to query multiple catalogs in a single SQL statement. See `scripts/federated_query_examples.sql` for the full DDL and DML setup scripts.

### 0. Setup Sample Data
Run these in the [Query UI](http://localhost:5001) to prepare the environment:
```sql
-- SQL Server
CREATE TABLE IF NOT EXISTS sqlserver.dbo.customers (id int, name varchar);
INSERT INTO sqlserver.dbo.customers VALUES (1, 'John Doe'), (2, 'Jane Smith');

-- Oracle
CREATE TABLE IF NOT EXISTS oracle.pdbadmin.orders (order_id int, customer_id int, total_amount double);
INSERT INTO oracle.pdbadmin.orders VALUES (101, 1, 150.50), (102, 2, 200.00);

-- Iceberg
CREATE TABLE IF NOT EXISTS iceberg.default.customer_loyalty (customer_id int, tier varchar, points int) 
WITH (format = 'PARQUET', location = 'file:/iceberg/warehouse/customer_loyalty');
INSERT INTO iceberg.default.customer_loyalty VALUES (1, 'GOLD', 5000), (2, 'SILVER', 1500);
```

### 1. Cross-Catalog Join (SQL Server + Oracle)
```sql
SELECT c.name, o.total_amount
FROM sqlserver.dbo.customers c
JOIN oracle.pdbadmin.orders o ON c.id = o.customer_id;
```

### 2. Ingest from SQL Server to Iceberg
```sql
INSERT INTO iceberg.default.weather_raw (id, station_name, observation_time)
SELECT sensor_id, location, reading_time
FROM sqlserver.dbo.sensor_readings;
```

### 3. Join Iceberg with SQL Server
```sql
SELECT w.station_name, s.region
FROM iceberg.default.weather_raw w
JOIN sqlserver.dbo.stations s ON w.station_name = s.name;
```

### 4. Multi-Platform 3-Way Join (SQL Server + Oracle + Iceberg)
```sql
SELECT 
    c.name as customer_name, 
    o.total_amount as order_amount, 
    l.tier as loyalty_tier
FROM sqlserver.dbo.customers c
JOIN oracle.pdbadmin.orders o ON c.id = o.customer_id
JOIN iceberg.default.customer_loyalty l ON c.id = l.customer_id;
```

### 5. Query the Sample Iceberg Table
```sql
SELECT * FROM iceberg.default.sample_iceberg;
```

## 🖥️ Trino Query UI (SQL Editor)
A custom web-based SQL editor is included for easy interaction with the federated catalogs.

- **URL:** [http://localhost:5001](http://localhost:5001)
- **Features:** Write SQL, execute against any catalog, and view results in a responsive table.

## 🧪 Using the Trino CLI
A lightweight wrapper is provided for local SQL interaction.

#### 🪟 Windows (PowerShell/CMD)
```powershell
# Check version
.\tools\trino.cmd --version

# Connect to the Iceberg catalog
.\tools\trino.cmd --server http://localhost:9080 --catalog iceberg --schema default
```

#### 🐧 Linux / macOS / WSL2
```bash
# Ensure the jar is executable if using a custom script, 
# or run directly via java:
java -jar tools/trino-cli-451.jar --server http://localhost:9080 --catalog iceberg --schema default
```

## 📚 Data Engineering Library
The `include/data_eng_lib.py` is a unified utility library for interacting with:
- **Cloud Storage:** AWS S3, GCS, Azure Blob.
- **Databases:** Trino, Snowflake, SQL Server, Oracle.
- **Metadata:** AWS Glue Catalog.

Example usage in a DAG:
```python
from data_eng_lib import execute_trino_query

results = execute_trino_query("SELECT * FROM iceberg.default.weather_raw LIMIT 10")
```

## 🛡️ Data Governance with Trino
Trino acts as the central security and governance layer for the entire Lakehouse.

- **Centralized Access Control:** Manage permissions for SQL Server, Oracle, and Iceberg in one place.
- **Fine-Grained Security:** Supports column-level masking (e.g., hiding PII) and row-level filtering (e.g., multi-tenancy) across all federated sources.
- **Audit & Lineage:** Every query is logged at the gateway, providing a complete audit trail of who accessed what data and when.
- **Integration:** Ready for enterprise governance engines like **Apache Ranger** or **Open Policy Agent (OPA)**.

## 🏢 Enterprise Integration

### Office 365 & Azure AD
The stack is designed to integrate seamlessly with the Microsoft ecosystem:

- **Authentication (SSO):** Both Trino and Airflow can be configured to use **Azure AD (Microsoft Entra ID)** for Single Sign-On.
- **AD Group Mapping:** Map your corporate AD groups directly to Trino catalogs or Airflow roles (e.g., `Finance_AD_Group` -> `Oracle Finance Catalog`).
- **Data Ingestion:** Use Airflow to ingest data directly from **SharePoint** or **OneDrive** using the Microsoft Graph API.
- **Consumption:** Connect **Power BI** or **Excel** directly to Trino to build live reports that join data across your entire infrastructure.

## 🔧 Troubleshooting
- **Memory Issues:** If services fail to start, ensure Docker has enough memory (8GB+ recommended).
- **Port Conflicts:** If ports 8080, 9080, or 1433 are in use, modify the `ports` section in `docker-compose.yml`.
- **Logs:** View service logs using `docker-compose logs -f [service_name]`.

## 📖 Connectivity Reference

This section contains the configuration templates for the various connectors used in this project.

### Trino Catalog Configurations (`trino/catalog/`)

#### Iceberg (`iceberg.properties`)
```properties
connector.name=iceberg
iceberg.catalog.type=HIVE_METASTORE
hive.metastore.uri=thrift://hive-metastore:9083
iceberg.file-format=PARQUET
iceberg.format-version=2
```

#### SQL Server (`sqlserver.properties`)
```properties
connector.name=sqlserver
connection-url=jdbc:sqlserver://sqlserver:1433;encrypt=false
connection-user=sa
connection-password=Sqlserver!Password123
```

#### Oracle (`oracle.properties`)
```properties
connector.name=oracle
connection-url=jdbc:oracle:thin:@oracle:1521/FREEPDB1
connection-user=system
connection-password=Oracle!Password123
```

#### Snowflake (Template)
```properties
connector.name=snowflake
snowflake.account=${ENV:SNOWFLAKE_ACCOUNT}
snowflake.user=${ENV:SNOWFLAKE_USER}
snowflake.password=${ENV:SNOWFLAKE_PASSWORD}
snowflake.warehouse=COMPUTE_WH
snowflake.database=YOUR_DB
snowflake.role=ALL_USERS_ROLE
```

### Airflow Connection Templates

#### Amazon S3 (AWS)
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

#### Google Cloud Storage (GCS)
- **Conn Id**: `google_cloud_default`
- **Conn Type**: `Google Cloud`
- **Keyfile JSON**: Paste your service account JSON key here.

#### Azure Blob Storage
- **Conn Id**: `azure_default`
- **Conn Type**: `Azure Blob Storage`
- **Extra**:
```json
{
  "connection_string": "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
}
```

#### Snowflake
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
```
