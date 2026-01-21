# Data Governance and Office 365 Authentication Configuration

This document contains the configuration files and instructions for enabling Data Governance (RBAC, Masking, Filtering) and Office 365 (Azure AD) Authentication in the Lakehouse Lab.

## 🛡️ Data Governance Configuration

### 1. Trino Access Control Properties
**File Path:** `trino/access-control.properties`
```properties
access-control.name=file
security.config-file=/etc/trino/rules.json
security.refresh-period=10s
```

### 2. Trino Access Control Rules
**File Path:** `trino/rules.json`
```json
{
  "catalogs": [
    {
      "user": "admin",
      "allow": "all"
    },
    {
      "catalog": "system",
      "allow": "all"
    },
    {
      "catalog": "iceberg",
      "allow": "all"
    },
    {
      "catalog": "sqlserver",
      "allow": "all"
    },
    {
      "catalog": "oracle",
      "allow": "all"
    }
  ],
  "tables": [
    {
      "user": "admin",
      "privileges": [
        "SELECT",
        "INSERT",
        "DELETE",
        "OWNERSHIP"
      ]
    },
    {
      "user": "analyst",
      "catalog": "iceberg",
      "schema": "default",
      "table": "weather_raw",
      "privileges": [
        "SELECT"
      ],
      "columns": [
        {
          "column": "raw_metadata",
          "mask": "'REDACTED'"
        }
      ]
    },
    {
      "user": "public",
      "catalog": "iceberg",
      "schema": "default",
      "table": "weather_raw",
      "privileges": [
        "SELECT"
      ],
      "filter": "station_name = 'CYVR'",
      "columns": [
        {
          "column": "station_name",
          "allow": true
        },
        {
          "column": "observation_time",
          "allow": true
        },
        {
          "column": ".*",
          "allow": false
        }
      ]
    },
    {
      "catalog": ".*",
      "schema": ".*",
      "table": ".*",
      "privileges": [
        "SELECT"
      ]
    }
  ]
}
```

---

## 🏢 Office 365 (Azure AD) Authentication

### 1. Trino Config Properties
**File Path:** `trino/config.properties`
```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080

# Internal Communication (Required when authentication is enabled)
internal-communication.shared-secret=a-very-secret-shared-key-for-lab-use

# Office 365 (Azure AD) Authentication
http-server.authentication.type=oauth2
http-server.authentication.oauth2.issuer=https://login.microsoftonline.com/${ENV:AZURE_TENANT_ID}/v2.0
http-server.authentication.oauth2.client-id=${ENV:AZURE_CLIENT_ID}
http-server.authentication.oauth2.client-secret=${ENV:AZURE_CLIENT_SECRET}
http-server.authentication.oauth2.scopes=openid,profile,email
```

---

## ⚙️ Infrastructure Integration (docker-compose.yml)

To enable these features conditionally, use the following logic in your `docker-compose.yml`:

### Trino Service Entrypoint
```yaml
        if [ "$$ENABLE_DATAGOVERNANCE" = "true" ]; then
          cp /etc/trino/templates/access-control.properties /etc/trino/access-control.properties
          cp /etc/trino/templates/rules.json /etc/trino/rules.json
        fi
        if [ "$$ENABLE_O365_AUTH" = "true" ]; then
          cp /etc/trino/templates/config.properties /etc/trino/config.properties
        fi
```

### Trino Service Volumes
```yaml
    volumes:
      - ./trino/catalog:/etc/trino/catalog
      - ./trino/config.properties:/etc/trino/templates/config.properties
      - ./trino/access-control.properties:/etc/trino/templates/access-control.properties
      - ./trino/rules.json:/etc/trino/templates/rules.json
      - ./DWH/iceberg:/iceberg/warehouse
```

### Trino Service Environment
```yaml
      - ENABLE_DATAGOVERNANCE=${ENABLE_DATAGOVERNANCE}
      - ENABLE_O365_AUTH=${ENABLE_O365_AUTH}
      - AZURE_TENANT_ID=${AZURE_TENANT_ID}
      - AZURE_CLIENT_ID=${AZURE_CLIENT_ID}
      - AZURE_CLIENT_SECRET=${AZURE_CLIENT_SECRET}
```

---

## 🔐 Environment Variables (.env)

Add these to your `.env` and `.env.example` files:

```bash
# Data Governance Configuration
ENABLE_DATAGOVERNANCE=false

# Office 365 (Azure AD) Authentication
ENABLE_O365_AUTH=false
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
```

---

## 🧪 Testing Governance

Once enabled, you can test the rules using the Trino CLI:

1. **Admin (Full Access):**
   ```bash
   .\tools\trino.cmd --server http://localhost:9080 --user admin --catalog iceberg --execute "SELECT * FROM default.weather_raw LIMIT 5"
   ```

2. **Analyst (Masked Data):**
   ```bash
   .\tools\trino.cmd --server http://localhost:9080 --user analyst --catalog iceberg --execute "SELECT raw_metadata FROM default.weather_raw LIMIT 5"
   ```

3. **Public (Row Filtered & Column Restricted):**
   ```bash
   .\tools\trino.cmd --server http://localhost:9080 --user public --catalog iceberg --execute "SELECT * FROM default.weather_raw"
