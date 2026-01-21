-- =============================================================================
-- TRINO FEDERATED QUERY EXAMPLES
-- These examples demonstrate how to query multiple catalogs (Iceberg, SQL Server, Oracle)
-- from the same Trino interface.
-- =============================================================================

-- 0. SETUP SAMPLE DATA (DDL & DML)
-- Run these to prepare the environment for the examples below.

-- SQL Server Setup
CREATE TABLE IF NOT EXISTS sqlserver.dbo.customers (id int, name varchar);
INSERT INTO sqlserver.dbo.customers VALUES (1, 'John Doe'), (2, 'Jane Smith');

-- Oracle Setup (using pdbadmin schema)
CREATE TABLE IF NOT EXISTS oracle.pdbadmin.orders (order_id int, customer_id int, total_amount double);
INSERT INTO oracle.pdbadmin.orders VALUES (101, 1, 150.50), (102, 2, 200.00);

-- Iceberg Setup
CREATE TABLE IF NOT EXISTS iceberg.default.customer_loyalty (customer_id int, tier varchar, points int) 
WITH (format = 'PARQUET', location = 'file:/iceberg/warehouse/customer_loyalty');
INSERT INTO iceberg.default.customer_loyalty VALUES (1, 'GOLD', 5000), (2, 'SILVER', 1500);


-- 1. Basic Cross-Catalog Join
-- Join a table from SQL Server with a table from Oracle.
-- (Assumes 'customers' exists in SQL Server and 'orders' exists in Oracle)
SELECT 
    c.id, 
    c.name, 
    o.total_amount
FROM sqlserver.dbo.customers c
JOIN oracle.pdbadmin.orders o ON c.id = o.customer_id;


-- 2. Data Ingestion: SQL Server to Iceberg
-- Extract data from a relational source (SQL Server) and insert it into 
-- the high-performance Lakehouse table (Iceberg).
INSERT INTO iceberg.default.weather_raw (id, station_name, observation_time, ingested_at, raw_metadata)
SELECT 
    CAST(sensor_id AS VARCHAR), 
    location_name, 
    CAST(reading_time AS TIMESTAMP(6) WITH TIME ZONE), 
    current_timestamp, 
    '{"source": "sqlserver_migration"}'
FROM sqlserver.dbo.sensor_readings
WHERE reading_time > (SELECT MAX(observation_time) FROM iceberg.default.weather_raw);


-- 3. Federated Analysis: Iceberg + SQL Server
-- Join real-time weather data (Iceberg) with station metadata (SQL Server)
-- to perform an enriched analysis.
SELECT 
    w.station_name,
    s.region,
    s.elevation,
    AVG(CAST(json_extract_scalar(w.raw_metadata, '$.properties.value') AS DOUBLE)) as avg_temp
FROM iceberg.default.weather_raw w
JOIN sqlserver.dbo.stations s ON w.station_name = s.name
GROUP BY 1, 2, 3
ORDER BY avg_temp DESC;


-- 4. Multi-Platform 3-Way Join (SQL Server + Oracle + Iceberg)
-- This query joins data from three different platforms in a single statement.
-- SQL Server: Customer Names
-- Oracle: Order Amounts
-- Iceberg: Loyalty Tiers and Points
SELECT 
    c.name as customer_name, 
    o.total_amount as order_amount, 
    l.tier as loyalty_tier, 
    l.points as loyalty_points
FROM sqlserver.dbo.customers c
JOIN oracle.pdbadmin.orders o ON c.id = o.customer_id
JOIN iceberg.default.customer_loyalty l ON c.id = l.customer_id;
