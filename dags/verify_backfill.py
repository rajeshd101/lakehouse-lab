import os
import logging
from trino.dbapi import connect

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_data():
    host = os.getenv("TRINO_HOST", "trino-coordinator") # Internal docker hostname likely
    # But if running from outside or inside...
    # I am running this script INSIDE the scheduler container which can reach trino-coordinator
    
    port = int(os.getenv("TRINO_PORT", "8080"))
    user = os.getenv("TRINO_USER", "admin")
    catalog = os.getenv("TRINO_CATALOG", "iceberg")
    schema = os.getenv("SCHEMA", "default")
    
    conn = connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema,
    )
    cur = conn.cursor()
    
    print("Checking total row count (any date)...")
    sql = "SELECT count(*) FROM iceberg.default.weather_raw"
    cur.execute(sql)
    rows = cur.fetchall()
    print(f"Total row count: {rows[0][0]}")

    print("Checking row count for 2026-01-14...")
    # Since we are partitioning by hour, we can check a range or just total count
    sql = """
    SELECT count(*) 
    FROM iceberg.default.weather_raw 
    WHERE observation_time >= TIMESTAMP '2026-01-14 00:00:00 UTC'
      AND observation_time < TIMESTAMP '2026-01-15 00:00:00 UTC'
    """
    cur.execute(sql)
    rows = cur.fetchall()
    print(f"Row count for 2026-01-14: {rows[0][0]}")
    
    print("Checking partitions...")
    sql_parts = "SELECT * FROM iceberg.default.\"weather_raw$partitions\""
    try:
        cur.execute(sql_parts)
        parts = cur.fetchall()
        print("Partitions found:")
        if not parts:
            print("No partitions found.")
        for p in parts:
            print(p)
    except Exception as e:
        print(f"Could not read partitions: {e}")

if __name__ == "__main__":
    verify_data()
