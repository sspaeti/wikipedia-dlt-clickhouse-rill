#!/usr/bin/env python3
"""Check what's stored in ClickHouse vs DuckDB"""

import duckdb
from pipeline.clickhouse_utils import ClickHouseManager
from pipeline.config import CLICKHOUSE_CONFIG

# Check ClickHouse
print("=" * 60)
print("CLICKHOUSE (Real Data Warehouse)")
print("=" * 60)
ch = ClickHouseManager(CLICKHOUSE_CONFIG)
ch.connect()

result = ch.execute_query("SELECT count() as total_rows FROM wikistat_data_engineering")
print(f"wikistat_data_engineering: {result[0][0]:,} rows")

result = ch.execute_query("SELECT count() as keywords FROM data_engineering_keywords")
print(f"data_engineering_keywords: {result[0][0]} keywords")

ch.disconnect()

# Check DuckDB
print("\n" + "=" * 60)
print("DUCKDB (dlt Pipeline Metadata)")
print("=" * 60)
conn = duckdb.connect('wikipedia_data_engineering.duckdb')

tables = conn.execute("""
    SELECT table_name, estimated_size
    FROM duckdb_tables()
    WHERE schema_name = 'wikipedia_analytics'
    ORDER BY table_name
""").fetchall()

for table, size in tables:
    count = conn.execute(f"SELECT count(*) FROM wikipedia_analytics.{table}").fetchone()[0]
    print(f"{table}: {count} rows")

conn.close()
