"""
Configuration for the Wikipedia data engineering pipeline
"""

# ClickHouse connection settings
CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 9000,
    'user': 'default',
    'password': '',
    'database': 'default'
}

# Wikipedia dumps configuration
# Start with just a few days to test
WIKIPEDIA_CONFIG = {
    'base_url': 'https://dumps.wikimedia.org/other/pageviews/',
    'year': 2025,
    'month': 1,  # January 2025
    'days': list(range(1, 3)),  # Just first 2 days for testing
    'hours': list(range(0, 2)),  # Just first 2 hours per day for testing
}

# SQL files directory
SQL_DIR = 'sql'

# dlt configuration
DLT_CONFIG = {
    'pipeline_name': 'wikipedia_data_engineering',
    'destination': 'duckdb',
    'dataset_name': 'wikipedia_analytics'
}
