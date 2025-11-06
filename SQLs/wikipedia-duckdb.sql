
-- This is the actual public data format
describe
FROM read_csv('https://dumps.wikimedia.org/other/pageviews/2025/2025-10/pageviews-20251001-000000.gz',
    delim=' ',
    header=false,
    compression='gzip')
LIMIT 10;


--auto detection:
SELECT *
FROM read_csv(
    'https://dumps.wikimedia.org/other/pageviews/2025/2025-10/pageviews-20251001-000000.gz',
    delim=' ',
    header=false,
    compression='gzip',
    all_varchar=true  -- Initially read all as text to see the raw data
)
LIMIT 20;

--manually
-- When using the columns parameter, use the names you defined
SELECT
    project,
    page_title,
    view_count,
    bytes_served
FROM read_csv(
    'https://dumps.wikimedia.org/other/pageviews/2025/2025-10/pageviews-20251001-000000.gz',
    delim=' ',
    header=false,
    compression='gzip',
    columns={
        'project': 'VARCHAR',
        'page_title': 'VARCHAR',
        'view_count': 'BIGINT',
        'bytes_served': 'BIGINT'
    }
)
WHERE project IS NOT NULL
LIMIT 10;


---predicate pushdown

SELECT
    page_title,
    view_count,
    bytes_served
FROM read_csv(
    'https://dumps.wikimedia.org/other/pageviews/2025/2025-10/pageviews-20251001-000000.gz',
    delim=' ',
    header=false,
    compression='gzip',
    columns={
        'project': 'VARCHAR',
        'page_title': 'VARCHAR',
        'view_count': 'BIGINT',
        'bytes_served': 'BIGINT'
    }
)
WHERE page_title IN (
    'DuckDB',
    'Apache_Spark',
    'Apache_Kafka',
    'Data_engineering',
    'Extract,_transform,_load',
    'ClickHouse',
    'Snowflake_(company)',
    'Apache_Airflow',
    'Databricks',
    'Data_warehouse',
    'Data_lake',
    'dbt_(software)',
    'Apache_Flink',
    'PostgreSQL',
    'BigQuery'
)
LIMIT 1000;


SELECT
    page_title,
    view_count,
    bytes_served
FROM read_csv(
    'https://dumps.wikimedia.org/other/pageviews/2025/2025-10/pageviews-20251001-000000.gz',
    delim=' ',
    header=false,
    compression='gzip',
    columns={
        'project': 'VARCHAR',
        'page_title': 'VARCHAR',
        'view_count': 'BIGINT',
        'bytes_served': 'BIGINT'
    }
)
WHERE page_title LIKE '%data%engineering%'
   OR page_title LIKE '%ETL%'
   OR page_title LIKE '%pipeline%'
   OR page_title IN ('DuckDB', 'ClickHouse', 'Apache_Spark', 'dlt')
LIMIT 1000;
