import duckdb
from datasets import load_dataset #huggin face package

# Connect to DuckDB
con = duckdb.connect('wikipedia_analysis.db')

# Load one year of Wikipedia data (start with 2023)
# Using streaming=True to handle large dataset efficiently
ds = load_dataset(
    "wikimedia/wikipedia",
    "20231101.en",  # November 2023 snapshot
    split="train"
)

# Convert to DuckDB-queryable format
con.execute("CREATE TABLE wikipedia_2023 AS SELECT * FROM ds")

# Query for data engineering related content
query = """
WITH data_engineering_articles AS (
    SELECT 
        id,
        title,
        text,
        url,
        LENGTH(text) as article_length,
        -- Check if article is related to data engineering
        CASE 
            WHEN LOWER(text) LIKE '%data engineering%' 
                OR LOWER(text) LIKE '%data pipeline%'
                OR LOWER(text) LIKE '%etl%'
                OR LOWER(text) LIKE '%data warehouse%'
                OR LOWER(text) LIKE '%data lake%'
                OR LOWER(text) LIKE '%apache spark%'
                OR LOWER(text) LIKE '%apache kafka%'
                OR LOWER(text) LIKE '%dbt%'
                OR LOWER(text) LIKE '%airflow%'
                OR LOWER(title) LIKE '%data%engineering%'
            THEN 1 ELSE 0 
        END as is_data_engineering
    FROM wikipedia_2023
)
SELECT 
    title,
    url,
    article_length,
    -- Count keyword mentions
    (LENGTH(text) - LENGTH(REPLACE(LOWER(text), 'data engineering', ''))) / LENGTH('data engineering') as de_mentions,
    (LENGTH(text) - LENGTH(REPLACE(LOWER(text), 'etl', ''))) / LENGTH('etl') as etl_mentions,
    (LENGTH(text) - LENGTH(REPLACE(LOWER(text), 'data pipeline', ''))) / LENGTH('data pipeline') as pipeline_mentions,
    (LENGTH(text) - LENGTH(REPLACE(LOWER(text), 'apache spark', ''))) / LENGTH('apache spark') as spark_mentions,
    -- Extract first paragraph as summary
    SPLIT_PART(text, '\n\n', 1) as summary
FROM data_engineering_articles
WHERE is_data_engineering = 1
ORDER BY article_length DESC
LIMIT 100;
"""

result = con.execute(query).df()
print(result)

# Export to Parquet for dlt ingestion
con.execute("""
    COPY (
        SELECT * FROM data_engineering_articles WHERE is_data_engineering = 1
    ) TO 'wikipedia_data_engineering_2023.parquet' (FORMAT PARQUET)
""")
