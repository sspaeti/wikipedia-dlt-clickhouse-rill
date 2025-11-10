# Architecture: DuckDB vs ClickHouse Usage

## Current Setup

### ClickHouse (Data Warehouse)
- **Purpose**: Stores actual Wikipedia pageview data
- **Tables**:
  - `wikistat_data_engineering` - Main data (time, project, path, hits)
  - `data_engineering_keywords` - Lookup table for filtering
- **Why ClickHouse?**
  - Designed for analytical queries on large datasets
  - Native HTTP/S3 reading (downloads directly from Wikipedia)
  - Excellent compression (ZSTD)
  - Fast aggregations and filtering

### DuckDB (Pipeline Metadata)
- **Purpose**: Stores dlt pipeline tracking and metadata
- **Tables**:
  - `_dlt_loads` - Track pipeline runs
  - `_dlt_pipeline_state` - Store pipeline state
  - `pageview_load_stats` - Track which files were loaded
  - `summary_statistics` - Cached summary stats
- **Why DuckDB?**
  - Lightweight, embedded database (single file)
  - Perfect for local metadata storage
  - No server needed
  - Easy to query and inspect

## Data Flow

```
Wikipedia Dumps (HTTPS)
    ↓
ClickHouse reads directly via url() function
    ↓
Filters & processes (SQL in ClickHouse)
    ↓
Stores in wikistat_data_engineering table
    ↓
Python pipeline tracks metadata → DuckDB
```

## Two Pipeline Versions

### 1. Basic Pipeline (`run_pipeline.py`)
- **File**: `pipeline/wikipedia_pipeline.py`
- **Behavior**: Loads all configured data every time
- **Issue**: Will create duplicates if run multiple times
- **Use case**: One-time loads or manual deduplication

### 2. Incremental Pipeline (`run_incremental_pipeline.py`)
- **File**: `pipeline/wikipedia_pipeline_incremental.py`
- **Behavior**: Tracks processed files in dlt state
- **Feature**: Skips already loaded files automatically
- **Use case**: Recurring loads, production pipelines

## Preventing Duplicates

### Option 1: Use Incremental Pipeline (Recommended)

```bash
python run_incremental_pipeline.py
```

This uses dlt's `incremental` decorator to track state:
```python
@dlt.resource(write_disposition="append")
def wikipedia_pageviews_incremental(
    self,
    processed_files: Optional[dlt.sources.incremental[str]] = dlt.sources.incremental(
        "date_pattern",
        initial_value=None
    )
)
```

### Option 2: Deduplication in ClickHouse

Add a ReplacingMergeTree or use DISTINCT queries:

```sql
-- Change table engine to deduplicate automatically
CREATE TABLE wikistat_data_engineering (
    ...
) ENGINE = ReplacingMergeTree(ingestion_date)
PARTITION BY toYYYYMM(time)
ORDER BY (path, time);
```

### Option 3: Manual Cleanup

```sql
-- Remove duplicates manually
OPTIMIZE TABLE wikistat_data_engineering FINAL;

-- Or recreate table
CREATE TABLE wikistat_data_engineering_dedup AS
SELECT time, project, subproject, path, sum(hits) as hits, max(ingestion_date) as ingestion_date
FROM wikistat_data_engineering
GROUP BY time, project, subproject, path;

DROP TABLE wikistat_data_engineering;
RENAME TABLE wikistat_data_engineering_dedup TO wikistat_data_engineering;
```

## Alternative: Use ClickHouse as dlt Destination

You could also store dlt metadata in ClickHouse instead of DuckDB:

**Change in `pipeline/config.py`:**
```python
DLT_CONFIG = {
    'pipeline_name': 'wikipedia_data_engineering',
    'destination': 'clickhouse',  # Changed from 'duckdb'
    'dataset_name': 'wikipedia_analytics'
}
```

**Pros:**
- Single database to manage
- All data in one place

**Cons:**
- Requires ClickHouse credentials in dlt config
- Metadata queries hit your data warehouse
- Slightly more complex setup

## Checking Pipeline State

### View processed files (DuckDB):
```bash
uv run python -c "
import duckdb
conn = duckdb.connect('wikipedia_data_engineering.duckdb')
result = conn.execute('''
    SELECT date_pattern, rows_inserted, load_timestamp, status
    FROM wikipedia_analytics.pageview_load_stats
    ORDER BY load_timestamp DESC
    LIMIT 10
''').fetchdf()
print(result)
conn.close()
"
```

### View data in ClickHouse:
```bash
clickhouse client --query "
SELECT
    count() as total_rows,
    min(time) as earliest_date,
    max(time) as latest_date,
    count(DISTINCT path) as unique_pages
FROM wikistat_data_engineering
"
```

## Recommendations

1. **For Production**: Use `run_incremental_pipeline.py`
2. **For Development**: Use `run_pipeline.py` but clear data between runs
3. **For Monitoring**: Query DuckDB for pipeline health, ClickHouse for data analytics
4. **For Deduplication**: Use incremental loading or ReplacingMergeTree

## Next Steps

- Add data quality checks
- Implement backfill logic for historical data
- Add monitoring/alerting
- Schedule with cron or Airflow
- Export to BI tools (Rill, Grafana, etc.)
