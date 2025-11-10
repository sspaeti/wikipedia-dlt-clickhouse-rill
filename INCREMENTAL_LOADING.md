# Incremental Loading - Complete Guide

## Problem

Running `python run_pipeline.py` multiple times will create **duplicate data** in ClickHouse because it doesn't track what's been loaded.

## Solution: Simple Incremental Pipeline ✅

Use `run_simple_incremental.py` - it tracks processed files in a DuckDB table.

### Usage

```bash
# First run - loads all data
python run_simple_incremental.py
# → Processed 4 files, inserted 1,596 rows

# Second run - skips everything (no duplicates!)
python run_simple_incremental.py
# → "All files already processed! No work to do."
```

### How It Works

1. **Tracking Table** (`wikipedia_tracking.duckdb`):
   ```sql
   CREATE TABLE processed_files (
       file_pattern VARCHAR PRIMARY KEY,
       rows_inserted INTEGER,
       processed_at TIMESTAMP,
       status VARCHAR
   )
   ```

2. **Before Processing**: Queries tracking table to get already processed files
3. **During Processing**: Only loads new files
4. **After Processing**: Marks files as processed in tracking table

### Check Tracking Status

```bash
uv run python -c "
import duckdb
conn = duckdb.connect('wikipedia_tracking.duckdb')
result = conn.execute('SELECT * FROM processed_files ORDER BY processed_at DESC').fetchdf()
print(result)
conn.close()
"
```

## Architecture Summary

```
┌─────────────────────────────────────────────────────────┐
│ wikipedia_tracking.duckdb (Tracking)                    │
│   - processed_files table                               │
│   - Prevents duplicate loads                            │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ ClickHouse (Data Warehouse)                             │
│   - wikistat_data_engineering (pageviews data)          │
│   - data_engineering_keywords (lookup table)            │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ wikipedia_data_engineering.duckdb (dlt Metadata)        │
│   - _dlt_loads, _dlt_pipeline_state                     │
│   - pageview_load_stats, summary_statistics             │
└─────────────────────────────────────────────────────────┘
```

## Three Pipeline Options

### 1. Basic Pipeline (⚠️ Creates Duplicates)
```bash
python run_pipeline.py
```
- **Use for**: One-time loads, testing
- **Warning**: Will duplicate data if run multiple times

### 2. Simple Incremental ✅ (Recommended)
```bash
python run_simple_incremental.py
```
- **Tracks**: Files processed in `wikipedia_tracking.duckdb`
- **Use for**: Production, recurring loads
- **Benefit**: Simple, reliable, easy to debug

### 3. dlt Incremental (🚧 Advanced)
```bash
python run_incremental_pipeline.py
```
- **Tracks**: Uses dlt's internal state
- **Use for**: Complex dlt integrations
- **Note**: More complex, requires understanding dlt state management

## Answers to Your Questions

### Q1: "If I rerun, it will load existing data again?"

**Answer**:
- `run_pipeline.py` → YES, duplicates
- `run_simple_incremental.py` → NO, tracks & skips! ✅

### Q2: "dlt has incremental load integrated?"

**Answer**: Yes, but it's complex:
- dlt has `@dlt.resource(incremental=...)` decorator
- Works great for databases with cursors/timestamps
- For files, manual tracking is simpler (our approach)
- See: https://dlthub.com/docs/general-usage/incremental-loading

### Q3: "We use duckdb as dlt destination, not clickhouse?"

**Answer**: Correct! Here's why:

**Current Architecture**:
```
ClickHouse = Real data warehouse (Wikipedia pageviews)
DuckDB #1 (wikipedia_data_engineering.duckdb) = dlt metadata
DuckDB #2 (wikipedia_tracking.duckdb) = Incremental tracking
```

**Why This Setup?**:
- **ClickHouse**: Best for large analytical queries, native HTTP reading
- **DuckDB**: Best for lightweight metadata, embedded, no server needed
- **Separation**: Keeps pipeline metadata separate from business data

**Alternative** (All in ClickHouse):
```python
# In pipeline/config.py
DLT_CONFIG = {
    'destination': 'clickhouse',  # Instead of 'duckdb'
    ...
}
```

Pros: Single database
Cons: More setup, metadata queries hit your warehouse

## Loading More Data

To load new data, just update the config:

```python
# pipeline/config.py
WIKIPEDIA_CONFIG = {
    'year': 2025,
    'month': 1,
    'days': list(range(1, 31)),  # Full January
    'hours': list(range(0, 24)),  # All 24 hours
}
```

Then run:
```bash
python run_simple_incremental.py
```

It will automatically:
- Skip already processed files
- Load only new files
- Update tracking table

## Cleaning Up Duplicates

If you already have duplicates from using `run_pipeline.py`:

### Option 1: Start Fresh
```bash
clickhouse client --query "TRUNCATE TABLE wikistat_data_engineering"
python run_simple_incremental.py
```

### Option 2: Deduplicate in ClickHouse
```sql
-- Create deduplicated table
CREATE TABLE wikistat_data_engineering_clean AS
SELECT
    time,
    project,
    subproject,
    path,
    sum(hits) as hits,
    max(ingestion_date) as ingestion_date
FROM wikistat_data_engineering
GROUP BY time, project, subproject, path;

-- Replace old table
DROP TABLE wikistat_data_engineering;
RENAME TABLE wikistat_data_engineering_clean TO wikistat_data_engineering;
```

## Best Practices

1. **Production**: Always use `run_simple_incremental.py`
2. **Monitoring**: Check `wikipedia_tracking.duckdb` regularly
3. **Backfills**: Add older dates to config, run incremental (auto-skips existing)
4. **Failures**: Check tracking table - failed files marked with status='failed'
5. **Reset**: Delete `wikipedia_tracking.duckdb` to reprocess everything

## Querying Results

```bash
# Check what's loaded
clickhouse client --query "
SELECT
    count() as rows,
    count(DISTINCT toDate(time)) as days,
    min(time) as earliest,
    max(time) as latest
FROM wikistat_data_engineering
"

# Check tracking
uv run python -c "
import duckdb
conn = duckdb.connect('wikipedia_tracking.duckdb')
stats = conn.execute('''
    SELECT
        status,
        count(*) as files,
        sum(rows_inserted) as total_rows
    FROM processed_files
    GROUP BY status
''').fetchdf()
print(stats)
"
```

## Files Created

- `run_simple_incremental.py` - Main incremental runner ✅
- `pipeline/wikipedia_pipeline_simple_incremental.py` - Implementation
- `wikipedia_tracking.duckdb` - Tracks processed files
- `ARCHITECTURE.md` - Detailed architecture doc
- `INCREMENTAL_LOADING.md` - This guide

## Summary

✅ **Use `run_simple_incremental.py` for production**
✅ **Tracks processed files reliably**
✅ **No duplicate data**
✅ **Simple to understand and debug**
✅ **Scales to any amount of data**
