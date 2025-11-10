# Quick Start Guide

## Setup (One Time)

```bash
# 1. Install dependencies
uv sync

# 2. Start ClickHouse (if not running)
clickhouse server
```

## Running the Pipeline

### ✅ Recommended: Incremental (No Duplicates)
```bash
python run_simple_incremental.py
```
- Tracks what's loaded
- Skips duplicates automatically
- **Use this for production!**

### Basic (⚠️ Will Create Duplicates)
```bash
python run_pipeline.py
```
- No tracking
- Use only for one-time loads

## Configure Data Range

Edit `pipeline/config.py`:
```python
WIKIPEDIA_CONFIG = {
    'year': 2025,
    'month': 1,
    'days': list(range(1, 3)),    # Days 1-2
    'hours': list(range(0, 2)),   # Hours 0-1
}
```

## Query Data

```bash
# Start ClickHouse client
clickhouse client

# Top pages by views
SELECT path, sum(hits) as views
FROM wikistat_data_engineering
GROUP BY path
ORDER BY views DESC
LIMIT 10;

# Views by category
SELECT kw.category, sum(w.hits) as views
FROM wikistat_data_engineering w
JOIN data_engineering_keywords kw ON w.path = kw.keyword
GROUP BY kw.category
ORDER BY views DESC;
```

## Check What's Loaded

```bash
# Python helper
python check_databases.py

# Or query directly
clickhouse client --query "SELECT count(), min(time), max(time) FROM wikistat_data_engineering"
```

## Architecture

```
Wikipedia → ClickHouse (data) → Tracked in DuckDB → No duplicates!
```

## Files

- `run_simple_incremental.py` - **Use this!** ✅
- `run_pipeline.py` - Basic (creates duplicates)
- `check_databases.py` - Check current state
- `INCREMENTAL_LOADING.md` - Full guide
- `ARCHITECTURE.md` - Technical details

## Common Tasks

### Load more data
1. Edit `pipeline/config.py` (add more days/hours)
2. Run `python run_simple_incremental.py`
3. Done! It skips existing files automatically

### Reset and start over
```bash
rm wikipedia_tracking.duckdb
clickhouse client --query "TRUNCATE TABLE wikistat_data_engineering"
python run_simple_incremental.py
```

### Check tracking status
```bash
uv run python -c "import duckdb; conn = duckdb.connect('wikipedia_tracking.duckdb'); print(conn.execute('SELECT * FROM processed_files').fetchdf()); conn.close()"
```

## That's It!

For more details, see:
- `INCREMENTAL_LOADING.md` - Complete incremental loading guide
- `ARCHITECTURE.md` - Why DuckDB + ClickHouse
- `README.md` - Original setup instructions
