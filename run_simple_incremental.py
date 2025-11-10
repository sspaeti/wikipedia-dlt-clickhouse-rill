#!/usr/bin/env python3
"""
Run the simple incremental pipeline
Uses DuckDB tracking table to avoid duplicates
"""

from pipeline.wikipedia_pipeline_simple_incremental import run_simple_incremental

if __name__ == '__main__':
    try:
        result = run_simple_incremental()

        print("\n" + "=" * 60)
        if result['new_files'] == 0:
            print("✓ No new files to process - all up to date!")
        else:
            print(f"✓ Successfully processed {result['new_files']} new files")
            print(f"✓ Inserted {result['rows_inserted']:,} rows")

        print(f"\nTotal data in ClickHouse: {result['total_rows']:,} rows")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        raise
