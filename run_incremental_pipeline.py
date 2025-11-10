#!/usr/bin/env python3
"""
Run the incremental Wikipedia data engineering pipeline
This version uses dlt's state management to avoid reloading duplicate data
"""

from pipeline.wikipedia_pipeline_incremental import run_incremental_pipeline

if __name__ == '__main__':
    print("Starting Wikipedia Data Engineering Pipeline (INCREMENTAL)...")
    print("=" * 60)
    print("This version tracks what's been loaded and skips duplicates")
    print("=" * 60)

    try:
        result = run_incremental_pipeline()
        print("\n" + "=" * 60)
        print("Pipeline completed successfully!")
        print(f"Total rows in ClickHouse: {result['summary']['total_rows']:,}")
        print("\nTop 5 pages:")
        for i, page in enumerate(result['summary']['top_pages'][:5], 1):
            print(f"  {i}. {page['path']}: {page['hits']} views")
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"Pipeline failed with error: {e}")
        raise
