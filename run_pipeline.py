#!/usr/bin/env python3
"""
Simple script to run the Wikipedia data engineering pipeline
"""

from pipeline.wikipedia_pipeline import run_pipeline

if __name__ == '__main__':
    print("Starting Wikipedia Data Engineering Pipeline...")
    print("=" * 60)

    try:
        result = run_pipeline()
        print("\n" + "=" * 60)
        print("Pipeline completed successfully!")
        print(f"Result: {result}")
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"Pipeline failed with error: {e}")
        raise
