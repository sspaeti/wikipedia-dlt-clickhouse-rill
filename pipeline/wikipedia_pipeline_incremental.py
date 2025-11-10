"""
Incremental Wikipedia data engineering pipeline using dlt and ClickHouse
Uses dlt's state management to track processed files and avoid duplicates
"""

import dlt
from datetime import datetime
from typing import Iterator, Dict, Any, List, Optional
import logging
from pathlib import Path

from .config import CLICKHOUSE_CONFIG, WIKIPEDIA_CONFIG, SQL_DIR, DLT_CONFIG
from .clickhouse_utils import ClickHouseManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WikipediaDataEngineeringPipelineIncremental:
    """
    Incremental pipeline to extract Wikipedia pageviews for data engineering topics
    Uses dlt's state management to track what's been loaded
    """

    def __init__(self):
        self.ch_manager = ClickHouseManager(CLICKHOUSE_CONFIG)
        self.sql_dir = SQL_DIR

    def setup_tables(self):
        """
        Execute SQL files to create tables and insert keywords
        """
        logger.info("Setting up ClickHouse tables and keywords...")

        # Execute SQL files in order: 01, 02, 03
        setup_files = [
            '01_create_tables.sql',
            '02_create_keywords.sql',
            '03_insert_keywords.sql'
        ]

        for sql_file in setup_files:
            sql_path = Path(self.sql_dir) / sql_file
            logger.info(f"Executing {sql_file}")
            self.ch_manager.execute_sql_file(str(sql_path))

        # Verify setup
        keywords_count = self.ch_manager.get_table_row_count('data_engineering_keywords')
        logger.info(f"Keywords table contains {keywords_count} entries")

    def generate_date_patterns(self, year: int, month: int, days: List[int], hours: List[int]) -> List[str]:
        """
        Generate date patterns for Wikipedia dump files

        Args:
            year: Year (e.g., 2025)
            month: Month (e.g., 10)
            days: List of days (e.g., [1, 2, 3])
            hours: List of hours (e.g., [0, 1, 2])

        Returns:
            List of URLs like '2025/2025-10/pageviews-20251001-000000.gz'
        """
        patterns = []
        for day in days:
            for hour in hours:
                date_str = f"{year}{month:02d}{day:02d}"
                hour_str = f"{hour:02d}0000"
                pattern = f"{year}/{year}-{month:02d}/pageviews-{date_str}-{hour_str}.gz"
                patterns.append(pattern)
        return patterns

    def load_pageviews_for_date(self, date_pattern: str) -> int:
        """
        Load pageviews for a specific date pattern using ClickHouse

        Args:
            date_pattern: Pattern like '2025/2025-01/pageviews-20250101-000000.gz'

        Returns:
            Number of rows inserted
        """
        logger.info(f"Loading pageviews for pattern: {date_pattern}")

        # Get row count before insert
        before_count = self.ch_manager.get_table_row_count('wikistat_data_engineering')

        # Execute the load SQL with parameters
        sql_path = Path(self.sql_dir) / '04_load_filtered_pageviews.sql'
        params = {
            'base_url': WIKIPEDIA_CONFIG['base_url'],
            'date_pattern': date_pattern
        }

        self.ch_manager.execute_sql_file(str(sql_path), params=params)

        # Get row count after insert
        after_count = self.ch_manager.get_table_row_count('wikistat_data_engineering')
        rows_inserted = after_count - before_count

        logger.info(f"Inserted {rows_inserted} rows for {date_pattern}")
        return rows_inserted

    def load_wikipedia_pageviews_incremental(self, pipeline_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Load Wikipedia pageviews data into ClickHouse incrementally
        Uses pipeline state to track processed files and avoid duplicates

        Args:
            pipeline_state: dlt pipeline state dict

        Returns:
            Statistics about loaded data
        """
        # Generate all date patterns
        all_patterns = self.generate_date_patterns(
            WIKIPEDIA_CONFIG['year'],
            WIKIPEDIA_CONFIG['month'],
            WIKIPEDIA_CONFIG['days'],
            WIKIPEDIA_CONFIG['hours']
        )

        # Get processed files from state
        processed_files = set(pipeline_state.get('processed_files', []))
        logger.info(f"Found {len(processed_files)} already processed files in state")

        # Filter out already processed files
        patterns_to_process = [p for p in all_patterns if p not in processed_files]

        if len(patterns_to_process) == 0:
            logger.info("No new files to process!")
            return []

        logger.info(f"Will process {len(patterns_to_process)} new files (skipping {len(processed_files)})")

        total_rows = 0
        stats = []

        # Load data for each new date pattern
        for pattern in patterns_to_process:
            try:
                rows_inserted = self.load_pageviews_for_date(pattern)
                total_rows += rows_inserted

                stats.append({
                    'date_pattern': pattern,
                    'rows_inserted': rows_inserted,
                    'load_timestamp': datetime.now(),
                    'status': 'success'
                })

                # Mark as processed
                processed_files.add(pattern)

            except Exception as e:
                logger.error(f"Error loading {pattern}: {e}")
                stats.append({
                    'date_pattern': pattern,
                    'rows_inserted': 0,
                    'load_timestamp': datetime.now(),
                    'status': 'failed',
                    'error_message': str(e)
                })

        # Update state with newly processed files
        pipeline_state['processed_files'] = list(processed_files)

        logger.info(f"Pipeline complete. Total rows inserted: {total_rows}")
        return stats

    def get_summary_statistics(self) -> Dict[str, Any]:
        """
        Generate summary statistics after pipeline runs
        """
        # Get total rows in main table
        total_rows = self.ch_manager.get_table_row_count('wikistat_data_engineering')

        # Get top pages by hits
        top_pages_query = """
        SELECT
            path,
            sum(hits) as total_hits,
            count() as records
        FROM wikistat_data_engineering
        GROUP BY path
        ORDER BY total_hits DESC
        LIMIT 10
        """
        top_pages = self.ch_manager.execute_query(top_pages_query)

        # Get category breakdown
        category_query = """
        SELECT
            kw.category,
            count(DISTINCT w.path) as unique_pages,
            sum(w.hits) as total_hits
        FROM wikistat_data_engineering w
        INNER JOIN data_engineering_keywords kw ON w.path = kw.keyword
        GROUP BY kw.category
        ORDER BY total_hits DESC
        """
        categories = self.ch_manager.execute_query(category_query)

        return {
            'total_rows': total_rows,
            'top_pages': [{'path': p[0], 'hits': p[1], 'records': p[2]} for p in top_pages],
            'category_breakdown': [{'category': c[0], 'unique_pages': c[1], 'hits': c[2]} for c in categories],
            'generated_at': datetime.now()
        }

    def run(self):
        """
        Execute the full incremental pipeline
        """
        logger.info("Starting Wikipedia Data Engineering Pipeline (Incremental)")

        # Step 1: Setup tables and keywords
        self.setup_tables()

        # Step 2: Create dlt pipeline
        pipeline = dlt.pipeline(
            pipeline_name=DLT_CONFIG['pipeline_name'] + '_incremental',
            destination='duckdb',  # Use DuckDB for pipeline metadata/stats
            dataset_name=DLT_CONFIG['dataset_name']
        )

        # Step 3: Get pipeline state
        state = pipeline.state.setdefault('sources', {}).setdefault('wikipedia', {})
        logger.info(f"Current pipeline state: processed_files={len(state.get('processed_files', []))}")

        # Step 4: Load Wikipedia pageviews incrementally
        load_stats = self.load_wikipedia_pageviews_incremental(state)

        if load_stats:
            # Store stats and update state in dlt
            load_info = pipeline.run([
                dlt.resource(load_stats, name="pageview_load_stats", write_disposition="append")
            ])

            # IMPORTANT: Explicitly save the updated state
            pipeline.state['sources'] = {'wikipedia': state}

            logger.info(f"Pipeline completed: {load_info}")
            logger.info(f"State updated: {len(state.get('processed_files', []))} files now tracked")
        else:
            logger.info("No new data to load")
            load_info = None

        # Step 5: Generate summary statistics
        summary = self.get_summary_statistics()

        logger.info(f"\nSummary Statistics:")
        logger.info(f"  Total rows in ClickHouse: {summary['total_rows']:,}")
        logger.info(f"  Top pages: {len(summary['top_pages'])}")
        logger.info(f"  Categories: {len(summary['category_breakdown'])}")

        # Store summary separately
        if summary['total_rows'] > 0:
            pipeline.run(
                dlt.resource([summary], name="summary_statistics", write_disposition="replace")
            )

        # Step 6: Cleanup
        self.ch_manager.disconnect()

        return {
            'load_info': load_info,
            'load_stats': load_stats,
            'summary': summary
        }


def run_incremental_pipeline():
    """
    Entry point for running the incremental pipeline
    """
    pipeline = WikipediaDataEngineeringPipelineIncremental()
    return pipeline.run()


if __name__ == '__main__':
    run_incremental_pipeline()
