"""
Simple incremental Wikipedia pipeline using a tracking table in DuckDB
More reliable than dlt's internal state for this use case
"""

import dlt
import duckdb
from datetime import datetime
from typing import List, Dict, Any
import logging
from pathlib import Path

from .config import CLICKHOUSE_CONFIG, WIKIPEDIA_CONFIG, SQL_DIR, DLT_CONFIG
from .clickhouse_utils import ClickHouseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SimpleIncrementalPipeline:
    """Simple incremental pipeline using DuckDB for tracking"""

    def __init__(self):
        self.ch_manager = ClickHouseManager(CLICKHOUSE_CONFIG)
        self.sql_dir = SQL_DIR
        self.tracking_db = 'wikipedia_tracking.duckdb'

    def setup_tables(self):
        """Setup ClickHouse tables and DuckDB tracking"""
        logger.info("Setting up ClickHouse tables and keywords...")

        setup_files = [
            '01_create_tables.sql',
            '02_create_keywords.sql',
            '03_insert_keywords.sql'
        ]

        for sql_file in setup_files:
            sql_path = Path(self.sql_dir) / sql_file
            logger.info(f"Executing {sql_file}")
            self.ch_manager.execute_sql_file(str(sql_path))

        keywords_count = self.ch_manager.get_table_row_count('data_engineering_keywords')
        logger.info(f"Keywords table contains {keywords_count} entries")

        # Setup tracking table in DuckDB
        conn = duckdb.connect(self.tracking_db)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                file_pattern VARCHAR PRIMARY KEY,
                rows_inserted INTEGER,
                processed_at TIMESTAMP,
                status VARCHAR
            )
        """)
        conn.close()
        logger.info("Tracking table ready in DuckDB")

    def get_processed_files(self) -> set:
        """Get set of already processed files from DuckDB"""
        conn = duckdb.connect(self.tracking_db)
        result = conn.execute("SELECT file_pattern FROM processed_files WHERE status = 'success'").fetchall()
        conn.close()
        return {row[0] for row in result}

    def mark_file_processed(self, file_pattern: str, rows_inserted: int, status: str):
        """Mark a file as processed in DuckDB"""
        conn = duckdb.connect(self.tracking_db)
        conn.execute("""
            INSERT OR REPLACE INTO processed_files (file_pattern, rows_inserted, processed_at, status)
            VALUES (?, ?, ?, ?)
        """, [file_pattern, rows_inserted, datetime.now(), status])
        conn.close()

    def generate_date_patterns(self, year: int, month: int, days: List[int], hours: List[int]) -> List[str]:
        """Generate file patterns for Wikipedia dumps"""
        patterns = []
        for day in days:
            for hour in hours:
                date_str = f"{year}{month:02d}{day:02d}"
                hour_str = f"{hour:02d}0000"
                pattern = f"{year}/{year}-{month:02d}/pageviews-{date_str}-{hour_str}.gz"
                patterns.append(pattern)
        return patterns

    def load_pageviews_for_date(self, date_pattern: str) -> int:
        """Load pageviews for a specific pattern"""
        logger.info(f"Loading: {date_pattern}")

        before_count = self.ch_manager.get_table_row_count('wikistat_data_engineering')

        sql_path = Path(self.sql_dir) / '04_load_filtered_pageviews.sql'
        params = {
            'base_url': WIKIPEDIA_CONFIG['base_url'],
            'date_pattern': date_pattern
        }

        self.ch_manager.execute_sql_file(str(sql_path), params=params)

        after_count = self.ch_manager.get_table_row_count('wikistat_data_engineering')
        rows_inserted = after_count - before_count

        logger.info(f"  → Inserted {rows_inserted} rows")
        return rows_inserted

    def run(self):
        """Execute the incremental pipeline"""
        logger.info("=" * 60)
        logger.info("INCREMENTAL PIPELINE (Simple Tracking)")
        logger.info("=" * 60)

        # Setup
        self.setup_tables()

        # Get all files to process
        all_patterns = self.generate_date_patterns(
            WIKIPEDIA_CONFIG['year'],
            WIKIPEDIA_CONFIG['month'],
            WIKIPEDIA_CONFIG['days'],
            WIKIPEDIA_CONFIG['hours']
        )

        # Filter out already processed
        processed = self.get_processed_files()
        new_patterns = [p for p in all_patterns if p not in processed]

        logger.info(f"\nFiles status:")
        logger.info(f"  Total configured: {len(all_patterns)}")
        logger.info(f"  Already processed: {len(processed)}")
        logger.info(f"  New to process: {len(new_patterns)}")

        if not new_patterns:
            logger.info("\n✓ All files already processed! No work to do.")
            self.ch_manager.disconnect()
            total_rows = self.ch_manager.get_table_row_count('wikistat_data_engineering')
            return {'new_files': 0, 'total_rows': total_rows}

        # Process new files
        logger.info("\nProcessing new files:")
        total_inserted = 0

        for pattern in new_patterns:
            try:
                rows = self.load_pageviews_for_date(pattern)
                self.mark_file_processed(pattern, rows, 'success')
                total_inserted += rows
            except Exception as e:
                logger.error(f"  ✗ Failed {pattern}: {e}")
                self.mark_file_processed(pattern, 0, 'failed')

        # Summary
        self.ch_manager.disconnect()
        total_rows = self.ch_manager.get_table_row_count('wikistat_data_engineering')

        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)
        logger.info(f"New rows inserted: {total_inserted:,}")
        logger.info(f"Total rows in ClickHouse: {total_rows:,}")

        return {
            'new_files': len(new_patterns),
            'rows_inserted': total_inserted,
            'total_rows': total_rows
        }


def run_simple_incremental():
    """Entry point"""
    pipeline = SimpleIncrementalPipeline()
    return pipeline.run()


if __name__ == '__main__':
    run_simple_incremental()
