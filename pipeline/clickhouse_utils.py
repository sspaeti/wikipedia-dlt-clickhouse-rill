"""
ClickHouse utility functions for executing SQL and managing connections
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any, List
from clickhouse_driver import Client
import logging

logger = logging.getLogger(__name__)


class ClickHouseManager:
    """
    Manages ClickHouse connections and SQL execution
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize ClickHouse connection

        Args:
            config: Dictionary with host, port, user, password, database
        """
        self.config = config
        self.client = None

    def connect(self) -> Client:
        """Establish connection to ClickHouse"""
        if self.client is None:
            self.client = Client(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database']
            )
            logger.info(f"Connected to ClickHouse at {self.config['host']}:{self.config['port']}")
        return self.client

    def disconnect(self):
        """Close connection to ClickHouse"""
        if self.client:
            self.client.disconnect()
            self.client = None
            logger.info("Disconnected from ClickHouse")

    def execute_sql_file(self, sql_file_path: str, params: Optional[Dict[str, str]] = None) -> Any:
        """
        Execute SQL from a file with optional parameter substitution

        Args:
            sql_file_path: Path to SQL file
            params: Dictionary of parameters to substitute in SQL (e.g., {date_pattern: '2025-10-*'})

        Returns:
            Query results if SELECT, otherwise None
        """
        client = self.connect()

        # Read SQL file
        with open(sql_file_path, 'r') as f:
            sql = f.read()

        # Substitute parameters if provided
        if params:
            for key, value in params.items():
                sql = sql.replace(f'{{{key}}}', value)

        logger.info(f"Executing SQL from {sql_file_path}")

        # Split by semicolons and execute each statement
        statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]

        results = []
        for stmt in statements:
            if stmt:
                logger.debug(f"Executing: {stmt[:100]}...")
                result = client.execute(stmt)
                results.append(result)

        return results[-1] if results else None

    def execute_query(self, query: str, params: Optional[Dict[str, str]] = None) -> List:
        """
        Execute a single SQL query

        Args:
            query: SQL query string
            params: Optional parameters to substitute

        Returns:
            Query results
        """
        client = self.connect()

        if params:
            for key, value in params.items():
                query = query.replace(f'{{{key}}}', value)

        logger.info(f"Executing query: {query[:100]}...")
        return client.execute(query)

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in ClickHouse"""
        client = self.connect()
        result = client.execute(
            f"EXISTS TABLE {table_name}"
        )
        return result[0][0] == 1

    def get_table_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table"""
        client = self.connect()
        result = client.execute(f"SELECT count() FROM {table_name}")
        return result[0][0]


def load_sql_files_in_order(sql_dir: str, ch_manager: ClickHouseManager) -> None:
    """
    Load and execute all SQL files in order (sorted by filename)

    Args:
        sql_dir: Directory containing SQL files
        ch_manager: ClickHouseManager instance
    """
    sql_files = sorted(Path(sql_dir).glob('*.sql'))

    for sql_file in sql_files:
        logger.info(f"Processing {sql_file.name}")
        ch_manager.execute_sql_file(str(sql_file))
