"""Database connection pooling, schema initialization, and high-throughput batch operations.

Supports PostgreSQL via psycopg2 connection pooling with robust error handling,
automatic retries, and high-performance execute_values bulk inserts.
Also includes a SQLite fallback adapter for local standalone testing without Docker.
"""

from __future__ import annotations

import logging
import sqlite3
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

# Ensure repository root is on sys.path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import pandas as pd

from config.settings import DatabaseSettings, settings

logger = logging.getLogger(__name__)

# Check psycopg2 availability
try:
    import psycopg2
    from psycopg2 import pool
    from psycopg2.extras import DictCursor, execute_values
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    logger.warning("psycopg2 is not installed. Database will run in SQLite compatibility mode.")


class DatabaseManager:
    """Enterprise Database Manager supporting connection pooling, ACID transactions,
    and bulk loading for real-time telemetry streaming.
    """

    def __init__(self, db_settings: Optional[DatabaseSettings] = None, use_sqlite_fallback: bool = False):
        self.settings = db_settings or settings.db
        self.use_sqlite = use_sqlite_fallback or not PSYCOPG2_AVAILABLE
        self._pool: Optional[Any] = None
        self._sqlite_path: str = "realtime_analytics_local.db"

        if not self.use_sqlite:
            self._init_postgres_pool()

    def _init_postgres_pool(self, max_retries: int = 4, retry_delay: float = 2.0) -> None:
        """Initialize PostgreSQL Threaded Connection Pool with exponential backoff."""
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    "Connecting to PostgreSQL at %s:%s/%s (Attempt %d/%d)...",
                    self.settings.host,
                    self.settings.port,
                    self.settings.database,
                    attempt,
                    max_retries,
                )
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=self.settings.min_pool_size,
                    maxconn=self.settings.max_pool_size,
                    host=self.settings.host,
                    port=self.settings.port,
                    database=self.settings.database,
                    user=self.settings.user,
                    password=self.settings.password,
                    connect_timeout=self.settings.connect_timeout,
                )
                logger.info("PostgreSQL connection pool established successfully.")
                return
            except Exception as exc:
                logger.warning("PostgreSQL connection failed (attempt %d/%d): %s", attempt, max_retries, exc)
                if attempt < max_retries:
                    time.sleep(retry_delay * attempt)
                else:
                    logger.error("Could not connect to PostgreSQL. Falling back to local SQLite engine.")
                    self.use_sqlite = True

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """Context manager yielding a transactional database connection."""
        if self.use_sqlite:
            conn = sqlite3.connect(self._sqlite_path, timeout=15.0)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
        else:
            if not self._pool:
                self._init_postgres_pool()
                if self.use_sqlite:
                    # Switched to SQLite fallback
                    with self.get_connection() as conn:
                        yield conn
                    return

            conn = self._pool.getconn()
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                if self._pool:
                    self._pool.putconn(conn)

    def health_check(self) -> bool:
        """Check whether the active database engine is responsive."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1;")
                cursor.fetchone()
                return True
        except Exception as exc:
            logger.error("Database health check failed: %s", exc)
            return False

    def init_schema(self, ddl_path: Optional[Union[str, Path]] = None) -> None:
        """Execute DDL statements to set up tables, constraints, and indexes."""
        if self.use_sqlite:
            self._init_sqlite_schema()
            return

        if ddl_path is None:
            # Default to sql/real_time_queries.sql relative to repo root
            root_dir = Path(__file__).resolve().parent.parent
            ddl_path = root_dir / "sql" / "real_time_queries.sql"

        path_obj = Path(ddl_path)
        if not path_obj.exists():
            logger.warning("DDL script not found at %s. Creating minimal tables.", path_obj)
            self._init_minimal_postgres_schema()
            return

        logger.info("Applying PostgreSQL schema DDL from: %s", path_obj)
        sql_content = path_obj.read_text(encoding="utf-8")
        
        # Split and execute DDL statements (up to the query section)
        ddl_statements = []
        for stmt in sql_content.split(";"):
            cleaned = stmt.strip()
            # Stop when reaching analytical queries
            if "-- QUERY 1:" in cleaned or "WITH minute_revenue AS" in cleaned:
                break
            if cleaned and not cleaned.startswith("--"):
                ddl_statements.append(cleaned)

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for statement in ddl_statements:
                    try:
                        cur.execute(statement)
                    except Exception as err:
                        logger.debug("Statement skipped or notice: %s (%s)", statement[:50], err)
        logger.info("PostgreSQL schema successfully initialized.")

    def _init_minimal_postgres_schema(self) -> None:
        """Create baseline tables directly in PostgreSQL."""
        commands = [
            """
            CREATE TABLE IF NOT EXISTS raw_orders (
                order_id VARCHAR(64) PRIMARY KEY,
                customer_id VARCHAR(64) NOT NULL,
                product_id VARCHAR(64) NOT NULL,
                product_name VARCHAR(128) NOT NULL,
                category VARCHAR(64) NOT NULL,
                quantity INT NOT NULL,
                unit_price NUMERIC(10, 2) NOT NULL,
                total_amount NUMERIC(12, 2) NOT NULL,
                payment_method VARCHAR(32) NOT NULL,
                country VARCHAR(32) NOT NULL,
                city VARCHAR(64) NOT NULL,
                status VARCHAR(24) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS raw_pageviews (
                event_id VARCHAR(64) PRIMARY KEY,
                session_id VARCHAR(64) NOT NULL,
                user_id VARCHAR(64) NOT NULL,
                page_type VARCHAR(32) NOT NULL,
                referrer VARCHAR(64) NOT NULL,
                device_type VARCHAR(24) NOT NULL,
                latency_ms INT NOT NULL,
                bounce BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS processed_transactions (
                order_id VARCHAR(64) PRIMARY KEY,
                customer_id VARCHAR(64) NOT NULL,
                product_id VARCHAR(64) NOT NULL,
                category VARCHAR(64) NOT NULL,
                quantity INT NOT NULL,
                unit_price NUMERIC(10, 2) NOT NULL,
                gross_amount NUMERIC(12, 2) NOT NULL,
                tax_amount NUMERIC(10, 2) NOT NULL,
                net_revenue NUMERIC(12, 2) NOT NULL,
                estimated_margin NUMERIC(12, 2) NOT NULL,
                payment_method VARCHAR(32) NOT NULL,
                country VARCHAR(32) NOT NULL,
                city VARCHAR(64) NOT NULL,
                status VARCHAR(24) NOT NULL,
                is_fraud_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
                anomaly_reason VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS realtime_metrics_1m (
                window_start TIMESTAMP WITH TIME ZONE PRIMARY KEY,
                window_end TIMESTAMP WITH TIME ZONE NOT NULL,
                total_orders INT NOT NULL,
                completed_orders INT NOT NULL,
                failed_orders INT NOT NULL,
                total_gross_revenue NUMERIC(14, 2) NOT NULL,
                total_net_revenue NUMERIC(14, 2) NOT NULL,
                avg_order_value NUMERIC(10, 2) NOT NULL,
                unique_customers INT NOT NULL,
                anomaly_count INT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS dead_letter_events (
                dlq_id BIGSERIAL PRIMARY KEY,
                event_type VARCHAR(32) NOT NULL,
                raw_payload JSONB NOT NULL,
                error_reason TEXT NOT NULL,
                source_ip VARCHAR(45),
                failed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
        ]
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for cmd in commands:
                    cur.execute(cmd)

    def _init_sqlite_schema(self) -> None:
        """Initialize SQLite schema for local standalone development."""
        logger.info("Initializing SQLite local tables in %s", self._sqlite_path)
        queries = [
            """
            CREATE TABLE IF NOT EXISTS raw_orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                product_name TEXT NOT NULL,
                category TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                total_amount REAL NOT NULL,
                payment_method TEXT NOT NULL,
                country TEXT NOT NULL,
                city TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                ingested_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS raw_pageviews (
                event_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                page_type TEXT NOT NULL,
                referrer TEXT NOT NULL,
                device_type TEXT NOT NULL,
                latency_ms INTEGER NOT NULL,
                bounce INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                ingested_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS processed_transactions (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                category TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                gross_amount REAL NOT NULL,
                tax_amount REAL NOT NULL,
                net_revenue REAL NOT NULL,
                estimated_margin REAL NOT NULL,
                payment_method TEXT NOT NULL,
                country TEXT NOT NULL,
                city TEXT NOT NULL,
                status TEXT NOT NULL,
                is_fraud_anomaly INTEGER NOT NULL DEFAULT 0,
                anomaly_reason TEXT,
                created_at TEXT NOT NULL,
                processed_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS realtime_metrics_1m (
                window_start TEXT PRIMARY KEY,
                window_end TEXT NOT NULL,
                total_orders INTEGER NOT NULL,
                completed_orders INTEGER NOT NULL,
                failed_orders INTEGER NOT NULL,
                total_gross_revenue REAL NOT NULL,
                total_net_revenue REAL NOT NULL,
                avg_order_value REAL NOT NULL,
                unique_customers INTEGER NOT NULL,
                anomaly_count INTEGER NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS dead_letter_events (
                dlq_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                error_reason TEXT NOT NULL,
                source_ip TEXT,
                failed_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """
        ]
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for q in queries:
                cursor.execute(q)

    # -------------------------------------------------------------------------
    # High Performance Bulk Insertion Operations
    # -------------------------------------------------------------------------

    def insert_orders_batch(self, orders: List[Dict[str, Any]]) -> int:
        """High-throughput bulk insert of order events into raw_orders."""
        if not orders:
            return 0

        columns = [
            "order_id", "customer_id", "product_id", "product_name",
            "category", "quantity", "unit_price", "total_amount",
            "payment_method", "country", "city", "status", "created_at"
        ]

        if self.use_sqlite:
            placeholders = ", ".join(["?"] * len(columns))
            sql = f"INSERT OR IGNORE INTO raw_orders ({', '.join(columns)}) VALUES ({placeholders})"
            data = [tuple(o.get(c) for c in columns) for o in orders]
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.executemany(sql, data)
                return len(orders)

        # PostgreSQL optimized insert using execute_values
        query = f"""
            INSERT INTO raw_orders ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (order_id) DO NOTHING;
        """
        data = [[o.get(c) for c in columns] for o in orders]
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, query, data)
        return len(orders)

    def insert_pageviews_batch(self, pageviews: List[Dict[str, Any]]) -> int:
        """High-throughput bulk insert of telemetry pageviews."""
        if not pageviews:
            return 0

        columns = [
            "event_id", "session_id", "user_id", "page_type",
            "referrer", "device_type", "latency_ms", "bounce", "created_at"
        ]

        if self.use_sqlite:
            placeholders = ", ".join(["?"] * len(columns))
            sql = f"INSERT OR IGNORE INTO raw_pageviews ({', '.join(columns)}) VALUES ({placeholders})"
            data = [tuple(p.get(c) for c in columns) for p in pageviews]
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.executemany(sql, data)
                return len(pageviews)

        query = f"""
            INSERT INTO raw_pageviews ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (event_id) DO NOTHING;
        """
        data = [[p.get(c) for c in columns] for p in pageviews]
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, query, data)
        return len(pageviews)

    def insert_processed_transactions_batch(self, txs: List[Dict[str, Any]]) -> int:
        """Insert enriched transactions into processed_transactions table."""
        if not txs:
            return 0

        columns = [
            "order_id", "customer_id", "product_id", "category", "quantity",
            "unit_price", "gross_amount", "tax_amount", "net_revenue",
            "estimated_margin", "payment_method", "country", "city",
            "status", "is_fraud_anomaly", "anomaly_reason", "created_at"
        ]

        if self.use_sqlite:
            placeholders = ", ".join(["?"] * len(columns))
            sql = f"INSERT OR REPLACE INTO processed_transactions ({', '.join(columns)}) VALUES ({placeholders})"
            data = [tuple(t.get(c) for c in columns) for t in txs]
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.executemany(sql, data)
                return len(txs)

        query = f"""
            INSERT INTO processed_transactions ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (order_id) DO UPDATE SET
                status = EXCLUDED.status,
                is_fraud_anomaly = EXCLUDED.is_fraud_anomaly,
                anomaly_reason = EXCLUDED.anomaly_reason;
        """
        data = [[t.get(c) for c in columns] for t in txs]
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, query, data)
        return len(txs)

    def upsert_metrics_1m(self, metrics: Dict[str, Any]) -> None:
        """Upsert 1-minute window rollup metrics into realtime_metrics_1m."""
        columns = [
            "window_start", "window_end", "total_orders", "completed_orders",
            "failed_orders", "total_gross_revenue", "total_net_revenue",
            "avg_order_value", "unique_customers", "anomaly_count"
        ]

        if self.use_sqlite:
            placeholders = ", ".join(["?"] * len(columns))
            sql = f"INSERT OR REPLACE INTO realtime_metrics_1m ({', '.join(columns)}) VALUES ({placeholders})"
            values = tuple(metrics.get(c) for c in columns)
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(sql, values)
            return

        query = f"""
            INSERT INTO realtime_metrics_1m ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))})
            ON CONFLICT (window_start) DO UPDATE SET
                total_orders = EXCLUDED.total_orders,
                completed_orders = EXCLUDED.completed_orders,
                failed_orders = EXCLUDED.failed_orders,
                total_gross_revenue = EXCLUDED.total_gross_revenue,
                total_net_revenue = EXCLUDED.total_net_revenue,
                avg_order_value = EXCLUDED.avg_order_value,
                unique_customers = EXCLUDED.unique_customers,
                anomaly_count = EXCLUDED.anomaly_count;
        """
        values = tuple(metrics.get(c) for c in columns)
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, values)

    def insert_dlq_event(self, event_type: str, raw_payload: str, error_reason: str, source_ip: str = "127.0.0.1") -> None:
        """Send bad schema or malformed events to Dead Letter Queue."""
        with self.get_connection() as conn:
            cur = conn.cursor()
            if self.use_sqlite:
                cur.execute(
                    "INSERT INTO dead_letter_events (event_type, raw_payload, error_reason, source_ip) VALUES (?, ?, ?, ?)",
                    (event_type, str(raw_payload), error_reason, source_ip),
                )
            else:
                cur.execute(
                    "INSERT INTO dead_letter_events (event_type, raw_payload, error_reason, source_ip) VALUES (%s, %s::jsonb, %s, %s)",
                    (event_type, str(raw_payload), error_reason, source_ip),
                )

    # -------------------------------------------------------------------------
    # Analytical Query Helpers for Dashboard
    # -------------------------------------------------------------------------

    def query_to_dataframe(self, sql: str, params: Optional[Union[tuple, list]] = None) -> pd.DataFrame:
        """Execute analytical query and return clean Pandas DataFrame."""
        with self.get_connection() as conn:
            try:
                if self.use_sqlite:
                    # SQLite does not support interval or postgres date_trunc directly,
                    # callers provide compatible queries or fallback
                    df = pd.read_sql_query(sql, conn, params=params)
                else:
                    df = pd.read_sql_query(sql, conn, params=params)
                return df
            except Exception as exc:
                logger.error("Query execution failed: %s | SQL: %s", exc, sql[:100])
                return pd.DataFrame()

    def close(self) -> None:
        """Close connection pool gracefully."""
        if self._pool and not self._pool.closed:
            self._pool.closeall()
            logger.info("PostgreSQL connection pool closed.")


# Global database manager instance
db = DatabaseManager()
