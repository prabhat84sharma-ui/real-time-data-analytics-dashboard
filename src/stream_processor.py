"""Real-time Stream Processor applying validation, enrichment, anomaly detection,
windowed aggregations, and high-throughput batch sinking.

Architectural highlights:
- Pydantic schema validation with automatic Dead Letter Queue (DLQ) routing
- Real-time stream enrichment (tax computation, margin estimation, geo-tagging)
- Online sliding/tumbling window state tracking (1-min & 5-min rollups)
- Batch micro-flushing to PostgreSQL/SQLite storage
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

# Ensure repository root is on sys.path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from pydantic import BaseModel, Field, ValidationError, field_validator

from config.settings import settings
from src.database import DatabaseManager, db
from src.data_generator import EventGenerator

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# 1. Schema Definitions & Validation Models
# -----------------------------------------------------------------------------

class ValidatedOrder(BaseModel):
    """Pydantic model enforcing strict order event contract."""
    order_id: str = Field(min_length=3, max_length=64)
    customer_id: str = Field(min_length=3, max_length=64)
    product_id: str = Field(min_length=3, max_length=64)
    product_name: str = Field(min_length=1, max_length=128)
    category: str = Field(min_length=1, max_length=64)
    quantity: int = Field(gt=0, le=1000)
    unit_price: float = Field(ge=0.0)
    total_amount: float = Field(ge=0.0)
    payment_method: str = Field(min_length=2, max_length=32)
    country: str = Field(min_length=2, max_length=32)
    city: str = Field(min_length=1, max_length=64)
    status: str = Field(pattern="^(COMPLETED|FAILED|PENDING|REFUNDED)$")
    created_at: str

    @field_validator("created_at")
    @classmethod
    def validate_timestamp(cls, val: str) -> str:
        try:
            # Confirm ISO-8601 parsable
            datetime.fromisoformat(val.replace("Z", "+00:00"))
            return val
        except Exception as exc:
            raise ValueError(f"Invalid timestamp format: {val}") from exc


class ValidatedPageview(BaseModel):
    """Pydantic model enforcing clickstream telemetry contract."""
    event_id: str = Field(min_length=3, max_length=64)
    session_id: str = Field(min_length=3, max_length=64)
    user_id: str = Field(min_length=3, max_length=64)
    page_type: str = Field(pattern="^(home|search|product_detail|checkout)$")
    referrer: str = Field(min_length=1, max_length=64)
    device_type: str = Field(pattern="^(Desktop|Mobile|Tablet)$")
    latency_ms: int = Field(ge=0, le=30000)
    bounce: bool = False
    created_at: str


# -----------------------------------------------------------------------------
# 2. Window Aggregation State Store
# -----------------------------------------------------------------------------

class WindowMetrics:
    """Maintains state for a discrete time window."""

    def __init__(self, window_start: datetime, window_end: datetime):
        self.window_start = window_start
        self.window_end = window_end
        self.total_orders = 0
        self.completed_orders = 0
        self.failed_orders = 0
        self.total_gross_revenue = 0.0
        self.total_net_revenue = 0.0
        self.unique_customers: set[str] = set()
        self.anomaly_count = 0

    def add_order(self, order: Dict[str, Any], is_anomaly: bool) -> None:
        self.total_orders += 1
        gross = float(order.get("total_amount", 0.0))
        net = float(order.get("net_revenue", gross * 0.90))

        if order.get("status") == "COMPLETED":
            self.completed_orders += 1
            self.total_gross_revenue += gross
            self.total_net_revenue += net
        elif order.get("status") == "FAILED":
            self.failed_orders += 1

        self.unique_customers.add(order.get("customer_id", ""))
        if is_anomaly:
            self.anomaly_count += 1

    def to_dict(self) -> Dict[str, Any]:
        avg_order_value = (
            round(self.total_gross_revenue / self.completed_orders, 2)
            if self.completed_orders > 0
            else 0.0
        )
        return {
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "total_orders": self.total_orders,
            "completed_orders": self.completed_orders,
            "failed_orders": self.failed_orders,
            "total_gross_revenue": round(self.total_gross_revenue, 2),
            "total_net_revenue": round(self.total_net_revenue, 2),
            "avg_order_value": avg_order_value,
            "unique_customers": len(self.unique_customers),
            "anomaly_count": self.anomaly_count,
        }


# -----------------------------------------------------------------------------
# 3. Core Stream Processor
# -----------------------------------------------------------------------------

class StreamProcessor:
    """Real-time event stream processor with batching, validation, and analytics sinking."""

    CATEGORY_MARGINS = {
        "Electronics": 0.28,
        "Fashion": 0.45,
        "Home & Kitchen": 0.35,
        "Sports & Fitness": 0.38,
        "Books & Education": 0.50,
    }

    COUNTRY_TAX_RATES = {
        "United States": 0.0825,
        "United Kingdom": 0.20,
        "Germany": 0.19,
        "India": 0.18,
        "Japan": 0.10,
        "Canada": 0.13,
        "Australia": 0.10,
    }

    def __init__(self, database_manager: Optional[DatabaseManager] = None):
        self.db = database_manager or db
        self.batch_size = settings.processor.batch_size
        self.flush_interval = settings.processor.flush_interval_seconds
        self.high_value_threshold = settings.processor.high_value_threshold

        # Buffers for micro-batch sinking
        self._raw_orders_buffer: List[Dict[str, Any]] = []
        self._raw_pageviews_buffer: List[Dict[str, Any]] = []
        self._processed_tx_buffer: List[Dict[str, Any]] = []
        self._last_flush_time = time.time()

        # Sliding statistical window for online anomaly detection
        self._recent_order_amounts: Deque[float] = deque(maxlen=200)

        # 1-minute tumbling window tracking
        self._current_1m_window: Optional[WindowMetrics] = None

        # Statistics
        self.processed_count = 0
        self.dlq_count = 0
        self.running = True

    def process_order(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Validate, enrich, and calculate business fields for an incoming order."""
        try:
            validated = ValidatedOrder(**payload)
        except (ValidationError, Exception) as err:
            logger.warning("Order validation failed: %s -> Routing to DLQ", err)
            self.dlq_count += 1
            if settings.processor.enable_dlq:
                self.db.insert_dlq_event(
                    event_type="ORDER_VALIDATION_ERROR",
                    raw_payload=json.dumps(payload, default=str),
                    error_reason=str(err),
                )
            return None

        data = validated.model_dump()
        gross = data["total_amount"]
        category = data["category"]
        country = data["country"]

        # 1. Tax & Margin Enrichment
        tax_rate = self.COUNTRY_TAX_RATES.get(country, 0.10)
        tax_amount = round(gross * tax_rate, 2)
        net_revenue = round(gross - tax_amount, 2)
        margin_pct = self.CATEGORY_MARGINS.get(category, 0.30)
        estimated_margin = round(net_revenue * margin_pct, 2)

        # 2. Real-Time Anomaly Detection
        is_fraud_anomaly, anomaly_reason = self._detect_anomaly(gross, data["status"])

        enriched_tx = {
            **data,
            "gross_amount": gross,
            "tax_amount": tax_amount,
            "net_revenue": net_revenue,
            "estimated_margin": estimated_margin,
            "is_fraud_anomaly": is_fraud_anomaly,
            "anomaly_reason": anomaly_reason,
        }

        # Update statistical sample
        self._recent_order_amounts.append(gross)

        # Add to batch buffers
        self._raw_orders_buffer.append(data)
        self._processed_tx_buffer.append(enriched_tx)

        # Update 1-minute window rollup
        self._update_window_metrics(enriched_tx, is_fraud_anomaly)

        self.processed_count += 1
        return enriched_tx

    def process_pageview(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Validate and buffer clickstream pageview telemetry."""
        try:
            validated = ValidatedPageview(**payload)
        except (ValidationError, Exception) as err:
            logger.warning("Pageview validation failed: %s -> Routing to DLQ", err)
            self.dlq_count += 1
            if settings.processor.enable_dlq:
                self.db.insert_dlq_event(
                    event_type="PAGEVIEW_VALIDATION_ERROR",
                    raw_payload=json.dumps(payload, default=str),
                    error_reason=str(err),
                )
            return None

        data = validated.model_dump()
        self._raw_pageviews_buffer.append(data)
        self.processed_count += 1
        return data

    def _detect_anomaly(self, total_amount: float, status: str) -> Tuple[bool, Optional[str]]:
        """Online anomaly detection based on statistical thresholds and status checks."""
        reasons = []
        is_anomaly = False

        if total_amount >= self.high_value_threshold:
            reasons.append(f"High ticket spike (${total_amount:.2f} >= ${self.high_value_threshold:.2f})")
            is_anomaly = True

        # Statistical outlier detection if sufficient baseline exists
        if len(self._recent_order_amounts) >= 30:
            mean = sum(self._recent_order_amounts) / len(self._recent_order_amounts)
            variance = sum((x - mean) ** 2 for x in self._recent_order_amounts) / len(self._recent_order_amounts)
            stddev = variance ** 0.5
            if stddev > 0:
                z_score = (total_amount - mean) / stddev
                if z_score >= 3.2:
                    reasons.append(f"Statistical outlier (Z-Score: {z_score:.2f})")
                    is_anomaly = True

        if status == "FAILED" and is_anomaly:
            reasons.append("Failed high-value attempt")

        return is_anomaly, "; ".join(reasons) if reasons else None

    def _update_window_metrics(self, order: Dict[str, Any], is_anomaly: bool) -> None:
        """Accumulate metrics into current 1-minute tumbling window."""
        now = datetime.now(timezone.utc)
        current_minute_start = now.replace(second=0, microsecond=0)
        current_minute_end = current_minute_start.replace(second=59, microsecond=999999)

        if not self._current_1m_window:
            self._current_1m_window = WindowMetrics(current_minute_start, current_minute_end)

        # Check if window boundary crossed
        if now > self._current_1m_window.window_end:
            # Flush previous window to database
            metrics_dict = self._current_1m_window.to_dict()
            try:
                self.db.upsert_metrics_1m(metrics_dict)
                logger.info("Window flushed: %s | Revenue: $%s | Orders: %d",
                            metrics_dict["window_start"], metrics_dict["total_gross_revenue"], metrics_dict["total_orders"])
            except Exception as exc:
                logger.error("Failed to persist window metrics: %s", exc)

            # Start new window
            self._current_1m_window = WindowMetrics(current_minute_start, current_minute_end)

        self._current_1m_window.add_order(order, is_anomaly)

    def check_flush(self, force: bool = False) -> None:
        """Flush buffered events to database if batch size or time threshold is reached."""
        time_elapsed = time.time() - self._last_flush_time
        should_flush = (
            force
            or len(self._raw_orders_buffer) >= self.batch_size
            or len(self._raw_pageviews_buffer) >= self.batch_size
            or (time_elapsed >= self.flush_interval and (self._raw_orders_buffer or self._raw_pageviews_buffer))
        )

        if not should_flush:
            return

        # Flush raw orders & processed transactions
        if self._raw_orders_buffer:
            orders_to_flush = self._raw_orders_buffer[:]
            tx_to_flush = self._processed_tx_buffer[:]
            self._raw_orders_buffer.clear()
            self._processed_tx_buffer.clear()

            try:
                self.db.insert_orders_batch(orders_to_flush)
                self.db.insert_processed_transactions_batch(tx_to_flush)
                logger.debug("Flushed %d orders and %d processed transactions to storage.", len(orders_to_flush), len(tx_to_flush))
            except Exception as exc:
                logger.error("Error sinking orders batch: %s", exc)

        # Flush raw pageviews
        if self._raw_pageviews_buffer:
            pv_to_flush = self._raw_pageviews_buffer[:]
            self._raw_pageviews_buffer.clear()

            try:
                self.db.insert_pageviews_batch(pv_to_flush)
                logger.debug("Flushed %d pageview events to storage.", len(pv_to_flush))
            except Exception as exc:
                logger.error("Error sinking pageviews batch: %s", exc)

        self._last_flush_time = time.time()

    def run_pipeline(self, generator: EventGenerator, max_events: Optional[int] = None) -> None:
        """Run the live ingestion pipeline reading from the generator."""
        logger.info("Initializing database schema...")
        self.db.init_schema()

        def handle_shutdown(signum: int, frame: Any) -> None:
            logger.info("Received interrupt signal (%s). Committing remaining buffers...", signum)
            self.running = False

        signal.signal(signal.SIGINT, handle_shutdown)
        signal.signal(signal.SIGTERM, handle_shutdown)

        logger.info("Stream Processor active. Ingesting live stream...")
        start_time = time.time()

        for event in generator.stream(total_events=max_events):
            if not self.running:
                break

            ev_type = event.get("type")
            payload = event.get("payload", {})

            if ev_type == "order":
                self.process_order(payload)
            elif ev_type == "pageview":
                self.process_pageview(payload)

            self.check_flush()

        # Final cleanup flush
        self.check_flush(force=True)
        if self._current_1m_window:
            self.db.upsert_metrics_1m(self._current_1m_window.to_dict())

        duration = time.time() - start_time
        throughput = self.processed_count / duration if duration > 0 else 0
        logger.info(
            "Pipeline completed: Processed %d events in %.2fs (Throughput: %.1f evt/s, DLQ: %d)",
            self.processed_count,
            duration,
            throughput,
            self.dlq_count,
        )


def main() -> None:
    """CLI execution entrypoint."""
    parser = argparse.ArgumentParser(description="Real-Time Stream Processor Pipeline")
    parser.add_argument("--rate", type=float, default=15.0, help="Simulated event rate")
    parser.add_argument("--count", type=int, default=150, help="Total events to process (0 for infinite)")
    parser.add_argument("--sqlite", action="store_true", help="Force SQLite fallback mode")
    args = parser.parse_args()

    custom_db = DatabaseManager(use_sqlite_fallback=args.sqlite)
    processor = StreamProcessor(database_manager=custom_db)
    generator = EventGenerator(events_per_second=args.rate)

    processor.run_pipeline(generator, max_events=args.count if args.count > 0 else None)


if __name__ == "__main__":
    main()
