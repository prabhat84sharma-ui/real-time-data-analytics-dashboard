"""Unit and Integration tests for the Real-Time Stream Processor Pipeline.

Tests schema validation, tax/margin business enrichment, statistical anomaly detection,
windowed metric rollups, and Dead Letter Queue routing.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

# Ensure repository root is on sys.path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import pytest
from pydantic import ValidationError

from src.database import DatabaseManager
from src.data_generator import EventGenerator
from src.stream_processor import (
    StreamProcessor,
    ValidatedOrder,
    ValidatedPageview,
    WindowMetrics,
)


@pytest.fixture
def mock_db() -> DatabaseManager:
    """Fixture providing an isolated in-memory or mock database manager."""
    manager = DatabaseManager(use_sqlite_fallback=True)
    manager._sqlite_path = ":memory:"
    manager.init_schema()
    return manager


@pytest.fixture
def processor(mock_db: DatabaseManager) -> StreamProcessor:
    """Fixture providing StreamProcessor instance with mocked database."""
    return StreamProcessor(database_manager=mock_db)


@pytest.fixture
def valid_order_payload() -> Dict[str, Any]:
    """Sample valid order payload."""
    return {
        "order_id": "ORD-TEST-1001",
        "customer_id": "CUST-5555",
        "product_id": "PROD-ELEC-001",
        "product_name": "UltraHD 4K Monitor",
        "category": "Electronics",
        "quantity": 2,
        "unit_price": 499.99,
        "total_amount": 999.98,
        "payment_method": "Credit Card",
        "country": "United States",
        "city": "Austin",
        "status": "COMPLETED",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


@pytest.fixture
def valid_pageview_payload() -> Dict[str, Any]:
    """Sample valid pageview payload."""
    return {
        "event_id": "PV-TEST-2001",
        "session_id": "SESS-TEST-888",
        "user_id": "CUST-5555",
        "page_type": "product_detail",
        "referrer": "google_search",
        "device_type": "Desktop",
        "latency_ms": 65,
        "bounce": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


# -----------------------------------------------------------------------------
# 1. Pydantic Contract Validation Tests
# -----------------------------------------------------------------------------

def test_valid_order_schema(valid_order_payload: Dict[str, Any]):
    """Ensure properly formatted order passes validation."""
    model = ValidatedOrder(**valid_order_payload)
    assert model.order_id == "ORD-TEST-1001"
    assert model.quantity == 2
    assert model.total_amount == 999.98


def test_invalid_order_negative_quantity(valid_order_payload: Dict[str, Any]):
    """Ensure orders with invalid negative quantity fail validation."""
    payload = valid_order_payload.copy()
    payload["quantity"] = -5
    with pytest.raises(ValidationError):
        ValidatedOrder(**payload)


def test_invalid_order_bad_status(valid_order_payload: Dict[str, Any]):
    """Ensure unrecognized order status fails regex pattern."""
    payload = valid_order_payload.copy()
    payload["status"] = "UNKNOWN_STATUS"
    with pytest.raises(ValidationError):
        ValidatedOrder(**payload)


def test_valid_pageview_schema(valid_pageview_payload: Dict[str, Any]):
    """Ensure properly formatted pageview event passes validation."""
    model = ValidatedPageview(**valid_pageview_payload)
    assert model.event_id == "PV-TEST-2001"
    assert model.page_type == "product_detail"
    assert model.latency_ms == 65


def test_invalid_pageview_page_type(valid_pageview_payload: Dict[str, Any]):
    """Ensure pageview with unlisted page type fails validation."""
    payload = valid_pageview_payload.copy()
    payload["page_type"] = "admin_dashboard"
    with pytest.raises(ValidationError):
        ValidatedPageview(**payload)


# -----------------------------------------------------------------------------
# 2. Business Enrichment & Calculation Tests
# -----------------------------------------------------------------------------

def test_tax_and_margin_enrichment(processor: StreamProcessor, valid_order_payload: Dict[str, Any]):
    """Verify exact tax and gross margin business calculations."""
    enriched = processor.process_order(valid_order_payload)
    assert enriched is not None

    # United States tax rate = 8.25%
    expected_tax = round(999.98 * 0.0825, 2)
    expected_net = round(999.98 - expected_tax, 2)
    # Electronics margin = 28%
    expected_margin = round(expected_net * 0.28, 2)

    assert enriched["tax_amount"] == expected_tax
    assert enriched["net_revenue"] == expected_net
    assert enriched["estimated_margin"] == expected_margin


# -----------------------------------------------------------------------------
# 3. Anomaly & Outlier Detection Tests
# -----------------------------------------------------------------------------

def test_high_value_anomaly_detection(processor: StreamProcessor, valid_order_payload: Dict[str, Any]):
    """Ensure transactions exceeding threshold are flagged as fraud/anomalies."""
    payload = valid_order_payload.copy()
    payload["total_amount"] = 3500.00  # Exceeds $1,500 threshold
    enriched = processor.process_order(payload)

    assert enriched is not None
    assert enriched["is_fraud_anomaly"] is True
    assert "High ticket spike" in enriched["anomaly_reason"]


def test_statistical_outlier_zscore_detection(processor: StreamProcessor, valid_order_payload: Dict[str, Any]):
    """Ensure sudden volume spike triggers Z-Score statistical anomaly."""
    # Seed 35 normal transactions around $100
    for i in range(35):
        p = valid_order_payload.copy()
        p["order_id"] = f"ORD-SEED-{i}"
        p["total_amount"] = 100.0 + (i % 5)
        processor.process_order(p)

    # Inject transaction of $1,400 (under $1,500 threshold, but > 10 std deviations!)
    spike_payload = valid_order_payload.copy()
    spike_payload["order_id"] = "ORD-SPIKE-Z"
    spike_payload["total_amount"] = 1400.00
    enriched = processor.process_order(spike_payload)

    assert enriched is not None
    assert enriched["is_fraud_anomaly"] is True
    assert "Statistical outlier" in enriched["anomaly_reason"]


# -----------------------------------------------------------------------------
# 4. Dead Letter Queue (DLQ) Routing Tests
# -----------------------------------------------------------------------------

def test_dead_letter_queue_routing(processor: StreamProcessor):
    """Ensure malformed events are safely routed to DLQ without crashing the pipeline."""
    corrupted_payload = {
        "order_id": None,
        "quantity": -10,
        "total_amount": "invalid_number",
    }
    result = processor.process_order(corrupted_payload)
    assert result is None
    assert processor.dlq_count == 1


# -----------------------------------------------------------------------------
# 5. Tumbling Window Aggregation State Tests
# -----------------------------------------------------------------------------

def test_window_metrics_aggregation():
    """Verify in-memory window metrics accumulation and AOV logic."""
    start = datetime(2026, 10, 15, 12, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 10, 15, 12, 0, 59, tzinfo=timezone.utc)
    metrics = WindowMetrics(window_start=start, window_end=end)

    order1 = {"total_amount": 100.0, "net_revenue": 90.0, "status": "COMPLETED", "customer_id": "C1"}
    order2 = {"total_amount": 200.0, "net_revenue": 180.0, "status": "COMPLETED", "customer_id": "C2"}
    order3 = {"total_amount": 50.0, "net_revenue": 45.0, "status": "FAILED", "customer_id": "C1"}

    metrics.add_order(order1, is_anomaly=False)
    metrics.add_order(order2, is_anomaly=False)
    metrics.add_order(order3, is_anomaly=True)

    d = metrics.to_dict()
    assert d["total_orders"] == 3
    assert d["completed_orders"] == 2
    assert d["failed_orders"] == 1
    assert d["total_gross_revenue"] == 300.0
    assert d["total_net_revenue"] == 270.0
    assert d["avg_order_value"] == 150.0  # 300 / 2 completed
    assert d["unique_customers"] == 2
    assert d["anomaly_count"] == 1


# -----------------------------------------------------------------------------
# 6. Event Generator Contract Tests
# -----------------------------------------------------------------------------

def test_event_generator_structure():
    """Verify synthetic event generator outputs complete contract."""
    gen = EventGenerator(random_seed=42)
    order = gen.generate_order_event()
    assert order["order_id"].startswith("ORD-")
    assert order["quantity"] >= 1
    assert order["total_amount"] > 0
    assert order["status"] in ["COMPLETED", "FAILED", "PENDING", "REFUNDED"]

    pageview = gen.generate_pageview_event()
    assert pageview["event_id"].startswith("PV-")
    assert pageview["latency_ms"] >= 0
    assert pageview["page_type"] in ["home", "search", "product_detail", "checkout"]
