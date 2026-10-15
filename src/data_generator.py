"""Realistic event stream generator simulating high-velocity e-commerce and IoT clickstream data.

Generates realistic order transactions, user browsing events, and controlled
anomalies (spikes, high-value fraud, payment gateway errors, and malformed DLQ payloads).
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

# Ensure repository root is on sys.path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from config.settings import settings

logger = logging.getLogger(__name__)

# Sample Catalog for realistic e-commerce simulation
PRODUCT_CATALOG = [
    {"product_id": "PROD-ELEC-001", "name": "UltraHD 4K Curved Monitor", "category": "Electronics", "price": 499.99},
    {"product_id": "PROD-ELEC-002", "name": "Wireless Noise-Canceling Headphones", "category": "Electronics", "price": 199.50},
    {"product_id": "PROD-ELEC-003", "name": "Mechanical RGB Gaming Keyboard", "category": "Electronics", "price": 129.00},
    {"product_id": "PROD-ELEC-004", "name": "Smart Home Mesh WiFi Router", "category": "Electronics", "price": 249.99},
    {"product_id": "PROD-FASH-101", "name": "Waterproof Trench Coat", "category": "Fashion", "price": 159.00},
    {"product_id": "PROD-FASH-102", "name": "Classic Leather Oxford Shoes", "category": "Fashion", "price": 120.00},
    {"product_id": "PROD-FASH-103", "name": "Merino Wool Sweater", "category": "Fashion", "price": 89.95},
    {"product_id": "PROD-HOME-201", "name": "Smart Robotic Vacuum Cleaner", "category": "Home & Kitchen", "price": 349.00},
    {"product_id": "PROD-HOME-202", "name": "Espresso Machine & Milk Frother", "category": "Home & Kitchen", "price": 420.00},
    {"product_id": "PROD-HOME-203", "name": "Air Purifier with HEPA Filter", "category": "Home & Kitchen", "price": 185.00},
    {"product_id": "PROD-SPRT-301", "name": "Adjustable Smart Dumbbell Set", "category": "Sports & Fitness", "price": 299.99},
    {"product_id": "PROD-SPRT-302", "name": "Carbon Fiber Road Bike Helmet", "category": "Sports & Fitness", "price": 145.00},
    {"product_id": "PROD-BOOK-401", "name": "Designing Data-Intensive Applications", "category": "Books & Education", "price": 45.00},
    {"product_id": "PROD-BOOK-402", "name": "Fundamentals of Data Engineering", "category": "Books & Education", "price": 52.00},
]

COUNTRIES_CITIES = [
    ("United States", "New York"),
    ("United States", "San Francisco"),
    ("United States", "Austin"),
    ("United Kingdom", "London"),
    ("Germany", "Berlin"),
    ("Germany", "Munich"),
    ("India", "Bengaluru"),
    ("India", "New Delhi"),
    ("India", "Mumbai"),
    ("Japan", "Tokyo"),
    ("Canada", "Toronto"),
    ("Australia", "Sydney"),
]

PAYMENT_METHODS = ["Credit Card", "PayPal", "Apple Pay", "Google Pay", "Crypto", "Klarna Buy Now Pay Later"]
PAGE_TYPES = ["home", "search", "product_detail", "checkout"]
REFERRERS = ["google_search", "direct", "twitter", "linkedin", "newsletter_email", "affiliate"]
DEVICE_TYPES = ["Desktop", "Mobile", "Tablet"]


class EventGenerator:
    """Generates realistic synthetic streams of orders and clickstream pageviews."""

    def __init__(
        self,
        events_per_second: Optional[float] = None,
        anomaly_probability: Optional[float] = None,
        random_seed: Optional[int] = None,
    ):
        self.events_per_second = events_per_second or settings.generator.events_per_second
        self.anomaly_probability = anomaly_probability or settings.generator.anomaly_probability
        self.random = random.Random(random_seed or settings.generator.random_seed)

        # Pre-seed pools of user and session IDs for realistic cohort retention
        self._customer_pool = [f"CUST-{self.random.randint(1000, 9999)}" for _ in range(300)]
        self._active_sessions = {cust: f"SESS-{uuid.uuid4().hex[:10]}" for cust in self._customer_pool[:50]}

    def generate_order_event(self, force_anomaly: bool = False) -> Dict[str, Any]:
        """Generate a single order event with realistic pricing, tax, and status."""
        customer_id = self.random.choice(self._customer_pool)
        product = self.random.choice(PRODUCT_CATALOG)
        country, city = self.random.choice(COUNTRIES_CITIES)
        payment_method = self.random.choice(PAYMENT_METHODS)

        is_anomaly = force_anomaly or (self.random.random() < self.anomaly_probability)

        if is_anomaly:
            anomaly_type = self.random.choice(["HIGH_VALUE", "PAYMENT_FAILURE_BURST", "BULK_PURCHASE"])
            if anomaly_type == "HIGH_VALUE":
                quantity = self.random.randint(8, 25)
                unit_price = product["price"] * self.random.uniform(1.5, 3.0)
            elif anomaly_type == "BULK_PURCHASE":
                quantity = self.random.randint(30, 100)
                unit_price = product["price"]
            else:
                quantity = 1
                unit_price = product["price"]
        else:
            quantity = self.random.choices([1, 2, 3, 4], weights=[0.65, 0.20, 0.10, 0.05])[0]
            unit_price = product["price"]

        total_amount = round(quantity * unit_price, 2)

        # Status distribution: 88% Completed, 8% Failed, 3% Pending, 1% Refunded
        if is_anomaly and self.random.random() < 0.6:
            status = "FAILED"
        else:
            status = self.random.choices(
                ["COMPLETED", "FAILED", "PENDING", "REFUNDED"],
                weights=[0.88, 0.08, 0.03, 0.01],
            )[0]

        now = datetime.now(timezone.utc).isoformat()

        return {
            "order_id": f"ORD-{uuid.uuid4().hex[:12].upper()}",
            "customer_id": customer_id,
            "product_id": product["product_id"],
            "product_name": product["name"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": round(unit_price, 2),
            "total_amount": total_amount,
            "payment_method": payment_method,
            "country": country,
            "city": city,
            "status": status,
            "created_at": now,
        }

    def generate_pageview_event(self) -> Dict[str, Any]:
        """Generate a realistic web/mobile browsing telemetry event."""
        customer_id = self.random.choice(self._customer_pool)
        session_id = self._active_sessions.get(customer_id)
        if not session_id or self.random.random() < 0.15:
            session_id = f"SESS-{uuid.uuid4().hex[:10]}"
            self._active_sessions[customer_id] = session_id

        page_type = self.random.choices(PAGE_TYPES, weights=[0.40, 0.30, 0.20, 0.10])[0]
        referrer = self.random.choice(REFERRERS)
        device_type = self.random.choices(DEVICE_TYPES, weights=[0.55, 0.35, 0.10])[0]
        
        # Simulate network latency (mostly 40-180ms, occasional slow tail)
        latency_ms = int(self.random.gauss(80, 25))
        if latency_ms < 15:
            latency_ms = 15
        if self.random.random() < 0.05:  # Tail latency spike
            latency_ms += self.random.randint(300, 1200)

        bounce = page_type == "home" and self.random.random() < 0.35

        return {
            "event_id": f"PV-{uuid.uuid4().hex[:12].upper()}",
            "session_id": session_id,
            "user_id": customer_id,
            "page_type": page_type,
            "referrer": referrer,
            "device_type": device_type,
            "latency_ms": latency_ms,
            "bounce": bounce,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    def generate_malformed_event(self) -> Dict[str, Any]:
        """Generate a corrupted event to test Dead Letter Queue routing and schema validation."""
        return {
            "order_id": None,  # Primary key violation
            "customer_id": "CORRUPTED_PAYLOAD_TEST",
            "quantity": -99,   # Negative quantity violation
            "unit_price": "invalid_numeric_string",
            "total_amount": -500.0,
            "created_at": "corrupted_timestamp",
        }

    def stream(self, total_events: Optional[int] = None) -> Generator[Dict[str, Any], None, None]:
        """Continuous event generator yielding both orders and clickstream telemetry."""
        count = 0
        sleep_interval = 1.0 / self.events_per_second if self.events_per_second > 0 else 0.05

        logger.info(
            "Starting live event stream at ~%.1f events/second (Anomaly rate: %.1f%%)...",
            self.events_per_second,
            self.anomaly_probability * 100,
        )

        while total_events is None or count < total_events:
            # 1:3 ratio between orders and clickstream pageviews
            if self.random.random() < 0.25:
                # 1% chance of malformed event to exercise Dead Letter Queue
                if self.random.random() < 0.01:
                    event = {"type": "order", "payload": self.generate_malformed_event(), "is_malformed": True}
                else:
                    event = {"type": "order", "payload": self.generate_order_event(), "is_malformed": False}
            else:
                event = {"type": "pageview", "payload": self.generate_pageview_event(), "is_malformed": False}

            yield event
            count += 1

            if sleep_interval > 0:
                # Add natural jitter to inter-arrival times
                jittered_sleep = max(0.001, self.random.gauss(sleep_interval, sleep_interval * 0.2))
                time.sleep(jittered_sleep)


def main() -> None:
    """CLI Runner for standalone data generation testing."""
    parser = argparse.ArgumentParser(description="Real-Time Event Stream Generator")
    parser.add_argument("--rate", type=float, default=10.0, help="Events per second")
    parser.add_argument("--count", type=int, default=100, help="Total events to produce (0 for infinite)")
    parser.add_argument("--anomalies", action="store_true", help="Force high anomaly rate")
    args = parser.parse_args()

    anomaly_rate = 0.20 if args.anomalies else 0.03
    gen = EventGenerator(events_per_second=args.rate, anomaly_probability=anomaly_rate)

    print(f"\n🚀 Streaming simulated events at {args.rate} evt/s (Total: {args.count or 'Infinite'})...\n")
    try:
        events = gen.stream(total_events=args.count if args.count > 0 else None)
        for i, ev in enumerate(events, 1):
            p = ev["payload"]
            ev_type = ev["type"].upper()
            if ev_type == "ORDER":
                print(f"[{i:04d}] 🛒 {ev_type:<8} | ID: {p.get('order_id')} | Category: {p.get('category'):<16} | Amount: ${p.get('total_amount', 0):>8.2f} | Status: {p.get('status')}")
            else:
                print(f"[{i:04d}] 🌐 {ev_type:<8} | ID: {p.get('event_id')} | Page: {p.get('page_type'):<14} | User: {p.get('user_id')} | Latency: {p.get('latency_ms')}ms")
    except KeyboardInterrupt:
        print("\n🛑 Stream generator terminated by user.")


if __name__ == "__main__":
    main()
