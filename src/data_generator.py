"""
RealtimeStream — Clickstream Data Generator
Simulates realistic e-commerce clickstream events and produces them to Kafka.

Run directly:  python src/data_generator.py
Or container:  docker compose run generator
"""

import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

# Ensure src/ is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.base import validate_positive_int
from src.config import (
    EVENT_PROBABILITIES,
    GENERATOR_CONFIG,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    PRODUCER_CONFIG,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("data_generator")

# ─── Try importing Kafka; fall back to stdout for testing ─────────────
try:
    from kafka import KafkaProducer

    KAFKA_AVAILABLE = True
except ImportError:
    KafkaProducer = None
    KAFKA_AVAILABLE = False
    logger.warning("kafka-python not installed. Events will be printed to stdout.")


# ─── URL Patterns for realistic page paths ────────────────────────────

PAGE_URLS = {
    "page_view": [
        "/",
        "/products",
        "/products/{category}",
        "/products/{category}/{product}",
        "/promotions",
        "/new-arrivals",
        "/about",
        "/blog",
        "/blog/{slug}",
    ],
    "click": [
        "/products/{category}/{product}",
        "/cart",
        "/checkout",
        "/wishlist",
        "/account/orders",
        "/products/{category}",
    ],
    "add_to_cart": ["/products/{category}/{product}", "/cart"],
    "purchase": ["/checkout/confirm", "/checkout/payment", "/order/confirmation"],
    "login": ["/auth/login", "/auth/oauth", "/account"],
    "logout": ["/auth/logout"],
    "error": [
        "/checkout/payment",
        "/api/search",
        "/products/{category}/{product}",
        "/checkout",
    ],
    "search": ["/search?q={query}"],
}

SEARCH_QUERIES = [
    "wireless headphones",
    "running shoes",
    "laptop backpack",
    "organic coffee",
    "yoga mat",
    "smart watch",
    "bluetooth speaker",
    "gaming mouse",
    "phone case",
    "protein powder",
    "desk lamp",
    "water bottle",
    "sunscreen",
    "notebook",
    "sneakers",
]

BLOG_SLUGS = [
    "top-10-gadgets-2026",
    "how-to-choose-laptop",
    "best-running-shoes",
    "seasonal-fashion-tips",
    "tech-gift-guide",
]

CATEGORIES = [
    "electronics", "fashion", "home-garden", "sports", "beauty",
    "books", "toys", "automotive", "groceries", "health",
    "accessories", "office", "pet-supplies", "baby", "music",
]


class EventCounter:
    """Thread-safe event counter for tracking generator stats."""

    def __init__(self):
        self.total = 0
        self.by_type: Dict[str, int] = defaultdict(int)
        self.start_time = time.time()
        self.last_report = time.time()

    def increment(self, event_type: str):
        self.total += 1
        self.by_type[event_type] += 1

    def stats(self) -> Dict:
        elapsed = time.time() - self.start_time
        return {
            "total_events": self.total,
            "events_per_second": round(self.total / elapsed, 2) if elapsed > 0 else 0,
            "elapsed_seconds": round(elapsed, 1),
            "by_type": dict(self.by_type),
        }


class ClickstreamGenerator:
    """
    Generates realistic clickstream events with session simulation.
    """

    def __init__(self, config: Optional[Dict] = None):
        self.cfg = config or GENERATOR_CONFIG
        self.rng = np.random.default_rng(seed=int(time.time()))
        self.counter = EventCounter()
        self.running = True

        # Pre-generate user pool
        self.user_ids = [f"user-{i:05d}" for i in range(1, self.cfg["num_users"] + 1)]

        # Pre-generate product catalog
        self.products = self._generate_products()

        # Active sessions: user_id -> session info
        self.active_sessions: Dict[str, Dict] = {}
        self.session_timeout = 1800  # 30 minutes

        # Weighted choices from config
        self.event_types, self.event_weights = zip(
            *[(k, v) for k, v in EVENT_PROBABILITIES.items()]
        )
        self.event_weights = list(self.event_weights)

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    def _generate_products(self) -> List[Dict]:
        """Generate a product catalog with IDs, names, prices, and categories."""
        products = []
        for i in range(1, self.cfg["num_products"] + 1):
            category = self.rng.choice(CATEGORIES)
            price = round(
                self.rng.lognormal(mean=3.5, sigma=1.0), 2
            )  # ~$33 median, range $1-$500
            price = max(0.99, min(5000.0, price))
            products.append(
                {
                    "product_id": f"prod-{i:03d}",
                    "category": category,
                    "price": price,
                    "name": f"Product {i}",
                }
            )
        return products

    def _pick_country_city(self) -> Tuple[str, str]:
        """Pick a country and city based on geographic config."""
        country = self.rng.choice(self.cfg["countries"])
        city = self.rng.choice(self.cfg["cities"][country])
        return country, city

    def _pick_device_browser(self) -> Tuple[str, str, str]:
        """Pick device type, browser, and OS based on weighted config."""
        device = self.rng.choice(self.cfg["devices"], p=self.cfg["device_weights"])
        browser = self.rng.choice(self.cfg["browsers"], p=self.cfg["browser_weights"])

        os_map = {
            "desktop": {"Chrome": "Windows", "Firefox": "Linux", "Safari": "macOS", "Edge": "Windows", "Brave": "Windows"},
            "mobile": {"Chrome": "Android", "Firefox": "Android", "Safari": "iOS", "Edge": "Android", "Brave": "Android"},
            "tablet": {"Chrome": "Android", "Firefox": "Android", "Safari": "iPadOS", "Edge": "Android", "Brave": "Android"},
        }
        os_name = os_map.get(device, {}).get(browser, "Windows")
        return device, browser, os_name

    def _generate_user_agent(self, browser: str, os_name: str) -> str:
        """Generate a realistic User-Agent string."""
        versions = {
            "Chrome": f"{self.rng.integers(100, 125)}.0.{self.rng.integers(4000, 5000)}.{self.rng.integers(100, 200)}",
            "Firefox": f"{self.rng.integers(110, 130)}.0",
            "Safari": f"{self.rng.integers(600, 620)}.{self.rng.integers(1, 5)}",
            "Edge": f"{self.rng.integers(110, 125)}.0.{self.rng.integers(1800, 2300)}.{self.rng.integers(50, 100)}",
            "Brave": f"{self.rng.integers(1, 5)}.{self.rng.integers(50, 70)}.{self.rng.integers(100, 130)}",
        }
        ver = versions.get(browser, "1.0")
        return f"Mozilla/5.0 ({os_name}) AppleWebKit/537.36 (KHTML, like Gecko) {browser}/{ver} Safari/537.36"

    def _generate_ip(self) -> str:
        """Generate a random IPv4 address."""
        return f"{self.rng.integers(10, 223)}.{self.rng.integers(0, 255)}.{self.rng.integers(0, 255)}.{self.rng.integers(1, 254)}"

    def _generate_page_url(self, event_type: str, product: Optional[Dict] = None) -> str:
        """Generate a realistic page URL for the given event type."""
        urls = PAGE_URLS.get(event_type, ["/"])
        pattern = self.rng.choice(urls)

        url = pattern.format(
            category=product["category"] if product else self.rng.choice(CATEGORIES),
            product=product["product_id"] if product else self.rng.choice(self.products)["product_id"],
            slug=self.rng.choice(BLOG_SLUGS),
            query=self.rng.choice(SEARCH_QUERIES).replace(" ", "+"),
        )
        return url

    def _get_or_create_session(self, user_id: str, timestamp: datetime) -> Dict:
        """Get existing session or create a new one for the user."""
        now = timestamp.timestamp()

        # Check if user has an active session
        if user_id in self.active_sessions:
            session = self.active_sessions[user_id]
            # Check if session has expired
            if now - session["last_event"] < self.session_timeout:
                session["last_event"] = now
                session["event_count"] += 1
                return session

        # Create new session (using rng for seed-reproducible IDs)
        session_id = f"sess-{self.rng.bytes(6).hex()}"
        session = {
            "session_id": session_id,
            "user_id": user_id,
            "start_time": now,
            "last_event": now,
            "event_count": 1,
            "has_purchased": False,
            "products_in_cart": [],
        }
        self.active_sessions[user_id] = session

        # Clean up stale sessions occasionally
        if len(self.active_sessions) > 10000:
            self._cleanup_stale_sessions(now)

        return session

    def _cleanup_stale_sessions(self, now: float):
        """Remove sessions that have timed out."""
        stale = [
            uid for uid, s in self.active_sessions.items()
            if now - s["last_event"] > self.session_timeout
        ]
        for uid in stale:
            del self.active_sessions[uid]

    def generate_event(self, force_type: Optional[str] = None) -> Optional[Dict]:
        """
        Generate a single clickstream event.

        Returns a dict representing the event, or None if generation fails.
        """
        # Pick event type
        if force_type:
            event_type = force_type
        else:
            event_type = self.rng.choice(self.event_types, p=self.event_weights)

        # Pick user
        user_id = self.rng.choice(self.user_ids)
        timestamp = datetime.now(timezone.utc)

        # Get or create session
        session = self._get_or_create_session(user_id, timestamp)

        # Pick product (sometimes None for non-product events)
        product = None
        if event_type in ("page_view", "click", "add_to_cart", "purchase"):
            product = self.rng.choice(self.products)
        elif event_type == "error" and self.rng.random() < 0.7:
            product = self.rng.choice(self.products)

        # Generate event fields
        country, city = self._pick_country_city()
        device, browser, os_name = self._pick_device_browser()

        event = {
            "event_id": f"evt-{int(timestamp.timestamp() * 1000000):015d}-{self.rng.integers(1000, 9999)}",
            "event_type": event_type,
            "user_id": user_id,
            "session_id": session["session_id"],
            "timestamp": timestamp.isoformat(),
            "page_url": self._generate_page_url(event_type, product),
            "referrer_url": self.rng.choice(["https://google.com", "https://facebook.com", "https://twitter.com", "/", None]),
            "user_agent": self._generate_user_agent(browser, os_name),
            "device_type": device,
            "browser": browser,
            "os": os_name,
            "country": country,
            "city": city,
            "ip_address": self._generate_ip(),
        }

        # Event-specific fields
        if event_type == "purchase" or (event_type == "add_to_cart" and product):
            currency = self.rng.choice(self.cfg["currencies"], p=self.cfg["currency_weights"])
            price = product["price"]
            # Add some items already in cart for purchases
            if event_type == "purchase":
                # Include previous cart items + current product
                cart_total = price + sum(
                    p["price"] for p in session.get("products_in_cart", [])
                )
                event["amount"] = round(cart_total, 2)
                event["currency"] = currency
                event["product_id"] = product["product_id"]
                event["category"] = product["category"]
                session["has_purchased"] = True
                session["products_in_cart"] = []
            else:
                event["amount"] = price
                event["currency"] = currency
                event["product_id"] = product["product_id"]
                event["category"] = product["category"]
                session["products_in_cart"].append(product)
        elif product:
            event["product_id"] = product["product_id"]
            event["category"] = product["category"]
            event["amount"] = None
            event["currency"] = None

        # Error-specific fields
        if event_type == "error":
            error_codes = [400, 403, 404, 500, 502, 503]
            weights = [0.15, 0.10, 0.30, 0.25, 0.10, 0.10]
            event["error_code"] = int(self.rng.choice(error_codes, p=weights))
            event["status_code"] = event["error_code"]
            # Errors have higher response times
            event["response_time_ms"] = int(
                self.rng.lognormal(mean=7.0, sigma=1.5)  # ~1100ms avg
            )
        else:
            event["error_code"] = None
            event["status_code"] = 200
            # Normal response times
            event["response_time_ms"] = int(
                max(10, self.rng.lognormal(mean=5.0, sigma=0.8))  # ~150ms avg
            )

        self.counter.increment(event_type)
        return event

    def generate_batch(self, count: int) -> List[Dict]:
        """Generate a batch of events."""
        return [self.generate_event() for _ in range(count) if self.running]


def produce_events(
    generator: ClickstreamGenerator,
    events_per_second: int,
    topic: str = "raw_events",
    write_fn: Optional[Callable[[Dict], None]] = None,
):
    """
    Produce events to Kafka at the configured rate.

    Falls back to stdout if Kafka is unavailable (useful for testing).
    Uses a custom ``write_fn`` if provided (e.g. for file output).

    Args:
        generator: The clickstream generator instance.
        events_per_second: Target event production rate.
        topic: Kafka topic to produce to.
        write_fn: Optional custom write callback. Receives each event dict.
                  If None, uses Kafka or stdout fallback.
    """
    producer = None
    if write_fn is None and KAFKA_AVAILABLE:
        try:
            producer = KafkaProducer(
                **PRODUCER_CONFIG,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                acks="1",
                compression_type="gzip",
                batch_size=16384,
                linger_ms=100,
            )
            logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.warning(f"Failed to connect to Kafka: {e}")
            logger.info("Falling back to stdout output")
            producer = None

    if write_fn is None:
        if producer:
            def _kafka_send(event):
                try:
                    future = producer.send(topic, value=event)
                    future.add_errback(lambda err: logger.error(f"Kafka send error: {err}"))
                except Exception as e:
                    logger.error(f"Kafka send error: {e}")
            write_fn = _kafka_send
        else:
            def _stdout_send(event):
                print(json.dumps(event, default=str))
            write_fn = _stdout_send

    batch_size = max(1, events_per_second // 10)  # Send in micro-batches
    interval = 0.1  # 100ms between batches

    logger.info(
        f"Starting generator: ~{events_per_second} events/sec "
        f"(batch={batch_size}, interval={interval}s)"
    )
    logger.info(f"Topics: {list(KAFKA_TOPICS.keys())}")

    report_interval = 15  # Log stats every N seconds
    last_report = time.time()

    while generator.running:
        batch_start = time.time()
        events = generator.generate_batch(batch_size)

        for event in events:
            try:
                write_fn(event)
            except Exception as e:
                logger.error(f"Write error: {e}")

        if producer:
            producer.flush()

        # Log stats periodically
        elapsed = time.time() - last_report
        if elapsed >= report_interval:
            stats = generator.counter.stats()
            logger.info(
                f"📊 Rate: {stats['events_per_second']} evt/s | "
                f"Total: {stats['total_events']:,} | "
                f"Elapsed: {stats['elapsed_seconds']}s"
            )
            logger.debug(f"Type breakdown: {stats['by_type']}")
            last_report = time.time()

        # Maintain target rate
        elapsed = time.time() - batch_start
        sleep_time = interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    # Shutdown
    if producer:
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed.")

    stats = generator.counter.stats()
    logger.info(
        f"\n{'='*50}\n"
        f"  Generator stopped.\n"
        f"  Total events: {stats['total_events']:,}\n"
        f"  Avg rate: {stats['events_per_second']} evt/s\n"
        f"  Runtime: {stats['elapsed_seconds']}s\n"
        f"  Breakdown: {stats['by_type']}\n"
        f"{'='*50}"
    )


def main():
    """CLI entry point for the data generator."""
    import argparse

    parser = argparse.ArgumentParser(description="RealtimeStream Clickstream Generator")
    parser.add_argument(
        "--rate",
        type=int,
        default=None,
        help=f"Events per second (default: {GENERATOR_CONFIG['events_per_second']})",
    )
    parser.add_argument(
        "--events",
        type=int,
        default=None,
        help="Total events to generate (default: run indefinitely)",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run in continuous mode (generate events indefinitely). This is the default when --events is not set.",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print events to stdout instead of Kafka",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Path to output JSON file (writes events as JSONL). Implies --stdout with file output.",
    )
    parser.add_argument(
        "--topic",
        default="raw_events",
        help="Kafka topic to produce to (default: raw_events)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility",
    )

    args = parser.parse_args()

    # Validate rate
    rate = args.rate
    if rate is not None:
        try:
            rate = validate_positive_int(str(rate), "event rate")
        except ValueError as e:
            parser.error(str(e))
    else:
        rate = GENERATOR_CONFIG["events_per_second"]

    # Validate event count
    if args.events is not None:
        try:
            args.events = validate_positive_int(str(args.events), "event count")
        except ValueError as e:
            parser.error(str(e))

    generator = ClickstreamGenerator()

    if args.seed:
        generator.rng = np.random.default_rng(seed=args.seed)

    # Output handling
    output_file = None
    if args.output:
        output_file = open(args.output, "w", encoding="utf-8")
        logger.info(f"Writing events to file: {args.output}")

    def write_event(event: Dict):
        """Write an event to the configured output."""
        line = json.dumps(event, default=str)
        if output_file:
            output_file.write(line + "\n")
        if args.stdout or not output_file:
            print(line)

    if args.events:
        # Generate a fixed number of events and exit
        logger.info(f"Generating {args.events:,} events at ~{rate}/sec...")
        count = 0
        while count < args.events and generator.running:
            batch = generator.generate_batch(min(rate // 10, args.events - count))
            for event in batch:
                write_event(event)
                count += 1
            time.sleep(0.1)

        stats = generator.counter.stats()
        logger.info(f"Generated {stats['total_events']:,} events in {stats['elapsed_seconds']}s")
    else:
        # Run continuously — use produce_events with optional write_fn
        if args.stdout or output_file:
            logger.info(f"Continuous mode: producing events to {'stdout' if not output_file else args.output}")
            produce_events(generator, rate, topic=args.topic, write_fn=write_event)
        else:
            produce_events(generator, rate, topic=args.topic)

    if output_file:
        output_file.close()
        logger.info(f"Output file closed: {args.output}")


if __name__ == "__main__":
    main()
