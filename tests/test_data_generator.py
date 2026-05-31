"""
Tests for the RealtimeStream data generator module.
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
import numpy as np

from src.data_generator import ClickstreamGenerator, EventCounter
from src.config import EVENT_TYPES, GENERATOR_CONFIG


class TestEventCounter:
    """Tests for the EventCounter stats tracker."""

    def test_increment_tracks_total(self):
        counter = EventCounter()
        counter.increment("page_view")
        counter.increment("click")
        assert counter.total == 2

    def test_increment_tracks_by_type(self):
        counter = EventCounter()
        counter.increment("page_view")
        counter.increment("page_view")
        counter.increment("click")
        assert counter.by_type["page_view"] == 2
        assert counter.by_type["click"] == 1

    def test_stats_returns_summary(self):
        counter = EventCounter()
        counter.increment("purchase")
        stats = counter.stats()
        assert stats["total_events"] == 1
        assert "events_per_second" in stats
        assert "elapsed_seconds" in stats
        assert "by_type" in stats


class TestClickstreamGenerator:
    """Tests for the ClickstreamGenerator."""

    @pytest.fixture
    def generator(self):
        gen = ClickstreamGenerator()
        gen.rng = np.random.default_rng(seed=42)
        return gen

    def test_initializes_product_catalog(self, generator):
        assert len(generator.products) == GENERATOR_CONFIG["num_products"]
        for product in generator.products[:5]:
            assert "product_id" in product
            assert "category" in product
            assert "price" in product
            assert product["price"] > 0

    def test_generate_event_returns_valid_structure(self, generator):
        event = generator.generate_event()
        assert event is not None
        assert "event_id" in event
        assert "event_type" in event
        assert "user_id" in event
        assert "session_id" in event
        assert "timestamp" in event
        assert "page_url" in event
        assert "device_type" in event
        assert "browser" in event
        assert "country" in event
        assert "city" in event
        assert "response_time_ms" in event
        assert "status_code" in event

    def test_event_type_is_valid(self, generator):
        event = generator.generate_event()
        assert event["event_type"] in EVENT_TYPES

    def test_generate_all_event_types(self, generator):
        """Verify we can generate each event type at least once."""
        generated_types = set()
        for _ in range(500):
            event = generator.generate_event()
            generated_types.add(event["event_type"])
        for et in EVENT_TYPES:
            assert et in generated_types, f"Event type '{et}' was never generated"

    def test_session_consistency(self, generator):
        """Events from same user have consistent session_id."""
        events = []
        # Fix user to check session consistency
        generator.user_ids = ["test-user"]
        for _ in range(10):
            event = generator.generate_event()
            events.append(event)

        session_ids = {e["session_id"] for e in events}
        # All events should share the same session (no timeout)
        assert len(session_ids) == 1

    def test_new_session_after_timeout(self, generator):
        """Events after session timeout get a new session_id."""
        generator.user_ids = ["test-user"]
        event1 = generator.generate_event()

        # Simulate session timeout
        generator.session_timeout = 0  # Force expiry
        time.sleep(0.1)  # Ensure timestamp difference

        event2 = generator.generate_event()
        assert event1["session_id"] != event2["session_id"]

    def test_purchase_event_has_amount(self, generator):
        """Purchase events should have an amount."""
        for _ in range(200):
            event = generator.generate_event()
            if event["event_type"] == "purchase":
                assert event["amount"] is not None
                assert event["amount"] > 0
                assert event["currency"] is not None
                assert event["product_id"] is not None
                return
        pytest.skip("No purchase event generated in 200 tries")

    def test_error_event_has_error_code(self, generator):
        """Error events should have an error_code."""
        for _ in range(500):
            event = generator.generate_event()
            if event["event_type"] == "error":
                assert event["error_code"] is not None
                assert event["status_code"] == event["error_code"]
                return
        pytest.skip("No error event generated in 500 tries")

    def test_normal_event_has_200_status(self, generator):
        """Non-error events should have status 200."""
        for _ in range(100):
            event = generator.generate_event()
            if event["event_type"] != "error":
                assert event["status_code"] == 200
                assert event["error_code"] is None

    def test_generate_batch_count(self, generator):
        batch = generator.generate_batch(10)
        assert len(batch) == 10

    def test_generate_batch_empty_when_stopped(self, generator):
        generator.running = False
        batch = generator.generate_batch(10)
        assert len(batch) == 0

    def test_geographic_distribution(self, generator):
        """Verify country comes from configured list."""
        countries = set()
        for _ in range(200):
            event = generator.generate_event()
            countries.add(event["country"])
        for c in countries:
            assert c in GENERATOR_CONFIG["countries"]

    def test_device_distribution(self, generator):
        """Verify device comes from configured list."""
        devices = set()
        for _ in range(200):
            event = generator.generate_event()
            devices.add(event["device_type"])
        for d in devices:
            assert d in GENERATOR_CONFIG["devices"]

    def test_event_serializable_to_json(self, generator):
        """Events must be JSON-serializable for Kafka."""
        event = generator.generate_event()
        try:
            json.dumps(event, default=str)
        except (TypeError, OverflowError) as e:
            pytest.fail(f"Event not JSON-serializable: {e}")

    def test_increment_counter(self, generator):
        """Generator should increment the counter on each event."""
        initial = generator.counter.total
        generator.generate_event()
        assert generator.counter.total == initial + 1

    def test_reproducible_with_seed(self):
        """Same seed should produce same deterministic fields."""
        gen1 = ClickstreamGenerator()
        gen1.rng = np.random.default_rng(seed=123)

        gen2 = ClickstreamGenerator()
        gen2.rng = np.random.default_rng(seed=123)

        event1 = gen1.generate_event()
        event2 = gen2.generate_event()

        # Deterministic fields (event_id includes timestamp so it varies)
        assert event1["event_type"] == event2["event_type"]
        assert event1["user_id"] == event2["user_id"]
        assert event1["session_id"] == event2["session_id"]
        assert event1["device_type"] == event2["device_type"]
        assert event1["browser"] == event2["browser"]
        assert event1["country"] == event2["country"]
        assert event1["city"] == event2["city"]


class TestConfigIntegration:
    """Tests that the generator works correctly with the config module."""

    def test_event_probabilities_sum_to_one(self):
        from src.config import EVENT_PROBABILITIES
        total = sum(EVENT_PROBABILITIES.values())
        assert abs(total - 1.0) < 0.001, f"Probabilities sum to {total}, expected 1.0"

    def test_generator_config_has_required_keys(self):
        required = ["events_per_second", "num_users", "num_products", "countries"]
        for key in required:
            assert key in GENERATOR_CONFIG, f"Missing config key: {key}"
