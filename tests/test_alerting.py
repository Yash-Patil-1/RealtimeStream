"""
Tests for the RealtimeStream alerting module.
"""

import json
import sys
import os
from unittest.mock import patch, MagicMock
from typing import List

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.alerting import (
    Alert,
    AlertManager,
    AlertChannel,
    ConsoleChannel,
    WebhookChannel,
    EmailChannel,
    CHANNEL_REGISTRY,
    ALERTING_CONFIG_DEFAULTS,
    send_anomaly_alert,
)


# ─── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def sample_alert() -> Alert:
    return Alert(
        alert_id="alert-abc123-1717000000",
        event_id="abc123",
        anomaly_type="error_rate_spike",
        anomaly_score=0.85,
        event_type="error",
        timestamp="2026-05-29T12:00:00+00:00",
        response_time_ms=None,
        error_code=500,
    )


@pytest.fixture
def slow_response_alert() -> Alert:
    return Alert(
        alert_id="alert-def456-1717000100",
        event_id="def456",
        anomaly_type="slow_response",
        anomaly_score=0.72,
        event_type="purchase",
        timestamp="2026-05-29T12:05:00+00:00",
        response_time_ms=15000,
        error_code=None,
    )


@pytest.fixture
def sample_alerts(sample_alert, slow_response_alert) -> List[Alert]:
    return [sample_alert, slow_response_alert]


@pytest.fixture
def console_channel() -> ConsoleChannel:
    return ConsoleChannel({"min_alert_score": 0.0})


@pytest.fixture
def webhook_channel() -> WebhookChannel:
    return WebhookChannel({"webhook_url": "https://hooks.example.com/alerts", "min_alert_score": 0.0})


@pytest.fixture
def email_channel() -> EmailChannel:
    return EmailChannel({
        "smtp_host": "smtp.example.com",
        "email_to": "team@example.com",
        "smtp_port": 587,
        "use_tls": True,
        "smtp_user": "user",
        "smtp_password": "pass",
        "email_from": "alerts@realtimestream.dev",
        "min_alert_score": 0.0,
    })


# ─── Alert Data Model ─────────────────────────────────────────────────


class TestAlert:
    def test_alert_creation(self):
        alert = Alert(
            alert_id="test-1",
            event_id="evt-1",
            anomaly_type="error_rate_spike",
            anomaly_score=0.9,
            event_type="error",
            timestamp="2026-01-01T00:00:00Z",
        )
        assert alert.alert_id == "test-1"
        assert alert.anomaly_type == "error_rate_spike"
        assert alert.anomaly_score == 0.9

    def test_alert_default_message_error_spike(self, sample_alert):
        msg = sample_alert._default_message()
        assert "Error rate spike" in msg
        assert "0.85" in msg

    def test_alert_default_message_slow_response(self, slow_response_alert):
        msg = slow_response_alert._default_message()
        assert "Slow response" in msg
        assert "15000ms" in msg

    def test_alert_default_message_generic(self):
        alert = Alert(
            alert_id="test-2", event_id="evt-2",
            anomaly_type="traffic_spike", anomaly_score=0.6,
            event_type="page_view", timestamp="2026-01-01T00:00:00Z",
        )
        msg = alert._default_message()
        assert "traffic_spike" in msg
        assert "0.60" in msg

    def test_alert_to_dict(self, sample_alert):
        d = sample_alert.to_dict()
        assert d["alert_id"] == "alert-abc123-1717000000"
        assert d["anomaly_type"] == "error_rate_spike"
        assert d["anomaly_score"] == 0.85
        assert d["error_code"] == 500
        assert "message" in d
        assert "metadata" in d

    def test_alert_default_detected_at(self):
        alert = Alert(
            alert_id="t", event_id="e",
            anomaly_type="x", anomaly_score=0.5,
            event_type="click", timestamp="now",
        )
        assert alert.detected_at is not None

    def test_alert_default_message_fallback(self):
        alert = Alert(
            alert_id="t", event_id="e",
            anomaly_type="unknown_type", anomaly_score=0.3,
            event_type="click", timestamp="now",
        )
        msg = alert._default_message()
        assert "Anomaly detected" in msg
        assert "unknown_type" in msg

    def test_alert_repr(self, sample_alert):
        d = sample_alert.to_dict()
        assert isinstance(json.dumps(d, default=str), str)


# ─── ConsoleChannel ──────────────────────────────────────────────────


class TestConsoleChannel:
    def test_send_console_returns_true(self, console_channel, sample_alert):
        assert console_channel.send(sample_alert) is True

    def test_send_batch_console(self, console_channel, sample_alerts):
        result = console_channel.send_batch(sample_alerts)
        assert result == 2

    def test_channel_name(self, console_channel):
        assert console_channel.name == "console"

    def test_console_output(self, console_channel, sample_alert, capsys):
        console_channel.send(sample_alert)
        captured = capsys.readouterr()
        # Alert JSON should be printed to stderr
        assert "alert-abc123" in captured.err

    def test_send_below_threshold(self):
        channel = ConsoleChannel({"min_alert_score": 0.5})
        alert = Alert(
            alert_id="low", event_id="e",
            anomaly_type="test", anomaly_score=0.1,
            event_type="click", timestamp="now",
        )
        # Console channel doesn't filter by score itself; AlertManager does
        assert channel.send(alert) is True  # raw channel always sends


# ─── WebhookChannel ──────────────────────────────────────────────────


class TestWebhookChannel:
    def test_send_no_url(self):
        channel = WebhookChannel({"webhook_url": ""})
        alert = Alert(
            alert_id="t", event_id="e",
            anomaly_type="test", anomaly_score=0.5,
            event_type="click", timestamp="now",
        )
        assert channel.send(alert) is False

    def test_channel_name(self, webhook_channel):
        assert webhook_channel.name == "webhook"

    def test_is_slack_webhook_true(self):
        assert WebhookChannel._is_slack_webhook("https://hooks.slack.com/services/T00/B00/xxx")

    def test_is_slack_webhook_false(self):
        assert not WebhookChannel._is_slack_webhook("https://hooks.example.com/alerts")

    def test_format_slack_block(self, webhook_channel, sample_alert):
        payload = webhook_channel._format_slack_block(sample_alert)
        assert payload["channel"] == "#anomaly-alerts"
        assert len(payload["attachments"]) == 1
        assert "Error Rate Spike" in payload["attachments"][0]["title"]

    def test_format_slack_blocks(self, webhook_channel, sample_alerts):
        payload = webhook_channel._format_slack_blocks(sample_alerts)
        assert len(payload["attachments"]) == 1
        assert "2 Anomaly Alert(s)" in payload["attachments"][0]["title"]

    def test_format_payload(self, webhook_channel, sample_alert):
        payload = webhook_channel._format_payload(sample_alert)
        assert payload["alert_id"] == sample_alert.alert_id
        assert payload["anomaly_type"] == "error_rate_spike"

    @patch("urllib.request.urlopen")
    def test_post_success(self, mock_urlopen, webhook_channel):
        mock_urlopen.return_value.__enter__.return_value.status = 200
        result = webhook_channel._post("https://hooks.example.com", {"test": True})
        assert result is True
        mock_urlopen.assert_called_once()

    @patch("urllib.request.urlopen")
    def test_send_success(self, mock_urlopen, webhook_channel, sample_alert):
        mock_urlopen.return_value.__enter__.return_value.status = 200
        result = webhook_channel.send(sample_alert)
        assert result is True

    @patch("urllib.request.urlopen", side_effect=Exception("Connection refused"))
    def test_send_network_error(self, mock_urlopen, webhook_channel, sample_alert):
        result = webhook_channel.send(sample_alert)
        assert result is False

    def test_send_batch_no_url(self):
        channel = WebhookChannel({"webhook_url": ""})
        assert channel.send_batch([]) == 0

    @patch("urllib.request.urlopen")
    def test_send_batch_generic(self, mock_urlopen, webhook_channel, sample_alerts):
        mock_urlopen.return_value.__enter__.return_value.status = 200
        webhook_channel.config["webhook_url"] = "https://hooks.example.com/alerts"
        result = webhook_channel.send_batch(sample_alerts)
        assert result == 2

    @patch("urllib.request.urlopen")
    def test_send_batch_slack(self, mock_urlopen, sample_alerts):
        channel = WebhookChannel({"webhook_url": "https://hooks.slack.com/services/T00/B00/xxx"})
        mock_urlopen.return_value.__enter__.return_value.status = 200
        result = channel.send_batch(sample_alerts)
        assert result == 2


# ─── EmailChannel ────────────────────────────────────────────────────


class TestEmailChannel:
    def test_send_no_recipient(self):
        channel = EmailChannel({"email_to": ""})
        alert = Alert(
            alert_id="t", event_id="e",
            anomaly_type="test", anomaly_score=0.5,
            event_type="click", timestamp="now",
        )
        assert channel.send(alert) is False

    def test_channel_name(self, email_channel):
        assert email_channel.name == "email"

    def test_format_email_body(self, email_channel, sample_alert):
        body = email_channel._format_email_body(sample_alert)
        assert "alert-abc123-1717000000" in body
        assert "error_rate_spike" in body
        assert "500" in body

    def test_format_email_body_slow_response(self, email_channel, slow_response_alert):
        body = email_channel._format_email_body(slow_response_alert)
        assert "15000ms" in body

    def test_send_email_no_host(self):
        channel = EmailChannel({"email_to": "test@example.com", "smtp_host": ""})
        alert = Alert(
            alert_id="t", event_id="e",
            anomaly_type="test", anomaly_score=0.5,
            event_type="click", timestamp="now",
        )
        result = channel.send(alert)
        assert result is False

    def test_send_batch_no_recipient(self):
        channel = EmailChannel({"email_to": ""})
        assert channel.send_batch([]) == 0

    @patch("smtplib.SMTP")
    def test_send_email_success(self, mock_smtp, email_channel):
        mock_server = MagicMock()
        mock_smtp.__enter__.return_value = mock_server

        alert = Alert(
            alert_id="t", event_id="e",
            anomaly_type="test", anomaly_score=0.5,
            event_type="click", timestamp="now",
        )
        result = email_channel.send(alert)
        assert result is True
        mock_smtp.assert_called_once_with("smtp.example.com", 587)

    @patch("smtplib.SMTP")
    def test_send_batch_email(self, mock_smtp, email_channel, sample_alerts):
        mock_server = MagicMock()
        mock_smtp.__enter__.return_value = mock_server
        result = email_channel.send_batch(sample_alerts)
        assert result == 2

    @patch("smtplib.SMTP", side_effect=Exception("Connection failed"))
    def test_send_email_failure(self, mock_smtp, email_channel, sample_alert):
        result = email_channel.send(sample_alert)

        assert result is False


# ─── AlertChannel Base ───────────────────────────────────────────────


class TestAlertChannelBase:
    def test_send_not_implemented(self):
        channel = AlertChannel()
        alert = Alert(
            alert_id="t", event_id="e",
            anomaly_type="test", anomaly_score=0.5,
            event_type="click", timestamp="now",
        )
        with pytest.raises(NotImplementedError):
            channel.send(alert)

    def test_name_property(self):
        channel = AlertChannel()
        assert channel.name == "alert"

    def test_send_batch_default(self, sample_alerts):
        """Default batch iterates over individual sends."""
        channel = ConsoleChannel()
        result = channel.send_batch(sample_alerts)
        assert result == 2


# ─── Channel Registry ────────────────────────────────────────────────


class TestChannelRegistry:
    def test_registry_contains_expected_channels(self):
        assert "webhook" in CHANNEL_REGISTRY
        assert "email" in CHANNEL_REGISTRY
        assert "console" in CHANNEL_REGISTRY

    def test_registry_classes(self):
        assert CHANNEL_REGISTRY["webhook"] == WebhookChannel
        assert CHANNEL_REGISTRY["email"] == EmailChannel
        assert CHANNEL_REGISTRY["console"] == ConsoleChannel

    def test_registry_instantiation(self):
        channel = CHANNEL_REGISTRY["console"]()
        assert isinstance(channel, ConsoleChannel)
        assert channel.send(
            Alert(alert_id="t", event_id="e", anomaly_type="t", anomaly_score=0.5, event_type="click", timestamp="now")
        )


# ─── AlertManager ────────────────────────────────────────────────────


class TestAlertManager:
    def test_default_channels_include_console(self):
        manager = AlertManager({"webhook_url": "", "smtp_host": "", "email_to": ""})
        names = [c.name for c in manager.channels]
        assert "console" in names

    def test_includes_webhook_when_configured(self):
        manager = AlertManager({
            "webhook_url": "https://hooks.example.com",
            "smtp_host": "",
            "email_to": "",
        })
        names = [c.name for c in manager.channels]
        assert "webhook" in names

    def test_includes_email_when_configured(self):
        manager = AlertManager({
            "webhook_url": "",
            "smtp_host": "smtp.example.com",
            "email_to": "team@example.com",
        })
        names = [c.name for c in manager.channels]
        assert "email" in names

    def test_send_alert_below_threshold(self):
        manager = AlertManager({"min_alert_score": 0.5, "webhook_url": "", "smtp_host": "", "email_to": ""})
        alert = Alert(
            alert_id="low", event_id="e",
            anomaly_type="test", anomaly_score=0.1,
            event_type="click", timestamp="now",
        )
        result = manager.send_alert(alert)
        assert result == 0  # filtered by min score

    def test_send_alert_above_threshold(self):
        manager = AlertManager({"min_alert_score": 0.0, "webhook_url": "", "smtp_host": "", "email_to": ""})
        alert = Alert(
            alert_id="high", event_id="e",
            anomaly_type="test", anomaly_score=0.9,
            event_type="click", timestamp="now",
        )
        result = manager.send_alert(alert)
        assert result == 1  # console channel only

    def test_send_alert_multiple_channels(self):
        manager = AlertManager({
            "webhook_url": "https://hooks.example.com",
            "email_to": "",
            "smtp_host": "",
            "min_alert_score": 0.0,
        })
        alert = Alert(
            alert_id="multi", event_id="e",
            anomaly_type="test", anomaly_score=0.8,
            event_type="click", timestamp="now",
        )
        # webhook will fail (no real server), console succeeds
        result = manager.send_alert(alert)
        assert result >= 1  # at least console succeeds

    def test_send_alerts_empty(self):
        manager = AlertManager()
        result = manager.send_alerts([])
        assert result == {}

    def test_send_alerts_all_below_threshold(self):
        manager = AlertManager({"min_alert_score": 0.5})
        alerts = [
            Alert(alert_id="a1", event_id="e1", anomaly_type="t", anomaly_score=0.1, event_type="click", timestamp="now"),
            Alert(alert_id="a2", event_id="e2", anomaly_type="t", anomaly_score=0.2, event_type="click", timestamp="now"),
        ]
        result = manager.send_alerts(alerts)
        assert result == {}

    def test_send_alerts_batch(self):
        manager = AlertManager({"min_alert_score": 0.0, "webhook_url": "", "smtp_host": "", "email_to": ""})
        alerts = [
            Alert(alert_id=f"a{i}", event_id=f"e{i}", anomaly_type="t", anomaly_score=0.6, event_type="click", timestamp="now")
            for i in range(5)
        ]
        result = manager.send_alerts(alerts)
        assert "console" in result
        assert result["console"] == 5

    def test_send_alerts_truncates_max_batch(self):
        manager = AlertManager({
            "min_alert_score": 0.0,
            "max_alerts_per_batch": 3,
            "webhook_url": "",
            "smtp_host": "",
            "email_to": "",
        })
        alerts = [
            Alert(alert_id=f"a{i}", event_id=f"e{i}", anomaly_type="t", anomaly_score=0.6, event_type="click", timestamp="now")
            for i in range(10)
        ]
        result = manager.send_alerts(alerts)
        # Only 3 should be sent (max batch)
        assert result["console"] == 3

    def test_send_alerts_single_mode(self):
        """When batch_alerts=False, shouldn't use send_batch."""
        manager = AlertManager({
            "min_alert_score": 0.0,
            "batch_alerts": False,
            "webhook_url": "",
            "smtp_host": "",
            "email_to": "",
        })
        alerts = [
            Alert(alert_id=f"a{i}", event_id=f"e{i}", anomaly_type="t", anomaly_score=0.6, event_type="click", timestamp="now")
            for i in range(3)
        ]
        result = manager.send_alerts(alerts)
        assert result["console"] == 3

    def test_channel_summary(self):
        manager = AlertManager({"webhook_url": "", "smtp_host": "", "email_to": ""})
        summary = manager.channel_summary()
        assert len(summary) >= 1
        assert all(c["configured"] for c in summary)

    def test_channel_summary_with_webhook(self):
        manager = AlertManager({"webhook_url": "https://hooks.example.com", "smtp_host": "", "email_to": ""})
        summary = manager.channel_summary()
        names = [c["name"] for c in summary]
        assert "webhook" in names


# ─── Convenience Function ────────────────────────────────────────────


class TestSendAnomalyAlert:
    def test_send_one_shot(self):
        alert_data = {
            "event_id": "evt-one-shot",
            "anomaly_type": "error_rate_spike",
            "anomaly_score": 0.75,
            "event_type": "error",
            "timestamp": "2026-05-29T12:00:00+00:00",
            "error_code": 500,
        }
        result = send_anomaly_alert(alert_data, config={"webhook_url": "", "smtp_host": "", "email_to": ""})
        assert result == 1  # console channel

    def test_send_one_shot_below_threshold(self):
        alert_data = {
            "event_id": "evt-low",
            "anomaly_type": "test",
            "anomaly_score": 0.05,
            "event_type": "click",
            "timestamp": "now",
        }
        result = send_anomaly_alert(
            alert_data,
            config={"min_alert_score": 0.5, "webhook_url": "", "smtp_host": "", "email_to": ""},
        )
        assert result == 0

    def test_send_one_shot_with_metadata(self):
        alert_data = {
            "event_id": "evt-meta",
            "anomaly_type": "slow_response",
            "anomaly_score": 0.9,
            "event_type": "purchase",
            "timestamp": "now",
            "response_time_ms": 20000,
            "message": "Custom alert message",
            "metadata": {"region": "us-east-1", "severity": "P1"},
        }
        result = send_anomaly_alert(
            alert_data,
            config={"webhook_url": "", "smtp_host": "", "email_to": ""},
        )
        assert result == 1


# ─── ALERTING_CONFIG_DEFAULTS ────────────────────────────────────────


class TestConfigDefaults:
    def test_config_defaults_structure(self):
        assert "webhook_url" in ALERTING_CONFIG_DEFAULTS
        assert "smtp_host" in ALERTING_CONFIG_DEFAULTS
        assert "email_to" in ALERTING_CONFIG_DEFAULTS
        assert "min_alert_score" in ALERTING_CONFIG_DEFAULTS
        assert "batch_alerts" in ALERTING_CONFIG_DEFAULTS
        assert "max_alerts_per_batch" in ALERTING_CONFIG_DEFAULTS

    def test_default_min_score(self):
        assert ALERTING_CONFIG_DEFAULTS["min_alert_score"] == 0.0

    def test_default_batch_enabled(self):
        assert ALERTING_CONFIG_DEFAULTS["batch_alerts"] is True

    def test_default_max_batch(self):
        assert ALERTING_CONFIG_DEFAULTS["max_alerts_per_batch"] == 50
